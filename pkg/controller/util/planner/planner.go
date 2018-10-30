/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package planner

import (
	"hash/fnv"
	"sort"

	"github.com/golang/glog"
	fedschedulingv1a1 "github.com/kubernetes-sigs/federation-v2/pkg/apis/scheduling/v1alpha1"
)

// Planner decides how many out of the given replicas should be placed in each of the
// federated clusters.
type Planner struct {
	preferences *fedschedulingv1a1.ReplicaSchedulingPreference
}

type namedClusterPreferences struct {
	clusterName string
	hash        uint32
	fedschedulingv1a1.ClusterPreferences
}

type byWeight []*namedClusterPreferences

func (a byWeight) Len() int      { return len(a) }
func (a byWeight) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Preferences are sorted according by decreasing weight and increasing hash (built on top of cluster name and rs name).
// Sorting is made by a hash to avoid assigning single-replica rs to the alphabetically smallest cluster.
func (a byWeight) Less(i, j int) bool {
	return (a[i].Weight > a[j].Weight) || (a[i].Weight == a[j].Weight && a[i].hash < a[j].hash)
}

func NewPlanner(preferences *fedschedulingv1a1.ReplicaSchedulingPreference) *Planner {
	return &Planner{
		preferences: preferences,
	}
}

// Distribute the desired number of replicas among the given cluster according to the planner preferences.
// The function tries its best to assign each cluster the preferred number of replicas, however if
// sum of MinReplicas for all cluster is bigger than replicasToDistribute (TotalReplicas) then some cluster
// will not have all of the replicas assigned. In such case a cluster with higher weight has priority over
// cluster with lower weight (or with lexicographically smaller name in case of draw).
// It can also use the current replica count and estimated capacity to provide better planning and
// adhere to rebalance policy. To avoid prioritization of clusters with smaller lexicographical names
// a semi-random string (like replica set name) can be provided.
// Two maps are returned:
// * a map that contains information how many replicas will be possible to run in a cluster.
// * a map that contains information how many extra replicas would be nice to schedule in a cluster so,
//   if by chance, they are scheduled we will be closer to the desired replicas layout.
func (p *Planner) Plan(availableClusters []string, currentReplicaCount map[string]int64,
	estimatedCapacity map[string]int64, replicaSetKey string) (map[string]int64, map[string]int64) {

	preferences := make([]*namedClusterPreferences, 0, len(availableClusters))
	plan := make(map[string]int64, len(preferences))
	overflow := make(map[string]int64, len(preferences))

	named := func(name string, pref fedschedulingv1a1.ClusterPreferences) *namedClusterPreferences {
		// Seems to work better than addler for our case.
		hasher := fnv.New32()
		hasher.Write([]byte(name))
		hasher.Write([]byte(replicaSetKey))

		return &namedClusterPreferences{
			clusterName:        name,
			hash:               hasher.Sum32(),
			ClusterPreferences: pref,
		}
	}

	for _, cluster := range availableClusters {
		if localRSP, found := p.preferences.Spec.Clusters[cluster]; found {
			preferences = append(preferences, named(cluster, localRSP))
		} else {
			if localRSP, found := p.preferences.Spec.Clusters["*"]; found {
				preferences = append(preferences, named(cluster, localRSP))
			} else {
				plan[cluster] = int64(0)
			}
		}
	}
	sort.Sort(byWeight(preferences))

	// This is the requested total replicas in preferences
	remainingReplicas := int64(p.preferences.Spec.TotalReplicas)

	glog.Infof("About to place %d replicas", remainingReplicas)

	// Assign each cluster the minimum number of replicas it requested.
	for _, preference := range preferences {
		min := minInt64(preference.MinReplicas, remainingReplicas)
		glog.Infof("Want to assign minimum %d to %s", min, preference.clusterName)
		if capacity, hasCapacity := estimatedCapacity[preference.clusterName]; hasCapacity {
			min = minInt64(min, capacity)
			glog.Infof("Assigning %d to %s as its capicity is %d", min, preference.clusterName, capacity)
		}
		remainingReplicas -= min
		glog.Infof("Removed %d replicas from pool, %d left", min, remainingReplicas)
		plan[preference.clusterName] = min
	}

	glog.Infof("Placed minimum replicas in each cluster. %d left to assign", remainingReplicas)

	// This map contains information how many replicas were assigned to
	// the cluster based only on the current replica count and
	// rebalance=false preference. It will be later used in remaining replica
	// distribution code.
	preallocated := make(map[string]int64)

	if p.preferences.Spec.Rebalance == false {
		glog.Info("About to assign remaining replicas (for rebalance == false)")
		for _, preference := range preferences {
			planned := plan[preference.clusterName]
			count, hasSome := currentReplicaCount[preference.clusterName]
			glog.Infof("Now trying to assign to %s. It already has %d replicas", preference.clusterName, count)
			if hasSome && count > planned {
				glog.Info("It already has more replicas than we have planned to assign to it")
				target := count
				if preference.MaxReplicas != nil {
					target = minInt64(*preference.MaxReplicas, target)
					glog.Infof("Cluster has MaxReplicas %d, will only try to assign %d", preference.MaxReplicas, target)
				}
				if capacity, hasCapacity := estimatedCapacity[preference.clusterName]; hasCapacity {
					target = minInt64(capacity, target)
					glog.Infof("Cluster has capacity %d, will only try to assign %d", capacity, target)
				}
				glog.Infof("Target %d, planned %d, remainingReplicas %d", target, planned, remainingReplicas)
				extra := minInt64(target-planned, remainingReplicas)
				if extra < 0 {
					extra = 0
				}
				glog.Infof("Assigned %d more replicas to %s in this stage", target-planned, preference.clusterName)

				remainingReplicas -= extra
				preallocated[preference.clusterName] = extra
				plan[preference.clusterName] = extra + planned
				glog.Infof("%d replicas remaining, for %s will assign %d = %d", remainingReplicas, preference.clusterName, target, extra+planned)
			}
		}
	}

	modified := true

	// It is possible single pass of the loop is not enough to distribute all replicas among clusters due
	// to weight, max and rounding corner cases. In such case we iterate until either
	// there is no replicas or no cluster gets any more replicas or the number
	// of attempts is less than available cluster count. If there is no preallocated pods
	// every loop either distributes all remainingReplicas or maxes out at least one cluster.
	// If there are preallocated then the replica spreading may take longer.
	// We reduce the number of pending preallocated replicas by at least half with each iteration so
	// we may need log(replicasAtStart) iterations.
	// TODO: Prove that clusterCount * log(replicas) iterations solves the problem or adjust the number.
	// TODO: This algorithm is O(clusterCount^2 * log(replicas)) which is good for up to 100 clusters.
	// Find something faster.
	for trial := 0; modified && remainingReplicas > 0; trial++ {
		glog.Infof("Starting distribution trial %d", trial)

		modified = false
		weightSum := int64(0)
		for _, preference := range preferences {
			weightSum += preference.Weight
		}
		glog.Infof("Weight sum is %d", weightSum)
		newPreferences := make([]*namedClusterPreferences, 0, len(preferences))

		distributeInThisLoop := remainingReplicas

		glog.Infof("Distributing %d remaining replicas in this trial loop", distributeInThisLoop)

		for _, preference := range preferences {
			if weightSum > 0 {
				start := plan[preference.clusterName]
				glog.Infof("Trying to distribute to %s, was already planning to give %d replicas", preference.clusterName, start)
				// Distribute the remaining replicas, rounding fractions always up.
				extra := (distributeInThisLoop*preference.Weight + weightSum - 1) / weightSum
				glog.Infof("Want to distribute %d more replicas to %s", extra, preference.clusterName)
				extra = minInt64(extra, remainingReplicas)
				glog.Infof("Because of remainingReplicas (%d), can distribute %d to %s", remainingReplicas, extra, preference.clusterName)

				// Account preallocated.
				prealloc := preallocated[preference.clusterName]
				usedPrealloc := minInt64(extra, prealloc)
				preallocated[preference.clusterName] = prealloc - usedPrealloc
				extra = extra - usedPrealloc
				if usedPrealloc > 0 {
					modified = true
				}

				// In total there should be the amount that was there at start plus whatever is due
				// in this iteration
				total := start + extra

				// Check if we don't overflow the cluster, and if yes don't consider this cluster
				// in any of the following iterations.
				full := false
				if preference.MaxReplicas != nil && total > *preference.MaxReplicas {
					total = *preference.MaxReplicas
					full = true
				}
				if capacity, hasCapacity := estimatedCapacity[preference.clusterName]; hasCapacity && total > capacity {
					overflow[preference.clusterName] = total - capacity
					total = capacity
					full = true
				}

				if !full {
					newPreferences = append(newPreferences, preference)
				}

				glog.Infof("Allocated %d more replicas to %s", (total - start), preference.clusterName)
				// Only total-start replicas were actually taken.
				remainingReplicas -= (total - start)
				plan[preference.clusterName] = total

				// Something extra got scheduled on this cluster.
				if total > start {
					modified = true
				}
			} else {
				break
			}
		}
		preferences = newPreferences
	}

	glog.Info("Finished placing replicas")

	if p.preferences.Spec.Rebalance {
		return plan, overflow
	} else {
		// If rebalance = false then overflow is trimmed at the level
		// of replicas that it failed to place somewhere.
		newOverflow := make(map[string]int64)
		for key, value := range overflow {
			value = minInt64(value, remainingReplicas)
			if value > 0 {
				newOverflow[key] = value
			}
		}
		return plan, newOverflow
	}
}

func minInt64(a int64, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
