// Package kueue is the Kueue v0.20.1 profile. Kueue admits whole Jobs by
// quota (gangs need nothing extra), orders and preempts by
// WorkloadPriorityClass, shares capacity across a cohort with fair sharing,
// and places a pod set in one rack with topology-aware scheduling (R1, K1
// to K3). kube-scheduler still binds the pods.
package kueue

import (
	"bufio"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/apply"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/cluster"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/fleet"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/labels"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

// Version is the pinned Kueue release.
const Version = "v0.20.1"

//go:embed manifests/manifests.yaml manifests/SHA256SUMS
var files embed.FS

// FairSharing is the block added to Kueue's manager Configuration. Fair
// sharing is on whenever a fairSharing block is present (cmd/kueue/main.go
// at v0.20.1); the strategies are the ones the v0.20 preemption docs show.
const FairSharing = "fairSharing:\n  preemptionStrategies: [LessThanOrEqualToFinalShare, LessThanInitialShare]\n"

var (
	workloadGVR = schema.GroupVersionResource{Group: "kueue.x-k8s.io", Version: "v1beta2", Resource: "workloads"}
	cqGVR       = schema.GroupVersionResource{Group: "kueue.x-k8s.io", Version: "v1beta2", Resource: "clusterqueues"}
	rfGVR       = schema.GroupVersionResource{Group: "kueue.x-k8s.io", Version: "v1beta2", Resource: "resourceflavors"}
)

// base is created once per cluster: the topology (block, rack, host), one
// ResourceFlavor for the kwok fleet, and the two WorkloadPriorityClasses.
const base = `apiVersion: kueue.x-k8s.io/v1beta2
kind: Topology
metadata: {name: scs-topology, labels: {app.kubernetes.io/part-of: scheduler-capabilities-suite}}
spec:
  levels:
  - nodeLabel: scs.arangogutierrez.github.io/block
  - nodeLabel: scs.arangogutierrez.github.io/rack
  - nodeLabel: kubernetes.io/hostname
---
apiVersion: kueue.x-k8s.io/v1beta2
kind: ResourceFlavor
metadata: {name: gpu, labels: {app.kubernetes.io/part-of: scheduler-capabilities-suite}}
spec:
  nodeLabels: {type: kwok}
  topologyName: scs-topology
---
apiVersion: kueue.x-k8s.io/v1beta2
kind: WorkloadPriorityClass
metadata: {name: scs-low, labels: {app.kubernetes.io/part-of: scheduler-capabilities-suite}}
value: 100
---
apiVersion: kueue.x-k8s.io/v1beta2
kind: WorkloadPriorityClass
metadata: {name: scs-high, labels: {app.kubernetes.io/part-of: scheduler-capabilities-suite}}
value: 1000
`

// Manifest returns the embedded release manifest and its verified sha256.
func Manifest() ([]byte, string, error) {
	data, err := files.ReadFile("manifests/manifests.yaml")
	if err != nil {
		return nil, "", err
	}
	sums, err := files.ReadFile("manifests/SHA256SUMS")
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(data)
	got := hex.EncodeToString(sum[:])
	sc := bufio.NewScanner(strings.NewReader(string(sums)))
	for sc.Scan() {
		if f := strings.Fields(sc.Text()); len(f) == 2 && f[1] == "manifests.yaml" {
			if f[0] != got {
				return nil, "", fmt.Errorf("kueue: manifests.yaml has sha256 %s, SHA256SUMS says %s", got, f[0])
			}
			return data, got, nil
		}
	}
	return nil, "", fmt.Errorf("kueue: manifests.yaml is not listed in SHA256SUMS")
}

var fairSharingKey = regexp.MustCompile(`(?m)^fairSharing:`)

// WithFairSharing returns Kueue's manager Configuration with FairSharing
// appended, refusing one that already sets fairSharing.
func WithFairSharing(cfg string) (string, error) {
	if fairSharingKey.MatchString(cfg) {
		return "", fmt.Errorf("kueue: the manager configuration already sets fairSharing")
	}
	if !strings.HasSuffix(cfg, "\n") {
		cfg += "\n"
	}
	return cfg + FairSharing, nil
}

// Profile is the Kueue profile.
type Profile struct{}

// New returns the Kueue profile.
func New() *Profile { return &Profile{} }

// Name implements scheduler.Profile.
func (*Profile) Name() string { return "kueue" }

// ClaimsFile implements scheduler.Profile.
func (*Profile) ClaimsFile() string { return "claims/kueue.yaml" }

// WorkloadGVR implements scheduler.Profile. The resource must be named in
// full: scheduling.k8s.io also serves a "workloads" resource (R1, K1).
func (*Profile) WorkloadGVR() *schema.GroupVersionResource { g := workloadGVR; return &g }

// Install applies Kueue with fair sharing in its configuration, waits until
// its webhook answers (a cold install took 239 s in R1), then creates the
// base objects.
func (*Profile) Install(ctx context.Context, c *cluster.Cluster) (map[string]string, error) {
	data, sum, err := Manifest()
	if err != nil {
		return nil, err
	}
	objs, err := apply.Decode(data)
	if err != nil {
		return nil, err
	}
	edited := false
	for _, o := range objs {
		if o.GetKind() != "ConfigMap" || o.GetName() != "kueue-manager-config" {
			continue
		}
		cfg, _, _ := unstructured.NestedString(o.Object, "data", "controller_manager_config.yaml")
		if cfg, err = WithFairSharing(cfg); err != nil {
			return nil, err
		}
		if err := unstructured.SetNestedField(o.Object, cfg, "data", "controller_manager_config.yaml"); err != nil {
			return nil, err
		}
		edited = true
	}
	if !edited {
		return nil, fmt.Errorf("kueue: manifest has no kueue-manager-config ConfigMap")
	}
	if _, err := c.Applier.ApplyObjects(ctx, objs, 2*time.Minute); err != nil {
		return nil, err
	}
	start := time.Now()
	probe := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kueue.x-k8s.io/v1beta2", "kind": "ResourceFlavor",
		"metadata": map[string]any{"name": "scs-webhook-probe"},
	}}
	err = wait.PollUntilContextTimeout(ctx, 2*time.Second, 8*time.Minute, true, func(ctx context.Context) (bool, error) {
		_, err := c.Dynamic.Resource(rfGVR).Create(ctx, probe, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
		return err == nil, nil
	})
	if err != nil {
		return nil, fmt.Errorf("kueue: webhook did not answer: %w", err)
	}
	webhookWait := time.Since(start).Round(time.Second)
	if _, err := c.Applier.Apply(ctx, []byte(base), time.Minute); err != nil {
		return nil, err
	}
	return map[string]string{
		"kueue_version":           Version,
		"kueue_manifest_sha256":   sum,
		"kueue_config_added":      FairSharing,
		"kueue_webhook_wait":      webhookWait.String(),
		"kueue_queueing_strategy": "BestEffortFIFO",
		"scheduler":               "kube-scheduler from " + cluster.NodeImage + ", behind Kueue admission",
	}, nil
}

// ClusterQueueName is the ClusterQueue of one tenant of a probe.
func ClusterQueueName(t scheduler.Tenant) string { return "cq-" + t.Namespace }

// ClusterQueue is tenant t's ClusterQueue: its entitlement as nominal quota
// of the fleet's GPUs, BestEffortFIFO; with AllowPreemption it preempts
// lower priority work, and with several tenants the queues share a cohort
// and reclaim lent quota.
func ClusterQueue(s scheduler.Scenario, t scheduler.Tenant) map[string]any {
	spec := map[string]any{
		"namespaceSelector": map[string]any{},
		"queueingStrategy":  "BestEffortFIFO",
		"resourceGroups": []any{map[string]any{
			"coveredResources": []any{fleet.GPUResource},
			"flavors": []any{map[string]any{"name": "gpu", "resources": []any{
				map[string]any{"name": fleet.GPUResource, "nominalQuota": int64(t.EntitlementGPUs)},
			}}},
		}},
	}
	if s.AllowPreemption {
		pre := map[string]any{"withinClusterQueue": "LowerPriority"}
		if len(s.Tenants) > 1 {
			pre["reclaimWithinCohort"] = "Any"
		}
		spec["preemption"] = pre
	}
	if len(s.Tenants) > 1 {
		spec["cohortName"] = "scs-" + s.Probe
	}
	return map[string]any{
		"apiVersion": "kueue.x-k8s.io/v1beta2",
		"kind":       "ClusterQueue",
		"metadata":   map[string]any{"name": ClusterQueueName(t), "labels": map[string]any{labels.PartOf: labels.PartOfValue, labels.Probe: s.Probe}},
		"spec":       spec,
	}
}

// LocalQueue is the queue "lq" in tenant t's namespace.
func LocalQueue(t scheduler.Tenant) map[string]any {
	return map[string]any{
		"apiVersion": "kueue.x-k8s.io/v1beta2",
		"kind":       "LocalQueue",
		"metadata":   map[string]any{"name": "lq", "namespace": t.Namespace},
		"spec":       map[string]any{"clusterQueue": ClusterQueueName(t)},
	}
}

// Prepare creates the probe's namespaces, and each tenant's ClusterQueue and LocalQueue.
func (*Profile) Prepare(ctx context.Context, c *cluster.Cluster, s scheduler.Scenario) error {
	if err := scheduler.CreateNamespaces(ctx, c.Clientset, s); err != nil {
		return err
	}
	for _, t := range s.Tenants {
		for _, obj := range []map[string]any{ClusterQueue(s, t), LocalQueue(t)} {
			data, err := json.Marshal(obj)
			if err != nil {
				return err
			}
			if _, err := c.Applier.Apply(ctx, data, time.Minute); err != nil {
				return err
			}
		}
	}
	return nil
}

// Cleanup deletes the probe's ClusterQueues and waits until they are gone
// (Kueue holds one with a finalizer while it still has workloads).
func (*Profile) Cleanup(ctx context.Context, c *cluster.Cluster, s scheduler.Scenario) error {
	for _, t := range s.Tenants {
		if err := c.Dynamic.Resource(cqGVR).Delete(ctx, ClusterQueueName(t), metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		for _, t := range s.Tenants {
			if _, err := c.Dynamic.Resource(cqGVR).Get(ctx, ClusterQueueName(t), metav1.GetOptions{}); err == nil {
				return false, nil
			}
		}
		return true, nil
	})
}

// PriorityClass maps a job's priority to the installed WorkloadPriorityClass.
// Whether a high-priority job may preempt is the ClusterQueue's policy
// (Scenario.AllowPreemption), not the job's.
func PriorityClass(j workload.Job) string {
	if j.Priority == workload.High {
		return "scs-high"
	}
	return "scs-low"
}

// Job is j's batch Job routed to the tenant's LocalQueue with its priority
// class; a same-rack gang asks Kueue's topology-aware scheduling for one
// rack. Kueue's own Job webhook suspends the Job until it is admitted.
func Job(j workload.Job) *batchv1.Job {
	b := j.BatchJob()
	b.Labels["kueue.x-k8s.io/queue-name"] = "lq"
	b.Labels["kueue.x-k8s.io/priority-class"] = PriorityClass(j)
	if j.SameRack {
		b.Spec.Template.Annotations["kueue.x-k8s.io/podset-required-topology"] = labels.Rack
	}
	return b
}

// Submit creates the Job.
func (*Profile) Submit(ctx context.Context, c *cluster.Cluster, j workload.Job) error {
	if err := j.Validate(); err != nil {
		return err
	}
	_, err := c.Clientset.BatchV1().Jobs(j.Namespace).Create(ctx, Job(j), metav1.CreateOptions{})
	return err
}

// Remove deletes the Job and its pods; Kueue removes the Workload.
func (*Profile) Remove(ctx context.Context, c *cluster.Cluster, j workload.Job) error {
	bg := metav1.DeletePropagationBackground
	err := c.Clientset.BatchV1().Jobs(j.Namespace).Delete(ctx, j.Name, metav1.DeleteOptions{PropagationPolicy: &bg})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}
