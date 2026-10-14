// Package kai is the KAI Scheduler v0.18.5 profile. Every job is an
// explicit PodGroup (scheduling.run.ai/v2alpha2) in its tenant's leaf
// Queue, and kai-scheduler binds the pods: gangs by minMember, priority by
// PriorityClass, preemption and quota reclaim governed by the Queue's
// minimum runtimes, one rack by a topologyConstraint. Install applies the
// chart's own render without helm.
package kai

import (
	"bufio"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/apply"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/cluster"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/labels"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

// Version is the pinned KAI Scheduler release.
const Version = "v0.18.5"

// The chart manifests.yaml was rendered from, as pulled from
// oci://ghcr.io/kai-scheduler/kai-scheduler/kai-scheduler:v0.18.5.
const (
	ChartOCIDigest = "sha256:5737af77d749a18dd0ac77672c4c9df6da460978fdb2a30530df51851e1018d1"
	ChartSHA256    = "122c074ff7769cd13621194f64d23f7d81097ce37a129a0fab4f76387dfbcfef"
)

// manifests.yaml is the chart's default render, byte for byte:
//
//	helm template kai-scheduler kai-scheduler-v0.18.5.tgz --namespace kai-scheduler --include-crds
//
//go:embed manifests/manifests.yaml manifests/SHA256SUMS
var files embed.FS

// Namespace holds KAI's controllers; helm creates it with --create-namespace,
// so the render does not contain it.
const Namespace = "kai-scheduler"

// SchedulerName is the schedulerName KAI's pods carry.
const SchedulerName = "kai-scheduler"

// EvictionConditionArg makes kai-scheduler mark a pod it evicts with
// DisruptionTarget, reason PreemptionByScheduler, before deleting it, so the
// recorder writes the eviction as preempted rather than deleted. Its default
// is false (cmd/scheduler/app/options/options.go:124 at v0.18.5).
const EvictionConditionArg = "update-pod-eviction-condition"

var (
	queueGVR    = schema.GroupVersionResource{Group: "scheduling.run.ai", Version: "v2", Resource: "queues"}
	podGroupGVR = schema.GroupVersionResource{Group: "scheduling.run.ai", Version: "v2alpha2", Resource: "podgroups"}
)

// deployments are the kai-operator and the six Deployments it creates once
// the kai-config Config exists.
var deployments = []string{"kai-operator", "admission", "binder", "kai-scheduler-default", "pod-grouper", "podgroup-controller", "queue-controller"}

// Webhook configurations the operator creates at v0.18.5: pod admission
// (mutating and validating), Queue and PodGroup validation. All are
// failurePolicy Fail; the operator writes them after the chart is applied.
const mutatingWebhook = "mutating-kai-admission"

var validatingWebhooks = []string{"validating-kai-admission", "kai-queue-validation-v2", "kai-podgroup-validation-v2alpha2"}

// base is created once per cluster, after KAI answers: the two
// PriorityClasses and the topology (block, rack, host), its levels the
// labels pkg/fleet puts on the nodes.
const base = `apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata: {name: scs-low, labels: {app.kubernetes.io/part-of: scheduler-capabilities-suite}}
value: 100
---
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata: {name: scs-high, labels: {app.kubernetes.io/part-of: scheduler-capabilities-suite}}
value: 1000
---
apiVersion: kai.scheduler/v1alpha1
kind: Topology
metadata: {name: scs-topology, labels: {app.kubernetes.io/part-of: scheduler-capabilities-suite}}
spec:
  levels:
  - nodeLabel: ` + labels.Block + `
  - nodeLabel: ` + labels.Rack + `
  - nodeLabel: kubernetes.io/hostname
`

// Manifest returns the embedded chart render and its verified sha256.
func Manifest() ([]byte, string, error) {
	data, err := files.ReadFile("manifests/manifests.yaml")
	if err != nil {
		return nil, "", err
	}
	sums, err := files.ReadFile("manifests/SHA256SUMS")
	if err != nil {
		return nil, "", err
	}
	got, err := checkSum(data, sums)
	if err != nil {
		return nil, "", err
	}
	return data, got, nil
}

// checkSum returns data's sha256 when it matches the manifests.yaml line
// of sums.
func checkSum(data, sums []byte) (string, error) {
	sum := sha256.Sum256(data)
	got := hex.EncodeToString(sum[:])
	sc := bufio.NewScanner(strings.NewReader(string(sums)))
	for sc.Scan() {
		if f := strings.Fields(sc.Text()); len(f) == 2 && f[1] == "manifests.yaml" {
			if f[0] != got {
				return "", fmt.Errorf("kai: manifests.yaml has sha256 %s, SHA256SUMS says %s", got, f[0])
			}
			return got, nil
		}
	}
	return "", fmt.Errorf("kai: manifests.yaml is not listed in SHA256SUMS")
}

// installObjects turns the chart's render into what helm install would
// leave behind on a fresh cluster: the kai-scheduler Namespace first, the
// render without its hook objects, and last the kai-config Config that the
// post-install hook would have applied from its ConfigMap. The pre-install
// hooks only upgrade CRDs and migrate Kueue topologies, which a fresh
// cluster has none of, and the post-delete hook never runs because the
// suite deletes the cluster. The default SchedulingShard gets
// EvictionConditionArg.
func installObjects(objs []*unstructured.Unstructured) ([]*unstructured.Unstructured, error) {
	cfg, err := configFromHook(objs)
	if err != nil {
		return nil, err
	}
	ns := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": Namespace},
	}}
	out := []*unstructured.Unstructured{ns}
	for _, o := range objs {
		if _, hook := o.GetAnnotations()["helm.sh/hook"]; !hook {
			out = append(out, o)
		}
	}
	if err := addEvictionCondition(out); err != nil {
		return nil, err
	}
	return append(out, cfg), nil
}

// configFromHook decodes the Config held in the post-install hook's
// ConfigMap.
func configFromHook(objs []*unstructured.Unstructured) (*unstructured.Unstructured, error) {
	const key = "kai-config.yaml"
	for _, o := range objs {
		if o.GetKind() != "ConfigMap" || o.GetName() != "kai-config-manifest" {
			continue
		}
		data, found, err := unstructured.NestedString(o.Object, "data", key)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("kai: kai-config-manifest has no data key %s", key)
		}
		cfg, err := apply.Decode([]byte(data))
		if err != nil {
			return nil, fmt.Errorf("kai: kai-config-manifest: %w", err)
		}
		if len(cfg) != 1 || cfg[0].GetKind() != "Config" {
			kind := "no object"
			if len(cfg) > 0 {
				kind = cfg[0].GetKind()
			}
			return nil, fmt.Errorf("kai: kai-config-manifest holds a %s, want a Config", kind)
		}
		return cfg[0], nil
	}
	return nil, fmt.Errorf("kai: manifest has no kai-config-manifest ConfigMap")
}

// addEvictionCondition sets EvictionConditionArg in the default
// SchedulingShard's args, which the operator passes to kai-scheduler as
// flags, refusing a shard that already sets it.
func addEvictionCondition(objs []*unstructured.Unstructured) error {
	for _, o := range objs {
		if o.GetKind() != "SchedulingShard" || o.GetName() != "default" {
			continue
		}
		args, _, err := unstructured.NestedStringMap(o.Object, "spec", "args")
		if err != nil {
			return err
		}
		if _, set := args[EvictionConditionArg]; set {
			return fmt.Errorf("kai: SchedulingShard default already sets %s", EvictionConditionArg)
		}
		if args == nil {
			args = map[string]string{}
		}
		args[EvictionConditionArg] = "true"
		return unstructured.SetNestedStringMap(o.Object, args, "spec", "args")
	}
	return fmt.Errorf("kai: manifest has no SchedulingShard default")
}

// Profile is the KAI profile.
type Profile struct{}

// New returns the KAI profile.
func New() *Profile { return &Profile{} }

// Name implements scheduler.Profile.
func (*Profile) Name() string { return "kai" }

// ClaimsFile implements scheduler.Profile.
func (*Profile) ClaimsFile() string { return "claims/kai.yaml" }

// WorkloadGVR implements scheduler.Profile. A PodGroup carries none of the
// Kueue Workload conditions the recorder reads, and no verdict reads
// workload records.
func (*Profile) WorkloadGVR() *schema.GroupVersionResource { return nil }

// Install applies the chart's objects, waits until KAI is ready (its
// controllers come from the operator after the apply returns), then
// creates the base objects.
func (*Profile) Install(ctx context.Context, c *cluster.Cluster) (map[string]string, error) {
	data, sum, err := Manifest()
	if err != nil {
		return nil, err
	}
	objs, err := apply.Decode(data)
	if err != nil {
		return nil, err
	}
	if objs, err = installObjects(objs); err != nil {
		return nil, err
	}
	if _, err := c.Applier.ApplyObjects(ctx, objs, 2*time.Minute); err != nil {
		return nil, err
	}
	start := time.Now()
	if err := waitReady(ctx, c.Clientset, c.Dynamic, 2*time.Second, 8*time.Minute); err != nil {
		return nil, err
	}
	readyWait := time.Since(start).Round(time.Second)
	if _, err := c.Applier.Apply(ctx, []byte(base), time.Minute); err != nil {
		return nil, err
	}
	return map[string]string{
		"kai_version":          Version,
		"kai_chart_oci_digest": ChartOCIDigest,
		"kai_chart_sha256":     ChartSHA256,
		"kai_manifest_sha256":  sum,
		"kai_install":          "chart render without its helm hooks; kai-config applied directly; SchedulingShard default args " + EvictionConditionArg + "=true",
		"kai_ready_wait":       readyWait.String(),
		"scheduler":            SchedulerName + " " + Version + " (default actions: allocate, consolidation, reclaim, preempt, stalegangeviction)",
	}, nil
}

// waitReady polls every interval until the Deployments are available, the
// webhook configurations exist and a dry-run create of a Queue, a PodGroup
// and a kai-scheduler Pod is answered. A dry run alone proves nothing until
// the operator has registered the webhooks: before that the API server
// accepts it without calling them. On timeout it returns the last error,
// which names the cause.
func waitReady(ctx context.Context, cs kubernetes.Interface, dyn dynamic.Interface, interval, timeout time.Duration) error {
	var last error
	err := wait.PollUntilContextTimeout(ctx, interval, timeout, true, func(ctx context.Context) (bool, error) {
		last = ready(ctx, cs, dyn)
		return last == nil, nil
	})
	if err == nil {
		return nil
	}
	if last == nil {
		last = err
	}
	return fmt.Errorf("kai not ready after %s: %w", timeout, last)
}

func ready(ctx context.Context, cs kubernetes.Interface, dyn dynamic.Interface) error {
	for _, name := range deployments {
		d, err := cs.AppsV1().Deployments(Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("deployment %s/%s: %w", Namespace, name, err)
		}
		available := false
		for _, c := range d.Status.Conditions {
			if c.Type == appsv1.DeploymentAvailable && c.Status == corev1.ConditionTrue {
				available = true
			}
		}
		if !available {
			return fmt.Errorf("deployment %s/%s is not available", Namespace, name)
		}
	}
	if _, err := cs.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(ctx, mutatingWebhook, metav1.GetOptions{}); err != nil {
		return fmt.Errorf("webhook configuration %s: %w", mutatingWebhook, err)
	}
	for _, name := range validatingWebhooks {
		if _, err := cs.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(ctx, name, metav1.GetOptions{}); err != nil {
			return fmt.Errorf("webhook configuration %s: %w", name, err)
		}
	}
	dry := metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}}
	none := map[string]any{"quota": int64(0), "limit": int64(-1), "overQuotaWeight": int64(1)}
	q := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "scheduling.run.ai/v2", "kind": "Queue",
		"metadata": map[string]any{"name": "scs-webhook-probe"},
		"spec":     map[string]any{"resources": map[string]any{"gpu": none, "cpu": none, "memory": none}},
	}}
	if _, err := dyn.Resource(queueGVR).Create(ctx, q, dry); err != nil {
		return fmt.Errorf("dry-run Queue: %w", err)
	}
	pg := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "scheduling.run.ai/v2alpha2", "kind": "PodGroup",
		"metadata": map[string]any{"name": "scs-webhook-probe", "namespace": "default"},
		"spec":     map[string]any{"minMember": int64(1), "queue": "default-queue"},
	}}
	if _, err := dyn.Resource(podGroupGVR).Namespace("default").Create(ctx, pg, dry); err != nil {
		return fmt.Errorf("dry-run PodGroup: %w", err)
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "scs-webhook-probe", Namespace: "default"},
		Spec:       corev1.PodSpec{SchedulerName: SchedulerName, Containers: []corev1.Container{{Name: "main", Image: workload.FakeImage}}},
	}
	if _, err := cs.CoreV1().Pods("default").Create(ctx, pod, dry); err != nil {
		return fmt.Errorf("dry-run Pod: %w", err)
	}
	return nil
}

// ParentQueueName is the Queue that holds a probe's tenant queues.
func ParentQueueName(s scheduler.Scenario) string { return "scs-" + s.Probe }

// LeafQueueName is tenant t's Queue, named like its namespace.
func LeafQueueName(t scheduler.Tenant) string { return t.Namespace }

// resources is a Queue's resources: gpus as GPU quota, no CPU or memory
// quota, no limit.
func resources(gpus int) map[string]any {
	none := map[string]any{"quota": int64(0), "limit": int64(-1), "overQuotaWeight": int64(1)}
	return map[string]any{
		"gpu":    map[string]any{"quota": int64(gpus), "limit": int64(-1), "overQuotaWeight": int64(1)},
		"cpu":    none,
		"memory": none,
	}
}

// Queues are a probe's Queues, the parent first: the parent holds the sum
// of the tenants' entitlements and each tenant's leaf queue its own. A job
// in a leaf queue may be preempted (same queue) or reclaimed (over its
// quota) only once it has run the queue's minimum runtimes: 0s when the
// probe allows preemption, 24h otherwise. KAI does not read a
// PriorityClass's preemptionPolicy, so this is where a probe forbids it.
func Queues(s scheduler.Scenario) []map[string]any {
	minRuntime := "24h"
	if s.AllowPreemption {
		minRuntime = "0s"
	}
	total := 0
	for _, t := range s.Tenants {
		total += t.EntitlementGPUs
	}
	out := []map[string]any{{
		"apiVersion": "scheduling.run.ai/v2", "kind": "Queue",
		"metadata": map[string]any{"name": ParentQueueName(s), "labels": map[string]any{labels.PartOf: labels.PartOfValue, labels.Probe: s.Probe}},
		"spec":     map[string]any{"resources": resources(total)},
	}}
	for _, t := range s.Tenants {
		out = append(out, map[string]any{
			"apiVersion": "scheduling.run.ai/v2", "kind": "Queue",
			"metadata": map[string]any{"name": LeafQueueName(t), "labels": map[string]any{labels.PartOf: labels.PartOfValue, labels.Probe: s.Probe, labels.Tenant: t.Name}},
			"spec": map[string]any{
				"parentQueue":       ParentQueueName(s),
				"resources":         resources(t.EntitlementGPUs),
				"preemptMinRuntime": minRuntime,
				"reclaimMinRuntime": minRuntime,
			},
		})
	}
	return out
}

// Prepare creates the probe's namespaces and its Queues, the parent first.
func (*Profile) Prepare(ctx context.Context, c *cluster.Cluster, s scheduler.Scenario) error {
	if err := scheduler.CreateNamespaces(ctx, c.Clientset, s); err != nil {
		return err
	}
	for _, q := range Queues(s) {
		data, err := json.Marshal(q)
		if err != nil {
			return err
		}
		if _, err := c.Applier.Apply(ctx, data, time.Minute); err != nil {
			return err
		}
	}
	return nil
}

// Cleanup deletes the probe's leaf Queues, then the parent, and waits
// until they are gone.
func (*Profile) Cleanup(ctx context.Context, c *cluster.Cluster, s scheduler.Scenario) error {
	var names []string
	for _, t := range s.Tenants {
		names = append(names, LeafQueueName(t))
	}
	names = append(names, ParentQueueName(s))
	for _, n := range names {
		if err := c.Dynamic.Resource(queueGVR).Delete(ctx, n, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("kai: delete Queue %s: %w", n, err)
		}
	}
	return wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		for _, n := range names {
			_, err := c.Dynamic.Resource(queueGVR).Get(ctx, n, metav1.GetOptions{})
			if err == nil {
				return false, nil
			}
			if !apierrors.IsNotFound(err) {
				return false, fmt.Errorf("kai: get Queue %s: %w", n, err)
			}
		}
		return true, nil
	})
}

// PriorityClass maps a job's priority to the installed PriorityClass.
// Whether a high-priority job may preempt is the Queue's minimum runtime
// (Scenario.AllowPreemption), not the job's.
func PriorityClass(j workload.Job) string {
	if j.Priority == workload.High {
		return "scs-high"
	}
	return "scs-low"
}

// PodGroup is j's PodGroup, named like the Job: in the tenant's leaf queue,
// minMember all of a gang's pods and 1 otherwise, the job's priority class,
// preemptible whatever the priority (KAI would otherwise treat a priority
// of 100 or more as non-preemptible), and for a same-rack gang a required
// rack in the suite's topology.
func PodGroup(j workload.Job) map[string]any {
	minMember := 1
	if j.Gang {
		minMember = j.Pods
	}
	spec := map[string]any{
		"minMember":         int64(minMember),
		"queue":             j.Namespace,
		"priorityClassName": PriorityClass(j),
		"preemptibility":    "preemptible",
	}
	if j.SameRack {
		spec["topologyConstraint"] = map[string]any{"topology": "scs-topology", "requiredTopologyLevel": labels.Rack}
	}
	lbl := map[string]any{}
	for k, v := range j.Labels() {
		lbl[k] = v
	}
	return map[string]any{
		"apiVersion": "scheduling.run.ai/v2alpha2", "kind": "PodGroup",
		"metadata": map[string]any{"name": j.Name, "namespace": j.Namespace, "labels": lbl},
		"spec":     spec,
	}
}

// Job is j's batch Job for kai-scheduler: its pods join the PodGroup named
// like the Job, KAI's pod-grouper leaves the Job alone, and the pods carry
// the priority class so the recorded pod priority matches the PodGroup's.
func Job(j workload.Job) *batchv1.Job {
	b := j.BatchJob()
	b.Annotations = map[string]string{"kai.scheduler/skip-podgrouper": "true"}
	b.Spec.Template.Annotations["pod-group-name"] = j.Name
	b.Spec.Template.Spec.SchedulerName = SchedulerName
	b.Spec.Template.Spec.PriorityClassName = PriorityClass(j)
	return b
}

// Submit creates the PodGroup first (its queue decides where the pods may
// run), then the Job.
func (*Profile) Submit(ctx context.Context, c *cluster.Cluster, j workload.Job) error {
	if err := j.Validate(); err != nil {
		return err
	}
	pg := &unstructured.Unstructured{Object: PodGroup(j)}
	if _, err := c.Dynamic.Resource(podGroupGVR).Namespace(j.Namespace).Create(ctx, pg, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("podgroup for %s: %w", j.Name, err)
	}
	_, err := c.Clientset.BatchV1().Jobs(j.Namespace).Create(ctx, Job(j), metav1.CreateOptions{})
	return err
}

// Remove deletes the Job (and its pods) and its PodGroup.
func (*Profile) Remove(ctx context.Context, c *cluster.Cluster, j workload.Job) error {
	bg := metav1.DeletePropagationBackground
	if err := c.Clientset.BatchV1().Jobs(j.Namespace).Delete(ctx, j.Name, metav1.DeleteOptions{PropagationPolicy: &bg}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if err := c.Dynamic.Resource(podGroupGVR).Namespace(j.Namespace).Delete(ctx, j.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}
