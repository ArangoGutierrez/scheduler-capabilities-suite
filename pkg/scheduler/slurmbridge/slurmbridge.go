// Package slurmbridge is the slurm-bridge v1.2.3 profile: Slurm 26.05.4
// decides where every pod goes, and slurm-bridge's scheduler binds it.
// Each Job pod, or each gang as one scheduler-plugins PodGroup, becomes one
// Slurm external job on the kwok nodes, registered as Slurm external nodes.
// Slurm sees nodes, not GPUs: every job drops its GPU request, which is
// valid only because every probe pod takes a whole 8-GPU node. Priority is
// the job's Slurm priority, preemption is partition-tier preemption with
// CANCEL, topology is Slurm's default switch-aware placement, and there is
// no Slurm accounting, so no fair share. Install applies the charts' own
// renders without helm.
package slurmbridge

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/apply"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/cluster"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/fleet"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

// Version is the pinned release of slurm-bridge, slurm-operator and their
// charts; SlurmVersion is the Slurm in the pinned slurmctld image.
const (
	Version      = "v1.2.3"
	SlurmVersion = "26.05.4"
)

// The embedded files, in apply order, each pinned in manifests/SHA256SUMS.
// The four chart renders are byte for byte what these commands print, with
// helm v4.3.0 and the charts pulled from oci://ghcr.io/slinkyproject/charts
// at 1.2.3 (OCI digests in Sources):
//
//	helm template slurm-operator-crds slurm-operator-crds-1.2.3.tgz --kube-version v1.37.0
//	helm template slurm-operator slurm-operator-1.2.3.tgz --namespace slinky --kube-version v1.37.0 -f manifests/values/slurm-operator.yaml
//	helm template slurm slurm-1.2.3.tgz --namespace slurm --kube-version v1.37.0 -f manifests/values/slurm.yaml
//	helm template slurm-bridge slurm-bridge-1.2.3.tgz --namespace slurm --kube-version v1.37.0 -f manifests/values/slurm-bridge.yaml
//
// podgroups.yaml is scheduler-plugins' PodGroup CRD and token.yaml is
// slurm-bridge's hack/token.yaml, both downloaded unchanged.
var Files = []string{"slurm-operator-crds.yaml", "podgroups.yaml", "slurm-operator.yaml", "slurm.yaml", "slurm-bridge.yaml", "token.yaml"}

// Sources records where each embedded file came from.
var Sources = map[string]string{
	"slurm-operator-crds.yaml": "oci://ghcr.io/slinkyproject/charts/slurm-operator-crds:1.2.3@sha256:fd30eb2d1e80b88dc6f0fe01becb3e96ff35a9eccb07a97072f3a105df963ce6",
	"podgroups.yaml":           "https://github.com/kubernetes-sigs/scheduler-plugins/blob/v0.33.5/config/crd/bases/scheduling.x-k8s.io_podgroups.yaml",
	"slurm-operator.yaml":      "oci://ghcr.io/slinkyproject/charts/slurm-operator:1.2.3@sha256:be8f99c89f9667b9dc2a64633007379a59f031938621bc116527bfddc995d956",
	"slurm.yaml":               "oci://ghcr.io/slinkyproject/charts/slurm:1.2.3@sha256:f10b978a0adab2d24a3e0b3219eb847833fdcc388ff03c30b72739caaca8c67c",
	"slurm-bridge.yaml":        "oci://ghcr.io/slinkyproject/charts/slurm-bridge:1.2.3@sha256:22411d138ba7e094ec54e241054e7fe8ad48db714cb0f7344f4225f0d3957f65",
	"token.yaml":               "https://github.com/SlinkyProject/slurm-bridge/blob/v1.2.3/hack/token.yaml",
}

//go:embed manifests/*.yaml manifests/SHA256SUMS
var files embed.FS

// Namespaces: the operator runs in slinky; slurmctld, slurmrestd and
// slurm-bridge run in slurm. helm creates both with --create-namespace, so
// the renders do not contain them.
const (
	OperatorNamespace = "slinky"
	Namespace         = "slurm"
)

// SchedulerName is the schedulerName slurm-bridge's pods carry.
const SchedulerName = "slurm-bridge-scheduler"

// The Slurm partitions, both over every fleet node: jobs go to Partition
// (slurm-bridge's default, PriorityTier 1), a preempting job to
// PreemptPartition (PriorityTier 2), whose jobs may cancel those of the
// lower tier (manifests/values/slurm.yaml).
const (
	Partition        = "slurm-bridge"
	PreemptPartition = "slurm-bridge-preempt"
)

// Keys slurm-bridge reads (internal/wellknown at v1.2.3) and the
// scheduler-plugins pod-group label.
const (
	annGres           = "slurmjob.slinky.slurm.net/gres"
	annPriority       = "slurmjob.slinky.slurm.net/priority"
	annTimeLimit      = "slurmjob.slinky.slurm.net/timelimit"
	annPartition      = "slurmjob.slinky.slurm.net/partition"
	annNodePartitions = "scheduler.slinky.slurm.net/external-node-partitions"
	annNodeTopology   = "topology.slinky.slurm.net/spec"
	labelExternalNode = "scheduler.slinky.slurm.net/external-node"
	labelPodGroup     = "scheduling.x-k8s.io/pod-group"
	finalizer         = "scheduler.slinky.slurm.net/finalizer"
	managedNodeTaint  = "slinky.slurm.net/managed-node"
	topologyName      = "topo-switch"
)

// The auth key Secrets the slurm chart's values point at. Each key is 768
// random bytes in base64: 1024 characters, the length of the chart's own
// randAscii 1024.
const (
	slurmKeySecret = "slurm-auth-slurm"
	jwtKeySecret   = "slurm-auth-jwt"
	keyBytes       = 768
)

// leafSwitch maps each fleet rack to its leaf switch in the topology.yaml
// of manifests/values/slurm.yaml.
var leafSwitch = map[string]string{"rack-a": "s1", "rack-b": "s2"}

var podGroupGVR = schema.GroupVersionResource{Group: "scheduling.x-k8s.io", Version: "v1alpha1", Resource: "podgroups"}

// failOnDelete is the kwok Stage that does what a kubelet does to a deleted
// pod: it moves a deleted Pending or Running pod that still carries
// slurm-bridge's finalizer to Failed. slurm-bridge removes the finalizer
// only from a terminal pod, and the suite's pod-delete stage skips pods
// with finalizers, so without it such a pod stays Terminating for good.
// Failed, not Succeeded: a preempted victim that ends Succeeded makes the
// preemption rule refuse the probe.
const failOnDelete = `apiVersion: kwok.x-k8s.io/v1alpha1
kind: Stage
metadata:
  name: pod-fail-on-delete
  labels: {app.kubernetes.io/part-of: scheduler-capabilities-suite}
spec:
  resourceRef:
    apiGroup: v1
    kind: Pod
  selector:
    matchExpressions:
    - key: '.metadata.deletionTimestamp'
      operator: 'Exists'
    - key: '.metadata.finalizers.[]'
      operator: 'In'
      values:
      - '` + finalizer + `'
    - key: '.status.phase'
      operator: 'In'
      values:
      - 'Pending'
      - 'Running'
  weight: 1
  next:
    statusTemplate: |
      {{ $now := Now }}
      containerStatuses:
      {{ range $index, $item := .spec.containers }}
      - image: {{ $item.image | Quote }}
        name: {{ $item.name | Quote }}
        ready: false
        restartCount: 0
        started: false
        state:
          terminated:
            exitCode: 137
            finishedAt: {{ $now | Quote }}
            reason: Error
            startedAt: {{ $now | Quote }}
      {{ end }}
      phase: Failed
`

// priorityClasses are created once slurm-bridge is ready. They order the
// pods in the bridge's PrioritySort queue and give the recorder each pod's
// priority; neither may preempt in Kubernetes, since Slurm decides.
const priorityClasses = `apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata: {name: scs-low, labels: {app.kubernetes.io/part-of: scheduler-capabilities-suite}}
value: 100
preemptionPolicy: Never
---
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata: {name: scs-high, labels: {app.kubernetes.io/part-of: scheduler-capabilities-suite}}
value: 1000
preemptionPolicy: Never
`

// Manifest returns an embedded file and its verified sha256.
func Manifest(name string) ([]byte, string, error) {
	data, err := files.ReadFile("manifests/" + name)
	if err != nil {
		return nil, "", err
	}
	sums, err := files.ReadFile("manifests/SHA256SUMS")
	if err != nil {
		return nil, "", err
	}
	got, err := checkSum(name, data, sums)
	if err != nil {
		return nil, "", err
	}
	return data, got, nil
}

// checkSum returns data's sha256 when it matches name's line of sums.
func checkSum(name string, data, sums []byte) (string, error) {
	sum := sha256.Sum256(data)
	got := hex.EncodeToString(sum[:])
	sc := bufio.NewScanner(strings.NewReader(string(sums)))
	for sc.Scan() {
		if f := strings.Fields(sc.Text()); len(f) == 2 && f[1] == name {
			if f[0] != got {
				return "", fmt.Errorf("slurm-bridge: %s has sha256 %s, SHA256SUMS says %s", name, got, f[0])
			}
			return got, nil
		}
	}
	return "", fmt.Errorf("slurm-bridge: %s is not listed in SHA256SUMS", name)
}

// keyPatterns are what a chart render holds when it carries a key or a
// certificate: the slurm chart's key Secrets, the operator's webhook
// certificate Secret and caBundle (cert-manager off), and PEM text.
var keyPatterns = []struct {
	re   *regexp.Regexp
	what string
}{
	{regexp.MustCompile(`^\s*kind:\s*Secret\s*$`), "Secret object"},
	{regexp.MustCompile(`^\s*"?(slurm\.key|jwt\.key|tls\.key|tls\.crt|ca\.key|ca\.crt)"?\s*:`), "$1 data"},
	{regexp.MustCompile(`^\s*caBundle\s*:`), "caBundle"},
	{regexp.MustCompile(`-----BEGIN `), "PEM block"},
}

// keyMaterial returns, line by line, every sign of key or certificate
// material in data, as "name:line: what".
func keyMaterial(name string, data []byte) []string {
	var out []string
	for i, line := range strings.Split(string(data), "\n") {
		for _, p := range keyPatterns {
			if m := p.re.FindStringSubmatchIndex(line); m != nil {
				what := string(p.re.ExpandString(nil, p.what, line, m))
				out = append(out, fmt.Sprintf("%s:%d: %s", name, i+1, what))
			}
		}
	}
	return out
}

// newKey reads keyBytes from r and returns them base64-encoded.
func newKey(r io.Reader) (string, error) {
	b := make([]byte, keyBytes)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

// authSecrets generates the Slurm auth key and the JWT key from r, in the
// Secrets the Controller's slurmKeyRef and jwtKeyRef name.
func authSecrets(r io.Reader) ([]*unstructured.Unstructured, error) {
	var out []*unstructured.Unstructured
	for _, s := range []struct{ name, key string }{{slurmKeySecret, "slurm.key"}, {jwtKeySecret, "jwt.key"}} {
		k, err := newKey(r)
		if err != nil {
			return nil, fmt.Errorf("slurm-bridge: generate %s: %w", s.key, err)
		}
		out = append(out, &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Secret",
			"metadata": map[string]any{"name": s.name, "namespace": Namespace},
			"type":     "Opaque",
			"data":     map[string]any{s.key: base64.StdEncoding.EncodeToString([]byte(k))},
		}})
	}
	return out, nil
}

// installObjects is everything Install applies before the nodes: the two
// namespaces, the auth Secrets generated from r, every embedded file in
// Files order, and the failOnDelete Stage. It returns each file's sha256.
func installObjects(r io.Reader) ([]*unstructured.Unstructured, map[string]string, error) {
	var out []*unstructured.Unstructured
	for _, ns := range []string{OperatorNamespace, Namespace} {
		out = append(out, &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": ns},
		}})
	}
	secrets, err := authSecrets(r)
	if err != nil {
		return nil, nil, err
	}
	out = append(out, secrets...)
	sums := map[string]string{}
	for _, name := range Files {
		data, sum, err := Manifest(name)
		if err != nil {
			return nil, nil, err
		}
		objs, err := apply.Decode(data)
		if err != nil {
			return nil, nil, fmt.Errorf("slurm-bridge: %s: %w", name, err)
		}
		out = append(out, objs...)
		sums[name] = sum
	}
	stage, err := apply.Decode([]byte(failOnDelete))
	if err != nil {
		return nil, nil, err
	}
	return append(out, stage...), sums, nil
}

// NodePatch is n's registration as a Slurm external node: the partitions
// and its rack's leaf switch as annotations and the external-node label,
// in one merge patch. A node that slurm-bridge sees labelled before its
// partition annotation is registered with no partition and stays out of
// it, so the two must arrive together.
func NodePatch(n fleet.Node) ([]byte, error) {
	sw, ok := leafSwitch[n.Rack]
	if !ok {
		return nil, fmt.Errorf("slurm-bridge: node %s is in rack %s, which has no leaf switch in topology.yaml", n.Name, n.Rack)
	}
	return json.Marshal(map[string]any{"metadata": map[string]any{
		"annotations": map[string]string{
			annNodePartitions: Partition + "," + PreemptPartition,
			annNodeTopology:   topologyName + ":" + sw,
		},
		"labels": map[string]string{labelExternalNode: "true"},
	}})
}

// registerNodes patches every fleet node with its NodePatch.
func registerNodes(ctx context.Context, cs kubernetes.Interface, s fleet.Spec) error {
	for _, n := range s.Nodes {
		p, err := NodePatch(n)
		if err != nil {
			return err
		}
		if _, err := cs.CoreV1().Nodes().Patch(ctx, n.Name, types.MergePatchType, p, metav1.PatchOptions{}); err != nil {
			return fmt.Errorf("slurm-bridge: register node %s: %w", n.Name, err)
		}
	}
	return nil
}

// Profile is the slurm-bridge profile.
type Profile struct{}

// New returns the slurm-bridge profile.
func New() *Profile { return &Profile{} }

// Name implements scheduler.Profile.
func (*Profile) Name() string { return "slurm-bridge" }

// ClaimsFile implements scheduler.Profile.
func (*Profile) ClaimsFile() string { return "claims/slurm-bridge.yaml" }

// WorkloadGVR implements scheduler.Profile. A scheduler-plugins PodGroup
// carries none of the Kueue Workload conditions the recorder reads.
func (*Profile) WorkloadGVR() *schema.GroupVersionResource { return nil }

// Install applies the namespaces, generated keys, renders, CRD, Token and
// kwok Stage, registers the fleet nodes, waits until Slurm has them idle in
// both partitions, then creates the PriorityClasses.
func (*Profile) Install(ctx context.Context, c *cluster.Cluster) (map[string]string, error) {
	objs, sums, err := installObjects(rand.Reader)
	if err != nil {
		return nil, err
	}
	if _, err := c.Applier.ApplyObjects(ctx, objs, 2*time.Minute); err != nil {
		return nil, err
	}
	f := fleet.Default()
	if err := registerNodes(ctx, c.Clientset, f); err != nil {
		return nil, err
	}
	var names []string
	for _, n := range f.Nodes {
		names = append(names, n.Name)
	}
	start := time.Now()
	if err := waitReady(ctx, c.Clientset, restAPI(c.Clientset), names, 2*time.Second, 10*time.Minute); err != nil {
		return nil, err
	}
	readyWait := time.Since(start).Round(time.Second)
	if _, err := c.Applier.Apply(ctx, []byte(priorityClasses), time.Minute); err != nil {
		return nil, err
	}
	out := map[string]string{
		"slurm_bridge_version": Version,
		"slurm_version":        SlurmVersion,
		"slurm_conf_additions": "PreemptType=preempt/partition_prio, PreemptMode=CANCEL, partition " + PreemptPartition + " at PriorityTier 2 over the nodes of " + Partition + "; everything else as slurm-bridge's hack/slurm-bridge-external.yaml",
		"slurm_install":        "renders without helm; operator webhook off; slurm.key and jwt.key generated at install; PodGroup CRD only, no coscheduling controller; kwok Stage pod-fail-on-delete; nodes annotated and labelled in one patch",
		"slurm_gpus":           "GPU request dropped from every Slurm job (gres \"\"): Slurm places whole exclusive nodes, never GPUs",
		"slurm_ready_wait":     readyWait.String(),
		"scheduler":            SchedulerName + " " + Version + " (kube-scheduler framework v0.36.0) placing through slurmctld " + SlurmVersion,
	}
	for name, sum := range sums {
		out["manifest_"+name] = sum + " " + Sources[name]
	}
	return out, nil
}

// Prepare creates the probe's namespaces; Slurm needs no per-probe objects.
func (*Profile) Prepare(ctx context.Context, c *cluster.Cluster, s scheduler.Scenario) error {
	return scheduler.CreateNamespaces(ctx, c.Clientset, s)
}

// Cleanup waits, at most two minutes, until Slurm has no pending or running
// job for the probe's pods. The harness sees pods only; a Slurm job left
// behind would hold nodes from the next probe.
func (*Profile) Cleanup(ctx context.Context, c *cluster.Cluster, s scheduler.Scenario) error {
	return waitJobsGone(ctx, restAPI(c.Clientset), s, 2*time.Second, 2*time.Minute)
}

// nodeGPUs is the GPU count every fleet node shares.
func nodeGPUs(s fleet.Spec) (int, error) {
	if len(s.Nodes) == 0 {
		return 0, fmt.Errorf("slurm-bridge: the fleet has no nodes")
	}
	first := s.Nodes[0]
	for _, n := range s.Nodes[1:] {
		if n.GPUs != first.GPUs {
			return 0, fmt.Errorf("slurm-bridge: fleet nodes %s and %s have %d and %d GPUs; the profile needs every node alike", first.Name, n.Name, first.GPUs, n.GPUs)
		}
	}
	return first.GPUs, nil
}

// TimeLimitMinutes is the Slurm time limit for a job that runs d: whole
// minutes, rounded up, plus one, so Slurm does not end the job just before
// kwok completes it.
func TimeLimitMinutes(d time.Duration) int {
	return int(math.Ceil(d.Minutes())) + 1
}

// slurmPriority is a job's Slurm priority, the value of its PriorityClass.
func slurmPriority(j workload.Job) int {
	if j.Priority == workload.High {
		return 1000
	}
	return 100
}

// PriorityClass maps a job's priority to the installed PriorityClass.
func PriorityClass(j workload.Job) string {
	if j.Priority == workload.High {
		return "scs-high"
	}
	return "scs-low"
}

// Annotations are the Slurm job settings slurm-bridge reads: no GRES (the
// pod's GPU request would become gres/gpu, which no kwok node has), the
// priority, the time limit, and for a preempting job the higher-tier
// partition.
func Annotations(j workload.Job) map[string]string {
	a := map[string]string{
		annGres:      "",
		annPriority:  strconv.Itoa(slurmPriority(j)),
		annTimeLimit: strconv.Itoa(TimeLimitMinutes(j.RunFor)),
	}
	if j.Preempting {
		a[annPartition] = PreemptPartition
	}
	return a
}

// PodGroupName is a gang's PodGroup. slurm-bridge lists a group's pods by
// label in every namespace, so the name carries the namespace.
func PodGroupName(j workload.Job) string { return "pg-" + j.Namespace + "-" + j.Name }

// PodGroup is j's scheduler-plugins PodGroup: minMember all of its pods,
// and the Slurm annotations, which slurm-bridge reads from the PodGroup
// alone for a coscheduled group.
func PodGroup(j workload.Job) map[string]any {
	lbl := map[string]any{}
	for k, v := range j.Labels() {
		lbl[k] = v
	}
	ann := map[string]any{}
	for k, v := range Annotations(j) {
		ann[k] = v
	}
	return map[string]any{
		"apiVersion": "scheduling.x-k8s.io/v1alpha1", "kind": "PodGroup",
		"metadata": map[string]any{"name": PodGroupName(j), "namespace": j.Namespace, "labels": lbl, "annotations": ann},
		"spec":     map[string]any{"minMember": int64(j.Pods)},
	}
}

// Job is j's batch Job for slurm-bridge: its pods carry the scheduler name
// and priority class; a gang's pods join their PodGroup by label, and any
// other Job carries the Slurm annotations itself (each of its pods is one
// Slurm job). spec.schedulingGroup stays unset: slurm-bridge v1.2.3 would
// look for a scheduling.k8s.io/v1alpha2 PodGroup, which Kubernetes v1.37
// does not serve.
func Job(j workload.Job) *batchv1.Job {
	b := j.BatchJob()
	b.Spec.Template.Spec.SchedulerName = SchedulerName
	b.Spec.Template.Spec.PriorityClassName = PriorityClass(j)
	if j.Gang {
		b.Spec.Template.Labels[labelPodGroup] = PodGroupName(j)
	} else {
		b.Annotations = Annotations(j)
	}
	return b
}

// Submit refuses a job whose pods are not whole nodes, then creates a
// gang's PodGroup and the Job.
func (*Profile) Submit(ctx context.Context, c *cluster.Cluster, j workload.Job) error {
	if err := j.Validate(); err != nil {
		return err
	}
	gpus, err := nodeGPUs(fleet.Default())
	if err != nil {
		return err
	}
	if j.GPUsPerPod != gpus {
		return fmt.Errorf("slurm-bridge: job %s asks for %d GPUs per pod; the profile drops the GPU request from Slurm, which is valid only when every pod takes a whole %d-GPU node", j.Name, j.GPUsPerPod, gpus)
	}
	if j.Gang {
		pg := &unstructured.Unstructured{Object: PodGroup(j)}
		if _, err := c.Dynamic.Resource(podGroupGVR).Namespace(j.Namespace).Create(ctx, pg, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("podgroup for %s: %w", j.Name, err)
		}
	}
	_, err = c.Clientset.BatchV1().Jobs(j.Namespace).Create(ctx, Job(j), metav1.CreateOptions{})
	return err
}

// Remove deletes the Job (and its pods) and a gang's PodGroup.
func (*Profile) Remove(ctx context.Context, c *cluster.Cluster, j workload.Job) error {
	bg := metav1.DeletePropagationBackground
	if err := c.Clientset.BatchV1().Jobs(j.Namespace).Delete(ctx, j.Name, metav1.DeleteOptions{PropagationPolicy: &bg}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if j.Gang {
		if err := c.Dynamic.Resource(podGroupGVR).Namespace(j.Namespace).Delete(ctx, PodGroupName(j), metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// slurmrestd is reached through the API server's service proxy, with the
// token the operator writes for slurm-bridge's Token, on the API version
// slurm-bridge v1.2.3 speaks (slurm-client api/v0044). slurmrestd skips
// empty path segments (src/common/http.c, url_path_walk), so the paths
// need no trailing slash.
const (
	restService = "slurm-restapi:6820"
	tokenSecret = "slurm-bridge-token"
	tokenKey    = "auth-token"
	nodesPath   = "/slurm/v0.0.44/nodes"
	jobsPath    = "/slurm/v0.0.44/jobs"
)

// restGet GETs a slurmrestd path and returns the reply body.
type restGet func(ctx context.Context, path string) ([]byte, error)

// restAPI returns a restGet that reads the bridge's token from its Secret
// and calls slurmrestd through the API server's service proxy.
func restAPI(cs kubernetes.Interface) restGet {
	return func(ctx context.Context, path string) ([]byte, error) {
		s, err := cs.CoreV1().Secrets(Namespace).Get(ctx, tokenSecret, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("slurmrestd token: %w", err)
		}
		res := cs.CoreV1().RESTClient().Get().
			Namespace(Namespace).Resource("services").Name(restService).SubResource("proxy").Suffix(path).
			SetHeader("X-SLURM-USER-TOKEN", string(s.Data[tokenKey])).
			Do(ctx)
		// Error, unlike DoRaw, decodes the API server's Status, which
		// names the cause (no endpoints, say) rather than the HTTP code.
		if err := res.Error(); err != nil {
			return nil, fmt.Errorf("slurmrestd %s: %w", path, err)
		}
		return res.Raw()
	}
}

// slurmNode is the part of a slurmrestd node the profile reads.
type slurmNode struct {
	Name       string   `json:"name"`
	State      []string `json:"state"`
	Partitions []string `json:"partitions"`
}

// nodesReady checks a slurmrestd nodes reply: every named node exists, is
// idle, and is in both partitions.
func nodesReady(data []byte, names []string) error {
	var r struct {
		Nodes []slurmNode `json:"nodes"`
	}
	if err := json.Unmarshal(data, &r); err != nil {
		return fmt.Errorf("slurmrestd nodes reply: %w", err)
	}
	for _, name := range names {
		i := slices.IndexFunc(r.Nodes, func(n slurmNode) bool { return n.Name == name })
		if i < 0 {
			return fmt.Errorf("Slurm has no node %s", name)
		}
		n := r.Nodes[i]
		if !slices.Contains(n.State, "IDLE") {
			return fmt.Errorf("Slurm node %s is %s, not IDLE", name, strings.Join(n.State, ","))
		}
		if !slices.Contains(n.Partitions, Partition) || !slices.Contains(n.Partitions, PreemptPartition) {
			return fmt.Errorf("Slurm node %s is in partitions %s, want %s and %s", name, strings.Join(n.Partitions, ","), Partition, PreemptPartition)
		}
	}
	return nil
}

// deployments are the operator's and slurm-bridge's. The operator creates
// slurmctld and slurmrestd; slurmrestd answering nodesPath proves both up.
var deployments = []struct{ namespace, name string }{
	{OperatorNamespace, "slurm-operator"},
	{Namespace, "slurm-bridge-scheduler"},
	{Namespace, "slurm-bridge-controllers"},
}

// waitReady polls every interval until the Deployments are available,
// slurmrestd lists every named node idle in both partitions, and
// slurm-bridge has tainted each node as its own. On timeout it returns the
// last error, which names the cause.
func waitReady(ctx context.Context, cs kubernetes.Interface, get restGet, names []string, interval, timeout time.Duration) error {
	var last error
	err := wait.PollUntilContextTimeout(ctx, interval, timeout, true, func(ctx context.Context) (bool, error) {
		last = ready(ctx, cs, get, names)
		return last == nil, nil
	})
	if err == nil {
		return nil
	}
	if last == nil {
		last = err
	}
	return fmt.Errorf("slurm-bridge not ready after %s: %w", timeout, last)
}

func ready(ctx context.Context, cs kubernetes.Interface, get restGet, names []string) error {
	for _, d := range deployments {
		dep, err := cs.AppsV1().Deployments(d.namespace).Get(ctx, d.name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("deployment %s/%s: %w", d.namespace, d.name, err)
		}
		available := false
		for _, c := range dep.Status.Conditions {
			if c.Type == appsv1.DeploymentAvailable && c.Status == corev1.ConditionTrue {
				available = true
			}
		}
		if !available {
			return fmt.Errorf("deployment %s/%s is not available", d.namespace, d.name)
		}
	}
	data, err := get(ctx, nodesPath)
	if err != nil {
		return fmt.Errorf("list Slurm nodes: %w", err)
	}
	if err := nodesReady(data, names); err != nil {
		return err
	}
	want := corev1.Taint{Key: managedNodeTaint, Value: SchedulerName, Effect: corev1.TaintEffectNoExecute}
	for _, name := range names {
		n, err := cs.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("node %s: %w", name, err)
		}
		if !slices.ContainsFunc(n.Spec.Taints, func(t corev1.Taint) bool { return t == want }) {
			return fmt.Errorf("node %s does not carry taint %s=%s:%s", name, want.Key, want.Value, want.Effect)
		}
	}
	return nil
}

// slurmJob is the part of a slurmrestd job the profile reads. slurm-bridge
// writes {"pods":["<namespace>/<name>",...]} into each job's admin_comment
// (internal/utils/externaljobinfo at v1.2.3).
type slurmJob struct {
	JobID        int64    `json:"job_id"`
	JobState     []string `json:"job_state"`
	AdminComment string   `json:"admin_comment"`
}

// liveJobs lists, as "<id> (<state>)", the pending or running jobs of a
// slurmrestd jobs reply that hold a pod in one of namespaces.
func liveJobs(data []byte, namespaces []string) ([]string, error) {
	var r struct {
		Jobs []slurmJob `json:"jobs"`
	}
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("slurmrestd jobs reply: %w", err)
	}
	var out []string
	for _, j := range r.Jobs {
		if !slices.Contains(j.JobState, "PENDING") && !slices.Contains(j.JobState, "RUNNING") {
			continue
		}
		var info struct {
			Pods []string `json:"pods"`
		}
		if json.Unmarshal([]byte(j.AdminComment), &info) != nil {
			continue // not a slurm-bridge job
		}
		if slices.ContainsFunc(info.Pods, func(p string) bool {
			ns, _, _ := strings.Cut(p, "/")
			return slices.Contains(namespaces, ns)
		}) {
			out = append(out, fmt.Sprintf("%d (%s)", j.JobID, strings.Join(j.JobState, ",")))
		}
	}
	return out, nil
}

// waitJobsGone polls every interval until Slurm has no pending or running
// job for a pod in s's namespaces. After timeout it names the jobs left,
// or the last error.
func waitJobsGone(ctx context.Context, get restGet, s scheduler.Scenario, interval, timeout time.Duration) error {
	var namespaces []string
	for _, t := range s.Tenants {
		namespaces = append(namespaces, t.Namespace)
	}
	var last error
	err := wait.PollUntilContextTimeout(ctx, interval, timeout, true, func(ctx context.Context) (bool, error) {
		data, err := get(ctx, jobsPath)
		if err != nil {
			last = fmt.Errorf("list Slurm jobs: %w", err)
			return false, nil
		}
		live, err := liveJobs(data, namespaces)
		if err != nil {
			last = err
			return false, nil
		}
		if len(live) > 0 {
			last = errors.New(strings.Join(live, ", "))
			return false, nil
		}
		return true, nil
	})
	if err == nil {
		return nil
	}
	if last == nil {
		last = err
	}
	return fmt.Errorf("slurm-bridge: probe %s left Slurm jobs after %s: %w", s.Probe, timeout, last)
}
