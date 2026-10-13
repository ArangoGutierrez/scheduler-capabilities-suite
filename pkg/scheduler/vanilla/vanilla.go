// Package vanilla is the baseline profile: kube-scheduler 1.37 with the
// Workload API. Gangs are standalone PodGroups (scheduling.k8s.io/v1beta1)
// named from the pod template's schedulingGroup; priority is a
// PriorityClass; tenants are namespaces with no policy (R1, V2 to V6).
package vanilla

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/cluster"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/labels"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

// PriorityClasses the profile installs. scs-high may not preempt (the
// priority probe); scs-high-preempt may (the preemption probe).
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
---
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata: {name: scs-high-preempt, labels: {app.kubernetes.io/part-of: scheduler-capabilities-suite}}
value: 1000
preemptionPolicy: PreemptLowerPriority
`

var podGroupGVR = schema.GroupVersionResource{Group: "scheduling.k8s.io", Version: "v1beta1", Resource: "podgroups"}

// Profile is the vanilla profile.
type Profile struct{}

// New returns the vanilla profile.
func New() *Profile { return &Profile{} }

// Name implements scheduler.Profile.
func (*Profile) Name() string { return "vanilla" }

// ClaimsFile implements scheduler.Profile.
func (*Profile) ClaimsFile() string { return "claims/vanilla.yaml" }

// WorkloadGVR implements scheduler.Profile: vanilla has no Workload to watch.
func (*Profile) WorkloadGVR() *schema.GroupVersionResource { return nil }

// Install creates the PriorityClasses.
func (*Profile) Install(ctx context.Context, c *cluster.Cluster) (map[string]string, error) {
	if _, err := c.Applier.Apply(ctx, []byte(priorityClasses), time.Minute); err != nil {
		return nil, err
	}
	return map[string]string{
		"scheduler":        "kube-scheduler from " + cluster.NodeImage,
		"priority_classes": "scs-low=100 Never, scs-high=1000 Never, scs-high-preempt=1000 PreemptLowerPriority",
		"gangs":            "standalone PodGroup scheduling.k8s.io/v1beta1, pod spec.schedulingGroup",
	}, nil
}

// Prepare creates the probe's namespaces; vanilla has no per-probe objects.
func (*Profile) Prepare(ctx context.Context, c *cluster.Cluster, s scheduler.Scenario) error {
	return scheduler.CreateNamespaces(ctx, c.Clientset, s)
}

// Cleanup implements scheduler.Profile; there is nothing per probe to remove.
func (*Profile) Cleanup(context.Context, *cluster.Cluster, scheduler.Scenario) error { return nil }

// PriorityClassName maps a job's priority to the installed class.
func PriorityClassName(j workload.Job) string {
	switch {
	case j.Priority == workload.High && j.Preempting:
		return "scs-high-preempt"
	case j.Priority == workload.High:
		return "scs-high"
	}
	return "scs-low"
}

// PodGroupName is the PodGroup a gang job's pods name.
func PodGroupName(j workload.Job) string { return "pg-" + j.Name }

// PodGroup is the gang object for j: minCount equal to its pods, the job's
// priority class, disruption mode all when it must be evicted whole, and a
// rack topology constraint when it must sit in one rack.
func PodGroup(j workload.Job) map[string]any {
	spec := map[string]any{
		"schedulingPolicy":  map[string]any{"gang": map[string]any{"minCount": int64(j.Pods)}},
		"priorityClassName": PriorityClassName(j),
	}
	if j.EvictWhole {
		spec["disruptionMode"] = map[string]any{"all": map[string]any{}}
	}
	if j.SameRack {
		spec["schedulingConstraints"] = map[string]any{"topology": []any{map[string]any{"key": labels.Rack}}}
	}
	lbl := map[string]any{}
	for k, v := range j.Labels() {
		lbl[k] = v
	}
	return map[string]any{
		"apiVersion": "scheduling.k8s.io/v1beta1",
		"kind":       "PodGroup",
		"metadata":   map[string]any{"name": PodGroupName(j), "namespace": j.Namespace, "labels": lbl},
		"spec":       spec,
	}
}

// Job is j's batch Job with the priority class and, for a gang, the
// schedulingGroup that names its PodGroup.
func Job(j workload.Job) *batchv1.Job {
	b := j.BatchJob()
	b.Spec.Template.Spec.PriorityClassName = PriorityClassName(j)
	if j.Gang {
		b.Spec.Template.Spec.SchedulingGroup = &corev1.PodSchedulingGroup{PodGroupName: ptr(PodGroupName(j))}
	}
	return b
}

func ptr[T any](v T) *T { return &v }

// Submit creates the PodGroup first (the scheduler holds a gang's pods until
// it exists), then the Job.
func (*Profile) Submit(ctx context.Context, c *cluster.Cluster, j workload.Job) error {
	if err := j.Validate(); err != nil {
		return err
	}
	if j.Gang {
		data, err := json.Marshal(PodGroup(j))
		if err != nil {
			return err
		}
		if _, err := c.Applier.Apply(ctx, data, time.Minute); err != nil {
			return fmt.Errorf("podgroup for %s: %w", j.Name, err)
		}
	}
	_, err := c.Clientset.BatchV1().Jobs(j.Namespace).Create(ctx, Job(j), metav1.CreateOptions{})
	return err
}

// Remove deletes the Job (and its pods) and the PodGroup.
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
