// Package workload describes the suite's jobs without naming a scheduler,
// and builds the batch/v1 Job every profile starts from.
package workload

import (
	"errors"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/fleet"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/labels"
)

// Priority is a job's priority level; profiles map it to their own objects.
type Priority string

// The two priority levels.
const (
	Low  Priority = "low"
	High Priority = "high"
)

// FakeImage is never pulled: kwok nodes do not run containers.
const FakeImage = "registry.invalid/scs/fake:0"

// BackoffLimit lets a Job survive evictions. A preempted pod counts as a Job
// failure; with a limit of 0 the Job controller fails the Job on the first
// eviction and deletes the surviving pods, which would make any scheduler
// look as if it evicted the whole workload (R1, V4).
const BackoffLimit = 10

// Job is one batch Job, described without naming a scheduler.
type Job struct {
	Name      string
	Namespace string
	Probe     string
	Tenant    string
	Priority  Priority
	// Preempting lets a High job evict lower-priority work.
	Preempting bool
	Pods       int
	GPUsPerPod int
	RunFor     time.Duration
	// Gang: all pods are placed together or not at all.
	Gang bool
	// EvictWhole: if preempted, the gang is evicted as a whole.
	EvictWhole bool
	// SameRack: all pods sit in one rack.
	SameRack bool
	// NodeName pins every pod to one node.
	NodeName string
}

// Validate returns the first inconsistency in j.
func (j Job) Validate() error {
	switch {
	case j.Name == "" || j.Namespace == "" || j.Probe == "" || j.Tenant == "":
		return errors.New("workload: name, namespace, probe and tenant must all be set")
	case j.Priority != Low && j.Priority != High:
		return fmt.Errorf("workload: %s: priority %q, want low or high", j.Name, j.Priority)
	case j.Pods < 1 || j.GPUsPerPod < 0 || j.RunFor <= 0:
		return fmt.Errorf("workload: %s: pods must be 1 or more, gpus not negative, run time positive", j.Name)
	case j.Preempting && j.Priority != High:
		return fmt.Errorf("workload: %s: only a high-priority job may preempt", j.Name)
	case (j.EvictWhole || j.SameRack) && !j.Gang:
		return fmt.Errorf("workload: %s: evict-whole and same-rack apply to gangs only", j.Name)
	}
	return nil
}

// Labels are the suite's labels for j's Job and pods.
func (j Job) Labels() map[string]string {
	return map[string]string{
		labels.PartOf: labels.PartOfValue,
		labels.Probe:  j.Probe,
		labels.Tenant: j.Tenant,
		labels.Job:    j.Name,
	}
}

// BatchJob builds the Job every profile starts from: parallelism and
// completions equal to Pods, pods that tolerate and select the kwok fleet,
// and kwok stage annotations that make each pod start at once and run
// exactly RunFor. kwok's jitter-delay must equal the delay: kwok draws the
// delay uniformly from [delay, jitter), and a jitter at or below the delay
// replaces it (R1, V1).
func (j Job) BatchJob() *batchv1.Job {
	n := int32(j.Pods)
	backoff := int32(BackoffLimit)
	grace := int64(0)
	run := j.RunFor.String()
	sel := map[string]string{"type": "kwok"}
	if j.NodeName != "" {
		sel["kubernetes.io/hostname"] = j.NodeName
	}
	limits := corev1.ResourceList{}
	if j.GPUsPerPod > 0 {
		limits[fleet.GPUResource] = resource.MustParse(fmt.Sprint(j.GPUsPerPod))
	}
	return &batchv1.Job{
		TypeMeta:   metav1.TypeMeta{APIVersion: "batch/v1", Kind: "Job"},
		ObjectMeta: metav1.ObjectMeta{Name: j.Name, Namespace: j.Namespace, Labels: j.Labels()},
		Spec: batchv1.JobSpec{
			Parallelism:  &n,
			Completions:  &n,
			BackoffLimit: &backoff,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: j.Labels(),
					Annotations: map[string]string{
						"pod-create.stage.kwok.x-k8s.io/delay":          "0s",
						"pod-create.stage.kwok.x-k8s.io/jitter-delay":   "0s",
						"pod-ready.stage.kwok.x-k8s.io/delay":           "0s",
						"pod-ready.stage.kwok.x-k8s.io/jitter-delay":    "0s",
						"pod-complete.stage.kwok.x-k8s.io/delay":        run,
						"pod-complete.stage.kwok.x-k8s.io/jitter-delay": run,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy:                 corev1.RestartPolicyNever,
					TerminationGracePeriodSeconds: &grace,
					NodeSelector:                  sel,
					Tolerations: []corev1.Toleration{{
						Key: "kwok.x-k8s.io/node", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule,
					}},
					Containers: []corev1.Container{{
						Name: "main", Image: FakeImage,
						Resources: corev1.ResourceRequirements{Limits: limits},
					}},
				},
			},
		},
	}
}
