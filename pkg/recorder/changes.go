package recorder

import (
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
)

// change is one transition found by comparing two versions of an object.
type change struct {
	tr        events.Transition
	apiTime   *time.Time
	precision string
	node      string
	reason    string
	message   string
}

func secTime(t metav1.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	v := t.UTC()
	return &v
}

func condition(p *corev1.Pod, t corev1.PodConditionType) *corev1.PodCondition {
	for i := range p.Status.Conditions {
		if p.Status.Conditions[i].Type == t {
			return &p.Status.Conditions[i]
		}
	}
	return nil
}

// podChanges returns, in order, the transitions between old and cur. A nil
// old means cur was just added; an already-progressed pod then yields every
// transition it has passed (an informer's initial list).
func podChanges(old, cur *corev1.Pod) []change {
	var out []change
	if old == nil {
		out = append(out, change{tr: events.Created, apiTime: secTime(cur.CreationTimestamp), precision: "s"})
		old = &corev1.Pod{}
	}
	if old.Spec.NodeName == "" && cur.Spec.NodeName != "" {
		c := change{tr: events.Scheduled, node: cur.Spec.NodeName}
		if sc := condition(cur, corev1.PodScheduled); sc != nil && sc.Status == corev1.ConditionTrue {
			c.apiTime, c.precision = secTime(sc.LastTransitionTime), "s"
		}
		out = append(out, c)
	}
	if nc := condition(cur, corev1.PodScheduled); nc != nil && nc.Status == corev1.ConditionFalse && nc.Reason == corev1.PodReasonUnschedulable {
		oc := condition(old, corev1.PodScheduled)
		if oc == nil || oc.Status != nc.Status || oc.Reason != nc.Reason {
			out = append(out, change{tr: events.Unschedulable, apiTime: secTime(nc.LastTransitionTime), precision: "s", reason: nc.Reason, message: nc.Message})
		}
	}
	if old.Status.Phase != corev1.PodRunning && cur.Status.Phase == corev1.PodRunning {
		c := change{tr: events.Running}
		if len(cur.Status.ContainerStatuses) > 0 && cur.Status.ContainerStatuses[0].State.Running != nil {
			c.apiTime, c.precision = secTime(cur.Status.ContainerStatuses[0].State.Running.StartedAt), "s"
		} else if cur.Status.StartTime != nil {
			c.apiTime, c.precision = secTime(*cur.Status.StartTime), "s"
		}
		out = append(out, c)
	}
	if old.Status.Phase != cur.Status.Phase && (cur.Status.Phase == corev1.PodSucceeded || cur.Status.Phase == corev1.PodFailed) {
		c := change{tr: events.Succeeded, reason: cur.Status.Reason}
		if cur.Status.Phase == corev1.PodFailed {
			c.tr = events.Failed
		}
		if len(cur.Status.ContainerStatuses) > 0 && cur.Status.ContainerStatuses[0].State.Terminated != nil {
			c.apiTime, c.precision = secTime(cur.Status.ContainerStatuses[0].State.Terminated.FinishedAt), "s"
		}
		out = append(out, c)
	}
	if nd := condition(cur, corev1.DisruptionTarget); nd != nil && nd.Status == corev1.ConditionTrue {
		if od := condition(old, corev1.DisruptionTarget); od == nil || od.Status != corev1.ConditionTrue {
			c := change{tr: events.Evicted, apiTime: secTime(nd.LastTransitionTime), precision: "s", reason: nd.Reason, message: nd.Message}
			if nd.Reason == corev1.PodReasonPreemptionByScheduler {
				c.tr = events.Preempted
			}
			out = append(out, c)
		}
	}
	for i := range out {
		if out[i].apiTime == nil {
			out[i].precision = ""
		}
	}
	return out
}

// eventReasons maps the Event reasons the recorder keeps to transitions.
// Every other reason is skipped: the cluster logs keep the full record.
var eventReasons = map[string]events.Transition{
	"Scheduled":        events.Scheduled,
	"FailedScheduling": events.Unschedulable,
	"Preempted":        events.Preempted,
	"QuotaReserved":    events.QuotaReserved,
	"Admitted":         events.Admitted,
	"Evicted":          events.Evicted,
}

// eventChange returns the transition an events.k8s.io Event records, or
// false when the recorder skips it. EventTime has microseconds; older
// events fall back to the one-second deprecated timestamps.
func eventChange(ev *eventsv1.Event) (change, bool) {
	tr, ok := eventReasons[ev.Reason]
	if !ok {
		return change{}, false
	}
	c := change{tr: tr, reason: ev.Reason, message: ev.Note}
	switch {
	case !ev.EventTime.IsZero():
		t := ev.EventTime.UTC()
		c.apiTime, c.precision = &t, "us"
	case !ev.DeprecatedLastTimestamp.IsZero():
		c.apiTime, c.precision = secTime(ev.DeprecatedLastTimestamp), "s"
	}
	return c, true
}

// jobCondition reports whether condition t is True on j, with its time.
func jobCondition(j *batchv1.Job, t batchv1.JobConditionType) (bool, metav1.Time) {
	if j == nil {
		return false, metav1.Time{}
	}
	for _, c := range j.Status.Conditions {
		if c.Type == t && c.Status == corev1.ConditionTrue {
			return true, c.LastTransitionTime
		}
	}
	return false, metav1.Time{}
}

// jobChanges returns a Job's transitions between old and cur (old nil on
// add): created, then succeeded or failed when the Complete or Failed
// condition turns True. A Job's creation is its submission: under Kueue a
// queued Job has no pods yet.
func jobChanges(old, cur *batchv1.Job) []change {
	var out []change
	if old == nil {
		out = append(out, change{tr: events.Created, apiTime: secTime(cur.CreationTimestamp), precision: "s"})
	}
	for _, x := range []struct {
		t  batchv1.JobConditionType
		tr events.Transition
	}{{batchv1.JobComplete, events.Succeeded}, {batchv1.JobFailed, events.Failed}} {
		now, at := jobCondition(cur, x.t)
		was, _ := jobCondition(old, x.t)
		if now && !was {
			c := change{tr: x.tr, apiTime: secTime(at)}
			if c.apiTime != nil {
				c.precision = "s"
			}
			out = append(out, c)
		}
	}
	return out
}
