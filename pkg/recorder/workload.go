package recorder

import (
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
)

// workloadConditions maps the Kueue Workload conditions the recorder keeps
// to transitions, in the order they are reported when several turn True in
// one update.
var workloadConditions = []struct {
	typ string
	tr  events.Transition
}{
	{"QuotaReserved", events.QuotaReserved},
	{"Admitted", events.Admitted},
	{"Preempted", events.Preempted},
	{"Evicted", events.Evicted},
	{"Requeued", events.Requeued},
}

// condTrue reports whether condition typ is True on u, with its transition
// time, reason and message.
func condTrue(u *unstructured.Unstructured, typ string) (bool, *time.Time, string, string) {
	if u == nil {
		return false, nil, "", ""
	}
	conds, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	for _, c := range conds {
		m, ok := c.(map[string]any)
		if !ok || m["type"] != typ {
			continue
		}
		if m["status"] != "True" {
			return false, nil, "", ""
		}
		var at *time.Time
		if s, ok := m["lastTransitionTime"].(string); ok {
			if t, err := time.Parse(time.RFC3339, s); err == nil {
				t = t.UTC()
				at = &t
			}
		}
		reason, _ := m["reason"].(string)
		msg, _ := m["message"].(string)
		return true, at, reason, msg
	}
	return false, nil, "", ""
}

// workloadChanges returns the transitions between old and cur; a nil old
// means cur was just added.
func workloadChanges(old, cur *unstructured.Unstructured) []change {
	var out []change
	if old == nil {
		ct := cur.GetCreationTimestamp()
		out = append(out, change{tr: events.Created, apiTime: secTime(ct), precision: "s"})
	}
	for _, wc := range workloadConditions {
		now, at, reason, msg := condTrue(cur, wc.typ)
		was, _, _, _ := condTrue(old, wc.typ)
		if now && !was {
			c := change{tr: wc.tr, apiTime: at, reason: reason, message: msg}
			if at != nil {
				c.precision = "s"
			}
			out = append(out, c)
		}
	}
	return out
}

func (r *Recorder) workloadBase(u *unstructured.Unstructured) (events.Event, bool) {
	info, ok := r.nsInfo(u.GetNamespace())
	if !ok {
		return events.Event{}, false
	}
	e := events.Event{Kind: "Workload", Namespace: u.GetNamespace(), Name: u.GetName(), UID: string(u.GetUID()),
		Probe: info.probe, Tenant: info.tenant}
	if refs := u.GetOwnerReferences(); len(refs) > 0 {
		e.Job = refs[0].Name
	}
	if p, ok, _ := unstructured.NestedInt64(u.Object, "spec", "priority"); ok {
		e.Priority = int32(p)
	}
	return e, true
}

func (r *Recorder) workload(old, cur *unstructured.Unstructured) {
	base, ok := r.workloadBase(cur)
	if !ok {
		return
	}
	for _, c := range workloadChanges(old, cur) {
		r.emit(base, c)
	}
}

func (r *Recorder) workloadDeleted(o any) {
	if d, ok := o.(cache.DeletedFinalStateUnknown); ok {
		o = d.Obj
	}
	u, ok := o.(*unstructured.Unstructured)
	if !ok {
		return
	}
	if base, ok := r.workloadBase(u); ok {
		r.emit(base, change{tr: events.Deleted})
	}
}
