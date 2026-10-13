package verdict

import (
	"time"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
)

// Job is one job's life as recorded from its batch/v1 Job object. Under
// Kueue a queued job has no pods (Kueue suspends it), so a job's submission
// is its Job's creation, not its first pod's.
type Job struct {
	Name    string
	Tenant  string
	GPUs    int64
	Created time.Time
	End     time.Time
	Seqs    []int64
}

// Timeline is one probe's evidence up to the end of its window.
type Timeline struct {
	Pods []*Pod
	Jobs map[string]*Job
	End  time.Time
}

// Build folds a probe's events, observed no later than until, into its
// pods and jobs. A job seen only through its pods (no Job events, as in
// hand-built fixtures) gets a Job derived from them: created with its first
// pod, its pods' GPUs, and ended with its last pod when all of them ended.
func Build(evs []events.Event, probe string, until time.Time) Timeline {
	t := Timeline{Pods: Pods(evs, probe, until), Jobs: map[string]*Job{}, End: until}
	for _, e := range evs {
		if e.Kind != "Job" || e.Probe != probe || e.ObservedAt.After(until) {
			continue
		}
		j, ok := t.Jobs[e.Name]
		if !ok {
			j = &Job{Name: e.Name, Tenant: e.Tenant, GPUs: e.GPUs}
			t.Jobs[e.Name] = j
		}
		j.Seqs = append(j.Seqs, e.Seq)
		switch e.Transition {
		case events.Created:
			setOnce(&j.Created, e.ObservedAt)
		case events.Succeeded, events.Failed, events.Deleted:
			setOnce(&j.End, e.ObservedAt)
		}
	}
	derived := map[string]*Job{}
	for _, p := range t.Pods {
		if _, recorded := t.Jobs[p.Job]; recorded {
			continue
		}
		j, ok := derived[p.Job]
		if !ok {
			j = &Job{Name: p.Job, Tenant: p.Tenant}
			derived[p.Job] = j
		}
		j.GPUs += p.GPUs
		if !p.Created.IsZero() && (j.Created.IsZero() || p.Created.Before(j.Created)) {
			j.Created = p.Created
		}
	}
	for name, j := range derived {
		allEnded := true
		for _, p := range ofJob(t.Pods, name) {
			if p.End.IsZero() {
				allEnded = false
			} else if p.End.After(j.End) {
				j.End = p.End
			}
		}
		if !allEnded {
			j.End = time.Time{}
		}
		t.Jobs[name] = j
	}
	return t
}

// job returns a job's pods, and whether the job was seen at all (as a Job
// or through its pods). A job seen without pods was never admitted.
func (t Timeline) job(name string) ([]*Pod, bool) {
	ps := ofJob(t.Pods, name)
	_, seen := t.Jobs[name]
	return ps, seen || len(ps) > 0
}

// submitted is when a job was submitted: its Job's creation.
func (t Timeline) submitted(name string) time.Time {
	if j, ok := t.Jobs[name]; ok {
		return j.Created
	}
	return time.Time{}
}

// jobSeqs returns the Job events' sequence numbers of the named jobs, as
// pseudo-pods so evidence() can merge them with pod evidence.
func (t Timeline) jobSeqs(names ...string) []*Pod {
	var out []*Pod
	for _, n := range names {
		if j, ok := t.Jobs[n]; ok && len(j.Seqs) > 0 {
			out = append(out, &Pod{Seqs: j.Seqs})
		}
	}
	return out
}
