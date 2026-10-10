package verdict_test

import (
	"math"
	"time"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
)

// t0 is the fixtures' time origin; at(s) is t0 plus s seconds.
var t0 = time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

func at(s float64) time.Time {
	return t0.Add(time.Duration(math.Round(s * float64(time.Second))))
}

// pod describes one fixture pod: its job, tenant and GPUs, and the second
// (after t0) of each transition; a negative second means "never".
type pod struct {
	name, job, tenant                string
	gpus                             int64
	node                             string
	created, scheduled, running, end float64
	endBy                            events.Transition
}

// job describes one fixture Job object: its tenant, total GPUs, and the
// second it was created and ended (negative: never ended).
type job struct {
	name, tenant string
	gpus         int64
	created, end float64
}

// fixture turns pods into a probe's events, ordered by time, with seq
// counting from 1.
func fixture(probe string, pods ...pod) []events.Event {
	return fixtureJobs(probe, nil, pods...)
}

// fixtureJobs is fixture with Job events as well, as the recorder writes
// them for every job (under Kueue, a queued job has a Job and no pods).
func fixtureJobs(probe string, jobs []job, pods ...pod) []events.Event {
	type item struct {
		at float64
		p  pod
		j  *job
		tr events.Transition
	}
	var items []item
	for i := range jobs {
		j := &jobs[i]
		items = append(items, item{at: j.created, j: j, tr: events.Created})
		if j.end >= 0 {
			items = append(items, item{at: j.end, j: j, tr: events.Succeeded})
		}
	}
	for _, p := range pods {
		items = append(items, item{at: p.created, p: p, tr: events.Created})
		if p.scheduled >= 0 {
			items = append(items, item{at: p.scheduled, p: p, tr: events.Scheduled})
		}
		if p.running >= 0 {
			items = append(items, item{at: p.running, p: p, tr: events.Running})
		}
		if p.end >= 0 {
			items = append(items, item{at: p.end, p: p, tr: p.endBy})
		}
	}
	// stable insertion sort by time keeps same-time items in input order
	for i := 1; i < len(items); i++ {
		for j := i; j > 0 && items[j].at < items[j-1].at; j-- {
			items[j], items[j-1] = items[j-1], items[j]
		}
	}
	out := make([]events.Event, 0, len(items))
	for i, it := range items {
		if it.j != nil {
			out = append(out, events.Event{
				Schema: 1, Seq: int64(i + 1), RunID: "fixture", ObservedAt: at(it.at),
				Kind: "Job", Namespace: "scs-" + probe, Name: it.j.name, UID: "uid-job-" + it.j.name,
				Probe: probe, Job: it.j.name, Tenant: it.j.tenant, GPUs: it.j.gpus, Transition: it.tr,
			})
			continue
		}
		e := events.Event{
			Schema: 1, Seq: int64(i + 1), RunID: "fixture", ObservedAt: at(it.at),
			Kind: "Pod", Namespace: "scs-" + probe, Name: it.p.name, UID: "uid-" + it.p.name,
			Probe: probe, Job: it.p.job, Tenant: it.p.tenant, GPUs: it.p.gpus, Transition: it.tr,
		}
		if it.tr == events.Scheduled {
			e.Node = it.p.node
		}
		out = append(out, e)
	}
	return out
}

// done is a pod that ran to completion on node.
func done(name, job string, node string, created, scheduled, running, end float64) pod {
	return pod{name: name, job: job, tenant: "t1", gpus: 8, node: node, created: created,
		scheduled: scheduled, running: running, end: end, endBy: events.Succeeded}
}

// pending is a pod that was created and never scheduled.
func pending(name, job string, created float64) pod {
	return pod{name: name, job: job, tenant: "t1", gpus: 8, created: created,
		scheduled: -1, running: -1, end: -1}
}
