package e2e

import (
	"time"

	. "github.com/onsi/ginkgo/v2"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

var _ = register(20, priorityProbe)

func priorityProbe() {
	It("priority", Label("priority"), func(ctx SpecContext) {
		p := startProbe(ctx, verdict.CapPriority)
		one := func(name string, pr workload.Priority, run time.Duration) workload.Job {
			return workload.Job{Name: name, Priority: pr, Pods: 1, GPUsPerPod: 8, RunFor: run}
		}
		fillers := []workload.Job{one("fill-30", workload.Low, 30*time.Second), one("fill-40", workload.Low, 40*time.Second),
			one("fill-50", workload.Low, 50*time.Second), one("fill-60", workload.Low, 60*time.Second)}
		p.submit(ctx, fillers...)
		p.mustAwait(ctx, "the four fillers run", func() bool {
			for _, f := range fillers {
				if p.running(ctx, f.Name) < 1 {
					return false
				}
			}
			return true
		}, time.Minute)
		queued := names("low", 4)
		for _, n := range queued {
			p.submit(ctx, one(n, workload.Low, 30*time.Second))
		}
		pause(ctx, 2*time.Second) // scenario offset: the high-priority pod arrives 2 s after the queued pods
		p.submit(ctx, one("high", workload.High, 30*time.Second))
		p.await(ctx, func() bool { return p.scheduled(ctx, "high") >= 1 }, 150*time.Second)
		p.finish(ctx, verdict.CapPriority, verdict.PriorityParams{HighJob: "high", QueuedJobs: queued})
	}, SpecTimeout(8*time.Minute))
}
