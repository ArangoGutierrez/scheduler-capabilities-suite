package e2e

import (
	"time"

	. "github.com/onsi/ginkgo/v2"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

var _ = register(50, backfillProbe)

func backfillProbe() {
	It("backfill", Label("backfill"), func(ctx SpecContext) {
		p := startProbe(ctx, verdict.CapBackfill)
		blockers := names("blocker", 3)
		for _, n := range blockers {
			p.submit(ctx, workload.Job{Name: n, Priority: workload.Low, Pods: 1, GPUsPerPod: 8, RunFor: 60 * time.Second})
		}
		p.mustAwait(ctx, "the three blockers run", func() bool {
			for _, n := range blockers {
				if p.running(ctx, n) < 1 {
					return false
				}
			}
			return true
		}, time.Minute)
		time.Sleep(time.Second) // scenario offset: the gang arrives at tb + 1 s
		p.submit(ctx, workload.Job{Name: "big", Priority: workload.Low, Pods: 4, GPUsPerPod: 8, RunFor: 20 * time.Second, Gang: true})
		shorts := names("short", 4)
		for _, n := range shorts {
			time.Sleep(time.Second) // scenario offset: short jobs arrive at tb + 2 .. tb + 5 s
			p.submit(ctx, workload.Job{Name: n, Priority: workload.Low, Pods: 1, GPUsPerPod: 8, RunFor: 25 * time.Second})
		}
		p.await(ctx, func() bool { return p.scheduled(ctx, "big") >= 4 }, 3*time.Minute)
		p.finish(ctx, verdict.CapBackfill, verdict.BackfillParams{BigJob: "big", BigPods: 4,
			BlockerJobs: blockers, SmallJobs: shorts, ToleranceSec: 5})
	}, SpecTimeout(10*time.Minute))
}
