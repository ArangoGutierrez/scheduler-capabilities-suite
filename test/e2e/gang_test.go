package e2e

import (
	"time"

	. "github.com/onsi/ginkgo/v2"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

// Scenario numbers are the spec's section 5. Waits that end in a verdict use
// await (a timeout is evidence); waits for a scenario to take shape use
// mustAwait (a timeout is a harness failure). A waiting gang can start up
// to about 17 s after capacity frees, because of the scheduler's gang retry
// backoff (R1, V2), so verdict waits leave room for it.

var _ = register(10, gangProbes)

func gangProbes() {
	gang := func(name string, isGang bool) workload.Job {
		return workload.Job{Name: name, Priority: workload.Low, Pods: 3, GPUsPerPod: 8, RunFor: 20 * time.Second, Gang: isGang}
	}
	params := verdict.GangParams{Jobs: []string{"gang-a", "gang-b"}, Size: 3, GraceSec: 5}

	It("gang", Label("gang"), func(ctx SpecContext) {
		p := startProbe(ctx, verdict.CapGang)
		p.submit(ctx, gang("gang-a", true), gang("gang-b", true))
		p.await(ctx, func() bool { return p.succeeded(ctx, "gang-a") >= 3 && p.succeeded(ctx, "gang-b") >= 3 }, 150*time.Second)
		p.finish(ctx, verdict.CapGang, params)
	}, SpecTimeout(8*time.Minute))

	It("gang-control", Label("gang-control"), func(ctx SpecContext) {
		if env.profile.Name() != "vanilla" {
			Skip("the gang control runs on the vanilla profile only")
		}
		p := startProbe(ctx, verdict.CapGangControl)
		p.submit(ctx, gang("gang-a", false), gang("gang-b", false))
		p.await(ctx, func() bool { return p.succeeded(ctx, "gang-a") >= 3 && p.succeeded(ctx, "gang-b") >= 3 }, 150*time.Second)
		p.finish(ctx, verdict.CapGangControl, params)
	}, SpecTimeout(8*time.Minute))
}
