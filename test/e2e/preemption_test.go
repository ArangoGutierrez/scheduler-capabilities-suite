package e2e

import (
	"time"

	. "github.com/onsi/ginkgo/v2"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

var _ = register(30, preemptionProbe)

func preemptionProbe() {
	It("preemption", Label("preemption"), func(ctx SpecContext) {
		p := startScenario(ctx, scheduler.Scenario{Probe: verdict.CapPreemption, AllowPreemption: true})
		victim := workload.Job{Name: "victim", Priority: workload.Low, Pods: 4, GPUsPerPod: 8, RunFor: 300 * time.Second, Gang: true, EvictWhole: true}
		p.submit(ctx, victim)
		p.mustAwait(ctx, "the victim gang runs on the whole fleet", func() bool { return p.running(ctx, "victim") >= 4 }, 2*time.Minute)
		pause(ctx, 2*time.Second) // scenario offset
		p.submit(ctx, workload.Job{Name: "high", Priority: workload.High, Preempting: true, Pods: 2, GPUsPerPod: 8, RunFor: 30 * time.Second})
		if p.await(ctx, func() bool { return p.scheduled(ctx, "high") >= 2 }, 90*time.Second) {
			// the rule judges the victims 5 s after the high-priority job's last pod is scheduled
			pause(ctx, 6*time.Second)
		}
		p.finish(ctx, verdict.CapPreemption, verdict.PreemptionParams{HighJob: "high", HighPods: 2, VictimJob: "victim", GraceSec: 5})
	}, SpecTimeout(8*time.Minute))
}
