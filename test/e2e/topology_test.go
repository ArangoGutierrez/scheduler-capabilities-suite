package e2e

import (
	"time"

	. "github.com/onsi/ginkgo/v2"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/fleet"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

var _ = register(60, topologyProbe)

func topologyProbe() {
	It("topology", Label("topology"), func(ctx SpecContext) {
		p := startProbe(ctx, verdict.CapTopology)
		p.submit(ctx, workload.Job{Name: "filler", Priority: workload.Low, Pods: 1, GPUsPerPod: 8, RunFor: 300 * time.Second, NodeName: "n1"})
		p.mustAwait(ctx, "the filler runs on n1", func() bool { return p.running(ctx, "filler") >= 1 }, time.Minute)
		attempts := names("topo", 3)
		for _, n := range attempts {
			j := workload.Job{Name: n, Priority: workload.Low, Pods: 2, GPUsPerPod: 8, RunFor: 20 * time.Second, Gang: true, SameRack: true}
			p.submit(ctx, j)
			p.await(ctx, func() bool { return p.scheduled(ctx, n) >= 2 }, 45*time.Second)
			p.remove(ctx, j)
		}
		p.finish(ctx, verdict.CapTopology, verdict.TopologyParams{AttemptJobs: attempts, PodsPerJob: 2, RackOf: fleet.Default().RackOf()})
	}, SpecTimeout(8*time.Minute))
}
