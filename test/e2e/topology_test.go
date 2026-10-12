package e2e

import (
	"time"

	. "github.com/onsi/ginkgo/v2"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/fleet"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

var _ = register(60, topologyProbes)

// topologyProbes: with one rack-a node busy, only rack B can hold a 2-pod
// gang whole. The control drops the one-rack constraint, to show what the
// scheduler does without it.
func topologyProbes() {
	run := func(ctx SpecContext, capability string, sameRack bool) {
		f := fleet.Default()
		busy := f.Nodes[0].Name // its rack has two nodes, so with it busy only the other rack takes the gang whole
		p := startProbe(ctx, capability)
		p.submit(ctx, workload.Job{Name: "filler", Priority: workload.Low, Pods: 1, GPUsPerPod: 8, RunFor: 300 * time.Second, NodeName: busy})
		p.mustAwait(ctx, "the filler runs on "+busy, func() bool { return p.running(ctx, "filler") >= 1 }, time.Minute)
		attempts := names("topo", 3)
		for _, n := range attempts {
			j := workload.Job{Name: n, Priority: workload.Low, Pods: 2, GPUsPerPod: 8, RunFor: 20 * time.Second, Gang: true, SameRack: sameRack}
			p.submit(ctx, j)
			p.await(ctx, func() bool { return p.scheduled(ctx, n) >= 2 }, 45*time.Second)
			p.remove(ctx, j)
		}
		p.finish(ctx, capability, verdict.TopologyParams{AttemptJobs: attempts, PodsPerJob: 2, RackOf: f.RackOf()})
	}

	It("topology", Label("topology"), func(ctx SpecContext) {
		run(ctx, verdict.CapTopology, true)
	}, SpecTimeout(8*time.Minute))

	It("topology-control", Label("topology-control"), func(ctx SpecContext) {
		if env.profile.Name() != "vanilla" {
			Skip("the topology control runs on the vanilla profile only")
		}
		run(ctx, verdict.CapTopologyControl, false)
	}, SpecTimeout(8*time.Minute))
}
