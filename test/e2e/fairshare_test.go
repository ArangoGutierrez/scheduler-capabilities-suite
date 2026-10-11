package e2e

import (
	"time"

	. "github.com/onsi/ginkgo/v2"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/fleet"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

var _ = register(40, fairShareProbe)

func fairShareProbe() {
	It("fair-share", Label("fair-share"), func(ctx SpecContext) {
		p := startScenario(ctx, scheduler.Scenario{Probe: verdict.CapFairShare, AllowPreemption: true,
			Tenants: []scheduler.Tenant{{Name: "a", EntitlementGPUs: 16}, {Name: "b", EntitlementGPUs: 16}}})
		one := func(name, tenant string) workload.Job {
			return workload.Job{Name: name, Tenant: tenant, Priority: workload.Low, Pods: 1, GPUsPerPod: 8, RunFor: 60 * time.Second}
		}
		for _, n := range names("a", 8) {
			p.submit(ctx, one(n, "a"))
		}
		time.Sleep(10 * time.Second) // scenario offset: B submits 10 s after A
		bSubmit := time.Now()
		for _, n := range names("b", 4) {
			p.submit(ctx, one(n, "b"))
		}
		// the rule reads B's GPUs 65 s after B's first submission; wait 5 s past it
		time.Sleep(time.Until(bSubmit.Add(70 * time.Second)))
		p.finish(ctx, verdict.CapFairShare, verdict.FairShareParams{TenantB: "b", EntitlementGPUs: 16,
			JobLengthSec: 60, ToleranceSec: 5, CapacityGPUs: int64(fleet.Default().GPUs()), WindowSec: 10})
	}, SpecTimeout(8*time.Minute))
}
