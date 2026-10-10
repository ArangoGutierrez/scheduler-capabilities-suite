package verdict_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
)

// tenantPod is a pod of tenant t with g GPUs; negative seconds mean never.
func tenantPod(name, t string, g int64, created, running, end float64) pod {
	p := pod{name: name, job: name, tenant: t, gpus: g, node: "n1", created: created,
		scheduled: running, running: running, end: end, endBy: events.Succeeded}
	if end < 0 {
		p.endBy = ""
	}
	return p
}

var _ = Describe("FairShare", func() {
	params := verdict.FairShareParams{TenantB: "b", EntitlementGPUs: 16, JobLengthSec: 60,
		ToleranceSec: 5, CapacityGPUs: 32, WindowSec: 10}

	// aPods: tenant A's four running pods (0 to 60 s) and four queued pods
	// that start at the given second (negative: never).
	aPods := func(queuedStart ...float64) []pod {
		var out []pod
		for _, n := range []string{"a0", "a1", "a2", "a3"} {
			out = append(out, tenantPod(n, "a", 8, 0, 0.5, 60))
		}
		for i, s := range queuedStart {
			n := []string{"a4", "a5", "a6", "a7"}[i]
			end := -1.0
			if s >= 0 {
				end = s + 60
			}
			out = append(out, tenantPod(n, "a", 8, 0, s, end))
		}
		return out
	}
	bPods := func(starts ...float64) []pod {
		var out []pod
		for i, s := range starts {
			n := []string{"b0", "b1", "b2", "b3"}[i]
			end := -1.0
			if s >= 0 {
				end = s + 60
			}
			out = append(out, tenantPod(n, "b", 8, 10, s, end))
		}
		return out
	}

	It("is supported when B reaches its entitlement within one job length", func() {
		tl := verdict.Build(fixture("fair-share", append(aPods(60.5, 60.5, 120.5, 120.5),
			bPods(60.5, 60.5, 120.5, 120.5)...)...), "fair-share", at(200))
		v, err := verdict.FairShare(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Supported))
		Expect(v.Reason).To(Equal("tenant b had 16 GPUs running within 65 s of submitting; its entitlement is 16"))
		Expect(v.Measurements["b_max_gpus_by_check"]).To(Equal(16.0))
		Expect(v.Measurements["check_offset_s"]).To(Equal(65.0))
	})

	It("is partial when B gets some but not all of its entitlement", func() {
		tl := verdict.Build(fixture("fair-share", append(aPods(60.5, 60.5, 60.5, 120.5),
			bPods(60.5, 120.5, 120.5, 180.5)...)...), "fair-share", at(250))
		v, err := verdict.FairShare(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Partial))
		Expect(v.Reason).To(Equal("tenant b had at most 8 of its 16 entitled GPUs running within 65 s of submitting"))
	})

	It("is not supported when B waits for A's whole backlog", func() {
		tl := verdict.Build(fixture("fair-share", append(aPods(60.5, 60.5, 60.5, 60.5),
			bPods(120.5, 120.5, 120.5, 120.5)...)...), "fair-share", at(200))
		v, err := verdict.FairShare(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.NotSupported))
		Expect(v.Reason).To(Equal("tenant b had no GPUs running within 65 s of submitting"))
	})

	It("counts the entitlement reached early, even if B is between waves at the check", func() {
		// Kueue reclaimed for B at 12 s: b0 and b1 ran 12 to 72 s; the next
		// wave starts at 75.1 s, after the check at 10 + 65 = 75 s. At the
		// instant of the check B runs nothing; within the window it had 16.
		pods := []pod{tenantPod("a0", "a", 8, 0, 0.5, 60), tenantPod("a1", "a", 8, 0, 0.5, 60),
			tenantPod("b0", "b", 8, 10, 12, 72), tenantPod("b1", "b", 8, 10, 12, 72),
			tenantPod("b2", "b", 8, 10, 75.1, 135.1), tenantPod("b3", "b", 8, 10, 75.1, 135.1)}
		tl := verdict.Build(fixture("fair-share", pods...), "fair-share", at(140))
		v, err := verdict.FairShare(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Supported))
		Expect(v.Measurements["b_max_gpus_by_check"]).To(Equal(16.0))
	})

	It("counts a B pod that starts exactly at the check", func() {
		tl := verdict.Build(fixture("fair-share", append(aPods(60.5, 60.5, 120.5, 120.5),
			bPods(75, 75, 120.5, 120.5)...)...), "fair-share", at(200))
		v, err := verdict.FairShare(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Measurements["b_max_gpus_by_check"]).To(Equal(16.0))
	})
})

var _ = Describe("fairness over time", func() {
	// capacity 4 GPUs, 10 s windows. Tenant a: 4 GPUs from 0 to 20 s.
	// Tenant b: 4 GPUs, submitted at 5 s, running 20 to 30 s.
	// Windows [0,10) and [10,20): entitlement 20 each, a delivers 40, b 0:
	// x = (2, 0), J = 0.5. Window [20,30): only b has demand, excluded from
	// J; b's shortfall is 20 + 20 + 0 = 40 GPU-seconds.
	It("computes per-window Jain's index and the worst shortfall by hand", func() {
		p := verdict.FairShareParams{TenantB: "b", EntitlementGPUs: 4, JobLengthSec: 10,
			ToleranceSec: 0, CapacityGPUs: 4, WindowSec: 10}
		tl := verdict.Build(fixture("fair-share",
			tenantPod("a1", "a", 4, 0, 0, 20), tenantPod("b1", "b", 4, 5, 20, 30),
		), "fair-share", at(30))
		v, err := verdict.FairShare(tl, p)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Measurements["jain_median"]).To(Equal(0.5))
		Expect(v.Measurements["jain_p5"]).To(Equal(0.5))
		Expect(v.Measurements["windows_used"]).To(Equal(2.0))
		Expect(v.Measurements["windows_excluded"]).To(Equal(1.0))
		Expect(v.Measurements["worst_shortfall_gpu_s"]).To(Equal(40.0))
	})

	// capacity 8, 10 s windows, horizon 30 s. Tenant a: one 4-GPU pod running
	// 0 to 30 s. Tenant b: a 4-GPU Job submitted at 0 that a queue holds
	// without a pod until 20 s, then runs to 30 s. Demand starts at the
	// Job's submission, so b wants 40 GPU-s in every window: windows 1 and 2
	// have entitlements 40 and 40, deliveries 40 and 0, x = (1, 0), J = 0.5;
	// window 3 has x = (1, 1), J = 1. Median 0.5, three windows used, and
	// b's shortfall is 40 + 40 = 80 GPU-s. Counting demand from the pod's
	// creation instead would leave b without demand in windows 1 and 2.
	It("counts demand from the Job's submission, while a queue holds it without pods", func() {
		p := verdict.FairShareParams{TenantB: "b", EntitlementGPUs: 4, JobLengthSec: 30,
			ToleranceSec: 0, CapacityGPUs: 8, WindowSec: 10}
		tl := verdict.Build(fixtureJobs("fair-share",
			[]job{{name: "a1", tenant: "a", gpus: 4, created: 0, end: 30}, {name: "b1", tenant: "b", gpus: 4, created: 0, end: 30}},
			tenantPod("a1", "a", 4, 0, 0, 30), tenantPod("b1", "b", 4, 20, 20, 30),
		), "fair-share", at(30))
		v, err := verdict.FairShare(tl, p)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Measurements["jain_median"]).To(Equal(0.5))
		Expect(v.Measurements["windows_used"]).To(Equal(3.0))
		Expect(v.Measurements["worst_shortfall_gpu_s"]).To(Equal(80.0))
	})

	// capacity 4, one 10 s window; a runs 3 GPUs, b runs 1 GPU, both the
	// whole window. Capped water-filling gives entitlements 30 and 10, so
	// x = (1, 1) and J = 1. An uncapped equal split (20, 20) would give 0.8.
	It("caps each tenant's entitlement at its demand", func() {
		p := verdict.FairShareParams{TenantB: "b", EntitlementGPUs: 1, JobLengthSec: 10,
			ToleranceSec: 0, CapacityGPUs: 4, WindowSec: 10}
		tl := verdict.Build(fixture("fair-share",
			tenantPod("a1", "a", 3, 0, 0, 10), tenantPod("b1", "b", 1, 0, 0, 10),
		), "fair-share", at(10))
		v, err := verdict.FairShare(tl, p)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Measurements["jain_median"]).To(Equal(1.0))
		Expect(v.Measurements["worst_shortfall_gpu_s"]).To(Equal(0.0))
	})
})
