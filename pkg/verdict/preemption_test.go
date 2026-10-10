package verdict_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
)

var _ = Describe("Preemption", func() {
	params := verdict.PreemptionParams{HighJob: "high", HighPods: 2, VictimJob: "victim", GraceSec: 5}

	victim := func(name, node string, end float64, by events.Transition) pod {
		return pod{name: name, job: "victim", tenant: "t1", gpus: 8, node: node,
			created: 0, scheduled: 1, running: 2, end: end, endBy: by}
	}
	high := func(name, node string, scheduled float64) pod {
		return pod{name: name, job: "high", tenant: "t1", gpus: 8, node: node,
			created: 10, scheduled: scheduled, running: scheduled + 0.5, end: -1}
	}

	It("is supported when the whole victim workload is evicted", func() {
		tl := verdict.Build(fixture("preemption",
			victim("v0", "n1", 10.5, events.Deleted), victim("v1", "n2", 10.5, events.Deleted),
			victim("v2", "n3", 10.5, events.Deleted), victim("v3", "n4", 10.5, events.Deleted),
			high("h0", "n1", 11.0), high("h1", "n2", 11.2),
		), "preemption", at(60))
		v, err := verdict.Preemption(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Supported))
		Expect(v.Reason).To(Equal("no victim pod was left running 5 s after the high-priority job was scheduled"))
		Expect(v.Measurements["victim_pods_before"]).To(Equal(4.0))
		Expect(v.Measurements["victim_pods_evicted"]).To(Equal(4.0))
		Expect(v.Measurements["victim_pods_remaining"]).To(Equal(0.0))
		Expect(v.Measurements["gpu_seconds_lost"]).To(BeNumerically("~", 272, 1e-9))
		Expect(v.Measurements["high_time_to_schedule_s"]).To(BeNumerically("~", 1.2, 1e-9))
	})

	It("is partial when only some victim pods are evicted, ignoring recreated ones", func() {
		tl := verdict.Build(fixture("preemption",
			victim("v0", "n1", 10.3, events.Preempted), victim("v1", "n2", 10.3, events.Preempted),
			victim("v2", "n3", -1, ""), victim("v3", "n4", -1, ""),
			pod{name: "v4", job: "victim", tenant: "t1", gpus: 8, node: "n3", created: 10.4, scheduled: 30, running: 30.5, end: -1},
			high("h0", "n1", 11.0), high("h1", "n2", 11.2),
		), "preemption", at(60))
		v, err := verdict.Preemption(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Partial))
		Expect(v.Reason).To(Equal("2 of 4 victim pods were left running 5 s after the high-priority job was scheduled"))
		Expect(v.Measurements["gpu_seconds_lost"]).To(BeNumerically("~", 132.8, 1e-9))
	})

	It("counts a victim that ends exactly at the grace as evicted", func() {
		tl := verdict.Build(fixture("preemption",
			victim("v0", "n1", 16.2, events.Deleted), victim("v1", "n2", 10.5, events.Deleted),
			victim("v2", "n3", 10.5, events.Deleted), victim("v3", "n4", 10.5, events.Deleted),
			high("h0", "n3", 11.0), high("h1", "n4", 11.2),
		), "preemption", at(60))
		v, err := verdict.Preemption(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Supported))
	})

	It("is not supported when the high-priority job never gets all its pods", func() {
		tl := verdict.Build(fixture("preemption",
			victim("v0", "n1", -1, ""), victim("v1", "n2", -1, ""),
			victim("v2", "n3", -1, ""), victim("v3", "n4", -1, ""),
			pending("h0", "high", 10), pending("h1", "high", 10),
		), "preemption", at(60))
		v, err := verdict.Preemption(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.NotSupported))
		Expect(v.Reason).To(Equal("the high-priority job did not get all 2 pods scheduled"))
	})

	It("fails, as a harness error, when no victim was running first", func() {
		tl := verdict.Build(fixture("preemption",
			pending("v0", "victim", 0), high("h0", "n1", 11.0), high("h1", "n2", 11.2),
		), "preemption", at(60))
		_, err := verdict.Preemption(tl, params)
		Expect(err).To(MatchError("no victim pod was running when the high-priority job was created"))
	})
})
