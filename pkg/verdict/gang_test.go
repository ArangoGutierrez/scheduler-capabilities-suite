package verdict_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
)

func TestVerdict(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "verdict") {
		t.Fatal("verdict specs failed")
	}
}

var _ = Describe("Pods", func() {
	It("ignores events observed after the window ends", func() {
		evs := fixture("gang", done("a0", "gang-a", "n1", 0, 1, 2, 22))
		pods := verdict.Pods(evs, "gang", at(10))
		Expect(pods).To(HaveLen(1))
		Expect(pods[0].Running).To(Equal(at(2)))
		Expect(pods[0].End.IsZero()).To(BeTrue())
		Expect(pods[0].Seqs).To(Equal([]int64{1, 2, 3}))
	})

	It("keeps only the named probe's Pod events", func() {
		evs := append(fixture("gang", done("a0", "gang-a", "n1", 0, 1, 2, 22)),
			fixture("priority", done("x", "low", "n1", 0, 1, 2, 3))...)
		Expect(verdict.Pods(evs, "priority", at(100))).To(HaveLen(1))
	})
})

var _ = Describe("Gang", func() {
	params := verdict.GangParams{Jobs: []string{"gang-a", "gang-b"}, Size: 2, GraceSec: 5}

	It("is supported when each gang is placed at once and both complete", func() {
		tl := verdict.Build(fixture("gang",
			done("a0", "gang-a", "n1", 0, 1.0, 2, 22), done("a1", "gang-a", "n2", 0, 1.5, 2, 22),
			done("b0", "gang-b", "n1", 0, 22.5, 23.5, 44), done("b1", "gang-b", "n2", 0, 23.0, 24, 44),
		), "gang", at(50))
		v, err := verdict.Gang(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Supported))
		Expect(v.Reason).To(Equal("no gang was partly placed for more than 5 s, and all 2 gangs completed"))
		Expect(v.Measurements).To(Equal(map[string]float64{"gangs": 2, "gangs_completed": 2, "max_partial_s": 0.5}))
		Expect(v.EvidenceSeq).To(Equal([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
	})

	It("still counts exactly the grace as placed at once", func() {
		tl := verdict.Build(fixture("gang",
			done("a0", "gang-a", "n1", 0, 1, 7, 27), done("a1", "gang-a", "n2", 0, 6, 7, 27),
			done("b0", "gang-b", "n1", 0, 28, 29, 49), done("b1", "gang-b", "n2", 0, 28, 29, 49),
		), "gang", at(60))
		v, err := verdict.Gang(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Supported))
		Expect(v.Measurements["max_partial_s"]).To(Equal(5.0))
	})

	It("is partial when a gang is partly placed past the grace but every gang completes", func() {
		three := verdict.GangParams{Jobs: []string{"gang-a", "gang-b"}, Size: 3, GraceSec: 5}
		tl := verdict.Build(fixture("gang-control",
			done("a0", "gang-a", "n1", 0, 1, 2, 22), done("a1", "gang-a", "n2", 0, 1, 2, 22),
			done("a2", "gang-a", "n1", 0, 22.5, 23, 43),
			done("b0", "gang-b", "n3", 0, 1, 2, 22), done("b1", "gang-b", "n4", 0, 1, 2, 22),
			done("b2", "gang-b", "n2", 0, 22.5, 23, 43),
		), "gang-control", at(60))
		v, err := verdict.Gang(tl, three)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Partial))
		Expect(v.Reason).To(Equal("gang gang-a was partly placed for 21.5 s; all 2 gangs completed"))
		Expect(v.Measurements["max_partial_s"]).To(Equal(21.5))
	})

	It("counts one millisecond past the grace as partly placed", func() {
		tl := verdict.Build(fixture("gang",
			done("a0", "gang-a", "n1", 0, 1, 7.001, 27), done("a1", "gang-a", "n2", 0, 6.001, 7.001, 27),
			done("b0", "gang-b", "n1", 0, 28, 29, 49), done("b1", "gang-b", "n2", 0, 28, 29, 49),
		), "gang", at(60))
		v, err := verdict.Gang(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Partial))
		Expect(v.Measurements["max_partial_s"]).To(BeNumerically("~", 5.001, 1e-9))
	})

	It("is not supported when a partial placement lasts until the probe ends", func() {
		tl := verdict.Build(fixture("gang",
			pod{name: "a0", job: "gang-a", tenant: "t1", gpus: 8, node: "n1", created: 0, scheduled: 1, running: 2, end: -1},
			pending("a1", "gang-a", 0),
			done("b0", "gang-b", "n2", 0, 1, 2, 22), done("b1", "gang-b", "n3", 0, 1, 2, 22),
		), "gang", at(120))
		v, err := verdict.Gang(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.NotSupported))
		Expect(v.Reason).To(Equal("gang gang-a was partly placed for 119 s, and gang gang-a did not complete"))
		Expect(v.Measurements["gangs_completed"]).To(Equal(1.0))
	})

	It("is not supported when a gang never gets a pod placed", func() {
		tl := verdict.Build(fixture("gang",
			done("a0", "gang-a", "n1", 0, 1, 2, 22), done("a1", "gang-a", "n2", 0, 1, 2, 22),
			pending("b0", "gang-b", 0), pending("b1", "gang-b", 0),
		), "gang", at(120))
		v, err := verdict.Gang(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.NotSupported))
		Expect(v.Reason).To(Equal("gang gang-b did not complete before the probe ended"))
		Expect(v.Measurements["max_partial_s"]).To(Equal(0.0))
	})

	It("does not count a deleted pod as completed", func() {
		tl := verdict.Build(fixture("gang",
			done("a0", "gang-a", "n1", 0, 1, 2, 22),
			pod{name: "a1", job: "gang-a", tenant: "t1", gpus: 8, node: "n2", created: 0, scheduled: 1, running: 2, end: 22, endBy: events.Deleted},
			done("b0", "gang-b", "n1", 0, 23, 24, 44), done("b1", "gang-b", "n2", 0, 23, 24, 44),
		), "gang", at(50))
		v, err := verdict.Gang(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.NotSupported))
		Expect(v.Reason).To(Equal("gang gang-a did not complete before the probe ended"))
	})

	It("is not supported when a queue never admits a gang, so it has a Job and no pods", func() {
		tl := verdict.Build(fixtureJobs("gang", []job{{name: "gang-b", tenant: "t1", gpus: 16, created: 0, end: -1}},
			done("a0", "gang-a", "n1", 0, 1, 2, 22), done("a1", "gang-a", "n2", 0, 1, 2, 22),
		), "gang", at(120))
		v, err := verdict.Gang(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.NotSupported))
		Expect(v.Reason).To(Equal("gang gang-b did not complete before the probe ended"))
		Expect(v.EvidenceSeq).To(ContainElement(int64(1)))
	})

	It("fails, as a harness error, when a gang has no recorded event at all", func() {
		tl := verdict.Build(fixture("gang", done("a0", "gang-a", "n1", 0, 1, 2, 22)), "gang", at(50))
		_, err := verdict.Gang(tl, params)
		Expect(err).To(MatchError("no events recorded for job gang-b"))
	})
})
