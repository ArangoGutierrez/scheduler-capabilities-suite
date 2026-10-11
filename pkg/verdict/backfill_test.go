package verdict_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
)

var _ = Describe("Backfill", func() {
	params := verdict.BackfillParams{BigJob: "big", BigPods: 4,
		BlockerJobs: []string{"blk0", "blk1", "blk2"}, SmallJobs: []string{"s0", "s1", "s2", "s3"}, ToleranceSec: 5}

	// blockers run on n1..n3 and end at 50, 55 and 60 s, so the gang's
	// earliest possible start is 60 s (the LAST blocker's end); its pods are scheduled
	// at bigAt; short jobs are scheduled at the given seconds (negative:
	// never) and run 25 s.
	scenario := func(bigAt float64, smallAt ...float64) verdict.Timeline {
		ps := []pod{
			done("blk0", "blk0", "n1", 0, 0, 0.5, 50), done("blk1", "blk1", "n2", 0, 0, 0.5, 55),
			done("blk2", "blk2", "n3", 0, 0, 0.5, 60),
		}
		for i, n := range []string{"b0", "b1", "b2", "b3"} {
			node := []string{"n1", "n2", "n3", "n4"}[i]
			if bigAt < 0 {
				ps = append(ps, pending(n, "big", 1))
				continue
			}
			ps = append(ps, pod{name: n, job: "big", tenant: "t1", gpus: 8, node: node,
				created: 1, scheduled: bigAt, running: bigAt + 0.5, end: -1})
		}
		for i, s := range smallAt {
			n := []string{"s0", "s1", "s2", "s3"}[i]
			if s < 0 {
				ps = append(ps, pending(n, n, 2))
				continue
			}
			ps = append(ps, done(n, n, "n4", 2, s, s+0.5, s+25))
		}
		return verdict.Build(fixture("backfill", ps...), "backfill", at(200))
	}

	It("is supported when short jobs fill the gap and the gang starts on time", func() {
		v, err := verdict.Backfill(scenario(61, 2, 27.5, 61.5, 86.5), params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Supported))
		Expect(v.Reason).To(Equal("2 short jobs ran before the gang, which started 1 s after its earliest possible start"))
		Expect(v.Measurements).To(Equal(map[string]float64{"small_before_big": 2, "big_start_delay_s": 1}))
	})

	It("counts a short job with two pods once", func() {
		// s0 has two pods, both scheduled before the gang (61 s); s1 also goes
		// first, s2 and s3 after: 2 short jobs ran first, not 3 as counting
		// pods would say. Delay 61 - 60 = 1 s, within the tolerance.
		tl := verdict.Build(fixture("backfill",
			done("blk0", "blk0", "n1", 0, 0, 0.5, 50), done("blk1", "blk1", "n2", 0, 0, 0.5, 55),
			done("blk2", "blk2", "n3", 0, 0, 0.5, 60),
			pod{name: "b0", job: "big", tenant: "t1", gpus: 8, node: "n1", created: 1, scheduled: 61, running: 61.5, end: -1},
			pod{name: "b1", job: "big", tenant: "t1", gpus: 8, node: "n2", created: 1, scheduled: 61, running: 61.5, end: -1},
			pod{name: "b2", job: "big", tenant: "t1", gpus: 8, node: "n3", created: 1, scheduled: 61, running: 61.5, end: -1},
			pod{name: "b3", job: "big", tenant: "t1", gpus: 8, node: "n4", created: 1, scheduled: 61, running: 61.5, end: -1},
			done("s0-0", "s0", "n4", 2, 2, 2.5, 27), done("s0-1", "s0", "n4", 2, 3, 3.5, 28),
			done("s1", "s1", "n4", 2, 28, 28.5, 53),
			done("s2", "s2", "n1", 2, 62, 62.5, 87), done("s3", "s3", "n2", 2, 63, 63.5, 88),
		), "backfill", at(200))
		v, err := verdict.Backfill(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Supported))
		Expect(v.Reason).To(Equal("2 short jobs ran before the gang, which started 1 s after its earliest possible start"))
		Expect(v.Measurements).To(Equal(map[string]float64{"small_before_big": 2, "big_start_delay_s": 1}))
	})

	It("still counts a start exactly at the tolerance as on time", func() {
		v, err := verdict.Backfill(scenario(65, 2, 27.5, 66, 91), params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Supported))
	})

	It("is partial when short jobs delay the gang past the tolerance", func() {
		v, err := verdict.Backfill(scenario(78, 2, 27.5, 52.5, 78.5), params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Partial))
		Expect(v.Reason).To(Equal("3 short jobs ran before the gang, which started 18 s after its earliest possible start"))
	})

	It("is partial when short jobs ran and the gang never started", func() {
		v, err := verdict.Backfill(scenario(-1, 2, 27.5, 52.5, 78), params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Partial))
		Expect(v.Reason).To(Equal("4 short jobs ran, and the gang never started"))
	})

	It("is not supported when the short jobs wait behind the gang", func() {
		v, err := verdict.Backfill(scenario(60.5, 61, 61, 86, 86), params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.NotSupported))
		Expect(v.Reason).To(Equal("no short job ran before the gang started"))
	})
})
