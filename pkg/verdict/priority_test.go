package verdict_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
)

var _ = Describe("Priority", func() {
	params := verdict.PriorityParams{HighJob: "high", QueuedJobs: []string{"q1", "q2", "q3", "q4"}}

	// queued returns the four queued low-priority pods, scheduled at the
	// given seconds (negative: never).
	queued := func(s1, s2, s3, s4 float64) []pod {
		var out []pod
		for i, s := range []float64{s1, s2, s3, s4} {
			name := []string{"q1", "q2", "q3", "q4"}[i]
			if s < 0 {
				out = append(out, pending(name, name, 1))
				continue
			}
			out = append(out, done(name, name, "n1", 1, s, s+0.5, s+20))
		}
		return out
	}

	It("is supported when the high-priority pod takes the first freed slot", func() {
		tl := verdict.Build(fixture("priority", append(queued(40, 50, 60, 70),
			done("h0", "high", "n1", 3, 30, 30.5, 50))...), "priority", at(100))
		v, err := verdict.Priority(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Supported))
		Expect(v.Reason).To(Equal("the high-priority job was scheduled before every queued lower-priority job"))
		Expect(v.Measurements).To(Equal(map[string]float64{"queued_jobs": 4, "queued_before_high": 0, "high_time_to_schedule_s": 27}))
	})

	It("does not count a queued pod scheduled at the same instant as ahead", func() {
		tl := verdict.Build(fixture("priority", append(queued(30, 50, 60, 70),
			done("h0", "high", "n2", 3, 30, 30.5, 50))...), "priority", at(100))
		v, err := verdict.Priority(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Supported))
	})

	It("is partial when some queued pods go first", func() {
		tl := verdict.Build(fixture("priority", append(queued(30, 40, 60, 70),
			done("h0", "high", "n1", 3, 50, 50.5, 70))...), "priority", at(100))
		v, err := verdict.Priority(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Partial))
		Expect(v.Reason).To(Equal("2 of 4 queued lower-priority jobs were scheduled before the high-priority job"))
	})

	It("counts a queued job with two pods ahead once", func() {
		// q1 has two pods, both scheduled before the high-priority pod (50 s);
		// q2 and q3 go first too, q4 after: 3 of the 4 queued jobs are ahead,
		// not "all 4" as counting pods (2 + 1 + 1) would say
		tl := verdict.Build(fixture("priority",
			done("q1-0", "q1", "n1", 1, 30, 30.5, 50), done("q1-1", "q1", "n2", 1, 31, 31.5, 51),
			done("q2", "q2", "n3", 1, 40, 40.5, 60), done("q3", "q3", "n4", 1, 45, 45.5, 65),
			done("q4", "q4", "n1", 1, 70, 70.5, 90), done("h0", "high", "n1", 3, 50, 50.5, 70),
		), "priority", at(100))
		v, err := verdict.Priority(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Partial))
		Expect(v.Reason).To(Equal("3 of 4 queued lower-priority jobs were scheduled before the high-priority job"))
		Expect(v.Measurements["queued_before_high"]).To(Equal(3.0))
	})

	It("is not supported when the pods run in arrival order", func() {
		tl := verdict.Build(fixture("priority", append(queued(30, 40, 50, 60),
			done("h0", "high", "n1", 3, 70, 70.5, 90))...), "priority", at(100))
		v, err := verdict.Priority(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.NotSupported))
		Expect(v.Reason).To(Equal("all 4 queued lower-priority jobs were scheduled before the high-priority job"))
	})

	It("is not supported when the high-priority pod is never scheduled", func() {
		tl := verdict.Build(fixture("priority", append(queued(30, 40, -1, -1),
			pending("h0", "high", 3))...), "priority", at(100))
		v, err := verdict.Priority(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.NotSupported))
		Expect(v.Reason).To(Equal("the high-priority job was never scheduled"))
		Expect(v.Measurements).To(Equal(map[string]float64{"queued_jobs": 4, "queued_before_high": 2}))
	})

	It("measures time to schedule from the Job's submission, not its pod's creation", func() {
		// Kueue holds a queued Job without pods: the Job is created at 2.2 s,
		// its pod only at admission (33.24 s), scheduled at 34.26 s.
		tl := verdict.Build(fixtureJobs("priority", []job{{name: "high", tenant: "t1", gpus: 8, created: 2.2, end: -1}},
			append(queued(44, 54, 64, 67),
				pod{name: "h0", job: "high", tenant: "t1", gpus: 8, node: "n1", created: 33.24, scheduled: 34.26, running: 34.3, end: -1})...),
			"priority", at(100))
		v, err := verdict.Priority(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Supported))
		Expect(v.Measurements["high_time_to_schedule_s"]).To(BeNumerically("~", 32.06, 1e-9))
	})

	It("is not supported when the high-priority job is held as a Job and never gets a pod", func() {
		tl := verdict.Build(fixtureJobs("priority", []job{{name: "high", tenant: "t1", gpus: 8, created: 3, end: -1}},
			queued(30, 40, 50, 60)...), "priority", at(100))
		v, err := verdict.Priority(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.NotSupported))
		Expect(v.Reason).To(Equal("the high-priority job was never scheduled"))
	})
})
