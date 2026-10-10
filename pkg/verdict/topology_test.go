package verdict_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
)

var _ = Describe("Topology", func() {
	racks := map[string]string{"n1": "rack-a", "n2": "rack-a", "n3": "rack-b", "n4": "rack-b"}
	params := verdict.TopologyParams{AttemptJobs: []string{"t1", "t2", "t3"}, PodsPerJob: 2, RackOf: racks}

	attempt := func(job, nodeA, nodeB string, start float64) []pod {
		return []pod{
			done(job+"-0", job, nodeA, start, start+1, start+1.5, start+20),
			done(job+"-1", job, nodeB, start, start+1, start+1.5, start+20),
		}
	}
	run := func(ps ...[]pod) verdict.Timeline {
		var all []pod
		for _, p := range ps {
			all = append(all, p...)
		}
		return verdict.Build(fixture("topology", all...), "topology", at(200))
	}

	It("is supported when every attempt lands in one rack", func() {
		v, err := verdict.Topology(run(attempt("t1", "n3", "n4", 0), attempt("t2", "n4", "n3", 30),
			attempt("t3", "n3", "n4", 60)), params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Supported))
		Expect(v.Reason).To(Equal("all 3 attempts placed every pod in one rack"))
		Expect(v.Measurements).To(Equal(map[string]float64{"attempts": 3, "attempts_one_rack": 3}))
	})

	It("is partial when some attempts split across racks", func() {
		v, err := verdict.Topology(run(attempt("t1", "n3", "n4", 0), attempt("t2", "n2", "n3", 30),
			attempt("t3", "n3", "n4", 60)), params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Partial))
		Expect(v.Reason).To(Equal("2 of 3 attempts placed every pod in one rack"))
	})

	It("is not supported when no attempt lands in one rack", func() {
		v, err := verdict.Topology(run(attempt("t1", "n2", "n3", 0), attempt("t2", "n2", "n4", 30),
			attempt("t3", "n2", "n3", 60)), params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.NotSupported))
	})

	It("fails, as a harness error, on a node outside the fleet", func() {
		_, err := verdict.Topology(run(attempt("t1", "n3", "n9", 0), attempt("t2", "n3", "n4", 30),
			attempt("t3", "n3", "n4", 60)), params)
		Expect(err).To(MatchError(`node "n9" is not in rack_of`))
	})
})
