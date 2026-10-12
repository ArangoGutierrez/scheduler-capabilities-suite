package verdict_test

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
)

var _ = Describe("Evaluate", func() {
	evs := fixture("gang",
		done("a0", "gang-a", "n1", 0, 1.0, 2, 22), done("a1", "gang-a", "n2", 0, 1.5, 2, 22),
		done("b0", "gang-b", "n1", 0, 22.5, 23.5, 44), done("b1", "gang-b", "n2", 0, 23.0, 24, 44),
		done("late", "gang-b", "n3", 55, 56, 57, 58),
	)

	It("decodes the parameters, applies the window and names the capability", func() {
		v, err := verdict.Evaluate(evs, verdict.Probe{
			Capability: "gang",
			Params:     json.RawMessage(`{"jobs":["gang-a","gang-b"],"size":2,"grace_sec":5}`),
			WindowEnd:  at(50),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Capability).To(Equal("gang"))
		Expect(v.Outcome).To(Equal(verdict.Supported))
		Expect(v.EvidenceSeq).To(HaveLen(16))
	})

	It("rejects a parameter the probe does not define", func() {
		_, err := verdict.Evaluate(evs, verdict.Probe{
			Capability: "gang",
			Params:     json.RawMessage(`{"jobs":["gang-a"],"size":2,"grace_sec":5,"colour":"red"}`),
			WindowEnd:  at(50),
		})
		Expect(err).To(MatchError(`gang: params: json: unknown field "colour"`))
	})

	It("judges the topology control with the topology rule, under its own name", func() {
		attempt := func(job, nodeA, nodeB string, start float64) []pod {
			return []pod{
				done(job+"-0", job, nodeA, start, start+1, start+1.5, start+20),
				done(job+"-1", job, nodeB, start, start+1, start+1.5, start+20),
			}
		}
		var all []pod
		for _, a := range [][]pod{attempt("t1", "n3", "n4", 0), attempt("t2", "n2", "n3", 30), attempt("t3", "n3", "n4", 60)} {
			all = append(all, a...)
		}
		v, err := verdict.Evaluate(fixture("topology-control", all...), verdict.Probe{
			Capability: "topology-control",
			Params: json.RawMessage(`{"attempt_jobs":["t1","t2","t3"],"pods_per_job":2,` +
				`"rack_of":{"n1":"rack-a","n2":"rack-a","n3":"rack-b","n4":"rack-b"}}`),
			WindowEnd: at(200),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Capability).To(Equal("topology-control"))
		Expect(v.Outcome).To(Equal(verdict.Partial))
		Expect(v.Reason).To(Equal("2 of 3 attempts placed every pod in one rack"))
	})

	It("rejects an unknown capability", func() {
		_, err := verdict.Evaluate(evs, verdict.Probe{Capability: "teleport", Params: json.RawMessage(`{}`), WindowEnd: at(50)})
		Expect(err).To(MatchError("teleport: unknown capability"))
	})

	DescribeTable("rejects malformed parameters before judging",
		func(capability, params, want string) {
			_, err := verdict.Evaluate(evs, verdict.Probe{Capability: capability, Params: json.RawMessage(params), WindowEnd: at(50)})
			Expect(err).To(MatchError(want))
		},
		Entry("gang size below 2", "gang", `{"jobs":["gang-a","gang-b"],"size":0,"grace_sec":5}`,
			"gang: size 0, want 2 or more"),
		Entry("priority without a high job", "priority", `{"high_job":"","queued_jobs":["q1"]}`,
			"priority: high_job is empty"),
		Entry("preemption with no high pods", "preemption", `{"high_job":"high","high_pods":0,"victim_job":"victim","grace_sec":5}`,
			"preemption: high_pods 0, want 1 or more"),
		Entry("fair-share with a zero window", "fair-share",
			`{"tenant_b":"b","entitlement_gpus":16,"job_length_sec":60,"tolerance_sec":5,"capacity_gpus":32,"window_sec":0}`,
			"fair-share: job_length_sec and window_sec must be positive, tolerance_sec not negative"),
		Entry("backfill with a negative tolerance", "backfill",
			`{"big_job":"big","big_pods":4,"blocker_jobs":["blk0"],"small_jobs":["s0"],"tolerance_sec":-1}`,
			"backfill: tolerance_sec -1 is negative"),
		Entry("topology with no pods per job", "topology", `{"attempt_jobs":["t1"],"pods_per_job":0,"rack_of":{"n1":"rack-a"}}`,
			"topology: pods_per_job 0, want 1 or more"),
	)

	It("rejects a probe without a window end", func() {
		_, err := verdict.Evaluate(evs, verdict.Probe{Capability: "gang", Params: json.RawMessage(`{}`)})
		Expect(err).To(MatchError("gang: window_end is missing"))
	})
})
