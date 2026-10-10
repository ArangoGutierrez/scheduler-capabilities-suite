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

	It("rejects an unknown capability", func() {
		_, err := verdict.Evaluate(evs, verdict.Probe{Capability: "teleport", Params: json.RawMessage(`{}`), WindowEnd: at(50)})
		Expect(err).To(MatchError("teleport: unknown capability"))
	})

	It("rejects a probe without a window end", func() {
		_, err := verdict.Evaluate(evs, verdict.Probe{Capability: "gang", Params: json.RawMessage(`{}`)})
		Expect(err).To(MatchError("gang: window_end is missing"))
	})
})
