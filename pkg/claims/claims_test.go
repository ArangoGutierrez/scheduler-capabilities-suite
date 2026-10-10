package claims_test

import (
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/claims"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
)

func TestClaims(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "claims") {
		t.Fatal("claims specs failed")
	}
}

const valid = `scheduler: vanilla
version: Kubernetes v1.37.0
claims:
  gang:
    claim: Pods of a group are scheduled together or not at all.
    citation: https://example.org/gang
    expected: supported
    mechanism: Workload API gang
  priority:
    claim: Higher-priority pods are ordered ahead in the scheduling queue.
    citation: https://example.org/priority
    expected: supported
    mechanism: PriorityClass
  preemption:
    claim: The scheduler evicts lower-priority pods to make room.
    citation: https://example.org/preemption
    expected: partial
    mechanism: PriorityClass with PreemptLowerPriority
  fair-share:
    claim: No tenant fair-share mechanism is documented.
    citation: https://example.org/fair-share
    expected: not_supported
    mechanism: namespaces as tenants, no policy
  backfill:
    claim: No backfill or reservation mechanism is documented.
    citation: https://example.org/backfill
    expected: partial
    mechanism: plain Jobs
  topology:
    claim: Required pod affinity keeps pods in one topology domain.
    citation: https://example.org/topology
    expected: supported
    mechanism: required pod affinity on the rack label
`

var _ = Describe("Parse", func() {
	It("reads every capability's claim", func() {
		f, err := claims.Parse([]byte(valid))
		Expect(err).NotTo(HaveOccurred())
		Expect(f.Scheduler).To(Equal("vanilla"))
		Expect(f.Version).To(Equal("Kubernetes v1.37.0"))
		Expect(f.Claims).To(HaveLen(6))
		Expect(f.Claims["fair-share"]).To(Equal(claims.Claim{
			Claim:     "No tenant fair-share mechanism is documented.",
			Citation:  "https://example.org/fair-share",
			Expected:  verdict.NotSupported,
			Mechanism: "namespaces as tenants, no policy",
		}))
	})

	It("rejects a capability the suite does not define", func() {
		in := strings.Replace(valid, "  backfill:", "  removed:", 1)
		_, err := claims.Parse([]byte(in))
		Expect(err).To(MatchError(`claims: unknown capability "removed"`))
	})

	It("rejects a file that leaves a capability out", func() {
		cut := valid[:strings.Index(valid, "  topology:")]
		_, err := claims.Parse([]byte(cut))
		Expect(err).To(MatchError(`claims: missing capability "topology"`))
	})

	It("rejects a citation that is not https", func() {
		in := strings.Replace(valid, "https://example.org/priority", "http://example.org/priority", 1)
		_, err := claims.Parse([]byte(in))
		Expect(err).To(MatchError(`claims: capability "priority": citation "http://example.org/priority" is not an https URL`))
	})

	It("rejects an expected outcome outside the three", func() {
		in := strings.Replace(valid, "expected: partial\n    mechanism: plain Jobs", "expected: maybe\n    mechanism: plain Jobs", 1)
		_, err := claims.Parse([]byte(in))
		Expect(err).To(MatchError(`claims: capability "backfill": expected "maybe", want supported, partial or not_supported`))
	})

	It("rejects a field the format does not define", func() {
		in := strings.Replace(valid, "version: Kubernetes v1.37.0", "version: Kubernetes v1.37.0\nvendor: someone", 1)
		_, err := claims.Parse([]byte(in))
		Expect(err).To(MatchError(ContainSubstring(`unknown field "vendor"`)))
	})
})
