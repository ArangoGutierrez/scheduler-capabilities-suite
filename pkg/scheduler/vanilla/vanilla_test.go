package vanilla_test

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/claims"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler/vanilla"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

func TestVanilla(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "vanilla") {
		t.Fatal("vanilla specs failed")
	}
}

var victim = workload.Job{Name: "victim", Namespace: "scs-preemption-t1", Probe: "preemption", Tenant: "t1",
	Priority: workload.Low, Pods: 4, GPUsPerPod: 8, RunFor: 300 * time.Second, Gang: true, EvictWhole: true}

var _ = Describe("PodGroup", func() {
	It("makes an evict-whole gang a PodGroup with minCount, priority and disruption mode all", func() {
		Expect(vanilla.PodGroup(victim)).To(Equal(map[string]any{
			"apiVersion": "scheduling.k8s.io/v1beta1",
			"kind":       "PodGroup",
			"metadata": map[string]any{"name": "pg-victim", "namespace": "scs-preemption-t1", "labels": map[string]any{
				"app.kubernetes.io/part-of":            "scheduler-capabilities-suite",
				"scs.arangogutierrez.github.io/probe":  "preemption",
				"scs.arangogutierrez.github.io/tenant": "t1",
				"scs.arangogutierrez.github.io/job":    "victim",
			}},
			"spec": map[string]any{
				"schedulingPolicy":  map[string]any{"gang": map[string]any{"minCount": int64(4)}},
				"priorityClassName": "scs-low",
				"disruptionMode":    map[string]any{"all": map[string]any{}},
			},
		}))
	})

	It("adds a rack topology constraint for a same-rack gang", func() {
		j := workload.Job{Name: "topo-1", Namespace: "scs-topology-t1", Probe: "topology", Tenant: "t1",
			Priority: workload.Low, Pods: 2, GPUsPerPod: 8, RunFor: 20 * time.Second, Gang: true, SameRack: true}
		spec := vanilla.PodGroup(j)["spec"].(map[string]any)
		Expect(spec["schedulingConstraints"]).To(Equal(map[string]any{"topology": []any{map[string]any{"key": "scs.arangogutierrez.github.io/rack"}}}))
		Expect(spec).NotTo(HaveKey("disruptionMode"))
	})
})

var _ = Describe("Job", func() {
	It("names the gang's PodGroup and sets the priority class", func() {
		b := vanilla.Job(victim)
		Expect(b.Spec.Template.Spec.PriorityClassName).To(Equal("scs-low"))
		Expect(*b.Spec.Template.Spec.SchedulingGroup.PodGroupName).To(Equal("pg-victim"))
	})

	It("leaves a job that is not a gang ungrouped, and maps a preempting high job", func() {
		j := workload.Job{Name: "high", Namespace: "n", Probe: "preemption", Tenant: "t1", Priority: workload.High,
			Preempting: true, Pods: 2, GPUsPerPod: 8, RunFor: 30 * time.Second}
		b := vanilla.Job(j)
		Expect(b.Spec.Template.Spec.SchedulingGroup).To(BeNil())
		Expect(b.Spec.Template.Spec.PriorityClassName).To(Equal("scs-high-preempt"))
		j.Preempting = false
		Expect(vanilla.PriorityClassName(j)).To(Equal("scs-high"))
	})
})

var _ = Describe("claims", func() {
	It("ships a claims file that loads, with a citation for every capability", func() {
		f, err := claims.Load("../../../claims/vanilla.yaml")
		Expect(err).NotTo(HaveOccurred())
		Expect(f.Scheduler).To(Equal("vanilla"))
		Expect(f.Claims["preemption"].Expected).To(Equal(verdict.Supported))
	})
})
