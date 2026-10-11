package kueue_test

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/claims"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler/kueue"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

func TestKueue(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "kueue") {
		t.Fatal("kueue specs failed")
	}
}

var _ = Describe("Manifest", func() {
	It("serves Kueue v0.20.1's release manifest with its pinned sha256", func() {
		data, sum, err := kueue.Manifest()
		Expect(err).NotTo(HaveOccurred())
		Expect(sum).To(Equal("c48514295365aae0c7dc578c70330befdd0015fec57cb4a06ba872bbef8750dd"))
		Expect(len(data)).To(Equal(1798228))
	})
})

var _ = Describe("WithFairSharing", func() {
	It("appends the fairSharing block to a configuration that only mentions it in a comment", func() {
		cfg := "apiVersion: config.kueue.x-k8s.io/v1beta2\nkind: Configuration\n#fairSharing:\n#  preemptionStrategies: []\n"
		got, err := kueue.WithFairSharing(cfg)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(cfg + "fairSharing:\n  preemptionStrategies: [LessThanOrEqualToFinalShare, LessThanInitialShare]\n"))
	})
	It("refuses a configuration that already sets fairSharing", func() {
		_, err := kueue.WithFairSharing("kind: Configuration\nfairSharing:\n  preemptionStrategies: []\n")
		Expect(err).To(MatchError("kueue: the manager configuration already sets fairSharing"))
	})
})

var _ = Describe("ClusterQueue", func() {
	a := scheduler.Tenant{Name: "a", Namespace: "scs-fair-share-a", EntitlementGPUs: 16}
	b := scheduler.Tenant{Name: "b", Namespace: "scs-fair-share-b", EntitlementGPUs: 16}

	It("gives each fair-share tenant its entitlement in a cohort that reclaims lent quota", func() {
		s := scheduler.Scenario{Probe: "fair-share", Tenants: []scheduler.Tenant{a, b}, AllowPreemption: true}
		cq := kueue.ClusterQueue(s, a)
		Expect(cq["metadata"].(map[string]any)["name"]).To(Equal("cq-scs-fair-share-a"))
		spec := cq["spec"].(map[string]any)
		Expect(spec["namespaceSelector"]).To(Equal(map[string]any{}))
		Expect(spec["cohortName"]).To(Equal("scs-fair-share"))
		Expect(spec["preemption"]).To(Equal(map[string]any{"withinClusterQueue": "LowerPriority", "reclaimWithinCohort": "Any"}))
		Expect(spec["resourceGroups"]).To(Equal([]any{map[string]any{
			"coveredResources": []any{"nvidia.com/gpu"},
			"flavors": []any{map[string]any{"name": "gpu", "resources": []any{
				map[string]any{"name": "nvidia.com/gpu", "nominalQuota": int64(16)},
			}}},
		}}))
	})

	It("leaves a single-tenant probe without a cohort, and without preemption unless asked", func() {
		t1 := scheduler.Tenant{Name: "t1", Namespace: "scs-priority-t1", EntitlementGPUs: 32}
		spec := kueue.ClusterQueue(scheduler.Scenario{Probe: "priority", Tenants: []scheduler.Tenant{t1}}, t1)["spec"].(map[string]any)
		Expect(spec).NotTo(HaveKey("cohortName"))
		Expect(spec).NotTo(HaveKey("preemption"))
		spec = kueue.ClusterQueue(scheduler.Scenario{Probe: "preemption", Tenants: []scheduler.Tenant{t1}, AllowPreemption: true}, t1)["spec"].(map[string]any)
		Expect(spec["preemption"]).To(Equal(map[string]any{"withinClusterQueue": "LowerPriority"}))
	})
})

var _ = Describe("Job", func() {
	It("routes the Job to the tenant's LocalQueue with its priority class", func() {
		j := workload.Job{Name: "high", Namespace: "scs-priority-t1", Probe: "priority", Tenant: "t1",
			Priority: workload.High, Pods: 1, GPUsPerPod: 8, RunFor: 30 * time.Second}
		b := kueue.Job(j)
		Expect(b.Labels).To(HaveKeyWithValue("kueue.x-k8s.io/queue-name", "lq"))
		Expect(b.Labels).To(HaveKeyWithValue("kueue.x-k8s.io/priority-class", "scs-high"))
		Expect(b.Spec.Template.Annotations).NotTo(HaveKey("kueue.x-k8s.io/podset-required-topology"))
	})

	It("asks topology-aware scheduling for one rack for a same-rack gang", func() {
		j := workload.Job{Name: "topo-1", Namespace: "scs-topology-t1", Probe: "topology", Tenant: "t1",
			Priority: workload.Low, Pods: 2, GPUsPerPod: 8, RunFor: 20 * time.Second, Gang: true, SameRack: true}
		b := kueue.Job(j)
		Expect(b.Spec.Template.Annotations).To(HaveKeyWithValue("kueue.x-k8s.io/podset-required-topology", "scs.arangogutierrez.github.io/rack"))
		Expect(b.Labels).To(HaveKeyWithValue("kueue.x-k8s.io/priority-class", "scs-low"))
	})
})

var _ = Describe("claims", func() {
	It("ships a claims file that loads, with a citation for every capability", func() {
		f, err := claims.Load("../../../claims/kueue.yaml")
		Expect(err).NotTo(HaveOccurred())
		Expect(f.Scheduler).To(Equal("kueue"))
	})
})
