package workload_test

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

func TestWorkload(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "workload") {
		t.Fatal("workload specs failed")
	}
}

var gang = workload.Job{Name: "gang-a", Namespace: "scs-gang", Probe: "gang", Tenant: "t1",
	Priority: workload.Low, Pods: 3, GPUsPerPod: 8, RunFor: 20 * time.Second, Gang: true}

var _ = Describe("BatchJob", func() {
	It("runs Pods pods at once, survives evictions, and carries exact kwok timing", func() {
		j := gang.BatchJob()
		Expect(j.Name).To(Equal("gang-a"))
		Expect(j.Namespace).To(Equal("scs-gang"))
		Expect(*j.Spec.Parallelism).To(Equal(int32(3)))
		Expect(*j.Spec.Completions).To(Equal(int32(3)))
		Expect(*j.Spec.BackoffLimit).To(Equal(int32(10)))
		Expect(j.Spec.Template.Annotations).To(Equal(map[string]string{
			"pod-create.stage.kwok.x-k8s.io/delay":          "0s",
			"pod-create.stage.kwok.x-k8s.io/jitter-delay":   "0s",
			"pod-ready.stage.kwok.x-k8s.io/delay":           "0s",
			"pod-ready.stage.kwok.x-k8s.io/jitter-delay":    "0s",
			"pod-complete.stage.kwok.x-k8s.io/delay":        "20s",
			"pod-complete.stage.kwok.x-k8s.io/jitter-delay": "20s",
		}))
		Expect(j.Spec.Template.Labels).To(Equal(map[string]string{
			"app.kubernetes.io/part-of":            "scheduler-capabilities-suite",
			"scs.arangogutierrez.github.io/probe":  "gang",
			"scs.arangogutierrez.github.io/tenant": "t1",
			"scs.arangogutierrez.github.io/job":    "gang-a",
		}))
	})

	It("selects and tolerates only the kwok fleet, and asks for the GPUs", func() {
		spec := gang.BatchJob().Spec.Template.Spec
		Expect(spec.NodeSelector).To(Equal(map[string]string{"type": "kwok"}))
		Expect(spec.Tolerations).To(Equal([]corev1.Toleration{{Key: "kwok.x-k8s.io/node", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}}))
		Expect(spec.RestartPolicy).To(Equal(corev1.RestartPolicyNever))
		q := spec.Containers[0].Resources.Limits["nvidia.com/gpu"]
		Expect(q.Value()).To(Equal(int64(8)))
		Expect(spec.Containers[0].Image).To(Equal("registry.invalid/scs/fake:0"))
	})

	It("writes a minute as kwok parses it, and pins a node when asked", func() {
		j := gang
		j.RunFor = 5 * time.Minute
		j.NodeName = "n1"
		b := j.BatchJob()
		Expect(b.Spec.Template.Annotations["pod-complete.stage.kwok.x-k8s.io/delay"]).To(Equal("5m0s"))
		Expect(b.Spec.Template.Spec.NodeSelector).To(Equal(map[string]string{"type": "kwok", "kubernetes.io/hostname": "n1"}))
	})
})

var _ = Describe("Validate", func() {
	It("accepts a well-formed gang", func() {
		Expect(gang.Validate()).To(Succeed())
	})
	It("rejects a low-priority job that preempts", func() {
		j := gang
		j.Preempting = true
		Expect(j.Validate()).To(MatchError("workload: gang-a: only a high-priority job may preempt"))
	})
	It("rejects same-rack on a job that is not a gang", func() {
		j := gang
		j.Gang, j.SameRack = false, true
		Expect(j.Validate()).To(MatchError("workload: gang-a: evict-whole and same-rack apply to gangs only"))
	})
})
