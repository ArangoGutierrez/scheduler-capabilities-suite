package fleet_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/apply"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/fleet"
)

func TestFleet(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "fleet") {
		t.Fatal("fleet specs failed")
	}
}

var _ = Describe("Default", func() {
	It("is four 8-GPU nodes in two racks of two", func() {
		s := fleet.Default()
		Expect(s.GPUs()).To(Equal(32))
		Expect(s.RackOf()).To(Equal(map[string]string{"n1": "rack-a", "n2": "rack-a", "n3": "rack-b", "n4": "rack-b"}))
	})

	It("renders nodes kwok manages, tainted, labeled with rack and block, with GPUs allocatable", func() {
		m, err := fleet.Default().Manifest()
		Expect(err).NotTo(HaveOccurred())
		objs, err := apply.Decode(m)
		Expect(err).NotTo(HaveOccurred())
		Expect(objs).To(HaveLen(4))
		n3 := objs[2]
		Expect(n3.GetName()).To(Equal("n3"))
		Expect(n3.GetAnnotations()).To(HaveKeyWithValue("kwok.x-k8s.io/node", "fake"))
		Expect(n3.GetLabels()).To(Equal(map[string]string{
			"type": "kwok", "kubernetes.io/hostname": "n3", "kubernetes.io/os": "linux", "kubernetes.io/arch": "amd64",
			"app.kubernetes.io/part-of":           "scheduler-capabilities-suite",
			"scs.arangogutierrez.github.io/rack":  "rack-b",
			"scs.arangogutierrez.github.io/block": "block-1",
		}))
		gpu, _, _ := unstructured.NestedString(n3.Object, "status", "allocatable", "nvidia.com/gpu")
		Expect(gpu).To(Equal("8"))
		taints, _, _ := unstructured.NestedSlice(n3.Object, "spec", "taints")
		Expect(taints).To(Equal([]any{map[string]any{"key": "kwok.x-k8s.io/node", "value": "fake", "effect": "NoSchedule"}}))
	})
})
