package cluster

import (
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// stampClock makes the fake API server do what a real one does on create:
// name the ConfigMap from its generateName and stamp it, here 3 s ahead of
// the host clock, truncated to the second as the server does. It leaves the
// object to the fake's tracker, so a later delete finds it.
func stampClock(cs *fake.Clientset) {
	cs.PrependReactor("create", "configmaps", func(a k8stesting.Action) (bool, runtime.Object, error) {
		cm := a.(k8stesting.CreateAction).GetObject().(*corev1.ConfigMap)
		cm.Name = cm.GenerateName + "x"
		cm.CreationTimestamp = metav1.NewTime(time.Now().Add(3 * time.Second).Truncate(time.Second))
		return false, nil, nil
	})
}

var _ = Describe("ClockOffset", func() {
	It("measures the server's stamp minus the host time, with a labelled ConfigMap it then deletes", func(ctx SpecContext) {
		cs := fake.NewClientset()
		stampClock(cs)
		offset, measured, err := ClockOffset(ctx, cs)
		Expect(err).NotTo(HaveOccurred())
		Expect(measured).To(BeTrue())
		// the stamp is now+3 s truncated to the second, so it lies in (2, 3]
		// seconds after the host time read just before the create
		Expect(offset).To(And(BeNumerically(">", 2), BeNumerically("<=", 3)))
		Expect(cs.Actions()).To(HaveLen(2))
		created := cs.Actions()[0].(k8stesting.CreateAction).GetObject().(*corev1.ConfigMap)
		Expect(created.Labels).To(Equal(map[string]string{"app.kubernetes.io/part-of": "scheduler-capabilities-suite"}))
		Expect(cs.Actions()[1].GetVerb()).To(Equal("delete"))
		left, err := cs.CoreV1().ConfigMaps("default").List(ctx, metav1.ListOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(left.Items).To(BeEmpty())
	})

	It("keeps the measurement when only the delete fails, and returns that error", func(ctx SpecContext) {
		cs := fake.NewClientset()
		stampClock(cs)
		cs.PrependReactor("delete", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("etcdserver: request timed out")
		})
		offset, measured, err := ClockOffset(ctx, cs)
		Expect(err).To(MatchError("delete clock ConfigMap scs-clock-x: etcdserver: request timed out"))
		Expect(measured).To(BeTrue())
		Expect(offset).To(And(BeNumerically(">", 2), BeNumerically("<=", 3)))
	})

	It("reports no measurement when the create fails", func(ctx SpecContext) {
		cs := fake.NewClientset()
		cs.PrependReactor("create", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("connection refused")
		})
		_, measured, err := ClockOffset(ctx, cs)
		Expect(err).To(MatchError("create clock ConfigMap: connection refused"))
		Expect(measured).To(BeFalse())
	})
})
