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

// stamped records the fake server's clock read and the stamp it gave.
type stamped struct{ now, stamp time.Time }

// stampClock makes the fake API server do what a real one does on create:
// name the ConfigMap from its generateName and stamp it, here 3 s ahead of
// the host clock, truncated to the second as the server does. It leaves the
// object to the fake's tracker, so a later delete finds it.
func stampClock(cs *fake.Clientset) *stamped {
	s := &stamped{}
	cs.PrependReactor("create", "configmaps", func(a k8stesting.Action) (bool, runtime.Object, error) {
		cm := a.(k8stesting.CreateAction).GetObject().(*corev1.ConfigMap)
		cm.Name = cm.GenerateName + "x"
		s.now = time.Now()
		s.stamp = s.now.Add(3 * time.Second).Truncate(time.Second)
		cm.CreationTimestamp = metav1.NewTime(s.stamp)
		return false, nil, nil
	})
	return s
}

// expectOffset checks offset = stamp - host read. ClockOffset reads the host
// clock after start and before the server stamps, so the offset lies between
// the stamp minus the server's read and the stamp minus start. A fixed bound
// such as (2, 3] fails when the create itself crosses a second boundary.
func expectOffset(offset float64, start time.Time, s *stamped) {
	GinkgoHelper()
	Expect(offset).To(BeNumerically(">", 2))
	Expect(offset).To(BeNumerically(">=", s.stamp.Sub(s.now).Seconds()))
	Expect(offset).To(BeNumerically("<=", s.stamp.Sub(start).Seconds()))
}

var _ = Describe("ClockOffset", func() {
	It("measures the server's stamp minus the host time, with a labelled ConfigMap it then deletes", func(ctx SpecContext) {
		cs := fake.NewClientset()
		s := stampClock(cs)
		start := time.Now()
		offset, measured, err := ClockOffset(ctx, cs)
		Expect(err).NotTo(HaveOccurred())
		Expect(measured).To(BeTrue())
		expectOffset(offset, start, s)
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
		s := stampClock(cs)
		cs.PrependReactor("delete", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("etcdserver: request timed out")
		})
		start := time.Now()
		offset, measured, err := ClockOffset(ctx, cs)
		Expect(err).To(MatchError("delete clock ConfigMap scs-clock-x: etcdserver: request timed out"))
		Expect(measured).To(BeTrue())
		expectOffset(offset, start, s)
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
