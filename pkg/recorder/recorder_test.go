package recorder

import (
	"bytes"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/labels"
)

func TestRecorder(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "recorder") {
		t.Fatal("recorder specs failed")
	}
}

var t0 = time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

func mt(s int) metav1.Time { return metav1.NewTime(t0.Add(time.Duration(s) * time.Second)) }

func basePod() *corev1.Pod {
	prio := int32(1000)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p0", Namespace: "scs-gang", UID: "uid-p0", CreationTimestamp: mt(0),
			Labels: map[string]string{labels.PartOf: labels.PartOfValue, labels.Probe: "gang", labels.Job: "gang-a", labels.Tenant: "t1"}},
		Spec: corev1.PodSpec{Priority: &prio, Containers: []corev1.Container{{Name: "main",
			Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("8")}}}}},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}
}

func bound(p *corev1.Pod) *corev1.Pod {
	q := p.DeepCopy()
	q.Spec.NodeName = "n2"
	q.Status.Conditions = append(q.Status.Conditions, corev1.PodCondition{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: mt(1)})
	return q
}

func running(p *corev1.Pod) *corev1.Pod {
	q := p.DeepCopy()
	q.Status.Phase = corev1.PodRunning
	q.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "main", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: mt(2)}}}}
	return q
}

func succeeded(p *corev1.Pod) *corev1.Pod {
	q := p.DeepCopy()
	q.Status.Phase = corev1.PodSucceeded
	q.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "main", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{FinishedAt: mt(22), StartedAt: mt(22)}}}}
	return q
}

func trs(cs []change) []events.Transition {
	out := []events.Transition{}
	for _, c := range cs {
		out = append(out, c.tr)
	}
	return out
}

var _ = Describe("podChanges", func() {
	It("records creation with the creation timestamp", func() {
		cs := podChanges(nil, basePod())
		Expect(trs(cs)).To(Equal([]events.Transition{events.Created}))
		Expect(*cs[0].apiTime).To(Equal(t0))
		Expect(cs[0].precision).To(Equal("s"))
	})

	It("records binding with the node and the PodScheduled time", func() {
		cs := podChanges(basePod(), bound(basePod()))
		Expect(trs(cs)).To(Equal([]events.Transition{events.Scheduled}))
		Expect(cs[0].node).To(Equal("n2"))
		Expect(*cs[0].apiTime).To(Equal(t0.Add(time.Second)))
	})

	It("records running from the container's start, and success from its finish", func() {
		cs := podChanges(bound(basePod()), running(bound(basePod())))
		Expect(trs(cs)).To(Equal([]events.Transition{events.Running}))
		Expect(*cs[0].apiTime).To(Equal(t0.Add(2 * time.Second)))
		cs = podChanges(running(bound(basePod())), succeeded(running(bound(basePod()))))
		Expect(trs(cs)).To(Equal([]events.Transition{events.Succeeded}))
		Expect(*cs[0].apiTime).To(Equal(t0.Add(22 * time.Second)))
	})

	It("records every passed transition for a pod first seen already running", func() {
		Expect(trs(podChanges(nil, running(bound(basePod()))))).To(Equal([]events.Transition{events.Created, events.Scheduled, events.Running}))
	})

	It("records nothing for an update that changes no tracked field", func() {
		p := running(bound(basePod()))
		Expect(podChanges(p, p.DeepCopy())).To(BeEmpty())
	})

	It("records preemption by the scheduler and eviction by anyone else", func() {
		p := running(bound(basePod()))
		pre := p.DeepCopy()
		pre.Status.Conditions = append(pre.Status.Conditions, corev1.PodCondition{Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue,
			Reason: corev1.PodReasonPreemptionByScheduler, LastTransitionTime: mt(10)})
		Expect(trs(podChanges(p, pre))).To(Equal([]events.Transition{events.Preempted}))
		ev := p.DeepCopy()
		ev.Status.Conditions = append(ev.Status.Conditions, corev1.PodCondition{Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue,
			Reason: "EvictionByEvictionAPI", LastTransitionTime: mt(10)})
		cs := podChanges(p, ev)
		Expect(trs(cs)).To(Equal([]events.Transition{events.Evicted}))
		Expect(cs[0].reason).To(Equal("EvictionByEvictionAPI"))
	})

	It("records an unschedulable pod once, not on every retry", func() {
		u := basePod()
		u.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable,
			Message: "0/4 nodes are available", LastTransitionTime: mt(1)}}
		cs := podChanges(basePod(), u)
		Expect(trs(cs)).To(Equal([]events.Transition{events.Unschedulable}))
		Expect(cs[0].message).To(Equal("0/4 nodes are available"))
		again := u.DeepCopy()
		again.Status.Conditions[0].Message = "0/4 nodes are available: 4 Insufficient nvidia.com/gpu"
		Expect(podChanges(u, again)).To(BeEmpty())
	})
})

var _ = Describe("jobChanges", func() {
	three := int32(3)
	newJob := func() *batchv1.Job {
		return &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: "gang-a", Namespace: "scs-gang-t1", CreationTimestamp: mt(0),
				Labels: map[string]string{labels.Probe: "gang", labels.Tenant: "t1"}},
			Spec: batchv1.JobSpec{Parallelism: &three, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{
				Name: "main", Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("8")}}}}}}},
		}
	}
	It("records a Job's creation, with parallelism times GPUs per pod", func() {
		cs := jobChanges(nil, newJob())
		Expect(trs(cs)).To(Equal([]events.Transition{events.Created}))
		Expect(*cs[0].apiTime).To(Equal(t0))
		Expect(jobBase(newJob()).GPUs).To(Equal(int64(24)))
		Expect(jobBase(newJob()).Tenant).To(Equal("t1"))
	})
	It("records completion once, when Complete turns True", func() {
		done := newJob()
		done.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: mt(22)}}
		cs := jobChanges(newJob(), done)
		Expect(trs(cs)).To(Equal([]events.Transition{events.Succeeded}))
		Expect(*cs[0].apiTime).To(Equal(t0.Add(22 * time.Second)))
		Expect(jobChanges(done, done.DeepCopy())).To(BeEmpty())
	})
})

var _ = Describe("workloadChanges", func() {
	wl := func(conds ...map[string]any) *unstructured.Unstructured {
		cs := make([]any, 0, len(conds))
		for _, c := range conds {
			cs = append(cs, c)
		}
		return &unstructured.Unstructured{Object: map[string]any{
			"metadata": map[string]any{"name": "job-gang-a-1", "namespace": "scs-gang", "creationTimestamp": "2026-10-10T10:00:00Z"},
			"status":   map[string]any{"conditions": cs},
		}}
	}
	It("records each condition that turns True, once", func() {
		pending := wl()
		admitted := wl(map[string]any{"type": "QuotaReserved", "status": "True", "lastTransitionTime": "2026-10-10T10:00:03Z"},
			map[string]any{"type": "Admitted", "status": "True", "lastTransitionTime": "2026-10-10T10:00:03Z"})
		Expect(trs(workloadChanges(nil, pending))).To(Equal([]events.Transition{events.Created}))
		cs := workloadChanges(pending, admitted)
		Expect(trs(cs)).To(Equal([]events.Transition{events.QuotaReserved, events.Admitted}))
		Expect(*cs[1].apiTime).To(Equal(t0.Add(3 * time.Second)))
		Expect(workloadChanges(admitted, admitted)).To(BeEmpty())
	})
})

// syncBuffer is a bytes.Buffer safe for the recorder's writer and the test's reader.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

var _ = Describe("Recorder", func() {
	It("writes a labeled pod's life and a suite Event, attributed by labels", func(ctx SpecContext) {
		cs := fake.NewClientset()
		var out syncBuffer
		clock := t0
		var clockMu sync.Mutex
		r := New(cs, nil, &out, Options{RunID: "r1", Now: func() time.Time {
			clockMu.Lock()
			defer clockMu.Unlock()
			clock = clock.Add(time.Millisecond)
			return clock
		}})
		Expect(r.Start(ctx)).To(Succeed())
		DeferCleanup(r.Stop)

		_, err := cs.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "scs-gang",
			Labels: map[string]string{labels.PartOf: labels.PartOfValue, labels.Probe: "gang", labels.Tenant: "t1"}}}, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())
		pods := cs.CoreV1().Pods("scs-gang")
		p := basePod()
		_, err = pods.Create(ctx, p, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())
		// let each update reach the informer before the next one
		lines := func() int { Expect(r.Flush()).To(Succeed()); return bytes.Count([]byte(out.String()), []byte("\n")) }
		Eventually(lines).Should(Equal(1))
		_, err = pods.Update(ctx, bound(p), metav1.UpdateOptions{})
		Expect(err).NotTo(HaveOccurred())
		Eventually(lines).Should(Equal(2))
		_, err = pods.Update(ctx, running(bound(p)), metav1.UpdateOptions{})
		Expect(err).NotTo(HaveOccurred())
		Eventually(lines).Should(Equal(3))
		_, err = pods.Update(ctx, succeeded(running(bound(p))), metav1.UpdateOptions{})
		Expect(err).NotTo(HaveOccurred())
		Eventually(lines).Should(Equal(4))
		Expect(pods.Delete(ctx, "p0", metav1.DeleteOptions{})).To(Succeed())
		Eventually(lines).Should(Equal(5))
		_, err = cs.EventsV1().Events("scs-gang").Create(ctx, &eventsv1.Event{
			ObjectMeta: metav1.ObjectMeta{Name: "p0.1", Namespace: "scs-gang"},
			EventTime:  metav1.NewMicroTime(t0.Add(1500 * time.Millisecond)),
			Reason:     "Scheduled", Note: "Successfully assigned scs-gang/p0 to n2",
			Regarding: corev1.ObjectReference{Kind: "Pod", Namespace: "scs-gang", Name: "p0", UID: "uid-p0"},
		}, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())
		Eventually(lines).Should(Equal(6))

		got, err := events.Read(bytes.NewReader([]byte(out.String())))
		Expect(err).NotTo(HaveOccurred())
		var tr []events.Transition
		for i, e := range got {
			tr = append(tr, e.Transition)
			Expect(e.ObservedAt).To(Equal(t0.Add(time.Duration(i+1) * time.Millisecond)))
		}
		Expect(tr).To(Equal([]events.Transition{events.Created, events.Scheduled, events.Running, events.Succeeded, events.Deleted, events.Scheduled}))
		created := t0
		Expect(got[0]).To(Equal(events.Event{Schema: 1, Seq: 1, RunID: "r1", ObservedAt: t0.Add(time.Millisecond),
			APITime: &created, APITimePrecision: "s", Kind: "Pod", Namespace: "scs-gang", Name: "p0", UID: "uid-p0",
			Probe: "gang", Job: "gang-a", Tenant: "t1", Priority: 1000, GPUs: 8, Transition: events.Created}))
		Expect(got[1].Node).To(Equal("n2"))
		Expect(got[5].Kind).To(Equal("Event"))
		Expect(got[5].Probe).To(Equal("gang"))
		Expect(got[5].APITimePrecision).To(Equal("us"))
		Expect(*got[5].APITime).To(Equal(t0.Add(1500 * time.Millisecond)))
		Expect(r.WatchErrors()).To(BeEmpty())
	}, SpecTimeout(30*time.Second))

	It("ignores Events in namespaces the suite did not create", func(ctx SpecContext) {
		cs := fake.NewClientset()
		var out syncBuffer
		r := New(cs, nil, &out, Options{RunID: "r1"})
		Expect(r.Start(ctx)).To(Succeed())
		DeferCleanup(r.Stop)
		_, err := cs.EventsV1().Events("default").Create(ctx, &eventsv1.Event{
			ObjectMeta: metav1.ObjectMeta{Name: "x.1", Namespace: "default"}, Reason: "Scheduled",
			Regarding: corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "x"},
		}, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())
		Consistently(func() string { Expect(r.Flush()).To(Succeed()); return out.String() }, "300ms", "50ms").Should(BeEmpty())
	}, SpecTimeout(30*time.Second))
})
