package recorder

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

var _ = Describe("Recorder relists", func() {
	It("records a relist after a pod watch ends with 410 Expired", func(ctx SpecContext) {
		cs := fake.NewClientset()
		// the first pod watch fails with 410 Expired once Start has returned;
		// the reflector then relists pods without calling the watch error handler
		release := make(chan struct{})
		var podLists, podWatches int32
		cs.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
			atomic.AddInt32(&podLists, 1)
			return false, nil, nil
		})
		cs.PrependWatchReactor("pods", func(k8stesting.Action) (bool, watch.Interface, error) {
			if atomic.AddInt32(&podWatches, 1) > 1 {
				return false, nil, nil
			}
			fw := watch.NewFake()
			go func() {
				<-release
				fw.Error(&metav1.Status{Status: metav1.StatusFailure, Code: 410, Reason: metav1.StatusReasonExpired, Message: "too old resource version"})
			}()
			return true, fw, nil
		})
		r := New(cs, nil, io.Discard, Options{RunID: "r1"})
		Expect(r.Start(ctx)).To(Succeed())
		DeferCleanup(r.Stop)
		close(release)
		// one list for the initial sync, one for the relist
		Eventually(func() int32 { return atomic.LoadInt32(&podLists) }, 10*time.Second).Should(Equal(int32(2)))
		Eventually(r.WatchErrors, 10*time.Second).Should(HaveLen(1))
	}, SpecTimeout(30*time.Second))
})

var _ = Describe("watchRelists", func() {
	var (
		r       *Recorder
		listErr error
		inner   *cache.ListWatch
	)
	BeforeEach(func() {
		listErr = nil
		r = New(fake.NewClientset(), nil, io.Discard, Options{RunID: "r1", Now: func() time.Time { return t0 }})
		inner = &cache.ListWatch{
			ListWithContextFunc: func(context.Context, metav1.ListOptions) (runtime.Object, error) {
				if listErr != nil {
					return nil, listErr
				}
				return &metav1.List{}, nil
			},
			WatchFuncWithContext: func(context.Context, metav1.ListOptions) (watch.Interface, error) {
				return watch.NewFake(), nil
			},
		}
	})
	sendInitial := func() metav1.ListOptions {
		yes := true
		return metav1.ListOptions{SendInitialEvents: &yes, ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan}
	}

	It("does not record a List before the initial sync", func(ctx SpecContext) {
		_, err := r.watchRelists(inner).ListWithContext(ctx, metav1.ListOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(r.WatchErrors()).To(BeEmpty())
	})

	It("records a List after the initial sync", func(ctx SpecContext) {
		r.synced.Store(true)
		_, err := r.watchRelists(inner).ListWithContext(ctx, metav1.ListOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(r.WatchErrors()).To(Equal([]time.Time{t0}))
	})

	It("does not record a List that fails after the initial sync", func(ctx SpecContext) {
		r.synced.Store(true)
		listErr = errors.New("injected list failure")
		_, err := r.watchRelists(inner).ListWithContext(ctx, metav1.ListOptions{})
		Expect(err).To(MatchError("injected list failure"))
		Expect(r.WatchErrors()).To(BeEmpty())
	})

	It("records a Watch that asks for the initial events again after the initial sync", func(ctx SpecContext) {
		r.synced.Store(true)
		_, err := r.watchRelists(inner).WatchWithContext(ctx, sendInitial())
		Expect(err).NotTo(HaveOccurred())
		Expect(r.WatchErrors()).To(Equal([]time.Time{t0}))
	})

	It("does not record a Watch from a resource version after the initial sync", func(ctx SpecContext) {
		r.synced.Store(true)
		_, err := r.watchRelists(inner).WatchWithContext(ctx, metav1.ListOptions{ResourceVersion: "42"})
		Expect(err).NotTo(HaveOccurred())
		Expect(r.WatchErrors()).To(BeEmpty())
	})

	It("does not record a Watch that asks for the initial events before the initial sync", func(ctx SpecContext) {
		_, err := r.watchRelists(inner).WatchWithContext(ctx, sendInitial())
		Expect(err).NotTo(HaveOccurred())
		Expect(r.WatchErrors()).To(BeEmpty())
	})

	It("forwards the wrapped client's watch-list opt-out, true and false", func() {
		type optOut interface{ IsWatchListSemanticsUnSupported() bool }
		// a fake clientset opts out of watch-list semantics
		w, ok := any(r.watchRelists(cache.ToListWatcherWithWatchListSemantics(inner, fake.NewClientset()))).(optOut)
		Expect(ok).To(BeTrue())
		Expect(w.IsWatchListSemanticsUnSupported()).To(BeTrue())
		// a client without the method does not opt out
		w, ok = any(r.watchRelists(cache.ToListWatcherWithWatchListSemantics(inner, struct{}{}))).(optOut)
		Expect(ok).To(BeTrue())
		Expect(w.IsWatchListSemanticsUnSupported()).To(BeFalse())
	})
})
