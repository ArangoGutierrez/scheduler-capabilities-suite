// Package recorder watches the suite's Pods, Events and (optionally) Kueue
// Workloads and writes every transition to events.jsonl. One mutex orders
// the writes, so seq and observed_at increase together.
package recorder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/labels"
)

// Options configure a Recorder.
type Options struct {
	RunID string
	// WorkloadGVR is Kueue's Workload resource; nil means not watched.
	WorkloadGVR *schema.GroupVersionResource
	// Now returns the observation time; nil means time.Now.
	Now func() time.Time
}

// Recorder writes the suite's object transitions as events.
type Recorder struct {
	cs   kubernetes.Interface
	dyn  dynamic.Interface
	opt  Options
	mu   sync.Mutex
	enc  *events.Encoder
	seq  int64
	err  error
	ns   map[string]nsInfo
	werr []time.Time
	stop context.CancelFunc
}

type nsInfo struct{ probe, tenant string }

// New returns a Recorder writing to w. dyn may be nil when opt.WorkloadGVR is nil.
func New(cs kubernetes.Interface, dyn dynamic.Interface, w io.Writer, opt Options) *Recorder {
	if opt.Now == nil {
		opt.Now = time.Now
	}
	return &Recorder{cs: cs, dyn: dyn, opt: opt, enc: events.NewEncoder(w), ns: map[string]nsInfo{}}
}

// SyncTimeout bounds how long Start waits for the watches to sync.
const SyncTimeout = 2 * time.Minute

// Start begins watching for as long as ctx lives (or until Stop) and returns
// once every watch has synced, or with an error after SyncTimeout.
func (r *Recorder) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	r.stop = cancel
	labeled := informers.NewSharedInformerFactoryWithOptions(r.cs, 0,
		informers.WithTweakListOptions(func(o *metav1.ListOptions) { o.LabelSelector = labels.Selector }))
	all := informers.NewSharedInformerFactory(r.cs, 0)

	nsInf := labeled.Core().V1().Namespaces().Informer()
	podInf := labeled.Core().V1().Pods().Informer()
	jobInf := labeled.Batch().V1().Jobs().Informer()
	evInf := all.Events().V1().Events().Informer()
	infs := []cache.SharedIndexInformer{nsInf, podInf, jobInf, evInf}

	if _, err := nsInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(o any) { r.namespace(o) },
		UpdateFunc: func(_, o any) { r.namespace(o) },
	}); err != nil {
		return err
	}
	if _, err := podInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(o any) { r.pod(nil, o.(*corev1.Pod)) },
		UpdateFunc: func(old, o any) { r.pod(old.(*corev1.Pod), o.(*corev1.Pod)) },
		DeleteFunc: func(o any) { r.podDeleted(o) },
	}); err != nil {
		return err
	}
	if _, err := jobInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(o any) { r.job(nil, o.(*batchv1.Job)) },
		UpdateFunc: func(old, o any) { r.job(old.(*batchv1.Job), o.(*batchv1.Job)) },
		DeleteFunc: func(o any) { r.jobDeleted(o) },
	}); err != nil {
		return err
	}
	if _, err := evInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(o any) { r.event(o.(*eventsv1.Event)) },
	}); err != nil {
		return err
	}
	var dynFactory dynamicinformer.DynamicSharedInformerFactory
	if r.opt.WorkloadGVR != nil {
		dynFactory = dynamicinformer.NewDynamicSharedInformerFactory(r.dyn, 0)
		wlInf := dynFactory.ForResource(*r.opt.WorkloadGVR).Informer()
		if _, err := wlInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    func(o any) { r.workload(nil, o.(*unstructured.Unstructured)) },
			UpdateFunc: func(old, o any) { r.workload(old.(*unstructured.Unstructured), o.(*unstructured.Unstructured)) },
			DeleteFunc: func(o any) { r.workloadDeleted(o) },
		}); err != nil {
			return err
		}
		infs = append(infs, wlInf)
	}
	for _, inf := range infs {
		if err := inf.SetWatchErrorHandlerWithContext(func(_ context.Context, _ *cache.Reflector, _ error) {
			r.mu.Lock()
			r.werr = append(r.werr, r.opt.Now())
			r.mu.Unlock()
		}); err != nil {
			return err
		}
	}
	labeled.Start(ctx.Done())
	all.Start(ctx.Done())
	if dynFactory != nil {
		dynFactory.Start(ctx.Done())
	}
	synced := make([]cache.InformerSynced, 0, len(infs))
	for _, inf := range infs {
		synced = append(synced, inf.HasSynced)
	}
	syncCtx, cancelSync := context.WithTimeout(ctx, SyncTimeout)
	defer cancelSync()
	if !cache.WaitForCacheSync(syncCtx.Done(), synced...) {
		return errors.New("recorder: watches did not sync")
	}
	return nil
}

// Flush writes buffered events and returns the first write error, if any.
func (r *Recorder) Flush() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.enc.Flush(); err != nil && r.err == nil {
		r.err = err
	}
	return r.err
}

// Stop ends every watch and flushes.
func (r *Recorder) Stop() error {
	if r.stop != nil {
		r.stop()
	}
	return r.Flush()
}

// WatchErrors returns when each watch error happened. A watch error means
// the informer relisted and the recorder may have missed a transition.
func (r *Recorder) WatchErrors() []time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Time(nil), r.werr...)
}

func (r *Recorder) emit(e events.Event, c change) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return
	}
	r.seq++
	e.Schema, e.Seq, e.RunID, e.ObservedAt = events.SchemaVersion, r.seq, r.opt.RunID, r.opt.Now().UTC()
	e.Transition, e.APITime, e.APITimePrecision = c.tr, c.apiTime, c.precision
	e.Node, e.Reason, e.Message = c.node, c.reason, c.message
	if err := r.enc.Encode(e); err != nil {
		r.err = fmt.Errorf("recorder: %w", err)
	}
}

func (r *Recorder) namespace(o any) {
	ns, ok := o.(*corev1.Namespace)
	if !ok {
		return
	}
	r.mu.Lock()
	r.ns[ns.Name] = nsInfo{probe: ns.Labels[labels.Probe], tenant: ns.Labels[labels.Tenant]}
	r.mu.Unlock()
}

func (r *Recorder) nsInfo(name string) (nsInfo, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	i, ok := r.ns[name]
	return i, ok
}

func podBase(p *corev1.Pod) events.Event {
	e := events.Event{
		Kind: "Pod", Namespace: p.Namespace, Name: p.Name, UID: string(p.UID),
		Probe: p.Labels[labels.Probe], Job: p.Labels[labels.Job], Tenant: p.Labels[labels.Tenant],
	}
	if p.Spec.Priority != nil {
		e.Priority = *p.Spec.Priority
	}
	for _, c := range p.Spec.Containers {
		if q, ok := c.Resources.Limits["nvidia.com/gpu"]; ok {
			e.GPUs += q.Value()
		}
	}
	return e
}

func (r *Recorder) pod(old, cur *corev1.Pod) {
	for _, c := range podChanges(old, cur) {
		r.emit(podBase(cur), c)
	}
}

func (r *Recorder) podDeleted(o any) {
	if d, ok := o.(cache.DeletedFinalStateUnknown); ok {
		o = d.Obj
	}
	p, ok := o.(*corev1.Pod)
	if !ok {
		return
	}
	c := change{tr: events.Deleted}
	if p.DeletionTimestamp != nil {
		c.apiTime, c.precision = secTime(*p.DeletionTimestamp), "s"
	}
	r.emit(podBase(p), c)
}

func (r *Recorder) event(ev *eventsv1.Event) {
	if ev.Regarding.Kind != "Pod" && ev.Regarding.Kind != "Workload" {
		return
	}
	info, ok := r.nsInfo(ev.Regarding.Namespace)
	if !ok {
		return
	}
	c, keep := eventChange(ev)
	if !keep {
		return
	}
	r.emit(events.Event{
		Kind: "Event", Namespace: ev.Regarding.Namespace, Name: ev.Regarding.Name, UID: string(ev.Regarding.UID),
		Probe: info.probe, Tenant: info.tenant,
	}, c)
}

// jobBase is a Job's event fields: its labels, and its GPUs as parallelism
// times the pod template's GPU limit.
func jobBase(j *batchv1.Job) events.Event {
	e := events.Event{
		Kind: "Job", Namespace: j.Namespace, Name: j.Name, UID: string(j.UID),
		Probe: j.Labels[labels.Probe], Job: j.Name, Tenant: j.Labels[labels.Tenant],
	}
	per := int64(0)
	for _, c := range j.Spec.Template.Spec.Containers {
		if q, ok := c.Resources.Limits["nvidia.com/gpu"]; ok {
			per += q.Value()
		}
	}
	n := int64(1)
	if j.Spec.Parallelism != nil {
		n = int64(*j.Spec.Parallelism)
	}
	e.GPUs = per * n
	return e
}

func (r *Recorder) job(old, cur *batchv1.Job) {
	for _, c := range jobChanges(old, cur) {
		r.emit(jobBase(cur), c)
	}
}

func (r *Recorder) jobDeleted(o any) {
	if d, ok := o.(cache.DeletedFinalStateUnknown); ok {
		o = d.Obj
	}
	j, ok := o.(*batchv1.Job)
	if !ok {
		return
	}
	c := change{tr: events.Deleted}
	if j.DeletionTimestamp != nil {
		c.apiTime, c.precision = secTime(*j.DeletionTimestamp), "s"
	}
	r.emit(jobBase(j), c)
}
