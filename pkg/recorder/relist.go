package recorder

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/watchlist"
)

// relistWatcher wraps an informer's ListerWatcher and records a relist
// whenever, after Start has synced, a List succeeds or a Watch that asks for
// the initial events again (a watch-list relist) starts. client-go relists
// after a 410 Expired watch without calling the watch error handler, so this
// is the only place every relist is seen. Failed calls are left to the watch
// error handler.
type relistWatcher struct {
	lw    cache.ListerWatcher
	inner cache.ListerWatcherWithContext
	r     *Recorder
}

func (r *Recorder) watchRelists(lw cache.ListerWatcher) *relistWatcher {
	return &relistWatcher{lw: lw, inner: cache.ToListerWatcherWithContext(lw), r: r}
}

func (w *relistWatcher) ListWithContext(ctx context.Context, opts metav1.ListOptions) (runtime.Object, error) {
	obj, err := w.inner.ListWithContext(ctx, opts)
	if err == nil && w.r.synced.Load() {
		w.r.relisted()
	}
	return obj, err
}

func (w *relistWatcher) WatchWithContext(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	wi, err := w.inner.WatchWithContext(ctx, opts)
	if err == nil && opts.SendInitialEvents != nil && *opts.SendInitialEvents && w.r.synced.Load() {
		w.r.relisted()
	}
	return wi, err
}

// List and Watch complete cache.ListerWatcher; the reflector calls the
// WithContext forms.
func (w *relistWatcher) List(opts metav1.ListOptions) (runtime.Object, error) {
	return w.ListWithContext(context.Background(), opts)
}

func (w *relistWatcher) Watch(opts metav1.ListOptions) (watch.Interface, error) {
	return w.WatchWithContext(context.Background(), opts)
}

// IsWatchListSemanticsUnSupported forwards the wrapped client's opt-out, so a
// fake clientset keeps its plain list and watch.
func (w *relistWatcher) IsWatchListSemanticsUnSupported() bool {
	return watchlist.DoesClientNotSupportWatchListSemantics(w.lw)
}

// relisted records a relist at the recorder's clock.
func (r *Recorder) relisted() {
	r.mu.Lock()
	r.werr = append(r.werr, r.opt.Now())
	r.mu.Unlock()
}

// informer builds an informer on lw, carrying client's watch-list opt-out,
// and records every relist after the initial sync.
func (r *Recorder) informer(client any, example runtime.Object, lw *cache.ListWatch) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(r.watchRelists(cache.ToListWatcherWithWatchListSemantics(lw, client)), example, 0, cache.Indexers{})
}

// listWatch adapts a typed client's List and Watch to a cache.ListWatch,
// with selector (when not empty) as every call's label selector.
func listWatch[L runtime.Object](
	list func(context.Context, metav1.ListOptions) (L, error),
	watchFn func(context.Context, metav1.ListOptions) (watch.Interface, error),
	selector string,
) *cache.ListWatch {
	sel := func(o metav1.ListOptions) metav1.ListOptions {
		if selector != "" {
			o.LabelSelector = selector
		}
		return o
	}
	return &cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, o metav1.ListOptions) (runtime.Object, error) {
			// a failed typed List returns a nil pointer, which must not reach
			// the reflector as a non-nil runtime.Object
			l, err := list(ctx, sel(o))
			if err != nil {
				return nil, err
			}
			return l, nil
		},
		WatchFuncWithContext: func(ctx context.Context, o metav1.ListOptions) (watch.Interface, error) {
			return watchFn(ctx, sel(o))
		},
	}
}
