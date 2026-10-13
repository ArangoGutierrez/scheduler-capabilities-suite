// Package apply server-side applies multi-document YAML through client-go:
// discovery-backed RESTMapper, dynamic client, field manager "scs", force.
package apply

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
)

const FieldManager = "scs"

var crdGVR = schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}

// Applier holds one dynamic client and one resettable RESTMapper.
type Applier struct {
	dyn    dynamic.Interface
	mapper *restmapper.DeferredDiscoveryRESTMapper
}

func New(cfg *rest.Config) (*Applier, error) {
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &Applier{dyn: dyn, mapper: restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(dc))}, nil
}

// Decode splits multi-document YAML or JSON into objects, skipping empty documents.
func Decode(data []byte) ([]*unstructured.Unstructured, error) {
	dec := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	var out []*unstructured.Unstructured
	for {
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, err
		}
		if len(m) == 0 {
			continue
		}
		u := &unstructured.Unstructured{Object: m}
		if u.GetKind() == "" || u.GetAPIVersion() == "" {
			return nil, fmt.Errorf("document %d has no kind or apiVersion", len(out)+1)
		}
		out = append(out, u)
	}
}

// Apply applies every document in data. Namespaces and CRDs go first; after
// them it waits for each CRD to be Established and resets the RESTMapper, so
// a custom resource may appear anywhere in data. The rest keep file order.
func (a *Applier) Apply(ctx context.Context, data []byte, timeout time.Duration) ([]string, error) {
	objs, err := Decode(data)
	if err != nil {
		return nil, err
	}
	return a.ApplyObjects(ctx, objs, timeout)
}

// ApplyObjects applies decoded objects in the order Apply describes, so a
// caller may edit a manifest's objects (a ConfigMap's data, say) first.
func (a *Applier) ApplyObjects(ctx context.Context, objs []*unstructured.Unstructured, timeout time.Duration) ([]string, error) {
	var first, rest []*unstructured.Unstructured
	for _, o := range objs {
		gk := o.GroupVersionKind().GroupKind()
		if gk == (schema.GroupKind{Kind: "Namespace"}) || gk == (schema.GroupKind{Group: "apiextensions.k8s.io", Kind: "CustomResourceDefinition"}) {
			first = append(first, o)
		} else {
			rest = append(rest, o)
		}
	}
	var applied []string
	var crds []string
	for _, o := range first {
		id, err := a.applyOne(ctx, o, timeout)
		if err != nil {
			return applied, err
		}
		applied = append(applied, id)
		if o.GetKind() == "CustomResourceDefinition" {
			crds = append(crds, o.GetName())
		}
	}
	for _, name := range crds {
		if err := a.waitEstablished(ctx, name, timeout); err != nil {
			return applied, err
		}
	}
	if len(crds) > 0 {
		a.mapper.Reset()
	}
	for _, o := range rest {
		id, err := a.applyOne(ctx, o, timeout)
		if err != nil {
			return applied, err
		}
		applied = append(applied, id)
	}
	return applied, nil
}

func (a *Applier) applyOne(ctx context.Context, o *unstructured.Unstructured, timeout time.Duration) (string, error) {
	gvk := o.GroupVersionKind()
	var mapping *meta.RESTMapping
	// A NoMatch right after a CRD is Established is a stale discovery cache:
	// reset and retry until the timeout.
	err := wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, timeout, true, func(ctx context.Context) (bool, error) {
		m, err := a.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if meta.IsNoMatchError(err) {
			a.mapper.Reset()
			return false, nil
		}
		if err != nil {
			return false, err
		}
		mapping = m
		return true, nil
	})
	if err != nil {
		return "", fmt.Errorf("mapping %s: %w", gvk, err)
	}
	var ri dynamic.ResourceInterface
	if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		ns := o.GetNamespace()
		if ns == "" {
			ns = "default"
			o.SetNamespace(ns)
		}
		ri = a.dyn.Resource(mapping.Resource).Namespace(ns)
	} else {
		// kwok.yaml's FlowSchema carries metadata.namespace although
		// FlowSchema is cluster-scoped.
		o.SetNamespace("")
		ri = a.dyn.Resource(mapping.Resource)
	}
	id := fmt.Sprintf("%s/%s %s", mapping.Resource.Resource, o.GetNamespace(), o.GetName())
	if _, err := ri.Apply(ctx, o.GetName(), o, metav1.ApplyOptions{FieldManager: FieldManager, Force: true}); err != nil {
		return "", fmt.Errorf("apply %s: %w", id, err)
	}
	return id, nil
}

func (a *Applier) waitEstablished(ctx context.Context, name string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, timeout, true, func(ctx context.Context) (bool, error) {
		crd, err := a.dyn.Resource(crdGVR).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		conds, _, _ := unstructured.NestedSlice(crd.Object, "status", "conditions")
		for _, c := range conds {
			cm, _ := c.(map[string]any)
			if cm["type"] == "Established" && cm["status"] == "True" {
				return true, nil
			}
		}
		return false, nil
	})
}
