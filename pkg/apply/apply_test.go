package apply

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/clientcmd"
)

func TestApply(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "apply") {
		t.Fatal("apply specs failed")
	}
}

// crBeforeCRD puts a custom resource BEFORE its CRD and Namespace: Apply
// must still create it, which needs the Established wait and the mapper reset.
const crBeforeCRD = `
apiVersion: spike.scs.io/v1
kind: Widget
metadata:
  name: w1
  namespace: spike-apply
spec:
  size: 3
---
apiVersion: v1
kind: Namespace
metadata:
  name: spike-apply
---
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: widgets.spike.scs.io
spec:
  group: spike.scs.io
  names: {kind: Widget, plural: widgets, singular: widget, listKind: WidgetList}
  scope: Namespaced
  versions:
  - name: v1
    served: true
    storage: true
    schema:
      openAPIV3Schema:
        type: object
        properties:
          spec:
            type: object
            properties:
              size: {type: integer}
`

var _ = Describe("Decode", func() {
	It("skips empty documents", func() {
		objs, err := Decode([]byte("---\n---\napiVersion: v1\nkind: Namespace\nmetadata: {name: a}\n---\n\n"))
		Expect(err).NotTo(HaveOccurred())
		Expect(objs).To(HaveLen(1))
		Expect(objs[0].GetName()).To(Equal("a"))
	})

	It("rejects a document without kind", func() {
		_, err := Decode([]byte("apiVersion: v1\nkind: Namespace\nmetadata: {name: a}\n---\napiVersion: v1\nmetadata: {name: b}\n"))
		Expect(err).To(MatchError("document 2 has no kind or apiVersion"))
	})
})

// This spec needs a live cluster: SCS_TEST_KUBECONFIG names its kubeconfig,
// whose current context must be one of the suite's kind clusters. make test
// excludes it by its label; run it with --label-filter=live.
var _ = Describe("Apply against a live cluster", func() {
	It("creates a custom resource that precedes its CRD, and re-applies without change", Label("live"), func(ctx SpecContext) {
		kc := os.Getenv("SCS_TEST_KUBECONFIG")
		if kc == "" {
			Skip("SCS_TEST_KUBECONFIG is not set")
		}
		raw, err := clientcmd.LoadFromFile(kc)
		Expect(err).NotTo(HaveOccurred())
		if !strings.HasPrefix(raw.CurrentContext, "kind-scs-") {
			Fail(fmt.Sprintf("SCS_TEST_KUBECONFIG must name a kind-scs-* context, not %s", raw.CurrentContext))
		}
		cfg, err := clientcmd.BuildConfigFromFlags("", kc)
		Expect(err).NotTo(HaveOccurred())
		a, err := New(cfg)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func(ctx SpecContext) {
			for _, o := range []struct {
				gvr  schema.GroupVersionResource
				name string
			}{
				{schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}, "spike-apply"},
				{schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}, "widgets.spike.scs.io"},
			} {
				err := a.dyn.Resource(o.gvr).Delete(ctx, o.name, metav1.DeleteOptions{})
				if !apierrors.IsNotFound(err) {
					Expect(err).NotTo(HaveOccurred(), "delete %s %s", o.gvr.Resource, o.name)
				}
			}
		}, NodeTimeout(time.Minute))
		got, err := a.Apply(ctx, []byte(crBeforeCRD), 60*time.Second)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]string{"namespaces/ spike-apply", "customresourcedefinitions/ widgets.spike.scs.io", "widgets/spike-apply w1"}))
		gvr := schema.GroupVersionResource{Group: "spike.scs.io", Version: "v1", Resource: "widgets"}
		w, err := a.dyn.Resource(gvr).Namespace("spike-apply").Get(ctx, "w1", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		m := w.GetManagedFields()
		Expect(m).To(HaveLen(1))
		Expect(m[0].Manager).To(Equal(FieldManager))
		Expect(m[0].Operation).To(Equal(metav1.ManagedFieldsOperationApply))
		rv := w.GetResourceVersion()
		_, err = a.Apply(context.Background(), []byte(crBeforeCRD), 60*time.Second)
		Expect(err).NotTo(HaveOccurred())
		w2, err := a.dyn.Resource(gvr).Namespace("spike-apply").Get(ctx, "w1", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(w2.GetResourceVersion()).To(Equal(rv))
	}, SpecTimeout(3*time.Minute))
})
