package cluster

import (
	"errors"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	kindcluster "sigs.k8s.io/kind/pkg/cluster"
)

func TestCluster(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "cluster") {
		t.Fatal("cluster specs failed")
	}
}

// fakeProvider records calls; it never touches Docker.
type fakeProvider struct {
	existing []string
	created  []string
	deleted  []string
}

func (f *fakeProvider) List() ([]string, error) { return f.existing, nil }
func (f *fakeProvider) Create(name string, _ ...kindcluster.CreateOption) error {
	f.created = append(f.created, name)
	return errors.New("fake: no docker")
}
func (f *fakeProvider) Delete(name, _ string) error   { f.deleted = append(f.deleted, name); return nil }
func (f *fakeProvider) CollectLogs(_, _ string) error { return nil }

var _ = Describe("Create", func() {
	It("refuses a cluster name that already exists, and creates or deletes nothing", func() {
		p := &fakeProvider{existing: []string{"bench-mokka", "scs-vanilla"}}
		_, err := Create(p, SuiteConfig("scs-vanilla", "/tmp/kc"))
		Expect(err).To(MatchError(`kind cluster "scs-vanilla" already exists; the suite does not reuse or delete it. Remove it with: kind delete cluster --name scs-vanilla`))
		Expect(p.created).To(BeEmpty())
		Expect(p.deleted).To(BeEmpty())
	})

	It("asks kind to create a new name", func() {
		p := &fakeProvider{existing: []string{"bench-mokka"}}
		_, err := Create(p, SuiteConfig("scs-kueue", "/tmp/kc"))
		Expect(err).To(MatchError(`create kind cluster "scs-kueue": fake: no docker`))
		Expect(p.created).To(Equal([]string{"scs-kueue"}))
	})
})

var _ = Describe("SuiteConfig", func() {
	It("pins the node image and turns on the Workload API in every component", func() {
		k := SuiteConfig("scs-vanilla", "/tmp/kc").kind()
		Expect(k.Nodes).To(HaveLen(1))
		Expect(k.Nodes[0].Image).To(Equal("kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5"))
		Expect(k.FeatureGates).To(Equal(map[string]bool{"GenericWorkload": true, "TopologyAwareWorkloadScheduling": true}))
		Expect(k.RuntimeConfig).To(Equal(map[string]string{"scheduling.k8s.io/v1beta1": "true"}))
	})
})

var _ = Describe("KeepSystemPodsOffFleet", func() {
	It("adds a type NotIn [kwok] node affinity to every kube-system DaemonSet", func(ctx SpecContext) {
		ready := appsv1.DaemonSetStatus{DesiredNumberScheduled: 1, UpdatedNumberScheduled: 1, NumberAvailable: 1}
		cs := fake.NewClientset(
			&appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "kindnet", Namespace: "kube-system"}, Status: ready},
			&appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "kube-proxy", Namespace: "kube-system"}, Status: ready},
		)
		Expect(KeepSystemPodsOffFleet(ctx, cs, 5*time.Second)).To(Succeed())
		for _, name := range []string{"kindnet", "kube-proxy"} {
			d, err := cs.AppsV1().DaemonSets("kube-system").Get(ctx, name, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			terms := d.Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
			Expect(terms).To(HaveLen(1))
			Expect(terms[0].MatchExpressions).To(HaveLen(1))
			Expect(terms[0].MatchExpressions[0].Key).To(Equal("type"))
			Expect(string(terms[0].MatchExpressions[0].Operator)).To(Equal("NotIn"))
			Expect(terms[0].MatchExpressions[0].Values).To(Equal([]string{"kwok"}))
		}
	}, SpecTimeout(30*time.Second))
})
