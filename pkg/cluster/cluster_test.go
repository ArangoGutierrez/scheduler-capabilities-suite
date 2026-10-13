package cluster

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
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

// fakeProvider records calls; it never touches Docker. Create fails unless
// createOK is set; Delete returns deleteErr.
type fakeProvider struct {
	existing  []string
	createOK  bool
	deleteErr error
	created   []string
	opts      []kindcluster.CreateOption
	deleted   []string
	deletedKC []string
	// onDelete, when set, runs inside Delete, before it returns
	onDelete func()
}

func (f *fakeProvider) List() ([]string, error) { return f.existing, nil }
func (f *fakeProvider) Create(name string, opts ...kindcluster.CreateOption) error {
	f.created = append(f.created, name)
	f.opts = opts
	if f.createOK {
		return nil
	}
	return errors.New("fake: no docker")
}
func (f *fakeProvider) Delete(name, kubeconfig string) error {
	f.deleted = append(f.deleted, name)
	f.deletedKC = append(f.deletedKC, kubeconfig)
	if f.onDelete != nil {
		f.onDelete()
	}
	return f.deleteErr
}
func (f *fakeProvider) CollectLogs(_, _ string) error { return nil }

// testConfig is SuiteConfig with the lock in a fresh directory, so specs
// never take the host-wide lock a live run holds.
func testConfig(name, kubeconfig string) Config {
	cfg := SuiteConfig(name, kubeconfig)
	cfg.LockDir = GinkgoT().TempDir()
	return cfg
}

// writeKubeconfig writes a kubeconfig the clients can be built from; nothing
// listens at its server, and building the clients does not dial.
func writeKubeconfig(path string) {
	GinkgoHelper()
	const kc = `apiVersion: v1
kind: Config
clusters:
- name: c
  cluster: {server: "https://127.0.0.1:1"}
contexts:
- name: kind-scs-vanilla
  context: {cluster: c, user: u}
current-context: kind-scs-vanilla
users:
- name: u
  user: {}
`
	Expect(os.WriteFile(path, []byte(kc), 0o600)).To(Succeed())
}

// kindOptions applies opts to a fresh kind ClusterOptions and returns it.
// kind keeps that type internal, so this reaches it by reflection: each
// CreateOption is a func taking *ClusterOptions. kind is pinned (v0.33.0);
// an upgrade that renames these fields makes the spec fail, never pass.
func kindOptions(opts []kindcluster.CreateOption) reflect.Value {
	Expect(opts).NotTo(BeEmpty())
	var o reflect.Value
	for _, opt := range opts {
		fn := reflect.ValueOf(opt)
		if !o.IsValid() {
			o = reflect.New(fn.Type().In(0).Elem())
		}
		Expect(fn.Call([]reflect.Value{o})[0].IsNil()).To(BeTrue())
	}
	return o.Elem()
}

var _ = Describe("Create", func() {
	It("refuses a cluster name that already exists, and creates or deletes nothing", func() {
		p := &fakeProvider{existing: []string{"bench-mokka", "scs-vanilla"}}
		_, err := Create(p, testConfig("scs-vanilla", "/tmp/kc"))
		Expect(err).To(MatchError(`kind cluster "scs-vanilla" already exists; the suite does not reuse or delete it. Remove it with: kind delete cluster --name scs-vanilla`))
		Expect(p.created).To(BeEmpty())
		Expect(p.deleted).To(BeEmpty())
	})

	It("asks kind to create a new name", func() {
		p := &fakeProvider{existing: []string{"bench-mokka"}}
		_, err := Create(p, testConfig("scs-kueue", "/tmp/kc"))
		Expect(err).To(MatchError(`create kind cluster "scs-kueue": fake: no docker`))
		Expect(p.created).To(Equal([]string{"scs-kueue"}))
		Expect(p.deleted).To(BeEmpty())
	})

	It("deletes the cluster it created when a later step fails, and returns that step's error", func() {
		// the fake creates nothing on disk, so the kubeconfig kind would have
		// written is missing and loading it fails after creation
		kc := filepath.Join(GinkgoT().TempDir(), "scs-vanilla.kubeconfig")
		p := &fakeProvider{existing: []string{"bench-mokka"}, createOK: true}
		_, err := Create(p, testConfig("scs-vanilla", kc))
		Expect(err).To(MatchError("stat " + kc + ": no such file or directory"))
		Expect(p.deleted).To(Equal([]string{"scs-vanilla"}))
		Expect(p.deletedKC).To(Equal([]string{kc}))
	})

	It("reports a failed cleanup alongside the original error", func() {
		kc := filepath.Join(GinkgoT().TempDir(), "scs-vanilla.kubeconfig")
		p := &fakeProvider{createOK: true, deleteErr: errors.New("fake: delete failed")}
		_, err := Create(p, testConfig("scs-vanilla", kc))
		Expect(err).To(MatchError("stat " + kc + ": no such file or directory\ndelete kind cluster \"scs-vanilla\": fake: delete failed"))
		Expect(p.deleted).To(Equal([]string{"scs-vanilla"}))
	})
})

var _ = Describe("Create's lock", func() {
	It("refuses a second run while one holds the profile's lock, before asking kind anything", func() {
		cfg := testConfig("scs-vanilla", filepath.Join(GinkgoT().TempDir(), "kc"))
		writeKubeconfig(cfg.Kubeconfig)
		first, err := Create(&fakeProvider{createOK: true}, cfg)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(first.Delete)
		p := &fakeProvider{createOK: true}
		_, err = Create(p, cfg)
		Expect(err).To(MatchError("another suite run holds " + filepath.Join(cfg.LockDir, "scs-vanilla.lock") + "; the suite runs one cluster per profile at a time"))
		Expect(p.created).To(BeEmpty())
		Expect(p.deleted).To(BeEmpty())
	})

	It("lets a new run in once the first cluster is deleted", func() {
		cfg := testConfig("scs-vanilla", filepath.Join(GinkgoT().TempDir(), "kc"))
		writeKubeconfig(cfg.Kubeconfig)
		first, err := Create(&fakeProvider{createOK: true}, cfg)
		Expect(err).NotTo(HaveOccurred())
		Expect(first.Delete()).To(Succeed())
		p := &fakeProvider{createOK: true}
		second, err := Create(p, cfg)
		Expect(err).NotTo(HaveOccurred())
		Expect(p.created).To(Equal([]string{"scs-vanilla"}))
		Expect(second.Delete()).To(Succeed())
	})

	It("asks kind to delete the cluster while it still holds the lock", func() {
		cfg := testConfig("scs-vanilla", filepath.Join(GinkgoT().TempDir(), "kc"))
		writeKubeconfig(cfg.Kubeconfig)
		p := &fakeProvider{createOK: true}
		// a second run that tries to start while kind is deleting the cluster
		var during error
		p.onDelete = func() { _, during = Create(&fakeProvider{createOK: true}, cfg) }
		c, err := Create(p, cfg)
		Expect(err).NotTo(HaveOccurred())
		Expect(c.Delete()).To(Succeed())
		Expect(p.deleted).To(Equal([]string{"scs-vanilla"}))
		Expect(p.deletedKC).To(Equal([]string{cfg.Kubeconfig}))
		Expect(during).To(MatchError("another suite run holds " + filepath.Join(cfg.LockDir, "scs-vanilla.lock") + "; the suite runs one cluster per profile at a time"))
	})

	It("returns kind's delete error and still releases the lock", func() {
		cfg := testConfig("scs-vanilla", filepath.Join(GinkgoT().TempDir(), "kc"))
		writeKubeconfig(cfg.Kubeconfig)
		c, err := Create(&fakeProvider{createOK: true, deleteErr: errors.New("fake: delete failed")}, cfg)
		Expect(err).NotTo(HaveOccurred())
		Expect(c.Delete()).To(MatchError("fake: delete failed"))
		p := &fakeProvider{createOK: true}
		second, err := Create(p, cfg)
		Expect(err).NotTo(HaveOccurred())
		Expect(p.created).To(Equal([]string{"scs-vanilla"}))
		Expect(second.Delete()).To(Succeed())
	})

	DescribeTable("releases the lock when Create fails",
		func(existing []string, createOK, kubeconfig bool, want string) {
			cfg := testConfig("scs-vanilla", filepath.Join(GinkgoT().TempDir(), "kc"))
			if kubeconfig {
				writeKubeconfig(cfg.Kubeconfig)
			}
			_, err := Create(&fakeProvider{existing: existing, createOK: createOK}, cfg)
			Expect(err).To(MatchError(strings.ReplaceAll(want, "KC", cfg.Kubeconfig)))
			writeKubeconfig(cfg.Kubeconfig)
			c, err := Create(&fakeProvider{createOK: true}, cfg)
			Expect(err).NotTo(HaveOccurred())
			Expect(c.Delete()).To(Succeed())
		},
		Entry("when kind's create fails", nil, false, true, `create kind cluster "scs-vanilla": fake: no docker`),
		Entry("when the name is taken", []string{"scs-vanilla"}, false, true,
			`kind cluster "scs-vanilla" already exists; the suite does not reuse or delete it. Remove it with: kind delete cluster --name scs-vanilla`),
		Entry("when connecting fails after kind created the cluster", nil, true, false, "stat KC: no such file or directory"),
	)

	It("refuses a config without a lock dir, before asking kind anything", func() {
		p := &fakeProvider{createOK: true}
		_, err := Create(p, Config{Name: "scs-vanilla", Kubeconfig: "/tmp/kc"})
		Expect(err).To(MatchError("cluster config has no lock dir"))
		Expect(p.created).To(BeEmpty())
	})
})

var _ = Describe("Create's kind options", func() {
	It("passes kind the pinned node image, the feature gates, the runtime config and the kubeconfig path", func() {
		p := &fakeProvider{}
		_, err := Create(p, testConfig("scs-vanilla", "/tmp/kc"))
		Expect(err).To(MatchError(`create kind cluster "scs-vanilla": fake: no docker`))
		o := kindOptions(p.opts)
		Expect(o.FieldByName("KubeconfigPath").String()).To(Equal("/tmp/kc"))
		Expect(o.FieldByName("Config").IsNil()).To(BeFalse())
		cfg := o.FieldByName("Config").Elem()
		nodes := cfg.FieldByName("Nodes")
		Expect(nodes.Len()).To(Equal(1))
		Expect(nodes.Index(0).FieldByName("Image").String()).To(Equal("kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5"))
		Expect(cfg.FieldByName("FeatureGates").Interface()).To(Equal(map[string]bool{"GenericWorkload": true, "TopologyAwareWorkloadScheduling": true}))
		Expect(cfg.FieldByName("RuntimeConfig").Interface()).To(Equal(map[string]string{"scheduling.k8s.io/v1beta1": "true"}))
	})
})

var _ = Describe("SuiteConfig", func() {
	It("pins the node image and turns on the Workload API in every component", func() {
		cfg := SuiteConfig("scs-vanilla", "/tmp/kc")
		Expect(cfg.LockDir).To(Equal("/tmp"))
		k := cfg.kind()
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
