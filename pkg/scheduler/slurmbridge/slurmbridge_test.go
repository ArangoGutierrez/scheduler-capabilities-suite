package slurmbridge_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/apply"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/claims"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/cluster"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/fleet"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler/slurmbridge"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

func TestSlurmBridge(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "slurmbridge") {
		t.Fatal("slurmbridge specs failed")
	}
}

var _ = Describe("Profile", func() {
	It("is named slurm-bridge, reads claims/slurm-bridge.yaml and watches no workload object", func() {
		p := slurmbridge.New()
		Expect(p.Name()).To(Equal("slurm-bridge"))
		Expect(p.ClaimsFile()).To(Equal("claims/slurm-bridge.yaml"))
		Expect(p.WorkloadGVR()).To(BeNil())
	})
})

var _ = Describe("Manifest", func() {
	// shasum -a 256 and wc -c of each file, run by hand on the files as
	// rendered or downloaded (see the build report).
	DescribeTable("serves each embedded file with its pinned sha256",
		func(name, sum string, size int) {
			data, got, err := slurmbridge.Manifest(name)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(sum))
			Expect(len(data)).To(Equal(size))
		},
		Entry(nil, "slurm-operator-crds.yaml", "8e851b2d42e38117fae32321bf0ae36a9d70f2f5c9d51fd8893bffb79bd26fa1", 127293),
		Entry(nil, "podgroups.yaml", "acf75c3eef75e1273f0d1ed53a4b230b558d8066a92acdddc10c9bc8c91ab42c", 5097),
		Entry(nil, "slurm-operator.yaml", "e170261e56f11486ea1248142e186fea6f344f2745e68402e05cd0bf7733c546", 6575),
		Entry(nil, "slurm.yaml", "e99d5a572d7804f991c70d5490fbbae82a02fdbeed9c58700ab3fbf0b3b51527", 4002),
		Entry(nil, "slurm-bridge.yaml", "b7645a8781f65c482307d30769c13008ed180922ab49181dbf118624176b62cd", 12661),
		Entry(nil, "token.yaml", "8dd1414d0dc51ff69c47d5874004e51a98357d5632cb5d6108e1952779cb4156", 284),
	)

	It("applies the files in dependency order: CRDs, operator, Slurm, bridge, then the bridge's Token", func() {
		Expect(slurmbridge.Files).To(Equal([]string{
			"slurm-operator-crds.yaml", "podgroups.yaml", "slurm-operator.yaml", "slurm.yaml", "slurm-bridge.yaml", "token.yaml",
		}))
	})
})

var _ = Describe("CheckSum", func() {
	// shasum -a 256 of "kind: ConfigMap\n" and of "Kind: ConfigMap\n", run by hand
	const sum = "bb6c7fb1ce4b8ac8baa8f6344623dd4602cf3d6ce859f9ff859a5a43787c5987"
	const changed = "cdd7ee20f9043b36baab42b5be801c811b51e6ae9427424ca1b9c2d112a72b34"
	sums := []byte("0000  other.yaml\n" + sum + "  slurm.yaml\n")

	It("accepts bytes that match their SHA256SUMS line", func() {
		got, err := slurmbridge.CheckSum("slurm.yaml", []byte("kind: ConfigMap\n"), sums)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(sum))
	})
	It("refuses a file with one byte changed, naming both sums", func() {
		_, err := slurmbridge.CheckSum("slurm.yaml", []byte("Kind: ConfigMap\n"), sums)
		Expect(err).To(MatchError("slurm-bridge: slurm.yaml has sha256 " + changed + ", SHA256SUMS says " + sum))
	})
	It("refuses a file SHA256SUMS does not list", func() {
		_, err := slurmbridge.CheckSum("token.yaml", []byte("kind: ConfigMap\n"), sums)
		Expect(err).To(MatchError("slurm-bridge: token.yaml is not listed in SHA256SUMS"))
	})
})

var _ = Describe("KeyMaterial", func() {
	It("finds no key or certificate in any file under manifests/, and scans every one of them", func() {
		var scanned []string
		err := filepath.WalkDir("manifests", func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return err
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			scanned = append(scanned, path)
			Expect(slurmbridge.KeyMaterial(path, data)).To(BeEmpty(), path)
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		sort.Strings(scanned)
		Expect(scanned).To(Equal([]string{
			"manifests/SHA256SUMS",
			"manifests/podgroups.yaml",
			"manifests/slurm-bridge.yaml",
			"manifests/slurm-operator-crds.yaml",
			"manifests/slurm-operator.yaml",
			"manifests/slurm.yaml",
			"manifests/token.yaml",
			"manifests/values/slurm-bridge.yaml",
			"manifests/values/slurm-operator.yaml",
			"manifests/values/slurm.yaml",
		}))
	})

	It("finds what the charts would have rendered: a Secret, a PEM block, a caBundle and key data", func() {
		// The shape of the slurm chart's slurm-auth-slurm Secret and of the
		// operator's webhook certificate with cert-manager off, with fake values.
		planted := strings.Join([]string{
			"apiVersion: v1",
			"kind: Secret",
			"metadata: {name: slurm-auth-slurm}",
			"data:",
			"  slurm.key: c2VjcmV0",
			"  \"tls.crt\": c2VjcmV0",
			"---",
			"webhooks:",
			"- clientConfig:",
			"    caBundle: c2VjcmV0",
			"stringData:",
			"  ca.crt: |",
			"    -----BEGIN " + "CERTIFICATE-----",
			"",
		}, "\n")
		Expect(slurmbridge.KeyMaterial("planted.yaml", []byte(planted))).To(Equal([]string{
			"planted.yaml:2: Secret object",
			"planted.yaml:5: slurm.key data",
			"planted.yaml:6: tls.crt data",
			"planted.yaml:10: caBundle",
			"planted.yaml:12: ca.crt data",
			"planted.yaml:13: PEM block",
		}))
	})

	It("does not mistake a reference to a key for the key", func() {
		ref := "slurmKeyRef:\n  name: slurm-auth-slurm\n  key: slurm.key\njwtKeyRef:\n  key: jwt.key\n"
		Expect(slurmbridge.KeyMaterial("ref.yaml", []byte(ref))).To(BeEmpty())
	})
})

var _ = Describe("NewKey", func() {
	It("encodes 768 random bytes as 1024 base64 characters", func() {
		key, err := slurmbridge.NewKey(bytes.NewReader(bytes.Repeat([]byte{0xff}, 768)))
		Expect(err).NotTo(HaveOccurred())
		Expect(key).To(Equal(strings.Repeat("/", 1024)))
	})
	It("fails rather than use a short key", func() {
		_, err := slurmbridge.NewKey(bytes.NewReader(make([]byte, 767)))
		Expect(err).To(MatchError("unexpected EOF"))
	})
})

var _ = Describe("InstallObjects", func() {
	ids := func(objs []*unstructured.Unstructured) []string {
		out := make([]string, 0, len(objs))
		for _, o := range objs {
			out = append(out, o.GetKind()+" "+o.GetNamespace()+"/"+o.GetName())
		}
		return out
	}
	find := func(objs []*unstructured.Unstructured, kind, name string) *unstructured.Unstructured {
		for _, o := range objs {
			if o.GetKind() == kind && o.GetName() == name {
				return o
			}
		}
		return nil
	}
	// keyOf decodes a generated Secret's one data key.
	keyOf := func(o *unstructured.Unstructured, key string) string {
		data, found, err := unstructured.NestedStringMap(o.Object, "data")
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		Expect(data).To(HaveLen(1))
		raw, err := base64.StdEncoding.DecodeString(data[key])
		Expect(err).NotTo(HaveOccurred())
		return string(raw)
	}
	// random counts modulo the prime 251, so its two 768-byte halves differ.
	random := func() *bytes.Reader {
		b := make([]byte, 2*768)
		for i := range b {
			b[i] = byte(i % 251)
		}
		return bytes.NewReader(b)
	}

	It("lists the namespaces, the generated Secrets, every embedded object in file order and the kwok Stage", func() {
		objs, _, err := slurmbridge.InstallObjects(random())
		Expect(err).NotTo(HaveOccurred())
		// The objects of each file listed with yq (see the build report).
		Expect(ids(objs)).To(Equal([]string{
			"Namespace /slinky",
			"Namespace /slurm",
			"Secret slurm/slurm-auth-slurm",
			"Secret slurm/slurm-auth-jwt",
			"CustomResourceDefinition /accountings.slinky.slurm.net",
			"CustomResourceDefinition /controllers.slinky.slurm.net",
			"CustomResourceDefinition /loginsets.slinky.slurm.net",
			"CustomResourceDefinition /nodesets.slinky.slurm.net",
			"CustomResourceDefinition /restapis.slinky.slurm.net",
			"CustomResourceDefinition /tokens.slinky.slurm.net",
			"CustomResourceDefinition /podgroups.scheduling.x-k8s.io",
			"ServiceAccount slinky/slurm-operator",
			"ClusterRole slinky/slurm-operator",
			"ClusterRoleBinding /slurm-operator",
			"Service slinky/slurm-operator",
			"Deployment slinky/slurm-operator",
			"ConfigMap slurm/slurm-config-extra",
			"Controller slurm/slurm",
			"RestApi slurm/slurm",
			"ServiceAccount slurm/slurm-bridge-controllers",
			"ServiceAccount slurm/slurm-bridge-scheduler",
			"ConfigMap slurm/slurm-bridge-config",
			"ConfigMap slurm/scheduler-config",
			"ClusterRole slurm/slurm-bridge-controllers",
			"ClusterRole /slurm-bridge-scheduler",
			"ClusterRoleBinding /slurm-bridge-controllers",
			"ClusterRoleBinding /slurm-bridge-scheduler",
			"RoleBinding kube-system/slurm-bridge-scheduler::extension-apiserver-authentication-reader",
			"Service slurm/slurm-bridge-controllers",
			"Deployment slurm/slurm-bridge-controllers",
			"Deployment slurm/slurm-bridge-scheduler",
			"Token slurm/slurm-bridge-token",
			"Stage /pod-fail-on-delete",
		}))
	})

	It("returns each embedded file's verified sha256", func() {
		_, sums, err := slurmbridge.InstallObjects(random())
		Expect(err).NotTo(HaveOccurred())
		Expect(sums).To(Equal(map[string]string{
			"slurm-operator-crds.yaml": "8e851b2d42e38117fae32321bf0ae36a9d70f2f5c9d51fd8893bffb79bd26fa1",
			"podgroups.yaml":           "acf75c3eef75e1273f0d1ed53a4b230b558d8066a92acdddc10c9bc8c91ab42c",
			"slurm-operator.yaml":      "e170261e56f11486ea1248142e186fea6f344f2745e68402e05cd0bf7733c546",
			"slurm.yaml":               "e99d5a572d7804f991c70d5490fbbae82a02fdbeed9c58700ab3fbf0b3b51527",
			"slurm-bridge.yaml":        "b7645a8781f65c482307d30769c13008ed180922ab49181dbf118624176b62cd",
			"token.yaml":               "8dd1414d0dc51ff69c47d5874004e51a98357d5632cb5d6108e1952779cb4156",
		}))
	})

	It("generates slurm.key and jwt.key from the reader, each 1024 characters and different", func() {
		objs, _, err := slurmbridge.InstallObjects(random())
		Expect(err).NotTo(HaveOccurred())
		slurmKey := keyOf(find(objs, "Secret", "slurm-auth-slurm"), "slurm.key")
		jwtKey := keyOf(find(objs, "Secret", "slurm-auth-jwt"), "jwt.key")
		Expect(slurmKey).To(HaveLen(1024))
		Expect(jwtKey).To(HaveLen(1024))
		Expect(slurmKey).NotTo(Equal(jwtKey))
		// the first three bytes of the reader, 0x00 0x01 0x02, in base64
		Expect(slurmKey[:4]).To(Equal("AAEC"))
	})

	It("names the Secret it could not fill when the random source runs dry", func() {
		_, _, err := slurmbridge.InstallObjects(bytes.NewReader(make([]byte, 1000)))
		Expect(err).To(MatchError("slurm-bridge: generate jwt.key: unexpected EOF"))
	})
})

var _ = Describe("FailOnDelete", func() {
	decode := func() *unstructured.Unstructured {
		objs, err := apply.Decode([]byte(slurmbridge.FailOnDelete))
		Expect(err).NotTo(HaveOccurred())
		Expect(objs).To(HaveLen(1))
		return objs[0]
	}

	It("selects a deleted Pending or Running pod that still carries slurm-bridge's finalizer", func() {
		s := decode()
		Expect(s.GetAPIVersion() + " " + s.GetKind() + " " + s.GetName()).To(Equal("kwok.x-k8s.io/v1alpha1 Stage pod-fail-on-delete"))
		sel, found, err := unstructured.NestedSlice(s.Object, "spec", "selector", "matchExpressions")
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		Expect(sel).To(Equal([]any{
			map[string]any{"key": ".metadata.deletionTimestamp", "operator": "Exists"},
			map[string]any{"key": ".metadata.finalizers.[]", "operator": "In", "values": []any{"scheduler.slinky.slurm.net/finalizer"}},
			map[string]any{"key": ".status.phase", "operator": "In", "values": []any{"Pending", "Running"}},
		}))
	})

	It("moves the pod to Failed, not Succeeded, so a deleted victim reads as evicted", func() {
		tmpl, found, err := unstructured.NestedString(decode().Object, "spec", "next", "statusTemplate")
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		lines := strings.Split(strings.TrimSpace(tmpl), "\n")
		Expect(lines[len(lines)-1]).To(Equal("phase: Failed"))
		Expect(tmpl).NotTo(ContainSubstring("Succeeded"))
	})
})

var _ = Describe("NodePatch", func() {
	It("annotates the partitions and the rack's leaf switch and labels the node external, in one patch", func() {
		p, err := slurmbridge.NodePatch(fleet.Node{Name: "n1", Rack: "rack-a", Block: "block-1", GPUs: 8})
		Expect(err).NotTo(HaveOccurred())
		Expect(string(p)).To(Equal(`{"metadata":{"annotations":{"scheduler.slinky.slurm.net/external-node-partitions":"slurm-bridge,slurm-bridge-preempt","topology.slinky.slurm.net/spec":"topo-switch:s1"},"labels":{"scheduler.slinky.slurm.net/external-node":"true"}}}`))
	})
	It("puts rack-b on leaf switch s2", func() {
		p, err := slurmbridge.NodePatch(fleet.Node{Name: "n3", Rack: "rack-b", Block: "block-1", GPUs: 8})
		Expect(err).NotTo(HaveOccurred())
		Expect(string(p)).To(ContainSubstring(`"topology.slinky.slurm.net/spec":"topo-switch:s2"`))
	})
	It("refuses a rack with no leaf switch in topology.yaml", func() {
		_, err := slurmbridge.NodePatch(fleet.Node{Name: "n9", Rack: "rack-z", Block: "block-1", GPUs: 8})
		Expect(err).To(MatchError("slurm-bridge: node n9 is in rack rack-z, which has no leaf switch in topology.yaml"))
	})
})

var _ = Describe("RegisterNodes", func() {
	It("sends each fleet node exactly one merge patch holding the annotations and the label together", func(ctx SpecContext) {
		var nodes []runtime.Object
		for _, n := range fleet.Default().Nodes {
			nodes = append(nodes, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: n.Name}})
		}
		cs := fake.NewClientset(nodes...)
		type patch struct {
			node string
			typ  types.PatchType
			body string
		}
		var patches []patch
		cs.PrependReactor("patch", "nodes", func(a k8stesting.Action) (bool, runtime.Object, error) {
			pa := a.(k8stesting.PatchAction)
			patches = append(patches, patch{pa.GetName(), pa.GetPatchType(), string(pa.GetPatch())})
			return false, nil, nil
		})
		Expect(slurmbridge.RegisterNodes(ctx, cs, fleet.Default())).To(Succeed())
		body := func(sw string) string {
			return `{"metadata":{"annotations":{"scheduler.slinky.slurm.net/external-node-partitions":"slurm-bridge,slurm-bridge-preempt","topology.slinky.slurm.net/spec":"topo-switch:` + sw + `"},"labels":{"scheduler.slinky.slurm.net/external-node":"true"}}}`
		}
		Expect(patches).To(Equal([]patch{
			{"n1", types.MergePatchType, body("s1")},
			{"n2", types.MergePatchType, body("s1")},
			{"n3", types.MergePatchType, body("s2")},
			{"n4", types.MergePatchType, body("s2")},
		}))
		n3, err := cs.CoreV1().Nodes().Get(ctx, "n3", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(n3.Labels).To(Equal(map[string]string{"scheduler.slinky.slurm.net/external-node": "true"}))
		Expect(n3.Annotations).To(HaveKeyWithValue("topology.slinky.slurm.net/spec", "topo-switch:s2"))
	})
})

var _ = Describe("RestAPI", func() {
	// server answers the token Secret and slurmrestd through the service
	// proxy, recording each request's path and slurmrestd token header.
	server := func(status int, body string) (*httptest.Server, *[]string) {
		var seen []string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			seen = append(seen, r.URL.Path+" token="+r.Header.Get("X-SLURM-USER-TOKEN"))
			w.Header().Set("Content-Type", "application/json")
			if r.URL.Path == "/api/v1/namespaces/slurm/secrets/slurm-bridge-token" {
				_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"Secret","metadata":{"name":"slurm-bridge-token","namespace":"slurm"},"data":{"auth-token":"` +
					base64.StdEncoding.EncodeToString([]byte("jwt-for-slurm")) + `"}}`))
				return
			}
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
		}))
		DeferCleanup(srv.Close)
		return srv, &seen
	}
	clientset := func(srv *httptest.Server) kubernetes.Interface {
		cs, err := kubernetes.NewForConfig(&rest.Config{Host: srv.URL})
		Expect(err).NotTo(HaveOccurred())
		return cs
	}

	It("reaches slurmrestd through the API server's service proxy with the bridge's token", func(ctx SpecContext) {
		srv, seen := server(http.StatusOK, `{"nodes":[]}`)
		data, err := slurmbridge.RestAPI(clientset(srv))(ctx, "/slurm/v0.0.44/nodes")
		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).To(Equal(`{"nodes":[]}`))
		Expect(*seen).To(Equal([]string{
			"/api/v1/namespaces/slurm/secrets/slurm-bridge-token token=",
			"/api/v1/namespaces/slurm/services/slurm-restapi:6820/proxy/slurm/v0.0.44/nodes token=jwt-for-slurm",
		}))
	})

	It("returns the proxy's error, naming the path", func(ctx SpecContext) {
		// what the API server answers while slurmrestd has no ready pod
		srv, _ := server(http.StatusServiceUnavailable, `{"kind":"Status","apiVersion":"v1","status":"Failure",`+
			`"message":"no endpoints available for service \"slurm-restapi\"","reason":"ServiceUnavailable","code":503}`)
		_, err := slurmbridge.RestAPI(clientset(srv))(ctx, "/slurm/v0.0.44/nodes")
		Expect(err).To(MatchError(`slurmrestd /slurm/v0.0.44/nodes: no endpoints available for service "slurm-restapi"`))
	})
})

// nodesJSON is a slurmrestd v0.0.44 nodes reply, trimmed to the fields read.
func nodesJSON(nodes ...string) []byte {
	return []byte(`{"nodes":[` + strings.Join(nodes, ",") + `]}`)
}

func slurmNode(name, state string, partitions ...string) string {
	return `{"name":"` + name + `","state":["` + state + `","DYNAMIC_NORM","EXTERNAL"],"partitions":["` + strings.Join(partitions, `","`) + `"]}`
}

var _ = Describe("NodesReady", func() {
	both := []string{"slurm-bridge", "slurm-bridge-preempt"}
	names := []string{"n1", "n2"}

	It("accepts nodes that are idle in both partitions", func() {
		Expect(slurmbridge.NodesReady(nodesJSON(slurmNode("n1", "IDLE", both...), slurmNode("n2", "IDLE", both...)), names)).To(Succeed())
	})
	It("names a node Slurm does not have", func() {
		err := slurmbridge.NodesReady(nodesJSON(slurmNode("n1", "IDLE", both...)), names)
		Expect(err).To(MatchError("Slurm has no node n2"))
	})
	It("names a node that is not idle", func() {
		err := slurmbridge.NodesReady(nodesJSON(slurmNode("n1", "IDLE", both...), slurmNode("n2", "ALLOCATED", both...)), names)
		Expect(err).To(MatchError("Slurm node n2 is ALLOCATED,DYNAMIC_NORM,EXTERNAL, not IDLE"))
	})
	It("names a node outside the preemption partition, which a node labelled before its annotation ends up", func() {
		err := slurmbridge.NodesReady(nodesJSON(slurmNode("n1", "IDLE", both...), slurmNode("n2", "IDLE", "slurm-bridge")), names)
		Expect(err).To(MatchError("Slurm node n2 is in partitions slurm-bridge, want slurm-bridge and slurm-bridge-preempt"))
	})
})

var _ = Describe("WaitReady", func() {
	deployment := func(ns, name string, available corev1.ConditionStatus) runtime.Object {
		return &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Status:     appsv1.DeploymentStatus{Conditions: []appsv1.DeploymentCondition{{Type: appsv1.DeploymentAvailable, Status: available}}},
		}
	}
	tainted := func(name string) runtime.Object {
		return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: corev1.NodeSpec{Taints: []corev1.Taint{
			{Key: "kwok.x-k8s.io/node", Value: "fake", Effect: corev1.TaintEffectNoSchedule},
			{Key: "slinky.slurm.net/managed-node", Value: "slurm-bridge-scheduler", Effect: corev1.TaintEffectNoExecute},
		}}}
	}
	untainted := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n2"}}
	// objects is a ready install with the scheduler's Available condition
	// as given and node n2 as given.
	objects := func(scheduler corev1.ConditionStatus, n2 runtime.Object) []runtime.Object {
		return []runtime.Object{
			deployment("slinky", "slurm-operator", corev1.ConditionTrue),
			deployment("slurm", "slurm-bridge-scheduler", scheduler),
			deployment("slurm", "slurm-bridge-controllers", corev1.ConditionTrue),
			tainted("n1"), n2,
		}
	}
	both := []string{"slurm-bridge", "slurm-bridge-preempt"}
	ready := nodesJSON(slurmNode("n1", "IDLE", both...), slurmNode("n2", "IDLE", both...))
	answer := func(paths *[]string, data []byte, err error) slurmbridge.RestGet {
		return func(_ context.Context, path string) ([]byte, error) {
			*paths = append(*paths, path)
			return data, err
		}
	}
	names := []string{"n1", "n2"}

	It("returns once the Deployments are available, Slurm has every node idle in both partitions and the bridge tainted them", func(ctx SpecContext) {
		var paths []string
		cs := fake.NewClientset(objects(corev1.ConditionTrue, tainted("n2"))...)
		Expect(slurmbridge.WaitReady(ctx, cs, answer(&paths, ready, nil), names, 10*time.Millisecond, 5*time.Second)).To(Succeed())
		Expect(paths).To(Equal([]string{"/slurm/v0.0.44/nodes"}))
	})
	It("names a Deployment that is not available", func(ctx SpecContext) {
		var paths []string
		cs := fake.NewClientset(objects(corev1.ConditionFalse, tainted("n2"))...)
		err := slurmbridge.WaitReady(ctx, cs, answer(&paths, ready, nil), names, 10*time.Millisecond, 200*time.Millisecond)
		Expect(err).To(MatchError("slurm-bridge not ready after 200ms: deployment slurm/slurm-bridge-scheduler is not available"))
		Expect(paths).To(BeEmpty())
	})
	It("names the last slurmrestd error", func(ctx SpecContext) {
		var paths []string
		cs := fake.NewClientset(objects(corev1.ConditionTrue, tainted("n2"))...)
		err := slurmbridge.WaitReady(ctx, cs, answer(&paths, nil, errors.New("no endpoints available for service \"slurm-restapi\"")), names, 10*time.Millisecond, 200*time.Millisecond)
		Expect(err).To(MatchError(`slurm-bridge not ready after 200ms: list Slurm nodes: no endpoints available for service "slurm-restapi"`))
	})
	It("waits for the bridge's NoExecute taint, which it adds once a node is registered", func(ctx SpecContext) {
		var paths []string
		cs := fake.NewClientset(objects(corev1.ConditionTrue, untainted)...)
		err := slurmbridge.WaitReady(ctx, cs, answer(&paths, ready, nil), names, 10*time.Millisecond, 200*time.Millisecond)
		Expect(err).To(MatchError("slurm-bridge not ready after 200ms: node n2 does not carry taint slinky.slurm.net/managed-node=slurm-bridge-scheduler:NoExecute"))
	})
})

// jobsJSON is a slurmrestd v0.0.44 jobs reply, trimmed to the fields read.
// admin_comment is where slurm-bridge records the pods of a job.
const jobsJSON = `{"jobs":[
 {"job_id":3,"job_state":["FAILED"],"admin_comment":"{\"pods\":[\"scs-gang-t1/gang-a-x\"]}"},
 {"job_id":7,"job_state":["RUNNING"],"admin_comment":"{\"pods\":[\"scs-gang-t1/gang-a-x\",\"scs-gang-t1/gang-a-y\"]}"},
 {"job_id":8,"job_state":["COMPLETED"],"admin_comment":"{\"pods\":[\"scs-gang-t1/gang-b-x\"]}"},
 {"job_id":9,"job_state":["PENDING"],"admin_comment":"{\"pods\":[\"scs-gang-t1/gang-b-y\"]}"},
 {"job_id":10,"job_state":["RUNNING"],"admin_comment":"{\"pods\":[\"scs-priority-t1/high-x\"]}"},
 {"job_id":11,"job_state":["RUNNING"]},
 {"job_id":12,"job_state":["CANCELLED"],"admin_comment":"{\"pods\":[\"scs-gang-t1/gang-b-z\"]}"}
]}`

var _ = Describe("LiveJobs", func() {
	It("lists the pending and running jobs whose pods are in the probe's namespaces", func() {
		live, err := slurmbridge.LiveJobs([]byte(jobsJSON), []string{"scs-gang-t1"})
		Expect(err).NotTo(HaveOccurred())
		Expect(live).To(Equal([]string{"7 (RUNNING)", "9 (PENDING)"}))
	})
	It("finds none for a probe whose jobs all ended", func() {
		live, err := slurmbridge.LiveJobs([]byte(jobsJSON), []string{"scs-topology-t1"})
		Expect(err).NotTo(HaveOccurred())
		Expect(live).To(BeEmpty())
	})
	It("refuses a reply that is not JSON", func() {
		_, err := slurmbridge.LiveJobs([]byte("<html>"), []string{"scs-gang-t1"})
		Expect(err).To(MatchError("slurmrestd jobs reply: invalid character '<' looking for beginning of value"))
	})
})

var _ = Describe("WaitJobsGone", func() {
	s := scheduler.Scenario{Probe: "gang", Tenants: []scheduler.Tenant{{Name: "t1", Namespace: "scs-gang-t1", EntitlementGPUs: 32}}}

	It("returns once Slurm has no pending or running job of the probe", func(ctx SpecContext) {
		var paths []string
		replies := [][]byte{[]byte(jobsJSON), []byte(`{"jobs":[{"job_id":7,"job_state":["COMPLETED"],"admin_comment":"{\"pods\":[\"scs-gang-t1/gang-a-x\"]}"}]}`)}
		get := func(_ context.Context, path string) ([]byte, error) {
			paths = append(paths, path)
			r := replies[0]
			if len(replies) > 1 {
				replies = replies[1:]
			}
			return r, nil
		}
		Expect(slurmbridge.WaitJobsGone(ctx, get, s, 10*time.Millisecond, 5*time.Second)).To(Succeed())
		Expect(paths).To(Equal([]string{"/slurm/v0.0.44/jobs", "/slurm/v0.0.44/jobs"}))
	})
	It("names the jobs still live when the bound runs out", func(ctx SpecContext) {
		get := func(context.Context, string) ([]byte, error) { return []byte(jobsJSON), nil }
		err := slurmbridge.WaitJobsGone(ctx, get, s, 10*time.Millisecond, 200*time.Millisecond)
		Expect(err).To(MatchError("slurm-bridge: probe gang left Slurm jobs after 200ms: 7 (RUNNING), 9 (PENDING)"))
	})
	It("names the last slurmrestd error when it never answers", func(ctx SpecContext) {
		get := func(context.Context, string) ([]byte, error) { return nil, errors.New("connection refused") }
		err := slurmbridge.WaitJobsGone(ctx, get, s, 10*time.Millisecond, 200*time.Millisecond)
		Expect(err).To(MatchError("slurm-bridge: probe gang left Slurm jobs after 200ms: list Slurm jobs: connection refused"))
	})
})

var _ = Describe("PriorityClasses", func() {
	It("installs scs-low at 100 and scs-high at 1000, neither preempting through Kubernetes", func() {
		objs, err := apply.Decode([]byte(slurmbridge.PriorityClasses))
		Expect(err).NotTo(HaveOccurred())
		Expect(objs).To(HaveLen(2))
		for i, want := range []struct {
			name  string
			value int64
		}{{"scs-low", 100}, {"scs-high", 1000}} {
			Expect(objs[i].GetKind() + " " + objs[i].GetName()).To(Equal("PriorityClass " + want.name))
			Expect(objs[i].Object["value"]).To(BeNumerically("==", want.value))
			Expect(objs[i].Object["preemptionPolicy"]).To(Equal("Never"))
		}
	})
})

var _ = Describe("NodeGPUs", func() {
	It("is the fleet's per-node GPU count, 8", func() {
		Expect(slurmbridge.NodeGPUs(fleet.Default())).To(Equal(8))
	})
	It("refuses a fleet whose nodes differ, where no single count is a whole node", func() {
		f := fleet.Spec{Nodes: []fleet.Node{{Name: "n1", GPUs: 8}, {Name: "n2", GPUs: 4}}}
		_, err := slurmbridge.NodeGPUs(f)
		Expect(err).To(MatchError("slurm-bridge: fleet nodes n1 and n2 have 8 and 4 GPUs; the profile needs every node alike"))
	})
})

var _ = Describe("TimeLimitMinutes", func() {
	DescribeTable("rounds the run time up to whole minutes and adds one, so Slurm never ends a job kwok is about to complete",
		func(run time.Duration, minutes int) {
			Expect(slurmbridge.TimeLimitMinutes(run)).To(Equal(minutes))
		},
		Entry("the gang probe's 20 s", 20*time.Second, 2),
		Entry("the backfill probe's 25 s short jobs", 25*time.Second, 2),
		Entry("59 s", 59*time.Second, 2),
		Entry("exactly a minute, the blockers", 60*time.Second, 2),
		Entry("one second past a minute", 61*time.Second, 3),
		Entry("the preemption victim's 300 s", 300*time.Second, 6),
	)
})

var _ = Describe("Annotations, PodGroup and Job", func() {
	low := workload.Job{Name: "fill-30", Namespace: "scs-priority-t1", Probe: "priority", Tenant: "t1",
		Priority: workload.Low, Pods: 1, GPUsPerPod: 8, RunFor: 30 * time.Second}
	high := workload.Job{Name: "high", Namespace: "scs-preemption-t1", Probe: "preemption", Tenant: "t1",
		Priority: workload.High, Preempting: true, Pods: 2, GPUsPerPod: 8, RunFor: 30 * time.Second}
	gang := workload.Job{Name: "gang-a", Namespace: "scs-gang-t1", Probe: "gang", Tenant: "t1",
		Priority: workload.Low, Pods: 3, GPUsPerPod: 8, RunFor: 20 * time.Second, Gang: true}

	It("drops the GPU request and sets priority and time limit for a low job, in the default partition", func() {
		Expect(slurmbridge.Annotations(low)).To(Equal(map[string]string{
			"slurmjob.slinky.slurm.net/gres":      "",
			"slurmjob.slinky.slurm.net/priority":  "100",
			"slurmjob.slinky.slurm.net/timelimit": "2",
		}))
	})
	It("sends a preempting job to the higher-tier partition at priority 1000", func() {
		Expect(slurmbridge.Annotations(high)).To(Equal(map[string]string{
			"slurmjob.slinky.slurm.net/gres":      "",
			"slurmjob.slinky.slurm.net/priority":  "1000",
			"slurmjob.slinky.slurm.net/timelimit": "2",
			"slurmjob.slinky.slurm.net/partition": "slurm-bridge-preempt",
		}))
	})
	It("keeps a high job that may not preempt in the default partition", func() {
		h := high
		h.Preempting = false
		Expect(slurmbridge.Annotations(h)).NotTo(HaveKey("slurmjob.slinky.slurm.net/partition"))
	})

	It("names a gang's PodGroup after its namespace too, since slurm-bridge finds a group's pods in every namespace", func() {
		Expect(slurmbridge.PodGroupName(gang)).To(Equal("pg-scs-gang-t1-gang-a"))
	})

	It("makes a gang one scheduler-plugins PodGroup of all its pods, carrying the Slurm annotations", func() {
		Expect(slurmbridge.PodGroup(gang)).To(Equal(map[string]any{
			"apiVersion": "scheduling.x-k8s.io/v1alpha1", "kind": "PodGroup",
			"metadata": map[string]any{
				"name": "pg-scs-gang-t1-gang-a", "namespace": "scs-gang-t1",
				"labels": map[string]any{
					"app.kubernetes.io/part-of":            "scheduler-capabilities-suite",
					"scs.arangogutierrez.github.io/probe":  "gang",
					"scs.arangogutierrez.github.io/tenant": "t1",
					"scs.arangogutierrez.github.io/job":    "gang-a",
				},
				"annotations": map[string]any{
					"slurmjob.slinky.slurm.net/gres":      "",
					"slurmjob.slinky.slurm.net/priority":  "100",
					"slurmjob.slinky.slurm.net/timelimit": "2",
				},
			},
			"spec": map[string]any{"minMember": int64(3)},
		}))
	})

	It("hands a job's pods to slurm-bridge with its priority class and the Slurm annotations on the Job", func() {
		b := slurmbridge.Job(high)
		Expect(b.Spec.Template.Spec.SchedulerName).To(Equal("slurm-bridge-scheduler"))
		Expect(b.Spec.Template.Spec.PriorityClassName).To(Equal("scs-high"))
		Expect(b.Annotations).To(Equal(slurmbridge.Annotations(high)))
		Expect(b.Spec.Template.Labels).NotTo(HaveKey("scheduling.x-k8s.io/pod-group"))
		Expect(slurmbridge.Job(low).Spec.Template.Spec.PriorityClassName).To(Equal("scs-low"))
	})

	It("puts a gang's pods in its PodGroup by label, leaves the Job unannotated and never sets schedulingGroup", func() {
		b := slurmbridge.Job(gang)
		Expect(b.Spec.Template.Labels).To(HaveKeyWithValue("scheduling.x-k8s.io/pod-group", "pg-scs-gang-t1-gang-a"))
		Expect(b.Annotations).To(BeEmpty())
		Expect(b.Spec.Template.Spec.SchedulingGroup).To(BeNil())
		Expect(b.Spec.Template.Spec.SchedulerName).To(Equal("slurm-bridge-scheduler"))
	})
})

var _ = Describe("Submit and Remove", func() {
	pgGVR := schema.GroupVersionResource{Group: "scheduling.x-k8s.io", Version: "v1alpha1", Resource: "podgroups"}
	gang := workload.Job{Name: "gang-a", Namespace: "scs-gang-t1", Probe: "gang", Tenant: "t1",
		Priority: workload.Low, Pods: 3, GPUsPerPod: 8, RunFor: 20 * time.Second, Gang: true}
	newCluster := func() *cluster.Cluster {
		dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{pgGVR: "PodGroupList"})
		return &cluster.Cluster{Clientset: fake.NewClientset(), Dynamic: dyn}
	}

	It("creates a gang's PodGroup before its Job", func(ctx SpecContext) {
		c := newCluster()
		var order []string
		c.Dynamic.(*dynamicfake.FakeDynamicClient).PrependReactor("create", "podgroups", func(k8stesting.Action) (bool, runtime.Object, error) {
			order = append(order, "podgroup")
			return false, nil, nil
		})
		c.Clientset.(*fake.Clientset).PrependReactor("create", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
			order = append(order, "job")
			return false, nil, nil
		})
		Expect(slurmbridge.New().Submit(ctx, c, gang)).To(Succeed())
		Expect(order).To(Equal([]string{"podgroup", "job"}))
		pg, err := c.Dynamic.Resource(pgGVR).Namespace("scs-gang-t1").Get(ctx, "pg-scs-gang-t1-gang-a", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(pg.Object["spec"]).To(HaveKeyWithValue("minMember", int64(3)))
	})

	It("creates no PodGroup for a job that is not a gang", func(ctx SpecContext) {
		c := newCluster()
		j := gang
		j.Gang = false
		Expect(slurmbridge.New().Submit(ctx, c, j)).To(Succeed())
		Expect(c.Dynamic.(*dynamicfake.FakeDynamicClient).Actions()).To(BeEmpty())
		b, err := c.Clientset.BatchV1().Jobs("scs-gang-t1").Get(ctx, "gang-a", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(b.Annotations).To(HaveKeyWithValue("slurmjob.slinky.slurm.net/gres", ""))
	})

	DescribeTable("refuses a pod smaller or larger than a whole node before creating anything, naming the job",
		func(gpus int, msg string) {
			ctx := context.Background()
			c := newCluster()
			j := gang
			j.GPUsPerPod = gpus
			Expect(slurmbridge.New().Submit(ctx, c, j)).To(MatchError(msg))
			Expect(c.Dynamic.(*dynamicfake.FakeDynamicClient).Actions()).To(BeEmpty())
			Expect(c.Clientset.(*fake.Clientset).Actions()).To(BeEmpty())
		},
		Entry("half a node", 4, "slurm-bridge: job gang-a asks for 4 GPUs per pod; the profile drops the GPU request from Slurm, which is valid only when every pod takes a whole 8-GPU node"),
		Entry("one GPU short", 7, "slurm-bridge: job gang-a asks for 7 GPUs per pod; the profile drops the GPU request from Slurm, which is valid only when every pod takes a whole 8-GPU node"),
		Entry("no GPU", 0, "slurm-bridge: job gang-a asks for 0 GPUs per pod; the profile drops the GPU request from Slurm, which is valid only when every pod takes a whole 8-GPU node"),
		Entry("two nodes' worth", 16, "slurm-bridge: job gang-a asks for 16 GPUs per pod; the profile drops the GPU request from Slurm, which is valid only when every pod takes a whole 8-GPU node"),
	)

	It("refuses an invalid job before creating anything", func(ctx SpecContext) {
		c := newCluster()
		bad := gang
		bad.SameRack, bad.Gang = true, false
		Expect(slurmbridge.New().Submit(ctx, c, bad)).To(MatchError("workload: gang-a: evict-whole and same-rack apply to gangs only"))
		Expect(c.Dynamic.(*dynamicfake.FakeDynamicClient).Actions()).To(BeEmpty())
	})

	It("deletes the Job with background propagation and its PodGroup, and takes either already gone as done", func(ctx SpecContext) {
		c := newCluster()
		Expect(slurmbridge.New().Submit(ctx, c, gang)).To(Succeed())
		var policies []*metav1.DeletionPropagation
		c.Clientset.(*fake.Clientset).PrependReactor("delete", "jobs", func(a k8stesting.Action) (bool, runtime.Object, error) {
			policies = append(policies, a.(k8stesting.DeleteActionImpl).DeleteOptions.PropagationPolicy)
			return false, nil, nil
		})
		Expect(slurmbridge.New().Remove(ctx, c, gang)).To(Succeed())
		Expect(policies).To(HaveLen(1))
		Expect(policies[0]).To(HaveValue(Equal(metav1.DeletionPropagation("Background"))))
		_, err := c.Clientset.BatchV1().Jobs("scs-gang-t1").Get(ctx, "gang-a", metav1.GetOptions{})
		Expect(err).To(MatchError(`jobs.batch "gang-a" not found`))
		_, err = c.Dynamic.Resource(pgGVR).Namespace("scs-gang-t1").Get(ctx, "pg-scs-gang-t1-gang-a", metav1.GetOptions{})
		Expect(err).To(MatchError(`podgroups.scheduling.x-k8s.io "pg-scs-gang-t1-gang-a" not found`))
		Expect(slurmbridge.New().Remove(ctx, c, gang)).To(Succeed())
	})
})

var _ = Describe("Cleanup", func() {
	It("asks slurmrestd for the jobs and returns when none of the probe's is live", func(ctx SpecContext) {
		var paths []string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			paths = append(paths, r.URL.Path)
			w.Header().Set("Content-Type", "application/json")
			if strings.HasSuffix(r.URL.Path, "/secrets/slurm-bridge-token") {
				_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"Secret","metadata":{"name":"slurm-bridge-token"},"data":{"auth-token":"dG9r"}}`))
				return
			}
			_, _ = w.Write([]byte(jobsJSON))
		}))
		DeferCleanup(srv.Close)
		cs, err := kubernetes.NewForConfig(&rest.Config{Host: srv.URL})
		Expect(err).NotTo(HaveOccurred())
		s := scheduler.Scenario{Probe: "topology", Tenants: []scheduler.Tenant{{Name: "t1", Namespace: "scs-topology-t1", EntitlementGPUs: 32}}}
		Expect(slurmbridge.New().Cleanup(ctx, &cluster.Cluster{Clientset: cs}, s)).To(Succeed())
		Expect(paths).To(Equal([]string{
			"/api/v1/namespaces/slurm/secrets/slurm-bridge-token",
			"/api/v1/namespaces/slurm/services/slurm-restapi:6820/proxy/slurm/v0.0.44/jobs",
		}))
	})
})

var _ = Describe("claims", func() {
	It("ships a claims file that loads, with a citation for every capability", func() {
		f, err := claims.Load("../../../claims/slurm-bridge.yaml")
		Expect(err).NotTo(HaveOccurred())
		Expect(f.Scheduler).To(Equal("slurm-bridge"))
		Expect(f.Version).To(Equal("slurm-bridge v1.2.3 and slurm-operator v1.2.3 with Slurm 26.05.4 on Kubernetes v1.37.0"))
		Expect(f.Claims["gang"].Expected).To(Equal(verdict.Supported))
		Expect(f.Claims["preemption"].Citation).To(Equal("https://github.com/SchedMD/slurm/blob/slurm-26-05-4-1/doc/html/preempt.shtml#L179"))
		Expect(f.Claims["fair-share"].Citation).To(Equal("https://github.com/SchedMD/slurm/blob/slurm-26-05-4-1/doc/html/priority_multifactor.shtml#L253"))
		Expect(f.Claims["fair-share"].Claim).To(ContainSubstring("Computing the fair-share factor requires the installation and operation of the Slurm Accounting Database"))
		Expect(f.Claims["fair-share"].Expected).To(Equal(verdict.NotSupported))
		Expect(f.Claims["backfill"].Expected).To(Equal(verdict.NotSupported))
		Expect(f.Claims["topology"].Expected).To(Equal(verdict.Supported))
	})
})
