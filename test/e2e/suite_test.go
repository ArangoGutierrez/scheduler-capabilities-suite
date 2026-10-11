// Package e2e runs the suite's capability probes against one profile on a
// fresh kind cluster with a kwok GPU fleet. Configure it with SCS_PROFILE
// (vanilla or kueue), SCS_RUN_ID and SCS_RESULTS (an absolute directory);
// hack/e2e.sh sets them.
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/cluster"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/fleet"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/kwok"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/recorder"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/report"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler/vanilla"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
)

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "e2e") {
		t.Fatal("e2e specs failed")
	}
}

// profiles maps SCS_PROFILE to a constructor.
var profiles = map[string]func() scheduler.Profile{
	"vanilla": func() scheduler.Profile { return vanilla.New() },
}

// env is the suite's state, set up in BeforeSuite.
var env struct {
	profile     scheduler.Profile
	cluster     *cluster.Cluster
	rec         *recorder.Recorder
	eventsFile  *os.File
	runID       string
	runDir      string
	profileDir  string
	eventsPath  string
	start       time.Time
	offsetStart float64
	installed   map[string]string
	kwokSums    map[string]string
	probes      []verdict.Probe
	verdicts    []verdict.Verdict
	failed      bool
}

var _ = ReportAfterEach(func(r SpecReport) {
	if r.Failed() {
		env.failed = true
	}
})

var _ = BeforeSuite(func(ctx SpecContext) {
	name := os.Getenv("SCS_PROFILE")
	mk, ok := profiles[name]
	Expect(ok).To(BeTrue(), "SCS_PROFILE=%q is not a known profile", name)
	env.runID = os.Getenv("SCS_RUN_ID")
	Expect(env.runID).NotTo(BeEmpty(), "SCS_RUN_ID is empty")
	results := os.Getenv("SCS_RESULTS")
	Expect(filepath.IsAbs(results)).To(BeTrue(), "SCS_RESULTS=%q must be an absolute path", results)
	env.profile = mk()
	env.runDir = filepath.Join(results, env.runID)
	dir, err := report.PrepareProfileDir(env.runDir, name)
	Expect(err).NotTo(HaveOccurred())
	env.profileDir = dir
	env.start = time.Now().UTC()

	By("creating kind cluster scs-" + name)
	env.cluster, err = cluster.Create(cluster.KindProvider(), cluster.SuiteConfig("scs-"+name, filepath.Join(dir, "scs.kubeconfig")))
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(teardownCluster, NodeTimeout(5*time.Minute))

	By("installing kwok and moving kind's DaemonSets off the fleet")
	env.kwokSums, err = kwok.Install(ctx, env.cluster.Applier, env.cluster.Clientset, 3*time.Minute)
	Expect(err).NotTo(HaveOccurred())
	Expect(cluster.KeepSystemPodsOffFleet(ctx, env.cluster.Clientset, 3*time.Minute)).To(Succeed())

	By("creating the fleet: " + fleet.Default().Describe())
	Expect(fleet.Create(ctx, env.cluster.Applier, fleet.Default())).To(Succeed())
	Expect(fleet.WaitReady(ctx, env.cluster.Clientset, fleet.Default(), 2*time.Minute)).To(Succeed())

	By("installing the " + name + " profile")
	env.installed, err = env.profile.Install(ctx, env.cluster)
	Expect(err).NotTo(HaveOccurred())
	env.offsetStart, err = clockOffset(ctx)
	Expect(err).NotTo(HaveOccurred())

	By("starting the recorder")
	env.eventsPath = filepath.Join(dir, report.EventsFile)
	env.eventsFile, err = os.Create(env.eventsPath)
	Expect(err).NotTo(HaveOccurred())
	env.rec = recorder.New(env.cluster.Clientset, env.cluster.Dynamic, env.eventsFile,
		recorder.Options{RunID: env.runID, WorkloadGVR: env.profile.WorkloadGVR()})
	// The watches must outlive BeforeSuite's context; Stop ends them.
	Expect(env.rec.Start(context.Background())).To(Succeed())
}, NodeTimeout(10*time.Minute))

var _ = AfterSuite(func(ctx SpecContext) {
	if env.rec == nil {
		return
	}
	Expect(env.rec.Stop()).To(Succeed())
	Expect(env.eventsFile.Close()).To(Succeed())
	// The manifest comes first: report.Write reads the run ID from it.
	offsetEnd, err := clockOffset(ctx)
	Expect(err).NotTo(HaveOccurred())
	cfg := cluster.SuiteConfig(env.cluster.Name, env.cluster.Kubeconfig)
	fleetJSON, err := json.Marshal(fleet.Default())
	Expect(err).NotTo(HaveOccurred())
	Expect(report.MergeManifest(env.runDir, report.Manifest{
		RunID: env.runID, GitCommit: os.Getenv("SCS_GIT_COMMIT"), GitDirty: os.Getenv("SCS_GIT_DIRTY") == "true",
		Host: hostInfo(), Tools: toolVersions(), Fleet: fleetJSON,
	}, env.profile.Name(), report.ProfileRun{
		Start: env.start, End: time.Now().UTC(), Cluster: env.cluster.Name,
		FeatureGates: cfg.FeatureGates, RuntimeConfig: cfg.RuntimeConfig,
		Images: images(ctx), Manifests: env.kwokSums, Scheduler: env.installed,
		ClockOffsetStartS: env.offsetStart, ClockOffsetEndS: offsetEnd, WatchErrors: len(env.rec.WatchErrors()),
	})).To(Succeed())
	if len(env.probes) == 0 {
		return
	}
	writeJSON(filepath.Join(env.profileDir, report.ProbesFile), env.probes)
	claims, err := os.ReadFile(filepath.Join("..", "..", env.profile.ClaimsFile()))
	Expect(err).NotTo(HaveOccurred())
	Expect(os.WriteFile(filepath.Join(env.profileDir, report.ClaimsFile), claims, 0o644)).To(Succeed())
	Expect(report.Write(env.runDir, env.runDir)).To(Succeed())
	// The run's verdicts and the offline rebuild must agree.
	entries, err := report.ProfileVerdicts(env.profileDir, env.profile.Name())
	Expect(err).NotTo(HaveOccurred())
	Expect(entries).To(HaveLen(len(env.verdicts)))
	for i, e := range entries {
		Expect(e.Observed).To(Equal(env.verdicts[i]), "probe %s", e.Capability)
	}
}, NodeTimeout(5*time.Minute))

// teardownCluster runs after AfterSuite, even when BeforeSuite failed part way.
func teardownCluster(ctx SpecContext) {
	By("exporting cluster logs")
	if err := env.cluster.ExportLogs(filepath.Join(env.profileDir, "cluster-logs")); err != nil {
		GinkgoWriter.Printf("exporting logs failed: %v\n", err)
	}
	keep := os.Getenv("KEEP_CLUSTER") == "1" || (os.Getenv("KEEP_CLUSTER_ON_FAILURE") == "1" && (env.failed || env.rec == nil))
	if keep {
		GinkgoWriter.Printf("keeping cluster %s; kubeconfig %s\n", env.cluster.Name, env.cluster.Kubeconfig)
		return
	}
	Expect(env.cluster.Delete()).To(Succeed())
}

// clockOffset returns the API server's creation timestamp for a new object
// minus the host time just before creating it, in seconds. The server
// stamps whole seconds, so values in (-1, +1) mean the clocks agree to the
// second.
func clockOffset(ctx context.Context) (float64, error) {
	before := time.Now()
	cm, err := env.cluster.Clientset.CoreV1().ConfigMaps("default").Create(ctx,
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{GenerateName: "scs-clock-"}}, metav1.CreateOptions{})
	if err != nil {
		return 0, err
	}
	_ = env.cluster.Clientset.CoreV1().ConfigMaps("default").Delete(ctx, cm.Name, metav1.DeleteOptions{})
	return cm.CreationTimestamp.Sub(before).Seconds(), nil
}

// images maps each running image reference in the system namespaces to its
// digest-qualified image ID.
func images(ctx context.Context) map[string]string {
	out := map[string]string{}
	for _, ns := range []string{"kube-system", "kueue-system"} {
		pods, err := env.cluster.Clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			continue
		}
		for _, p := range pods.Items {
			for _, s := range p.Status.ContainerStatuses {
				out[s.Image] = s.ImageID
			}
		}
	}
	return out
}

func hostInfo() map[string]string {
	h := map[string]string{"os": runtime.GOOS, "arch": runtime.GOARCH, "cpus": fmt.Sprint(runtime.NumCPU())}
	if out, err := exec.Command("docker", "info", "--format", "{{.ServerVersion}}|{{.NCPU}}|{{.MemTotal}}|{{.OperatingSystem}}").Output(); err == nil {
		f := strings.Split(strings.TrimSpace(string(out)), "|")
		if len(f) == 4 {
			h["docker_version"], h["docker_cpus"], h["docker_memory_bytes"], h["docker_os"] = f[0], f[1], f[2], f[3]
		}
	}
	return h
}

func toolVersions() map[string]string {
	t := map[string]string{"go": runtime.Version()}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, d := range bi.Deps {
			switch d.Path {
			case "github.com/onsi/ginkgo/v2", "github.com/onsi/gomega", "sigs.k8s.io/kind", "k8s.io/client-go":
				t[d.Path] = d.Version
			}
		}
	}
	return t
}

func writeJSON(path string, v any) {
	b, err := json.MarshalIndent(v, "", "  ")
	Expect(err).NotTo(HaveOccurred())
	Expect(os.WriteFile(path, append(b, '\n'), 0o644)).To(Succeed())
}

// registry holds each probe file's registration function by its order.
var registry = map[int]func(){}

// register adds a probe file's specs at position order; each probe file
// calls it once from a package-level var.
func register(order int, specs func()) bool {
	if _, dup := registry[order]; dup {
		panic(fmt.Sprintf("probe order %d registered twice", order))
	}
	registry[order] = specs
	return true
}

// The probes run in registration order, each on an empty fleet.
var _ = Describe("probes", Ordered, func() {
	orders := make([]int, 0, len(registry))
	for o := range registry {
		orders = append(orders, o)
	}
	sort.Ints(orders)
	for _, o := range orders {
		registry[o]()
	}
})
