package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gmeasure"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/fleet"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/labels"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

// probe is one probe's run: its scenario, start time, and the restart
// counts of the control-plane containers when it started.
type probe struct {
	name     string
	scenario scheduler.Scenario
	t0       time.Time
	restarts map[string]int32
}

// startProbe starts a probe whose scenario is just its tenants.
func startProbe(ctx context.Context, name string, tenants ...scheduler.Tenant) *probe {
	return startScenario(ctx, scheduler.Scenario{Probe: name, Tenants: tenants})
}

// startScenario checks that the fleet is empty, creates the tenants'
// namespaces (scs-<probe>-<tenant>) and per-probe scheduler objects, and
// registers the probe's cleanup. With no tenants, one tenant "t1" is
// entitled to the whole fleet.
func startScenario(ctx context.Context, s scheduler.Scenario) *probe {
	Expect(suitePods(ctx)).To(BeZero(), "probe %s found suite pods left over", s.Probe)
	if len(s.Tenants) == 0 {
		s.Tenants = []scheduler.Tenant{{Name: "t1", EntitlementGPUs: fleet.Default().GPUs()}}
	}
	for i := range s.Tenants {
		s.Tenants[i].Namespace = "scs-" + s.Probe + "-" + s.Tenants[i].Name
	}
	p := &probe{name: s.Probe, scenario: s, t0: time.Now(), restarts: controlPlaneRestarts(ctx)}
	Expect(env.profile.Prepare(ctx, env.cluster, p.scenario)).To(Succeed())
	DeferCleanup(p.cleanup, NodeTimeout(4*time.Minute))
	return p
}

// controlPlaneRestarts maps each system container to its restart count. A
// restart during a probe (R1 saw etcd stalls make the scheduler, the
// controller-manager and Kueue lose leader election) invalidates the
// probe's evidence.
func controlPlaneRestarts(ctx context.Context) map[string]int32 {
	out := map[string]int32{}
	for _, ns := range []string{"kube-system", "kueue-system"} {
		pods, err := env.cluster.Clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
		Expect(err).NotTo(HaveOccurred())
		for _, q := range pods.Items {
			for _, c := range q.Status.ContainerStatuses {
				out[ns+"/"+q.Name+"/"+c.Name] = c.RestartCount
			}
		}
	}
	return out
}

// fill completes a job with the probe's namespace and labels for tenant.
func (p *probe) fill(j workload.Job) workload.Job {
	if j.Tenant == "" {
		j.Tenant = "t1"
	}
	j.Probe = p.name
	j.Namespace = "scs-" + p.name + "-" + j.Tenant
	return j
}

func (p *probe) submit(ctx context.Context, jobs ...workload.Job) {
	for _, j := range jobs {
		Expect(env.profile.Submit(ctx, env.cluster, p.fill(j))).To(Succeed(), "submit %s", j.Name)
	}
}

// remove deletes a job and waits until its pods are gone.
func (p *probe) remove(ctx context.Context, j workload.Job) {
	j = p.fill(j)
	Expect(env.profile.Remove(ctx, env.cluster, j)).To(Succeed())
	Eventually(func() int { return len(p.pods(ctx, j.Name)) }).WithContext(ctx).
		WithTimeout(time.Minute).WithPolling(250*time.Millisecond).Should(BeZero(), "pods of %s still exist", j.Name)
}

// pods returns the pods of one job across the probe's namespaces.
func (p *probe) pods(ctx context.Context, job string) []corev1.Pod {
	var out []corev1.Pod
	for _, t := range p.scenario.Tenants {
		list, err := env.cluster.Clientset.CoreV1().Pods(t.Namespace).List(ctx, metav1.ListOptions{LabelSelector: labels.Job + "=" + job})
		Expect(err).NotTo(HaveOccurred())
		out = append(out, list.Items...)
	}
	return out
}

func (p *probe) count(ctx context.Context, job string, ok func(corev1.Pod) bool) int {
	n := 0
	for _, q := range p.pods(ctx, job) {
		if ok(q) {
			n++
		}
	}
	return n
}

func (p *probe) scheduled(ctx context.Context, job string) int {
	return p.count(ctx, job, func(q corev1.Pod) bool { return q.Spec.NodeName != "" })
}

func (p *probe) running(ctx context.Context, job string) int {
	return p.count(ctx, job, func(q corev1.Pod) bool { return q.Status.Phase == corev1.PodRunning })
}

func (p *probe) succeeded(ctx context.Context, job string) int {
	return p.count(ctx, job, func(q corev1.Pod) bool { return q.Status.Phase == corev1.PodSucceeded })
}

// mustAwait waits for a scenario precondition; missing it is a harness
// failure (the scenario never took shape), not a verdict.
func (p *probe) mustAwait(ctx context.Context, what string, cond func() bool, timeout time.Duration) {
	Eventually(cond).WithContext(ctx).WithTimeout(timeout).WithPolling(250*time.Millisecond).
		Should(BeTrue(), "probe %s: %s", p.name, what)
}

// await waits for an outcome a verdict rule judges; timing out is evidence,
// not a failure.
func (p *probe) await(ctx context.Context, cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(250 * time.Millisecond):
		}
	}
	return cond()
}

// pause waits d, or fails the spec as soon as ctx ends (an interrupt or the
// spec timeout), so an interrupted probe stops here instead of waking later
// into finish while AfterSuite writes probes.json.
func pause(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		Fail(fmt.Sprintf("interrupted while waiting: %v", context.Cause(ctx)))
	case <-t.C:
	}
}

// finish closes the probe's evidence window, evaluates its verdict from
// the recorded file (the same path scs-report takes), and attaches the
// verdict and its measurements to the Ginkgo report.
func (p *probe) finish(ctx context.Context, capability string, params any) verdict.Verdict {
	windowEnd := time.Now().UTC().Round(0)
	// The verdict reads only events whose probe label is the capability, so
	// a probe finished under another name would be judged on no events.
	Expect(capability).To(Equal(p.name), "probe %s was finished as capability %s", p.name, capability)
	Expect(env.rec.Flush()).To(Succeed())
	for _, t := range env.rec.WatchErrors() {
		Expect(t.Before(p.t0)).To(BeTrue(), "probe %s: a watch failed at %s, so its evidence may be incomplete", p.name, t)
	}
	Expect(controlPlaneRestarts(ctx)).To(Equal(p.restarts), "probe %s: a control-plane container restarted or was replaced during the probe, so its evidence is invalid", p.name)
	raw, err := json.Marshal(params)
	Expect(err).NotTo(HaveOccurred())
	pr := verdict.Probe{Capability: capability, Params: raw, WindowEnd: windowEnd}
	evs, err := events.ReadFile(env.eventsPath)
	Expect(err).NotTo(HaveOccurred())
	v, err := verdict.Evaluate(evs, pr)
	Expect(err).NotTo(HaveOccurred())
	env.probes = append(env.probes, pr)
	env.verdicts = append(env.verdicts, v)
	AddReportEntry("verdict", v)
	exp := gmeasure.NewExperiment(capability)
	keys := make([]string, 0, len(v.Measurements))
	for k := range v.Measurements {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		exp.RecordValue(k, v.Measurements[k])
	}
	AddReportEntry(exp.Name, exp)
	GinkgoWriter.Printf("%s on %s: %s (%s)\n", capability, env.profile.Name(), v.Outcome, v.Reason)
	return v
}

// cleanup deletes the probe's namespaces and waits until no suite pod is
// left; a fleet that does not drain would contaminate the next probe.
func (p *probe) cleanup(ctx SpecContext) {
	for _, t := range p.scenario.Tenants {
		err := env.cluster.Clientset.CoreV1().Namespaces().Delete(ctx, t.Namespace, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			Fail(fmt.Sprintf("delete namespace %s: %v", t.Namespace, err))
		}
	}
	Eventually(func() int {
		n := 0
		for _, t := range p.scenario.Tenants {
			if _, err := env.cluster.Clientset.CoreV1().Namespaces().Get(ctx, t.Namespace, metav1.GetOptions{}); err == nil {
				n++
			}
		}
		return n + suitePods(ctx)
	}).WithContext(ctx).WithTimeout(3*time.Minute).WithPolling(time.Second).Should(BeZero(), "probe %s did not drain", p.name)
	Expect(env.profile.Cleanup(ctx, env.cluster, p.scenario)).To(Succeed())
}

// suitePods counts the suite's pods in every namespace.
func suitePods(ctx context.Context) int {
	list, err := env.cluster.Clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{LabelSelector: labels.Selector})
	Expect(err).NotTo(HaveOccurred())
	return len(list.Items)
}

// names returns prefix-1 .. prefix-n.
func names(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s-%d", prefix, i+1)
	}
	return out
}
