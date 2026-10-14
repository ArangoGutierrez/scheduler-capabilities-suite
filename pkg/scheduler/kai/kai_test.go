package kai_test

import (
	"errors"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/apply"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/claims"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/cluster"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler/kai"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

func TestKAI(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "kai") {
		t.Fatal("kai specs failed")
	}
}

var _ = Describe("Profile", func() {
	It("is named kai, reads claims/kai.yaml and watches no workload object", func() {
		p := kai.New()
		Expect(p.Name()).To(Equal("kai"))
		Expect(p.ClaimsFile()).To(Equal("claims/kai.yaml"))
		Expect(p.WorkloadGVR()).To(BeNil())
	})
})

var _ = Describe("Manifest", func() {
	It("serves the KAI v0.18.5 chart render with its pinned sha256", func() {
		data, sum, err := kai.Manifest()
		Expect(err).NotTo(HaveOccurred())
		Expect(sum).To(Equal("9cf708ccf8f1f24a3b6bd831f50cc95234bb739159c67ece0ca7e1e5635579a9"))
		Expect(len(data)).To(Equal(1033876))
	})
})

var _ = Describe("CheckSum", func() {
	// shasum -a 256 of "kind: ConfigMap\n" and of "Kind: ConfigMap\n", run by hand
	const sum = "bb6c7fb1ce4b8ac8baa8f6344623dd4602cf3d6ce859f9ff859a5a43787c5987"
	const changed = "cdd7ee20f9043b36baab42b5be801c811b51e6ae9427424ca1b9c2d112a72b34"
	sums := []byte("0000  other.yaml\n" + sum + "  manifests.yaml\n")

	It("accepts bytes that match their SHA256SUMS line", func() {
		got, err := kai.CheckSum([]byte("kind: ConfigMap\n"), sums)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(sum))
	})
	It("refuses a manifest with one byte changed, naming both sums", func() {
		_, err := kai.CheckSum([]byte("Kind: ConfigMap\n"), sums)
		Expect(err).To(MatchError("kai: manifests.yaml has sha256 " + changed + ", SHA256SUMS says " + sum))
	})
	It("refuses a SHA256SUMS that does not list manifests.yaml", func() {
		_, err := kai.CheckSum([]byte("kind: ConfigMap\n"), []byte(sum+"  other.yaml\n"))
		Expect(err).To(MatchError("kai: manifests.yaml is not listed in SHA256SUMS"))
	})
})

var _ = Describe("InstallObjects", func() {
	decode := func(y string) []*unstructured.Unstructured {
		objs, err := apply.Decode([]byte(y))
		Expect(err).NotTo(HaveOccurred())
		return objs
	}
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
	const shard = "apiVersion: kai.scheduler/v1\nkind: SchedulingShard\nmetadata: {name: default}\nspec: {partitionLabelValue: \"\"}\n"
	const hookCM = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: kai-config-manifest\n  namespace: kai-scheduler\n  annotations: {helm.sh/hook: post-install}\n"

	It("turns the embedded render into the release's objects: no hooks, the kai-scheduler namespace first, the Config last", func() {
		data, _, err := kai.Manifest()
		Expect(err).NotTo(HaveOccurred())
		objs, err := kai.InstallObjects(decode(string(data)))
		Expect(err).NotTo(HaveOccurred())
		// The 37 objects without a helm.sh/hook annotation, listed with yq from
		// the render (see the build report), between the two the profile adds.
		Expect(ids(objs)).To(Equal([]string{
			"Namespace /kai-scheduler",
			"CustomResourceDefinition /configs.kai.scheduler",
			"CustomResourceDefinition /schedulingshards.kai.scheduler",
			"CustomResourceDefinition /topologies.kai.scheduler",
			"CustomResourceDefinition /bindrequests.scheduling.run.ai",
			"CustomResourceDefinition /podgroups.scheduling.run.ai",
			"CustomResourceDefinition /queues.scheduling.run.ai",
			"PriorityClass /build-preemptible",
			"PriorityClass /build",
			"PriorityClass /inference",
			"PriorityClass /train",
			"Namespace /kai-resource-reservation",
			"ServiceAccount kai-scheduler/kai-operator",
			"ServiceAccount kai-resource-reservation/kai-resource-reservation",
			"ClusterRole /kai-admission",
			"ClusterRole /kai-binder",
			"ClusterRole /kai-node-scale-adjuster",
			"ClusterRole /kai-numa-placement-exporter",
			"ClusterRole /kai-operator",
			"ClusterRole /kai-podgroup-controller",
			"ClusterRole /kai-podgrouper",
			"ClusterRole /queuecontroller",
			"ClusterRole /kai-resource-reservation",
			"ClusterRole /kai-scheduler",
			"ClusterRoleBinding /kai-admission",
			"ClusterRoleBinding /kai-binder",
			"ClusterRoleBinding /kai-node-scale-adjuster",
			"ClusterRoleBinding /kai-numa-placement-exporter",
			"ClusterRoleBinding /kai-operator",
			"ClusterRoleBinding /kai-podgroup-controller",
			"ClusterRoleBinding /kai-podgrouper",
			"ClusterRoleBinding /kai-queuecontroller",
			"ClusterRoleBinding /kai-scheduler",
			"RoleBinding kai-resource-reservation/kai-resource-reservation",
			"Deployment kai-scheduler/kai-operator",
			"Queue /default-parent-queue",
			"Queue /default-queue",
			"SchedulingShard /default",
			"Config /kai-config",
		}))
		cfg := find(objs, "Config", "kai-config")
		Expect(cfg.GetAPIVersion()).To(Equal("kai.scheduler/v1"))
		Expect(cfg.Object["spec"]).To(HaveKeyWithValue("namespace", "kai-scheduler"))
		Expect(cfg.Object["spec"]).To(HaveKeyWithValue("scheduler", HaveKeyWithValue("service", HaveKeyWithValue("image", HaveKeyWithValue("tag", "v0.18.5")))))
	})

	It("sets update-pod-eviction-condition on the default SchedulingShard, so evictions carry DisruptionTarget", func() {
		data, _, err := kai.Manifest()
		Expect(err).NotTo(HaveOccurred())
		objs, err := kai.InstallObjects(decode(string(data)))
		Expect(err).NotTo(HaveOccurred())
		Expect(find(objs, "SchedulingShard", "default").Object["spec"]).To(Equal(map[string]any{
			"partitionLabelValue": "",
			"goMemLimitRatio":     0.9,
			"placementStrategy":   map[string]any{"gpu": "binpack", "cpu": "binpack"},
			"args":                map[string]any{"update-pod-eviction-condition": "true"},
		}))
	})

	It("drops every object that carries a helm.sh/hook annotation", func() {
		objs := decode(shard + "---\n" + hookCM + "data: {kai-config.yaml: \"apiVersion: kai.scheduler/v1\\nkind: Config\\nmetadata: {name: kai-config}\\n\"}\n" +
			"---\napiVersion: batch/v1\nkind: Job\nmetadata: {name: crd-upgrader, namespace: kai-scheduler, annotations: {helm.sh/hook: pre-install}}\n" +
			"---\napiVersion: batch/v1\nkind: Job\nmetadata: {name: kept, namespace: kai-scheduler, annotations: {other: x}}\n")
		got, err := kai.InstallObjects(objs)
		Expect(err).NotTo(HaveOccurred())
		Expect(ids(got)).To(Equal([]string{"Namespace /kai-scheduler", "SchedulingShard /default", "Job kai-scheduler/kept", "Config /kai-config"}))
	})

	It("refuses a render without the kai-config-manifest ConfigMap", func() {
		_, err := kai.InstallObjects(decode(shard))
		Expect(err).To(MatchError("kai: manifest has no kai-config-manifest ConfigMap"))
	})
	It("refuses a kai-config-manifest ConfigMap without its kai-config.yaml key", func() {
		_, err := kai.InstallObjects(decode(shard + "---\n" + hookCM + "data: {config.yaml: \"kind: Config\\n\"}\n"))
		Expect(err).To(MatchError("kai: kai-config-manifest has no data key kai-config.yaml"))
	})
	It("refuses a kai-config.yaml that is not a Config", func() {
		_, err := kai.InstallObjects(decode(shard + "---\n" + hookCM + "data: {kai-config.yaml: \"apiVersion: v1\\nkind: ConfigMap\\nmetadata: {name: x}\\n\"}\n"))
		Expect(err).To(MatchError("kai: kai-config-manifest holds a ConfigMap, want a Config"))
	})
	It("refuses a render without the default SchedulingShard", func() {
		_, err := kai.InstallObjects(decode(hookCM + "data: {kai-config.yaml: \"apiVersion: kai.scheduler/v1\\nkind: Config\\nmetadata: {name: kai-config}\\n\"}\n"))
		Expect(err).To(MatchError("kai: manifest has no SchedulingShard default"))
	})
	It("refuses a SchedulingShard that already sets update-pod-eviction-condition", func() {
		set := "apiVersion: kai.scheduler/v1\nkind: SchedulingShard\nmetadata: {name: default}\nspec: {args: {update-pod-eviction-condition: \"false\"}}\n"
		_, err := kai.InstallObjects(decode(set + "---\n" + hookCM + "data: {kai-config.yaml: \"apiVersion: kai.scheduler/v1\\nkind: Config\\nmetadata: {name: kai-config}\\n\"}\n"))
		Expect(err).To(MatchError("kai: SchedulingShard default already sets update-pod-eviction-condition"))
	})
})

var _ = Describe("WaitReady", func() {
	deployments := []string{"kai-operator", "admission", "binder", "kai-scheduler-default", "pod-grouper", "podgroup-controller", "queue-controller"}
	deployment := func(name string, available corev1.ConditionStatus) runtime.Object {
		return &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "kai-scheduler"},
			Status:     appsv1.DeploymentStatus{Conditions: []appsv1.DeploymentCondition{{Type: appsv1.DeploymentAvailable, Status: available}}},
		}
	}
	validating := func(name string) runtime.Object {
		return &admissionregistrationv1.ValidatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: name}}
	}
	mutating := &admissionregistrationv1.MutatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: "mutating-kai-admission"}}
	// objects is a ready install, less the objects named in skip, with
	// binder's Available condition as given.
	objects := func(binder corev1.ConditionStatus, skip ...string) []runtime.Object {
		var out []runtime.Object
		for _, d := range deployments {
			s := corev1.ConditionTrue
			if d == "binder" {
				s = binder
			}
			out = append(out, deployment(d, s))
		}
		out = append(out, mutating)
		for _, v := range []string{"validating-kai-admission", "kai-queue-validation-v2", "kai-podgroup-validation-v2alpha2"} {
			out = append(out, validating(v))
		}
		kept := out[:0]
		for _, o := range out {
			keep := true
			for _, s := range skip {
				if o.(metav1.Object).GetName() == s {
					keep = false
				}
			}
			if keep {
				kept = append(kept, o)
			}
		}
		return kept
	}
	// dryRuns records each create and answers it as a dry run.
	type created struct{ resource, namespace string }
	dryRuns := func(f *k8stesting.Fake, resource string, calls *[]created, fail error) {
		f.PrependReactor("create", resource, func(a k8stesting.Action) (bool, runtime.Object, error) {
			Expect(a.(k8stesting.CreateActionImpl).CreateOptions.DryRun).To(Equal([]string{metav1.DryRunAll}), "create %s must be a dry run", resource)
			*calls = append(*calls, created{resource, a.GetNamespace()})
			if fail != nil {
				return true, nil, fail
			}
			return true, a.(k8stesting.CreateAction).GetObject(), nil
		})
	}

	It("returns once the Deployments are available, the webhook configurations exist and three dry-run creates are answered", func(ctx SpecContext) {
		cs := fake.NewClientset(objects(corev1.ConditionTrue)...)
		dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
		var calls []created
		dryRuns(&dyn.Fake, "queues", &calls, nil)
		dryRuns(&dyn.Fake, "podgroups", &calls, nil)
		dryRuns(&cs.Fake, "pods", &calls, nil)
		Expect(kai.WaitReady(ctx, cs, dyn, 10*time.Millisecond, 5*time.Second)).To(Succeed())
		Expect(calls).To(Equal([]created{{"queues", ""}, {"podgroups", "default"}, {"pods", "default"}}))
	})

	It("dry-runs a Pod for kai-scheduler, the only pods KAI's pod webhooks match", func(ctx SpecContext) {
		cs := fake.NewClientset(objects(corev1.ConditionTrue)...)
		dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
		var calls []created
		dryRuns(&dyn.Fake, "queues", &calls, nil)
		dryRuns(&dyn.Fake, "podgroups", &calls, nil)
		var schedulers []string
		cs.PrependReactor("create", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
			pod := a.(k8stesting.CreateAction).GetObject().(*corev1.Pod)
			schedulers = append(schedulers, pod.Spec.SchedulerName)
			return true, pod, nil
		})
		Expect(kai.WaitReady(ctx, cs, dyn, 10*time.Millisecond, 5*time.Second)).To(Succeed())
		Expect(schedulers).To(Equal([]string{"kai-scheduler"}))
	})

	It("names a Deployment that is not available", func(ctx SpecContext) {
		cs := fake.NewClientset(objects(corev1.ConditionFalse)...)
		dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
		err := kai.WaitReady(ctx, cs, dyn, 10*time.Millisecond, 200*time.Millisecond)
		Expect(err).To(MatchError("kai not ready after 200ms: deployment kai-scheduler/binder is not available"))
	})

	It("does not trust a dry run before the operator has created every webhook configuration", func(ctx SpecContext) {
		cs := fake.NewClientset(objects(corev1.ConditionTrue, "kai-podgroup-validation-v2alpha2")...)
		dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
		var calls []created
		dryRuns(&dyn.Fake, "queues", &calls, nil)
		dryRuns(&dyn.Fake, "podgroups", &calls, nil)
		dryRuns(&cs.Fake, "pods", &calls, nil)
		err := kai.WaitReady(ctx, cs, dyn, 10*time.Millisecond, 200*time.Millisecond)
		Expect(err).To(MatchError(`kai not ready after 200ms: webhook configuration kai-podgroup-validation-v2alpha2: validatingwebhookconfigurations.admissionregistration.k8s.io "kai-podgroup-validation-v2alpha2" not found`))
		Expect(calls).To(BeEmpty())
	})

	It("names the last dry-run error when a webhook never answers", func(ctx SpecContext) {
		cs := fake.NewClientset(objects(corev1.ConditionTrue)...)
		dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
		var calls []created
		dryRuns(&dyn.Fake, "queues", &calls, errors.New(`Internal error occurred: failed calling webhook "queue-validation.kai.scheduler": no endpoints available for service "queue-controller"`))
		err := kai.WaitReady(ctx, cs, dyn, 10*time.Millisecond, 200*time.Millisecond)
		Expect(err).To(MatchError(`kai not ready after 200ms: dry-run Queue: Internal error occurred: failed calling webhook "queue-validation.kai.scheduler": no endpoints available for service "queue-controller"`))
		Expect(len(calls)).To(BeNumerically(">", 1))
	})
})

var _ = Describe("Base", func() {
	decode := func() []*unstructured.Unstructured {
		objs, err := apply.Decode([]byte(kai.Base))
		Expect(err).NotTo(HaveOccurred())
		Expect(objs).To(HaveLen(3))
		return objs
	}

	It("installs scs-high above scs-low, the classes the PodGroups and pods name", func() {
		objs := decode()
		Expect(objs[0].GetKind() + " " + objs[0].GetName()).To(Equal("PriorityClass scs-low"))
		Expect(objs[1].GetKind() + " " + objs[1].GetName()).To(Equal("PriorityClass scs-high"))
		low, high := objs[0].Object["value"], objs[1].Object["value"]
		Expect(low).To(BeNumerically("==", 100))
		Expect(high).To(BeNumerically("==", 1000))
		Expect(high).To(BeNumerically(">", low))
	})

	It("installs scs-topology with the fleet's block, rack and host labels, outermost first", func() {
		topo := decode()[2]
		Expect(topo.GetAPIVersion() + " " + topo.GetKind() + " " + topo.GetName()).To(Equal("kai.scheduler/v1alpha1 Topology scs-topology"))
		levels, found, err := unstructured.NestedSlice(topo.Object, "spec", "levels")
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue())
		Expect(levels).To(Equal([]any{
			map[string]any{"nodeLabel": "scs.arangogutierrez.github.io/block"},
			map[string]any{"nodeLabel": "scs.arangogutierrez.github.io/rack"},
			map[string]any{"nodeLabel": "kubernetes.io/hostname"},
		}))
	})
})

var _ = Describe("Queues", func() {
	a := scheduler.Tenant{Name: "a", Namespace: "scs-fair-share-a", EntitlementGPUs: 16}
	b := scheduler.Tenant{Name: "b", Namespace: "scs-fair-share-b", EntitlementGPUs: 16}
	none := map[string]any{"quota": int64(0), "limit": int64(-1), "overQuotaWeight": int64(1)}
	gpus := func(n int64) map[string]any {
		return map[string]any{"quota": n, "limit": int64(-1), "overQuotaWeight": int64(1)}
	}

	It("gives the probe a parent queue holding the sum of the entitlements, and each tenant a leaf queue with its own, reclaimable at once", func() {
		qs := kai.Queues(scheduler.Scenario{Probe: "fair-share", Tenants: []scheduler.Tenant{a, b}, AllowPreemption: true})
		Expect(qs).To(Equal([]map[string]any{
			{
				"apiVersion": "scheduling.run.ai/v2", "kind": "Queue",
				"metadata": map[string]any{"name": "scs-fair-share", "labels": map[string]any{
					"app.kubernetes.io/part-of":           "scheduler-capabilities-suite",
					"scs.arangogutierrez.github.io/probe": "fair-share",
				}},
				"spec": map[string]any{"resources": map[string]any{"gpu": gpus(32), "cpu": none, "memory": none}},
			},
			{
				"apiVersion": "scheduling.run.ai/v2", "kind": "Queue",
				"metadata": map[string]any{"name": "scs-fair-share-a", "labels": map[string]any{
					"app.kubernetes.io/part-of":            "scheduler-capabilities-suite",
					"scs.arangogutierrez.github.io/probe":  "fair-share",
					"scs.arangogutierrez.github.io/tenant": "a",
				}},
				"spec": map[string]any{
					"parentQueue":       "scs-fair-share",
					"resources":         map[string]any{"gpu": gpus(16), "cpu": none, "memory": none},
					"preemptMinRuntime": "0s",
					"reclaimMinRuntime": "0s",
				},
			},
			{
				"apiVersion": "scheduling.run.ai/v2", "kind": "Queue",
				"metadata": map[string]any{"name": "scs-fair-share-b", "labels": map[string]any{
					"app.kubernetes.io/part-of":            "scheduler-capabilities-suite",
					"scs.arangogutierrez.github.io/probe":  "fair-share",
					"scs.arangogutierrez.github.io/tenant": "b",
				}},
				"spec": map[string]any{
					"parentQueue":       "scs-fair-share",
					"resources":         map[string]any{"gpu": gpus(16), "cpu": none, "memory": none},
					"preemptMinRuntime": "0s",
					"reclaimMinRuntime": "0s",
				},
			},
		}))
	})

	It("protects running work for 24h in a probe that does not allow preemption", func() {
		t1 := scheduler.Tenant{Name: "t1", Namespace: "scs-priority-t1", EntitlementGPUs: 32}
		qs := kai.Queues(scheduler.Scenario{Probe: "priority", Tenants: []scheduler.Tenant{t1}})
		Expect(qs).To(HaveLen(2))
		Expect(qs[0]["spec"]).To(Equal(map[string]any{"resources": map[string]any{"gpu": gpus(32), "cpu": none, "memory": none}}))
		Expect(qs[1]["spec"]).To(Equal(map[string]any{
			"parentQueue":       "scs-priority",
			"resources":         map[string]any{"gpu": gpus(32), "cpu": none, "memory": none},
			"preemptMinRuntime": "24h",
			"reclaimMinRuntime": "24h",
		}))
	})
})

var _ = Describe("PodGroup", func() {
	It("asks for all of a gang's pods at once, in the tenant's leaf queue, preemptible", func() {
		j := workload.Job{Name: "gang-a", Namespace: "scs-gang-t1", Probe: "gang", Tenant: "t1",
			Priority: workload.Low, Pods: 3, GPUsPerPod: 8, RunFor: 20 * time.Second, Gang: true}
		Expect(kai.PodGroup(j)).To(Equal(map[string]any{
			"apiVersion": "scheduling.run.ai/v2alpha2", "kind": "PodGroup",
			"metadata": map[string]any{"name": "gang-a", "namespace": "scs-gang-t1", "labels": map[string]any{
				"app.kubernetes.io/part-of":            "scheduler-capabilities-suite",
				"scs.arangogutierrez.github.io/probe":  "gang",
				"scs.arangogutierrez.github.io/tenant": "t1",
				"scs.arangogutierrez.github.io/job":    "gang-a",
			}},
			"spec": map[string]any{
				"minMember":         int64(3),
				"queue":             "scs-gang-t1",
				"priorityClassName": "scs-low",
				"preemptibility":    "preemptible",
			},
		}))
	})

	It("lets a job that is not a gang start pod by pod, at its priority", func() {
		j := workload.Job{Name: "high", Namespace: "scs-preemption-t1", Probe: "preemption", Tenant: "t1",
			Priority: workload.High, Preempting: true, Pods: 2, GPUsPerPod: 8, RunFor: 30 * time.Second}
		Expect(kai.PodGroup(j)["spec"]).To(Equal(map[string]any{
			"minMember":         int64(1),
			"queue":             "scs-preemption-t1",
			"priorityClassName": "scs-high",
			"preemptibility":    "preemptible",
		}))
	})

	It("requires one rack for a same-rack gang", func() {
		j := workload.Job{Name: "topo-1", Namespace: "scs-topology-t1", Probe: "topology", Tenant: "t1",
			Priority: workload.Low, Pods: 2, GPUsPerPod: 8, RunFor: 20 * time.Second, Gang: true, SameRack: true}
		Expect(kai.PodGroup(j)["spec"]).To(Equal(map[string]any{
			"minMember":         int64(2),
			"queue":             "scs-topology-t1",
			"priorityClassName": "scs-low",
			"preemptibility":    "preemptible",
			"topologyConstraint": map[string]any{
				"topology":              "scs-topology",
				"requiredTopologyLevel": "scs.arangogutierrez.github.io/rack",
			},
		}))
	})
})

var _ = Describe("Job", func() {
	It("hands the pods to kai-scheduler in the job's own PodGroup, with the pod-grouper kept out", func() {
		j := workload.Job{Name: "high", Namespace: "scs-priority-t1", Probe: "priority", Tenant: "t1",
			Priority: workload.High, Pods: 1, GPUsPerPod: 8, RunFor: 30 * time.Second}
		b := kai.Job(j)
		Expect(b.Annotations).To(Equal(map[string]string{"kai.scheduler/skip-podgrouper": "true"}))
		Expect(b.Spec.Template.Annotations).To(HaveKeyWithValue("pod-group-name", "high"))
		Expect(b.Spec.Template.Spec.SchedulerName).To(Equal("kai-scheduler"))
		Expect(b.Spec.Template.Spec.PriorityClassName).To(Equal("scs-high"))
		Expect(kai.Job(workload.Job{Name: "low", Namespace: "scs-priority-t1", Probe: "priority", Tenant: "t1",
			Priority: workload.Low, Pods: 1, GPUsPerPod: 8, RunFor: 30 * time.Second}).Spec.Template.Spec.PriorityClassName).To(Equal("scs-low"))
	})
})

var _ = Describe("Submit and Remove", func() {
	pgGVR := schema.GroupVersionResource{Group: "scheduling.run.ai", Version: "v2alpha2", Resource: "podgroups"}
	j := workload.Job{Name: "gang-a", Namespace: "scs-gang-t1", Probe: "gang", Tenant: "t1",
		Priority: workload.Low, Pods: 3, GPUsPerPod: 8, RunFor: 20 * time.Second, Gang: true}
	newCluster := func() *cluster.Cluster {
		dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{pgGVR: "PodGroupList"})
		return &cluster.Cluster{Clientset: fake.NewClientset(), Dynamic: dyn}
	}

	It("creates the PodGroup before the Job", func(ctx SpecContext) {
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
		Expect(kai.New().Submit(ctx, c, j)).To(Succeed())
		Expect(order).To(Equal([]string{"podgroup", "job"}))
		pg, err := c.Dynamic.Resource(pgGVR).Namespace("scs-gang-t1").Get(ctx, "gang-a", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(pg.Object["spec"]).To(HaveKeyWithValue("minMember", int64(3)))
	})

	It("deletes the Job and its PodGroup, and takes either already gone as done", func(ctx SpecContext) {
		c := newCluster()
		Expect(kai.New().Submit(ctx, c, j)).To(Succeed())
		_, err := c.Clientset.BatchV1().Jobs("scs-gang-t1").Get(ctx, "gang-a", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		_, err = c.Dynamic.Resource(pgGVR).Namespace("scs-gang-t1").Get(ctx, "gang-a", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(kai.New().Remove(ctx, c, j)).To(Succeed())
		_, err = c.Clientset.BatchV1().Jobs("scs-gang-t1").Get(ctx, "gang-a", metav1.GetOptions{})
		Expect(err).To(MatchError(`jobs.batch "gang-a" not found`))
		_, err = c.Dynamic.Resource(pgGVR).Namespace("scs-gang-t1").Get(ctx, "gang-a", metav1.GetOptions{})
		Expect(err).To(MatchError(`podgroups.scheduling.run.ai "gang-a" not found`))
		Expect(kai.New().Remove(ctx, c, j)).To(Succeed())
	})

	It("deletes the Job with background propagation, so its pods go with it", func(ctx SpecContext) {
		c := newCluster()
		Expect(kai.New().Submit(ctx, c, j)).To(Succeed())
		var policies []*metav1.DeletionPropagation
		c.Clientset.(*fake.Clientset).PrependReactor("delete", "jobs", func(a k8stesting.Action) (bool, runtime.Object, error) {
			policies = append(policies, a.(k8stesting.DeleteActionImpl).DeleteOptions.PropagationPolicy)
			return false, nil, nil
		})
		Expect(kai.New().Remove(ctx, c, j)).To(Succeed())
		Expect(policies).To(HaveLen(1))
		Expect(policies[0]).To(HaveValue(Equal(metav1.DeletionPropagation("Background"))))
	})

	It("refuses an invalid job before creating anything", func(ctx SpecContext) {
		c := newCluster()
		bad := j
		bad.SameRack, bad.Gang = true, false
		Expect(kai.New().Submit(ctx, c, bad)).To(MatchError("workload: gang-a: evict-whole and same-rack apply to gangs only"))
		Expect(c.Dynamic.(*dynamicfake.FakeDynamicClient).Actions()).To(BeEmpty())
	})
})

var _ = Describe("Cleanup", func() {
	a := scheduler.Tenant{Name: "a", Namespace: "scs-fair-share-a", EntitlementGPUs: 16}
	b := scheduler.Tenant{Name: "b", Namespace: "scs-fair-share-b", EntitlementGPUs: 16}
	s := scheduler.Scenario{Probe: "fair-share", Tenants: []scheduler.Tenant{a, b}}

	It("deletes the leaf queues before their parent, and returns once all are gone", func(ctx SpecContext) {
		dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
		var deleted []string
		dyn.PrependReactor("delete", "queues", func(a k8stesting.Action) (bool, runtime.Object, error) {
			deleted = append(deleted, a.(k8stesting.DeleteAction).GetName())
			return false, nil, nil
		})
		Expect(kai.New().Cleanup(ctx, &cluster.Cluster{Dynamic: dyn}, s)).To(Succeed())
		Expect(deleted).To(Equal([]string{"scs-fair-share-a", "scs-fair-share-b", "scs-fair-share"}))
	})
	It("returns a Get error other than not found instead of reading it as gone", func(ctx SpecContext) {
		dyn := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
		dyn.PrependReactor("get", "queues", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("etcdserver: request timed out")
		})
		err := kai.New().Cleanup(ctx, &cluster.Cluster{Dynamic: dyn}, s)
		Expect(err).To(MatchError("kai: get Queue scs-fair-share-a: etcdserver: request timed out"))
	})
})

var _ = Describe("claims", func() {
	It("ships a claims file that loads, with a citation for every capability", func() {
		f, err := claims.Load("../../../claims/kai.yaml")
		Expect(err).NotTo(HaveOccurred())
		Expect(f.Scheduler).To(Equal("kai"))
		Expect(f.Version).To(Equal("KAI Scheduler v0.18.5 on Kubernetes v1.37.0"))
		Expect(f.Claims["fair-share"].Citation).To(Equal("https://github.com/kai-scheduler/KAI-Scheduler/blob/v0.18.5/docs/fairness/README.md#L69"))
		Expect(f.Claims["fair-share"].Claim).To(ContainSubstring("Workloads from queues under their quota can evict workloads from queues that have exceeded their quota."))
		Expect(f.Claims["fair-share"].Expected).To(Equal(verdict.Supported))
		Expect(f.Claims["backfill"].Expected).To(Equal(verdict.Partial))
		Expect(f.Claims["topology"].Citation).To(Equal("https://github.com/kai-scheduler/KAI-Scheduler/blob/v0.18.5/pkg/apis/scheduling/v2alpha2/podgroup_types.go#L426-L429"))
	})
})
