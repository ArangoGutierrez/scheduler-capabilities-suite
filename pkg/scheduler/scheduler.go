// Package scheduler defines a Profile: how one scheduler expresses the
// suite's scheduler-neutral jobs. The e2e specs only ever talk to a Profile.
package scheduler

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/cluster"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/labels"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

// Tenant is one tenant of a probe: its namespace and its GPU entitlement.
type Tenant struct {
	Name            string
	Namespace       string
	EntitlementGPUs int
}

// Scenario is what a probe asks of the scheduler before it submits work.
// AllowPreemption says the probe expects the scheduler to evict running
// work (the preemption probe; reclaiming a tenant's share in the fair-share
// probe). Profiles that decide preemption per job ignore it.
type Scenario struct {
	Probe           string
	Tenants         []Tenant
	AllowPreemption bool
}

// Profile is one scheduler under test.
type Profile interface {
	// Name is "vanilla" or "kueue".
	Name() string
	// ClaimsFile is the path of the scheduler's claims, relative to the repository root.
	ClaimsFile() string
	// Install creates the scheduler's cluster-wide objects and returns what
	// the run manifest should record about them.
	Install(ctx context.Context, c *cluster.Cluster) (map[string]string, error)
	// Prepare creates a probe's namespaces and per-probe scheduler objects.
	Prepare(ctx context.Context, c *cluster.Cluster, s Scenario) error
	// Submit creates one job, and any object it needs first.
	Submit(ctx context.Context, c *cluster.Cluster, j workload.Job) error
	// Remove deletes one job and its companion objects.
	Remove(ctx context.Context, c *cluster.Cluster, j workload.Job) error
	// Cleanup deletes a probe's per-probe objects once its namespaces are gone.
	Cleanup(ctx context.Context, c *cluster.Cluster, s Scenario) error
	// WorkloadGVR is the resource the recorder should watch besides Pods and
	// Events, or nil.
	WorkloadGVR() *schema.GroupVersionResource
}

// CreateNamespaces creates each tenant's namespace with the suite's labels,
// so the recorder can attribute Events and Workloads in it.
func CreateNamespaces(ctx context.Context, cs kubernetes.Interface, s Scenario) error {
	for _, t := range s.Tenants {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: t.Namespace, Labels: map[string]string{
			labels.PartOf: labels.PartOfValue, labels.Probe: s.Probe, labels.Tenant: t.Name,
		}}}
		if _, err := cs.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	}
	return nil
}
