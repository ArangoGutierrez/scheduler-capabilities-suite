// Package cluster creates and deletes the suite's kind clusters through
// kind's Go library, and keeps kind's own DaemonSets off the kwok fleet.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/kind/pkg/apis/config/v1alpha4"
	kindcluster "sigs.k8s.io/kind/pkg/cluster"
	"sigs.k8s.io/kind/pkg/log"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/apply"
)

// NodeImage is the pinned kind node image: Kubernetes v1.37.0, a
// multi-architecture index (linux/amd64 and linux/arm64).
const NodeImage = "kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5"

// Config is one kind cluster: its name, the kubeconfig file kind writes,
// and the feature gates and API runtime config every profile shares.
type Config struct {
	Name          string
	Kubeconfig    string
	FeatureGates  map[string]bool
	RuntimeConfig map[string]string
}

// SuiteConfig returns the configuration both profiles use: the Workload API
// served, and GenericWorkload (gangs, workload-aware preemption) and
// TopologyAwareWorkloadScheduling (alpha; PodGroup topology constraints) on
// in every component.
func SuiteConfig(name, kubeconfig string) Config {
	return Config{
		Name:          name,
		Kubeconfig:    kubeconfig,
		FeatureGates:  map[string]bool{"GenericWorkload": true, "TopologyAwareWorkloadScheduling": true},
		RuntimeConfig: map[string]string{"scheduling.k8s.io/v1beta1": "true"},
	}
}

func (c Config) kind() *v1alpha4.Cluster {
	k := &v1alpha4.Cluster{
		TypeMeta:      v1alpha4.TypeMeta{Kind: "Cluster", APIVersion: "kind.x-k8s.io/v1alpha4"},
		Name:          c.Name,
		Nodes:         []v1alpha4.Node{{Role: v1alpha4.ControlPlaneRole, Image: NodeImage}},
		FeatureGates:  c.FeatureGates,
		RuntimeConfig: c.RuntimeConfig,
	}
	v1alpha4.SetDefaultsCluster(k)
	return k
}

// Provider is the part of kind's cluster.Provider the suite uses.
type Provider interface {
	List() ([]string, error)
	Create(name string, opts ...kindcluster.CreateOption) error
	Delete(name, kubeconfig string) error
	CollectLogs(name, dir string) error
}

// KindProvider returns kind's Docker provider with its progress output off.
func KindProvider() Provider {
	return kindcluster.NewProvider(kindcluster.ProviderWithDocker(), kindcluster.ProviderWithLogger(log.NoopLogger{}))
}

// Cluster is a created cluster and its clients.
type Cluster struct {
	Name       string
	Kubeconfig string
	REST       *rest.Config
	Clientset  kubernetes.Interface
	Dynamic    dynamic.Interface
	Applier    *apply.Applier
	p          Provider
}

// Create makes the cluster. It refuses when a kind cluster of that name
// already exists: the suite neither reuses nor deletes a cluster it did not
// create in this run.
func Create(p Provider, cfg Config) (*Cluster, error) {
	names, err := p.List()
	if err != nil {
		return nil, fmt.Errorf("list kind clusters: %w", err)
	}
	for _, n := range names {
		if n == cfg.Name {
			return nil, fmt.Errorf("kind cluster %q already exists; the suite does not reuse or delete it. Remove it with: kind delete cluster --name %s", cfg.Name, cfg.Name)
		}
	}
	if err := p.Create(cfg.Name,
		kindcluster.CreateWithV1Alpha4Config(cfg.kind()),
		kindcluster.CreateWithKubeconfigPath(cfg.Kubeconfig),
		kindcluster.CreateWithWaitForReady(3*time.Minute),
		kindcluster.CreateWithDisplayUsage(false),
		kindcluster.CreateWithDisplaySalutation(false),
	); err != nil {
		return nil, fmt.Errorf("create kind cluster %q: %w", cfg.Name, err)
	}
	c, err := connect(p, cfg)
	if err != nil {
		// the cluster exists now: remove it, or the next run refuses the name
		if delErr := p.Delete(cfg.Name, cfg.Kubeconfig); delErr != nil {
			return nil, errors.Join(err, fmt.Errorf("delete kind cluster %q: %w", cfg.Name, delErr))
		}
		return nil, err
	}
	return c, nil
}

// connect builds the clients of a cluster kind has just created.
func connect(p Provider, cfg Config) (*Cluster, error) {
	restCfg, err := clientcmd.BuildConfigFromFlags("", cfg.Kubeconfig)
	if err != nil {
		return nil, err
	}
	restCfg.QPS, restCfg.Burst = 50, 100
	cs, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, err
	}
	dyn, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		return nil, err
	}
	a, err := apply.New(restCfg)
	if err != nil {
		return nil, err
	}
	return &Cluster{Name: cfg.Name, Kubeconfig: cfg.Kubeconfig, REST: restCfg, Clientset: cs, Dynamic: dyn, Applier: a, p: p}, nil
}

// Delete deletes the cluster.
func (c *Cluster) Delete() error { return c.p.Delete(c.Name, c.Kubeconfig) }

// ExportLogs writes what `kind export logs` writes into dir.
func (c *Cluster) ExportLogs(dir string) error { return c.p.CollectLogs(c.Name, dir) }

// offFleet keeps a DaemonSet's pods off nodes labeled type=kwok. NotIn also
// matches nodes without the label, so the control-plane node still qualifies.
const offFleet = `{"spec":{"template":{"spec":{"affinity":{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"type","operator":"NotIn","values":["kwok"]}]}]}}}}}}}`

// KeepSystemPodsOffFleet patches every kube-system DaemonSet (kind's
// kindnet and kube-proxy tolerate every taint) so none lands on a kwok
// node, and waits for each rollout.
func KeepSystemPodsOffFleet(ctx context.Context, cs kubernetes.Interface, timeout time.Duration) error {
	list, err := cs.AppsV1().DaemonSets("kube-system").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, ds := range list.Items {
		if _, err := cs.AppsV1().DaemonSets("kube-system").Patch(ctx, ds.Name, types.StrategicMergePatchType, []byte(offFleet), metav1.PatchOptions{}); err != nil {
			return fmt.Errorf("patch daemonset %s: %w", ds.Name, err)
		}
		err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
			d, err := cs.AppsV1().DaemonSets("kube-system").Get(ctx, ds.Name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			return rolledOut(d), nil
		})
		if err != nil {
			return fmt.Errorf("daemonset %s rollout: %w", ds.Name, err)
		}
	}
	return nil
}

func rolledOut(d *appsv1.DaemonSet) bool {
	s := d.Status
	return s.ObservedGeneration >= d.Generation && s.UpdatedNumberScheduled == s.DesiredNumberScheduled && s.NumberAvailable == s.DesiredNumberScheduled
}
