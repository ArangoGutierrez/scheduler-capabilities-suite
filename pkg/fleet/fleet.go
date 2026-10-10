// Package fleet describes the suite's kwok GPU fleet and creates it.
package fleet

import (
	"context"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/yaml"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/apply"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/labels"
)

// GPUResource is the extended resource every fake node offers.
const GPUResource = "nvidia.com/gpu"

// Node is one fake node: name, rack, block and GPU count.
type Node struct {
	Name  string `json:"name"`
	Rack  string `json:"rack"`
	Block string `json:"block"`
	GPUs  int    `json:"gpus"`
}

// Spec is the fleet.
type Spec struct {
	Nodes []Node `json:"nodes"`
}

// Default is the spec's fleet: four 8-GPU nodes, two racks of two, one block.
func Default() Spec {
	return Spec{Nodes: []Node{
		{Name: "n1", Rack: "rack-a", Block: "block-1", GPUs: 8},
		{Name: "n2", Rack: "rack-a", Block: "block-1", GPUs: 8},
		{Name: "n3", Rack: "rack-b", Block: "block-1", GPUs: 8},
		{Name: "n4", Rack: "rack-b", Block: "block-1", GPUs: 8},
	}}
}

// GPUs is the fleet's total GPU count.
func (s Spec) GPUs() int {
	n := 0
	for _, x := range s.Nodes {
		n += x.GPUs
	}
	return n
}

// RackOf maps each node name to its rack.
func (s Spec) RackOf() map[string]string {
	m := map[string]string{}
	for _, x := range s.Nodes {
		m[x.Name] = x.Rack
	}
	return m
}

// Manifest renders the fleet as multi-document YAML. kwok manages a node
// because of the annotation kwok.x-k8s.io/node=fake; the NoSchedule taint
// keeps every pod that does not tolerate it (the suite's probes do) off the
// fake nodes.
func (s Spec) Manifest() ([]byte, error) {
	var out []byte
	for _, x := range s.Nodes {
		res := map[string]any{"cpu": "64", "memory": "512Gi", "pods": "110", GPUResource: strconv.Itoa(x.GPUs)}
		node := map[string]any{
			"apiVersion": "v1",
			"kind":       "Node",
			"metadata": map[string]any{
				"name":        x.Name,
				"annotations": map[string]any{"kwok.x-k8s.io/node": "fake", "node.alpha.kubernetes.io/ttl": "0"},
				"labels": map[string]any{
					"type": "kwok", "kubernetes.io/hostname": x.Name, "kubernetes.io/os": "linux", "kubernetes.io/arch": "amd64",
					labels.PartOf: labels.PartOfValue, labels.Rack: x.Rack, labels.Block: x.Block,
				},
			},
			"spec": map[string]any{"taints": []any{map[string]any{"key": "kwok.x-k8s.io/node", "value": "fake", "effect": "NoSchedule"}}},
			"status": map[string]any{
				"capacity": res, "allocatable": res,
				"nodeInfo": map[string]any{"architecture": "amd64", "operatingSystem": "linux", "kubeletVersion": "fake", "kubeProxyVersion": "fake"},
			},
		}
		b, err := yaml.Marshal(node)
		if err != nil {
			return nil, err
		}
		out = append(out, "---\n"...)
		out = append(out, b...)
	}
	return out, nil
}

// Create applies the fleet once per cluster (re-applying a Node's status is
// not relied on).
func Create(ctx context.Context, a *apply.Applier, s Spec) error {
	m, err := s.Manifest()
	if err != nil {
		return err
	}
	_, err = a.Apply(ctx, m, time.Minute)
	return err
}

// WaitReady waits until every fleet node is Ready with its GPUs allocatable.
func WaitReady(ctx context.Context, cs kubernetes.Interface, s Spec, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		for _, x := range s.Nodes {
			n, err := cs.CoreV1().Nodes().Get(ctx, x.Name, metav1.GetOptions{})
			if err != nil {
				return false, nil
			}
			q := n.Status.Allocatable[GPUResource]
			if q.Value() != int64(x.GPUs) || !ready(n) {
				return false, nil
			}
		}
		return true, nil
	})
}

func ready(n *corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// Describe returns a one-line description for logs.
func (s Spec) Describe() string {
	return fmt.Sprintf("%d nodes, %d GPUs", len(s.Nodes), s.GPUs())
}
