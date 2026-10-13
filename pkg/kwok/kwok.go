// Package kwok installs kwok v0.8.0 (controller, node stages and the
// "general" pod stages) from manifests embedded in this package and pinned
// by sha256.
package kwok

import (
	"bufio"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/apply"
)

//go:embed manifests/*.yaml manifests/SHA256SUMS
var files embed.FS

// Order is the apply order: the controller and its CRDs, the node stages,
// then the pod stages. Sources: the v0.8.0 release asset kwok.yaml, and
// kustomize/stage/... at tag v0.8.0 (commit 156033d7df7ea0e09cea82b715fe566ea68aeeb4).
var Order = []string{
	"kwok.yaml",
	"node-initialize.yaml",
	"node-heartbeat-with-lease.yaml",
	"pod-create.yaml",
	"pod-init-container-running.yaml",
	"pod-init-container-completed.yaml",
	"pod-ready.yaml",
	"pod-complete.yaml",
	"pod-delete.yaml",
}

// checkSum returns data's sha256 when sums (sha256sum format) lists that
// value for name, and an error otherwise.
func checkSum(name string, data []byte, sums string) (string, error) {
	sum := sha256.Sum256(data)
	got := hex.EncodeToString(sum[:])
	sc := bufio.NewScanner(strings.NewReader(sums))
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) == 2 && f[1] == name {
			if f[0] != got {
				return "", fmt.Errorf("kwok: %s has sha256 %s, SHA256SUMS says %s", name, got, f[0])
			}
			return got, nil
		}
	}
	return "", fmt.Errorf("kwok: %s is not listed in SHA256SUMS", name)
}

// Manifest returns an embedded manifest and its verified sha256.
func Manifest(name string) ([]byte, string, error) {
	data, err := files.ReadFile("manifests/" + name)
	if err != nil {
		return nil, "", err
	}
	sums, err := files.ReadFile("manifests/SHA256SUMS")
	if err != nil {
		return nil, "", err
	}
	sum, err := checkSum(name, data, string(sums))
	if err != nil {
		return nil, "", err
	}
	return data, sum, nil
}

// Install applies every manifest in Order and waits until the
// kwok-controller Deployment has an available replica. It returns each
// manifest's sha256 for the run manifest.
func Install(ctx context.Context, a *apply.Applier, cs kubernetes.Interface, timeout time.Duration) (map[string]string, error) {
	sums := map[string]string{}
	for _, name := range Order {
		data, sum, err := Manifest(name)
		if err != nil {
			return nil, err
		}
		if _, err := a.Apply(ctx, data, timeout); err != nil {
			return nil, fmt.Errorf("kwok: apply %s: %w", name, err)
		}
		sums[name] = sum
	}
	err := wait.PollUntilContextTimeout(ctx, time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		d, err := cs.AppsV1().Deployments("kube-system").Get(ctx, "kwok-controller", metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		return d.Status.AvailableReplicas >= 1, nil
	})
	if err != nil {
		return nil, fmt.Errorf("kwok: controller not available: %w", err)
	}
	return sums, nil
}
