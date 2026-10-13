package cluster

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/labels"
)

// ClockOffset returns the API server's creation timestamp for a new
// ConfigMap minus the host time just before creating it, in seconds. The
// server stamps whole seconds, so values in (-1, +1) mean the clocks agree
// to the second. measured is false only when the create failed; when the
// create succeeds the offset is returned even if deleting the ConfigMap
// then fails, together with that delete error, so a caller can record the
// measurement before it fails.
func ClockOffset(ctx context.Context, cs kubernetes.Interface) (offset float64, measured bool, err error) {
	before := time.Now()
	cm, err := cs.CoreV1().ConfigMaps("default").Create(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		GenerateName: "scs-clock-", Labels: map[string]string{labels.PartOf: labels.PartOfValue},
	}}, metav1.CreateOptions{})
	if err != nil {
		return 0, false, fmt.Errorf("create clock ConfigMap: %w", err)
	}
	offset = cm.CreationTimestamp.Sub(before).Seconds()
	if err := cs.CoreV1().ConfigMaps("default").Delete(ctx, cm.Name, metav1.DeleteOptions{}); err != nil {
		return offset, true, fmt.Errorf("delete clock ConfigMap %s: %w", cm.Name, err)
	}
	return offset, true, nil
}
