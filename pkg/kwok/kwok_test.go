package kwok

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestKwok(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "kwok") {
		t.Fatal("kwok specs failed")
	}
}

// The sha256 of each pinned file, recorded on 2026-10-10 from the v0.8.0
// release asset and tag; the same bytes R1 applied to a live cluster.
var pinned = map[string]string{
	"kwok.yaml":                         "a4c16e6431e382dcb5c1903139344b7a68652f16a6460337fe17a678a426f405",
	"node-heartbeat-with-lease.yaml":    "9becfc93321222915bd1ec8ab103fe769afe4b23e1386bf40553deb02e332c9c",
	"node-initialize.yaml":              "bb38f752d90248528b579a0ae8f0f660ff3428457eb5c1ebe6345c3246cc7ee5",
	"pod-complete.yaml":                 "7c24515ce76b597901e4e2f39fc372788c97cb2c40c580623450d0a2a13a9fe7",
	"pod-create.yaml":                   "e77798d07e6cd29b279a6edcd817bf8854291db07003b9e72d7a782019ccc49e",
	"pod-delete.yaml":                   "f4141c2a4e7c9aac18e6eedac238912b12935a597ec1267b59c0089ed9862f7a",
	"pod-init-container-completed.yaml": "48c96914db2186ef94d876c22161ec09c12a14a51b665088347be65467153a9c",
	"pod-init-container-running.yaml":   "cb26a694e43b3102703f891b56c5869f8e1da4e17d27c0a3ee9b7c3d48617e85",
	"pod-ready.yaml":                    "fd8335e528adc853303cfdfc1a86f4c669ef0b7f30170dcfb560c799c030369f",
}

var _ = Describe("Manifest", func() {
	It("serves every file in Order with its pinned sha256", func() {
		Expect(Order).To(HaveLen(len(pinned)))
		for _, name := range Order {
			data, sum, err := Manifest(name)
			Expect(err).NotTo(HaveOccurred(), name)
			Expect(data).NotTo(BeEmpty(), name)
			Expect(sum).To(Equal(pinned[name]), name)
		}
	})

	It("keeps the pod-complete stage that reads the per-pod delay annotations", func() {
		data, _, err := Manifest("pod-complete.yaml")
		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).To(ContainSubstring(`'.metadata.annotations["pod-complete.stage.kwok.x-k8s.io/delay"]'`))
		Expect(string(data)).To(ContainSubstring(`'.metadata.annotations["pod-complete.stage.kwok.x-k8s.io/jitter-delay"]'`))
	})
})

var _ = Describe("checkSum", func() {
	sums := "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae  a.yaml\n"
	It("accepts bytes whose sha256 is listed", func() {
		Expect(checkSum("a.yaml", []byte("foo"), sums)).To(Equal("2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae"))
	})
	It("rejects bytes that changed", func() {
		_, err := checkSum("a.yaml", []byte("fob"), sums)
		Expect(err).To(MatchError(ContainSubstring("kwok: a.yaml has sha256 ")))
	})
	It("rejects a file SHA256SUMS does not list", func() {
		_, err := checkSum("b.yaml", []byte("foo"), sums)
		Expect(err).To(MatchError("kwok: b.yaml is not listed in SHA256SUMS"))
	})
})
