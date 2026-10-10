package report_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/report"
)

var _ = Describe("MergeManifest", func() {
	base := report.Manifest{RunID: "r1", GitCommit: "abc1234", Host: map[string]string{"os": "darwin"}}

	It("keeps the first profile's entry when the second merges", func() {
		run := GinkgoT().TempDir()
		Expect(report.MergeManifest(run, base, "vanilla", report.ProfileRun{Cluster: "scs-vanilla", Start: t0})).To(Succeed())
		Expect(report.MergeManifest(run, base, "kueue", report.ProfileRun{Cluster: "scs-kueue", Start: t0.Add(time.Hour)})).To(Succeed())
		data, err := os.ReadFile(filepath.Join(run, "manifest.json"))
		Expect(err).NotTo(HaveOccurred())
		var m report.Manifest
		Expect(json.Unmarshal(data, &m)).To(Succeed())
		Expect(m.Schema).To(Equal(1))
		Expect(m.RunID).To(Equal("r1"))
		Expect(m.Profiles).To(HaveLen(2))
		Expect(m.Profiles["vanilla"].Cluster).To(Equal("scs-vanilla"))
		Expect(m.Profiles["kueue"].Start).To(Equal(t0.Add(time.Hour)))
	})

	It("refuses a manifest from another run", func() {
		run := GinkgoT().TempDir()
		Expect(report.MergeManifest(run, base, "vanilla", report.ProfileRun{})).To(Succeed())
		other := base
		other.RunID = "r2"
		Expect(report.MergeManifest(run, other, "kueue", report.ProfileRun{})).To(MatchError(filepath.Join(run, "manifest.json") + " belongs to run r1, not r2"))
	})
})

var _ = Describe("PrepareProfileDir", func() {
	It("creates the profile directory", func() {
		run := GinkgoT().TempDir()
		dir, err := report.PrepareProfileDir(run, "vanilla")
		Expect(err).NotTo(HaveOccurred())
		Expect(dir).To(Equal(filepath.Join(run, "vanilla")))
		Expect(dir).To(BeADirectory())
	})

	It("refuses a profile directory that already holds events", func() {
		run := GinkgoT().TempDir()
		dir := filepath.Join(run, "vanilla")
		Expect(os.MkdirAll(dir, 0o755)).To(Succeed())
		Expect(os.WriteFile(filepath.Join(dir, "events.jsonl"), []byte("{}\n"), 0o644)).To(Succeed())
		_, err := report.PrepareProfileDir(run, "vanilla")
		Expect(err).To(MatchError(dir + " already holds events.jsonl from an earlier run; choose another run ID"))
	})
})
