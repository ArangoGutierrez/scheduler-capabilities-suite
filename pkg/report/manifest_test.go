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

	// profileKeys merges pr as vanilla's entry and returns the keys
	// manifest.json holds for it.
	profileKeys := func(pr report.ProfileRun) map[string]any {
		run := GinkgoT().TempDir()
		Expect(report.MergeManifest(run, base, "vanilla", pr)).To(Succeed())
		data, err := os.ReadFile(filepath.Join(run, "manifest.json"))
		Expect(err).NotTo(HaveOccurred())
		var m struct {
			Profiles map[string]map[string]any `json:"profiles"`
		}
		Expect(json.Unmarshal(data, &m)).To(Succeed())
		return m.Profiles["vanilla"]
	}

	It("omits the end clock offset when the end check measured nothing", func() {
		keys := profileKeys(report.ProfileRun{Cluster: "scs-vanilla"})
		Expect(keys).To(HaveKeyWithValue("clock_offset_start_s", 0.0))
		Expect(keys).NotTo(HaveKey("clock_offset_end_s"))
	})

	It("keeps a measured end clock offset, zero included", func() {
		for raw, want := range map[string]float64{`{"clock_offset_end_s":-0.4}`: -0.4, `{"clock_offset_end_s":0}`: 0} {
			var pr report.ProfileRun
			Expect(json.Unmarshal([]byte(raw), &pr)).To(Succeed())
			Expect(profileKeys(pr)).To(HaveKeyWithValue("clock_offset_end_s", want), raw)
		}
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
