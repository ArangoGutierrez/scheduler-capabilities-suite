package report_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/report"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
)

func TestReport(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "report") {
		t.Fatal("report specs failed")
	}
}

var t0 = time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

func at(s float64) time.Time { return t0.Add(time.Duration(s * 1e9)) }

// podSpec is one fixture pod: probe, name, job, and the second of each
// transition (all four always happen here).
type podSpec struct {
	probe, name, job, node           string
	created, scheduled, running, end float64
}

func writeEvents(path string, pods []podSpec) {
	f, err := os.Create(path)
	Expect(err).NotTo(HaveOccurred())
	defer f.Close()
	enc := events.NewEncoder(f)
	seq := int64(0)
	for _, p := range pods {
		for _, step := range []struct {
			tr events.Transition
			s  float64
		}{{events.Created, p.created}, {events.Scheduled, p.scheduled}, {events.Running, p.running}, {events.Succeeded, p.end}} {
			seq++
			e := events.Event{Schema: 1, Seq: seq, RunID: "fixture-run", ObservedAt: at(step.s), Kind: "Pod",
				Namespace: "scs-" + p.probe, Name: p.name, UID: "uid-" + p.probe + "-" + p.name, Probe: p.probe,
				Job: p.job, Tenant: "t1", GPUs: 8, Transition: step.tr}
			if step.tr == events.Scheduled {
				e.Node = p.node
			}
			Expect(enc.Encode(e)).To(Succeed())
		}
	}
	Expect(enc.Flush()).To(Succeed())
}

const claimsYAML = `scheduler: vanilla
version: fixture
claims:
  gang: {claim: Gangs are placed whole., citation: "https://example.org/g", expected: supported, mechanism: fixture}
  priority: {claim: High priority goes first., citation: "https://example.org/p", expected: supported, mechanism: fixture}
  preemption: {claim: x, citation: "https://example.org/x", expected: partial, mechanism: fixture}
  fair-share: {claim: x, citation: "https://example.org/x", expected: not_supported, mechanism: fixture}
  backfill: {claim: x, citation: "https://example.org/x", expected: partial, mechanism: fixture}
  topology: {claim: x, citation: "https://example.org/x", expected: supported, mechanism: fixture}
`

const probesJSON = `[
 {"capability":"gang","params":{"jobs":["gang-a","gang-b"],"size":2,"grace_sec":5},"window_end":"2026-10-10T10:01:40Z"},
 {"capability":"gang-control","params":{"jobs":["gang-a","gang-b"],"size":2,"grace_sec":5},"window_end":"2026-10-10T10:01:40Z"},
 {"capability":"priority","params":{"high_job":"high","queued_jobs":["q1"]},"window_end":"2026-10-10T10:01:40Z"}
]`

const wantSummary = `# Scheduling capabilities: documented vs observed

Run fixture-run. Each verdict comes from a single run: evidence for the POC, not a result to quote.

| capability | profile | documented | observed | agree | evidence |
|---|---|---|---|---|---|
| gang | vanilla | supported | supported | yes | no gang was partly placed for more than 5 s, and all 2 gangs completed |
| priority | vanilla | supported | not_supported | no | all 1 queued lower-priority pods were scheduled before the high-priority pod |

## Findings

- vanilla, priority: documented supported, observed not_supported (all 1 queued lower-priority pods were scheduled before the high-priority pod).

## Controls

- vanilla, gang-control: partial (gang gang-b was partly placed for 21.5 s; all 2 gangs completed).
`

// fixtureRun builds a run directory with one profile (vanilla) and three
// probes: gang (agrees with its claim), gang-control (a control) and
// priority (disagrees with its claim).
func fixtureRun() string {
	run := GinkgoT().TempDir()
	Expect(os.WriteFile(filepath.Join(run, report.ManifestFile), []byte(`{"schema":1,"run_id":"fixture-run"}`), 0o644)).To(Succeed())
	dir := filepath.Join(run, "vanilla")
	Expect(os.MkdirAll(dir, 0o755)).To(Succeed())
	writeEvents(filepath.Join(dir, report.EventsFile), []podSpec{
		{"gang", "a0", "gang-a", "n1", 0, 1.0, 2, 22}, {"gang", "a1", "gang-a", "n2", 0, 1.5, 2, 22},
		{"gang", "b0", "gang-b", "n1", 0, 22.5, 23.5, 44}, {"gang", "b1", "gang-b", "n2", 0, 23.0, 24, 44},
		{"gang-control", "a0", "gang-a", "n1", 0, 1, 2, 22}, {"gang-control", "a1", "gang-a", "n2", 0, 1, 2, 22},
		{"gang-control", "b0", "gang-b", "n3", 0, 1, 2, 22}, {"gang-control", "b1", "gang-b", "n1", 0, 22.5, 23, 43},
		{"priority", "q1", "q1", "n1", 1, 30, 30.5, 50}, {"priority", "h0", "high", "n2", 3, 40, 40.5, 60},
	})
	Expect(os.WriteFile(filepath.Join(dir, report.ProbesFile), []byte(probesJSON), 0o644)).To(Succeed())
	Expect(os.WriteFile(filepath.Join(dir, report.ClaimsFile), []byte(claimsYAML), 0o644)).To(Succeed())
	return run
}

var _ = Describe("Write", func() {
	It("writes the summary: one row per claimed probe, the findings, the controls", func() {
		run := fixtureRun()
		Expect(report.Write(run, run)).To(Succeed())
		got, err := os.ReadFile(filepath.Join(run, report.SummaryFile))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(got)).To(Equal(wantSummary))
	})

	It("writes each probe's verdict, claim and agreement", func() {
		run := fixtureRun()
		Expect(report.Write(run, run)).To(Succeed())
		data, err := os.ReadFile(filepath.Join(run, "vanilla", report.VerdictsFile))
		Expect(err).NotTo(HaveOccurred())
		var got []report.Entry
		Expect(json.Unmarshal(data, &got)).To(Succeed())
		Expect(got).To(HaveLen(3))

		Expect(got[0].Capability).To(Equal("gang"))
		Expect(got[0].Observed.Outcome).To(Equal(verdict.Supported))
		Expect(got[0].Observed.Measurements).To(Equal(map[string]float64{"gangs": 2, "gangs_completed": 2, "max_partial_s": 0.5}))
		Expect(got[0].Observed.EvidenceSeq).To(Equal([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
		Expect(*got[0].Agree).To(BeTrue())

		Expect(got[1].Capability).To(Equal("gang-control"))
		Expect(got[1].Documented).To(BeNil())
		Expect(got[1].Agree).To(BeNil())
		Expect(got[1].Observed.Measurements["max_partial_s"]).To(Equal(21.5))
		Expect(got[1].Observed.EvidenceSeq).To(Equal([]int64{17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}))

		Expect(got[2].Observed.Measurements).To(Equal(map[string]float64{"queued_jobs": 1, "queued_before_high": 1, "high_time_to_schedule_s": 37}))
		Expect(got[2].Documented.Citation).To(Equal("https://example.org/p"))
		Expect(*got[2].Agree).To(BeFalse())
	})

	It("gives byte-identical output when rebuilt into another directory", func() {
		run := fixtureRun()
		other := GinkgoT().TempDir()
		Expect(report.Write(run, run)).To(Succeed())
		Expect(report.Write(run, other)).To(Succeed())
		for _, f := range []string{report.SummaryFile, filepath.Join("vanilla", report.VerdictsFile)} {
			a, err := os.ReadFile(filepath.Join(run, f))
			Expect(err).NotTo(HaveOccurred())
			b, err := os.ReadFile(filepath.Join(other, f))
			Expect(err).NotTo(HaveOccurred())
			Expect(b).To(Equal(a), f)
		}
	})

	It("fails when the manifest's run_id is empty", func() {
		run := fixtureRun()
		Expect(os.WriteFile(filepath.Join(run, report.ManifestFile), []byte(`{"schema":1,"run_id":""}`), 0o644)).To(Succeed())
		Expect(report.Write(run, run)).To(MatchError(filepath.Join(run, "manifest.json") + ": run_id is empty"))
	})

	It("fails when no profile directory has events", func() {
		run := GinkgoT().TempDir()
		Expect(os.WriteFile(filepath.Join(run, report.ManifestFile), []byte(`{"run_id":"x"}`), 0o644)).To(Succeed())
		Expect(report.Write(run, run)).To(MatchError(run + ": no profile directory has an events.jsonl"))
	})

	It("writes nothing when a later profile fails to evaluate", func() {
		run := fixtureRun()
		// kueue gets vanilla's events and claims, and a probe naming a job
		// that was never recorded, so it fails after vanilla evaluated
		kueue := filepath.Join(run, "kueue")
		Expect(os.MkdirAll(kueue, 0o755)).To(Succeed())
		for _, f := range []string{report.EventsFile, report.ClaimsFile} {
			data, err := os.ReadFile(filepath.Join(run, "vanilla", f))
			Expect(err).NotTo(HaveOccurred())
			Expect(os.WriteFile(filepath.Join(kueue, f), data, 0o644)).To(Succeed())
		}
		Expect(os.WriteFile(filepath.Join(kueue, report.ProbesFile), []byte(`[{"capability":"priority","params":{"high_job":"high","queued_jobs":["q9"]},"window_end":"2026-10-10T10:01:40Z"}]`), 0o644)).To(Succeed())
		out := GinkgoT().TempDir()
		Expect(report.Write(run, out)).To(MatchError("kueue: priority: no events recorded for job q9"))
		Expect(filepath.Join(out, "vanilla", report.VerdictsFile)).NotTo(BeAnExistingFile())
		Expect(filepath.Join(out, report.SummaryFile)).NotTo(BeAnExistingFile())
	})

	It("fails when a profile's probes.json lists no probes", func() {
		run := fixtureRun()
		Expect(os.WriteFile(filepath.Join(run, "vanilla", report.ProbesFile), []byte("[]"), 0o644)).To(Succeed())
		Expect(report.Write(run, run)).To(MatchError("vanilla: probes.json lists no probes"))
	})

	It("fails when a probe names a job the run never recorded, instead of guessing", func() {
		run := fixtureRun()
		p := filepath.Join(run, "vanilla", report.ProbesFile)
		Expect(os.WriteFile(p, []byte(`[{"capability":"priority","params":{"high_job":"high","queued_jobs":["q9"]},"window_end":"2026-10-10T10:01:40Z"}]`), 0o644)).To(Succeed())
		Expect(report.Write(run, run)).To(MatchError("vanilla: priority: no events recorded for job q9"))
	})
})
