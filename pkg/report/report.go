// Package report rebuilds a run's verdicts and summary from what the run
// recorded: events.jsonl, probes.json and the claims snapshot of each
// profile. The e2e suite and cmd/scs-report both call Write, so a run's own
// output and an offline rebuild come from the same code.
package report

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/claims"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
)

// Profiles lists every profile the suite knows, in report order.
var Profiles = []string{"vanilla", "kueue"}

// File names inside a run directory.
const (
	ManifestFile = "manifest.json"
	SummaryFile  = "summary.md"
	EventsFile   = "events.jsonl"
	ProbesFile   = "probes.json"
	ClaimsFile   = "claims.yaml"
	VerdictsFile = "verdicts.json"
)

// Entry is one probe's documented claim and observed verdict.
type Entry struct {
	Capability string          `json:"capability"`
	Profile    string          `json:"profile"`
	Documented *claims.Claim   `json:"documented,omitempty"`
	Observed   verdict.Verdict `json:"observed"`
	Agree      *bool           `json:"agree,omitempty"`
}

// ReadProbes reads a profile's probes.json.
func ReadProbes(path string) ([]verdict.Probe, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var probes []verdict.Probe
	if err := dec.Decode(&probes); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return probes, nil
}

// ProfileVerdicts evaluates every probe recorded in profileDir, in the
// order probes.json lists them.
func ProfileVerdicts(profileDir, profile string) ([]Entry, error) {
	evs, err := events.ReadFile(filepath.Join(profileDir, EventsFile))
	if err != nil {
		return nil, err
	}
	probes, err := ReadProbes(filepath.Join(profileDir, ProbesFile))
	if err != nil {
		return nil, err
	}
	if len(probes) == 0 {
		return nil, fmt.Errorf("%s: %s lists no probes", profile, ProbesFile)
	}
	cl, err := claims.Load(filepath.Join(profileDir, ClaimsFile))
	if err != nil {
		return nil, err
	}
	out := make([]Entry, 0, len(probes))
	for _, pr := range probes {
		v, err := verdict.Evaluate(evs, pr)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", profile, err)
		}
		e := Entry{Capability: pr.Capability, Profile: profile, Observed: v}
		if c, ok := cl.Claims[pr.Capability]; ok {
			agree := c.Expected == v.Outcome
			e.Documented, e.Agree = &c, &agree
		}
		out = append(out, e)
	}
	return out, nil
}

// Write evaluates every profile directory of runDir that has an events file
// and writes <outDir>/<profile>/verdicts.json and <outDir>/summary.md. It
// evaluates every profile before writing any file, so an error leaves no
// partial rebuild behind.
func Write(runDir, outDir string) error {
	runID, err := readRunID(filepath.Join(runDir, ManifestFile))
	if err != nil {
		return err
	}
	type profileEntries struct {
		profile string
		entries []Entry
	}
	var done []profileEntries
	var all []Entry
	for _, prof := range Profiles {
		dir := filepath.Join(runDir, prof)
		if _, err := os.Stat(filepath.Join(dir, EventsFile)); errors.Is(err, fs.ErrNotExist) {
			continue
		} else if err != nil {
			return err
		}
		entries, err := ProfileVerdicts(dir, prof)
		if err != nil {
			return err
		}
		done = append(done, profileEntries{prof, entries})
		all = append(all, entries...)
	}
	if len(all) == 0 {
		return fmt.Errorf("%s: no profile directory has an %s", runDir, EventsFile)
	}
	for _, d := range done {
		b, err := json.MarshalIndent(d.entries, "", "  ")
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Join(outDir, d.profile), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(outDir, d.profile, VerdictsFile), append(b, '\n'), 0o644); err != nil {
			return err
		}
	}
	return os.WriteFile(filepath.Join(outDir, SummaryFile), Summary(runID, all), 0o644)
}

func readRunID(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	var m struct {
		RunID string `json:"run_id"`
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return "", fmt.Errorf("%s: %w", path, err)
	}
	if m.RunID == "" {
		return "", fmt.Errorf("%s: run_id is empty", path)
	}
	return m.RunID, nil
}

// Summary renders summary.md: a row per capability and profile, then the
// findings (documented and observed disagree), then the controls.
func Summary(runID string, entries []Entry) []byte {
	var b strings.Builder
	b.WriteString("# Scheduling capabilities: documented vs observed\n\n")
	fmt.Fprintf(&b, "Run %s. Each verdict comes from a single run: evidence for the POC, not a result to quote.\n\n", runID)
	b.WriteString("| capability | profile | documented | observed | agree | evidence |\n")
	b.WriteString("|---|---|---|---|---|---|\n")
	for _, c := range verdict.Capabilities {
		for _, p := range Profiles {
			for _, e := range entries {
				if e.Capability != c || e.Profile != p || e.Documented == nil {
					continue
				}
				agree := "no"
				if *e.Agree {
					agree = "yes"
				}
				fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s |\n", c, p, e.Documented.Expected, e.Observed.Outcome, agree, e.Observed.Reason)
			}
		}
	}
	b.WriteString("\n## Findings\n\n")
	found := false
	for _, e := range entries {
		if e.Agree != nil && !*e.Agree {
			found = true
			fmt.Fprintf(&b, "- %s, %s: documented %s, observed %s (%s).\n", e.Profile, e.Capability, e.Documented.Expected, e.Observed.Outcome, e.Observed.Reason)
		}
	}
	if !found {
		b.WriteString("None: every observed verdict matches its documented claim.\n")
	}
	b.WriteString("\n## Controls\n\n")
	found = false
	for _, e := range entries {
		if e.Documented == nil {
			found = true
			fmt.Fprintf(&b, "- %s, %s: %s (%s).\n", e.Profile, e.Capability, e.Observed.Outcome, e.Observed.Reason)
		}
	}
	if !found {
		b.WriteString("None.\n")
	}
	return []byte(b.String())
}
