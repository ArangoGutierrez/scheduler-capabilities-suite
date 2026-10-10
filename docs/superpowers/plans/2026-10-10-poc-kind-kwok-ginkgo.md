# Scheduling Capabilities Suite POC (kind, kwok, Ginkgo) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Go harness that runs six scheduling-capability probes (gang, priority, preemption, fair-share, backfill, topology) on vanilla Kubernetes 1.37 and on Kueue v0.20.1, on kind with a kwok GPU fleet, records every object transition to `events.jsonl`, and derives documented-vs-observed verdicts that `cmd/scs-report` can rebuild byte for byte.

**Architecture:** Scheduler-neutral packages (`events`, `verdict`, `claims`, `report`, `recorder`) hold the data model and the verdict rules as pure functions over recorded events. Cluster packages (`apply`, `kwok`, `cluster`, `fleet`) build a kind cluster with a four-node kwok fleet. A `Profile` per scheduler (`vanilla`, `kueue`) translates scheduler-neutral jobs into that scheduler's objects; the Ginkgo e2e suite runs each probe once per profile.

**Tech Stack:** Go 1.27, Ginkgo v2.33.0 and Gomega v1.44.0 (unit and e2e), k8s.io/{api,apimachinery,client-go} v0.37.1, sigs.k8s.io/kind v0.33.0 (Go library), sigs.k8s.io/yaml v1.6.0, kwok v0.8.0, Kueue v0.20.1.

**Spec:** `docs/superpowers/specs/2026-10-10-poc-kind-kwok-ginkgo-design.md` (commit `1247043`; section 12, amendments D7 and A1 to A11, lands in the same commit as this plan and overrides the sections it names). Executors read both.

**Provenance of this plan's code.** Every file in this plan was compiled and tested in a scratch module before the plan was written: `go vet` clean at every task boundary, 115 unit specs green (1 live-cluster spec skipped without a kubeconfig), and 32 mutants of the verdict rules, the event reader and the recorder each killed by the specs below. The e2e harness ran live, one probe per cluster, on kind 1.37 with kwok v0.8.0 on this Mac (gang and gang control on vanilla, gang on Kueue, then every other probe on both profiles); each task's expected verdict lines come from those runs. The scheduler and cluster literals come from R1, a live-cluster spike on 2026-10-10 (`.superpowers/sdd/2026-10-10-scs-poc/r1-report.md` in the schedCapSuit workspace). Copy code exactly; if a step's expected output differs, stop and report rather than adapt.

## Global Constraints

- Worktree: `W=<your checkout of this repository>`, branch `feat/poc` of `github.com/ArangoGutierrez/scheduler-capabilities-suite`. Every command starts with `cd "$W"` or uses `git -C "$W"`; the shell's working directory does not persist between calls.
- Module path `github.com/ArangoGutierrez/scheduler-capabilities-suite`; `go 1.27`; dependencies exactly: ginkgo/v2 v2.33.0 (also as a `go tool`), gomega v1.44.0, k8s.io/api, k8s.io/apimachinery, k8s.io/client-go v0.37.1, sigs.k8s.io/kind v0.33.0, sigs.k8s.io/yaml v1.6.0. No Kueue Go dependency: Kueue objects are YAML applied through `pkg/apply` and read through the dynamic client.
- Every go command runs with the Bash sandbox disabled and `export GOMODCACHE=$HOME/go/pkg/mod GOCACHE=$HOME/Library/Caches/go-build` (this Mac's persisted go env points at a corrupt cache). Docker and kind commands also run with the sandbox disabled.
- The Bash tool's shell is zsh with interactive aliases: use `command rm -f`, `command cp -f`, `command mv -f`; put anything stateful in a `#!/bin/bash` script file and run it with `bash file`.
- Node image `kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5`; kwok v0.8.0 (tag commit `156033d7df7ea0e09cea82b715fe566ea68aeeb4`); Kueue v0.20.1. GPU resource `nvidia.com/gpu`.
- Labels on every suite object: `app.kubernetes.io/part-of: scheduler-capabilities-suite` and keys under `scs.arangogutierrez.github.io/` (constants in `pkg/labels`).
- Cluster names `scs-vanilla` and `scs-kueue`, one at a time, never concurrently. Never touch, delete or export any other kind cluster (`bench-mokka` exists on this Mac).
- Ginkgo bootstrap: each package's `func TestX(t *testing.T)` lives in the package's main spec file, not a separate `*_suite_test.go`, and reads `if !RunSpecs(t, "x") { t.Fatal("x specs failed") }`. The global test-quality hook flags a bootstrap-only file and a bare `RunSpecs` call (false positives); this form passes it honestly.
- Tests: every expected value is a literal or derived by hand in a comment beside it; never compute an expectation by calling the code under test.
- Text in every file: no em-dash, no emoji, none of the words the global anti-AI-tells rule bans (a hook enforces it on write).
- Commits: conventional (`type(scope): description`), body says why, `git -C "$W" commit -s -S`. Test and implementation of one step may share a commit only where a step says so; otherwise the RED test commit comes first. Never push, open a PR, or post anywhere.

## Review Focus

1. A gang that never gets any pod placed (neither partly placed nor completed) must be judged `not_supported` naming that gang, never `supported`. Pinned in Task 2 (`is not supported when a gang never gets a pod placed`).
2. A queued pod scheduled at the same instant as the high-priority pod must not count as ahead of it. Pinned in Task 3 (`does not count a queued pod scheduled at the same instant as ahead`).
3. A run interrupted mid-write leaves a truncated last line in `events.jsonl`; reading it must fail naming the line, not drop it silently. Pinned in Task 1 (`rejects a truncated last line and names it`).
4. A kind cluster left behind by `KEEP_CLUSTER=1` must make the next run refuse with the delete command, never reuse or delete it. Pinned in Task 9 (`refuses a cluster name that already exists`).
5. Re-running with a run ID whose results directory already holds `events.jsonl` must refuse rather than overwrite a previous run's data. Pinned in Task 7 (`refuses a profile directory that already holds events`).

---

### Task 1: Module scaffold and the event schema

**Files:**
- Create: `go.mod`, `go.sum` (by `go mod`), `Makefile`, `.gitignore`
- Create: `pkg/events/events.go`
- Test: `pkg/events/events_test.go`

**Interfaces:**
- Produces: package `events`: `const SchemaVersion = 1`; `type Transition string` with constants `Created, Scheduled, Running, Succeeded, Failed, Deleted, Preempted, Evicted, Admitted, QuotaReserved, Requeued, Unschedulable`; `type Event struct{...}` (fields below); `func (Event) Validate() error`; `func NewEncoder(io.Writer) *Encoder`; `func (*Encoder) Encode(Event) error`; `func (*Encoder) Flush() error`; `func Read(io.Reader) ([]Event, error)`; `func ReadFile(path string) ([]Event, error)`.

- [ ] **Step 1: Create the module, pin dependencies, add the Makefile and .gitignore**

```bash
cd "$W" && export GOMODCACHE=$HOME/go/pkg/mod GOCACHE=$HOME/Library/Caches/go-build && \
go mod init github.com/ArangoGutierrez/scheduler-capabilities-suite && \
go mod edit -go=1.27 && \
go get github.com/onsi/ginkgo/v2@v2.33.0 github.com/onsi/gomega@v1.44.0 k8s.io/api@v0.37.1 k8s.io/apimachinery@v0.37.1 k8s.io/client-go@v0.37.1 sigs.k8s.io/kind@v0.33.0 sigs.k8s.io/yaml@v1.6.0 && \
go get -tool github.com/onsi/ginkgo/v2/ginkgo@v2.33.0 && go tool ginkgo version
```

Expected: last line `Ginkgo Version 2.33.0`.

`Makefile`:

```make
SHELL := /bin/bash
GO ?= go

.PHONY: test lint

## test: every unit suite (no cluster); the e2e suite needs one and is skipped here
test:
	$(GO) tool ginkgo --no-color -r --skip-package=e2e

## lint: go vet and gofmt
lint:
	$(GO) vet ./...
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi
```

`.gitignore`:

```text
# run outputs: events, verdicts, reports, cluster logs
/results/
# kubeconfigs the suite writes
*.kubeconfig
```

- [ ] **Step 2: Write the failing spec file**

`pkg/events/events_test.go`:

```go
package events_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
)

func TestEvents(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "events") {
		t.Fatal("events specs failed")
	}
}

var t0 = time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

func ev(seq int64, tr events.Transition) events.Event {
	return events.Event{
		Schema: 1, Seq: seq, RunID: "r1", ObservedAt: t0.Add(time.Duration(seq) * time.Second),
		Kind: "Pod", Namespace: "scs-gang", Name: "p", UID: "u",
		Probe: "gang", Job: "gang-a", Tenant: "t1", GPUs: 8, Transition: tr,
	}
}

const line1 = `{"schema":1,"seq":1,"run_id":"r1","observed_at":"2026-10-10T10:00:01Z","kind":"Pod","namespace":"scs-gang","name":"p","uid":"u","probe":"gang","job":"gang-a","tenant":"t1","priority":0,"gpus":8,"transition":"created"}`

var _ = Describe("events", func() {
	It("writes one JSON object per line and reads the same events back", func() {
		var buf bytes.Buffer
		enc := events.NewEncoder(&buf)
		apiTime := time.Date(2026, 10, 10, 10, 0, 1, 0, time.UTC)
		second := ev(2, events.Scheduled)
		second.APITime = &apiTime
		second.APITimePrecision = "s"
		second.Node = "n1"
		Expect(enc.Encode(ev(1, events.Created))).To(Succeed())
		Expect(enc.Encode(second)).To(Succeed())
		Expect(enc.Flush()).To(Succeed())
		Expect(buf.String()).To(Equal(line1 + "\n" +
			`{"schema":1,"seq":2,"run_id":"r1","observed_at":"2026-10-10T10:00:02Z","api_time":"2026-10-10T10:00:01Z","api_time_precision":"s","kind":"Pod","namespace":"scs-gang","name":"p","uid":"u","probe":"gang","job":"gang-a","tenant":"t1","priority":0,"gpus":8,"transition":"scheduled","node":"n1"}` + "\n"))

		got, err := events.Read(&buf)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(HaveLen(2))
		Expect(got[0].Transition).To(Equal(events.Created))
		Expect(got[1].Node).To(Equal("n1"))
		Expect(got[1].APITime.Equal(apiTime)).To(BeTrue())
		Expect(got[1].ObservedAt.Equal(t0.Add(2 * time.Second))).To(BeTrue())
	})

	It("rejects a gap in seq and names the line", func() {
		in := line1 + "\n" + strings.Replace(line1, `"seq":1`, `"seq":3`, 1) + "\n"
		_, err := events.Read(strings.NewReader(in))
		Expect(err).To(MatchError("line 2: seq 3, want 2"))
	})

	It("rejects a truncated last line and names it", func() {
		in := line1 + "\n" + `{"schema":1,"seq":2,"run_id":"r1",` + "\n"
		_, err := events.Read(strings.NewReader(in))
		Expect(err).To(MatchError("line 2: unexpected EOF"))
	})

	It("rejects an unknown transition", func() {
		in := strings.Replace(line1, `"created"`, `"teleported"`, 1)
		_, err := events.Read(strings.NewReader(in))
		Expect(err).To(MatchError(`line 1: seq 1: unknown transition "teleported"`))
	})

	It("rejects a schema version other than 1", func() {
		in := strings.Replace(line1, `"schema":1`, `"schema":2`, 1)
		_, err := events.Read(strings.NewReader(in))
		Expect(err).To(MatchError("line 1: seq 1: schema 2, want 1"))
	})

	It("rejects a field the schema does not define", func() {
		in := strings.Replace(line1, `"kind":"Pod"`, `"kind":"Pod","colour":"red"`, 1)
		_, err := events.Read(strings.NewReader(in))
		Expect(err).To(MatchError(`line 1: json: unknown field "colour"`))
	})

	It("refuses to write an event with seq 0", func() {
		enc := events.NewEncoder(&bytes.Buffer{})
		Expect(enc.Encode(ev(0, events.Created))).To(MatchError("seq 0: seq must be 1 or more"))
	})

	It("refuses to write api_time without a precision", func() {
		e := ev(1, events.Created)
		apiTime := t0
		e.APITime = &apiTime
		enc := events.NewEncoder(&bytes.Buffer{})
		Expect(enc.Encode(e)).To(MatchError(`seq 1: api_time_precision "", want s or us`))
	})
})
```

- [ ] **Step 3: Run it and watch it fail**

Run: `cd "$W" && go test ./pkg/events/`
Expected: FAIL, build failure naming `undefined: events.Event` (and the other missing names).

- [ ] **Step 4: Commit the failing spec**

```bash
git -C "$W" add go.mod go.sum Makefile .gitignore pkg/events/events_test.go
git -C "$W" commit -s -S -m "test(events): specify the events.jsonl schema and its reader" -m "The probes' raw events are the source of truth for every verdict, so the line format and the reader's refusals (seq gaps, truncated lines, unknown fields) are pinned before the code."
```

- [ ] **Step 5: Implement**

`pkg/events/events.go`:

```go
// Package events defines the recorded-event schema (version 1) of the
// Scheduling Capabilities Suite and reads and writes it as JSON Lines.
package events

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"
)

// SchemaVersion is the line format this package writes and accepts.
const SchemaVersion = 1

// Transition names what changed about an object.
type Transition string

// The transitions the recorder writes.
const (
	Created       Transition = "created"
	Scheduled     Transition = "scheduled"
	Running       Transition = "running"
	Succeeded     Transition = "succeeded"
	Failed        Transition = "failed"
	Deleted       Transition = "deleted"
	Preempted     Transition = "preempted"
	Evicted       Transition = "evicted"
	Admitted      Transition = "admitted"
	QuotaReserved Transition = "quota_reserved"
	Requeued      Transition = "requeued"
	Unschedulable Transition = "unschedulable"
)

var knownTransitions = map[Transition]bool{
	Created: true, Scheduled: true, Running: true, Succeeded: true, Failed: true,
	Deleted: true, Preempted: true, Evicted: true, Admitted: true,
	QuotaReserved: true, Requeued: true, Unschedulable: true,
}

// Event is one line of events.jsonl. ObservedAt is when the recorder wrote
// the change; APITime is the object's own timestamp for it, when it has one.
type Event struct {
	Schema           int        `json:"schema"`
	Seq              int64      `json:"seq"`
	RunID            string     `json:"run_id"`
	ObservedAt       time.Time  `json:"observed_at"`
	APITime          *time.Time `json:"api_time,omitempty"`
	APITimePrecision string     `json:"api_time_precision,omitempty"`
	Kind             string     `json:"kind"`
	Namespace        string     `json:"namespace"`
	Name             string     `json:"name"`
	UID              string     `json:"uid"`
	Probe            string     `json:"probe,omitempty"`
	Job              string     `json:"job,omitempty"`
	Tenant           string     `json:"tenant,omitempty"`
	Priority         int32      `json:"priority"`
	GPUs             int64      `json:"gpus"`
	Transition       Transition `json:"transition"`
	Node             string     `json:"node,omitempty"`
	Reason           string     `json:"reason,omitempty"`
	Message          string     `json:"message,omitempty"`
}

// Validate returns the first way e breaks the schema, or nil.
func (e Event) Validate() error {
	if e.Schema != SchemaVersion {
		return fmt.Errorf("seq %d: schema %d, want %d", e.Seq, e.Schema, SchemaVersion)
	}
	if e.Seq < 1 {
		return fmt.Errorf("seq %d: seq must be 1 or more", e.Seq)
	}
	if e.ObservedAt.IsZero() {
		return fmt.Errorf("seq %d: observed_at is missing", e.Seq)
	}
	if e.Kind == "" {
		return fmt.Errorf("seq %d: kind is missing", e.Seq)
	}
	if !knownTransitions[e.Transition] {
		return fmt.Errorf("seq %d: unknown transition %q", e.Seq, e.Transition)
	}
	if e.APITime != nil && e.APITimePrecision != "s" && e.APITimePrecision != "us" {
		return fmt.Errorf("seq %d: api_time_precision %q, want s or us", e.Seq, e.APITimePrecision)
	}
	return nil
}

// Encoder writes events as JSON Lines. It is not safe for concurrent use.
type Encoder struct {
	w *bufio.Writer
}

// NewEncoder returns an Encoder that writes to w.
func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: bufio.NewWriter(w)}
}

// Encode validates e and writes it as one line.
func (enc *Encoder) Encode(e Event) error {
	if err := e.Validate(); err != nil {
		return err
	}
	b, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("seq %d: %w", e.Seq, err)
	}
	_, err = enc.w.Write(append(b, '\n'))
	return err
}

// Flush writes any buffered lines to the underlying writer.
func (enc *Encoder) Flush() error {
	return enc.w.Flush()
}

const maxLineBytes = 1 << 20

// Read parses JSON Lines from r. Every line must be a valid Event, and seq
// must run 1, 2, 3 and so on without a gap.
func Read(r io.Reader) ([]Event, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), maxLineBytes)
	var out []Event
	line := 0
	for sc.Scan() {
		line++
		dec := json.NewDecoder(bytes.NewReader(sc.Bytes()))
		dec.DisallowUnknownFields()
		var e Event
		if err := dec.Decode(&e); err != nil {
			return nil, fmt.Errorf("line %d: %w", line, err)
		}
		if err := e.Validate(); err != nil {
			return nil, fmt.Errorf("line %d: %w", line, err)
		}
		if want := int64(len(out) + 1); e.Seq != want {
			return nil, fmt.Errorf("line %d: seq %d, want %d", line, e.Seq, want)
		}
		out = append(out, e)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("line %d: %w", line+1, err)
	}
	return out, nil
}

// ReadFile reads and validates the events file at path.
func ReadFile(path string) ([]Event, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	evs, err := Read(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return evs, nil
}
```

- [ ] **Step 6: Run the specs and lint**

Run: `cd "$W" && go tool ginkgo --no-color ./pkg/events && make lint`
Expected: `Ran 8 of 8 Specs` and `SUCCESS!`; `make lint` exits 0 with no output from gofmt.

- [ ] **Step 7: Commit**

```bash
git -C "$W" add pkg/events/events.go
git -C "$W" commit -s -S -m "feat(events): write and read events.jsonl, schema version 1" -m "One validated JSON object per line with a contiguous seq, so an offline report can trust the file is complete or fail naming the bad line."
```

---

### Task 2: Verdict foundation and the gang rule

**Files:**
- Create: `pkg/verdict/verdict.go`, `pkg/verdict/timeline.go`, `pkg/verdict/gang.go`
- Test: `pkg/verdict/fixture_test.go`, `pkg/verdict/gang_test.go`

**Interfaces:**
- Consumes: `events.Event`, `events.Transition` constants (Task 1).
- Produces: `type Outcome string` (`Supported`, `Partial`, `NotSupported`; `Valid() bool`); `type Verdict struct{Capability string; Outcome Outcome; Reason string; Measurements map[string]float64; EvidenceSeq []int64}`; `type Pod struct{UID, Name, Job, Tenant string; Priority int32; GPUs int64; Node string; Created, Scheduled, Running, End time.Time; EndBy events.Transition; Seqs []int64}`; `func Pods(evs []events.Event, probe string, until time.Time) []*Pod`; `type Job struct{Name, Tenant string; GPUs int64; Created, End time.Time; Seqs []int64}`; `type Timeline struct{Pods []*Pod; Jobs map[string]*Job; End time.Time}`; `func Build(evs []events.Event, probe string, until time.Time) Timeline`; `type GangParams struct{Jobs []string; Size int; GraceSec float64}` (JSON `jobs`, `size`, `grace_sec`); `func Gang(t Timeline, p GangParams) (Verdict, error)`. Unexported helpers later tasks use: `ofJob`, `ofJobs`, `nthScheduled`, `earliestCreated`, `evidence`, `seconds`, `fmtSec`, `(Timeline).job`, `(Timeline).submitted`, `(Timeline).jobSeqs`. Test helpers later tasks use: `t0`, `at`, `pod`, `job`, `fixture`, `fixtureJobs`, `done`, `pending`.

Why a Timeline and not pods alone (R1, K2): under Kueue a queued job has no pods, only a Job (suspended) and a Kueue Workload. A job's submission is its Job's creation, and a job seen only as a Job was never admitted: a verdict, not a harness error.

- [ ] **Step 1: Write the fixture helpers and the failing gang specs**

`pkg/verdict/fixture_test.go`:

```go
package verdict_test

import (
	"math"
	"time"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
)

// t0 is the fixtures' time origin; at(s) is t0 plus s seconds.
var t0 = time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

func at(s float64) time.Time {
	return t0.Add(time.Duration(math.Round(s * float64(time.Second))))
}

// pod describes one fixture pod: its job, tenant and GPUs, and the second
// (after t0) of each transition; a negative second means "never".
type pod struct {
	name, job, tenant                string
	gpus                             int64
	node                             string
	created, scheduled, running, end float64
	endBy                            events.Transition
}

// job describes one fixture Job object: its tenant, total GPUs, and the
// second it was created and ended (negative: never ended).
type job struct {
	name, tenant string
	gpus         int64
	created, end float64
}

// fixture turns pods into a probe's events, ordered by time, with seq
// counting from 1.
func fixture(probe string, pods ...pod) []events.Event {
	return fixtureJobs(probe, nil, pods...)
}

// fixtureJobs is fixture with Job events as well, as the recorder writes
// them for every job (under Kueue, a queued job has a Job and no pods).
func fixtureJobs(probe string, jobs []job, pods ...pod) []events.Event {
	type item struct {
		at float64
		p  pod
		j  *job
		tr events.Transition
	}
	var items []item
	for i := range jobs {
		j := &jobs[i]
		items = append(items, item{at: j.created, j: j, tr: events.Created})
		if j.end >= 0 {
			items = append(items, item{at: j.end, j: j, tr: events.Succeeded})
		}
	}
	for _, p := range pods {
		items = append(items, item{at: p.created, p: p, tr: events.Created})
		if p.scheduled >= 0 {
			items = append(items, item{at: p.scheduled, p: p, tr: events.Scheduled})
		}
		if p.running >= 0 {
			items = append(items, item{at: p.running, p: p, tr: events.Running})
		}
		if p.end >= 0 {
			items = append(items, item{at: p.end, p: p, tr: p.endBy})
		}
	}
	// stable insertion sort by time keeps same-time items in input order
	for i := 1; i < len(items); i++ {
		for j := i; j > 0 && items[j].at < items[j-1].at; j-- {
			items[j], items[j-1] = items[j-1], items[j]
		}
	}
	out := make([]events.Event, 0, len(items))
	for i, it := range items {
		if it.j != nil {
			out = append(out, events.Event{
				Schema: 1, Seq: int64(i + 1), RunID: "fixture", ObservedAt: at(it.at),
				Kind: "Job", Namespace: "scs-" + probe, Name: it.j.name, UID: "uid-job-" + it.j.name,
				Probe: probe, Job: it.j.name, Tenant: it.j.tenant, GPUs: it.j.gpus, Transition: it.tr,
			})
			continue
		}
		e := events.Event{
			Schema: 1, Seq: int64(i + 1), RunID: "fixture", ObservedAt: at(it.at),
			Kind: "Pod", Namespace: "scs-" + probe, Name: it.p.name, UID: "uid-" + it.p.name,
			Probe: probe, Job: it.p.job, Tenant: it.p.tenant, GPUs: it.p.gpus, Transition: it.tr,
		}
		if it.tr == events.Scheduled {
			e.Node = it.p.node
		}
		out = append(out, e)
	}
	return out
}

// done is a pod that ran to completion on node.
func done(name, job string, node string, created, scheduled, running, end float64) pod {
	return pod{name: name, job: job, tenant: "t1", gpus: 8, node: node, created: created,
		scheduled: scheduled, running: running, end: end, endBy: events.Succeeded}
}

// pending is a pod that was created and never scheduled.
func pending(name, job string, created float64) pod {
	return pod{name: name, job: job, tenant: "t1", gpus: 8, created: created,
		scheduled: -1, running: -1, end: -1}
}
```

`pkg/verdict/gang_test.go`:

```go
package verdict_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
)

func TestVerdict(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "verdict") {
		t.Fatal("verdict specs failed")
	}
}

var _ = Describe("Pods", func() {
	It("ignores events observed after the window ends", func() {
		evs := fixture("gang", done("a0", "gang-a", "n1", 0, 1, 2, 22))
		pods := verdict.Pods(evs, "gang", at(10))
		Expect(pods).To(HaveLen(1))
		Expect(pods[0].Running).To(Equal(at(2)))
		Expect(pods[0].End.IsZero()).To(BeTrue())
		Expect(pods[0].Seqs).To(Equal([]int64{1, 2, 3}))
	})

	It("keeps only the named probe's Pod events", func() {
		evs := append(fixture("gang", done("a0", "gang-a", "n1", 0, 1, 2, 22)),
			fixture("priority", done("x", "low", "n1", 0, 1, 2, 3))...)
		Expect(verdict.Pods(evs, "priority", at(100))).To(HaveLen(1))
	})
})

var _ = Describe("Gang", func() {
	params := verdict.GangParams{Jobs: []string{"gang-a", "gang-b"}, Size: 2, GraceSec: 5}

	It("is supported when each gang is placed at once and both complete", func() {
		tl := verdict.Build(fixture("gang",
			done("a0", "gang-a", "n1", 0, 1.0, 2, 22), done("a1", "gang-a", "n2", 0, 1.5, 2, 22),
			done("b0", "gang-b", "n1", 0, 22.5, 23.5, 44), done("b1", "gang-b", "n2", 0, 23.0, 24, 44),
		), "gang", at(50))
		v, err := verdict.Gang(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Supported))
		Expect(v.Reason).To(Equal("no gang was partly placed for more than 5 s, and all 2 gangs completed"))
		Expect(v.Measurements).To(Equal(map[string]float64{"gangs": 2, "gangs_completed": 2, "max_partial_s": 0.5}))
		Expect(v.EvidenceSeq).To(Equal([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
	})

	It("still counts exactly the grace as placed at once", func() {
		tl := verdict.Build(fixture("gang",
			done("a0", "gang-a", "n1", 0, 1, 7, 27), done("a1", "gang-a", "n2", 0, 6, 7, 27),
			done("b0", "gang-b", "n1", 0, 28, 29, 49), done("b1", "gang-b", "n2", 0, 28, 29, 49),
		), "gang", at(60))
		v, err := verdict.Gang(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Supported))
		Expect(v.Measurements["max_partial_s"]).To(Equal(5.0))
	})

	It("is partial when a gang is partly placed past the grace but every gang completes", func() {
		three := verdict.GangParams{Jobs: []string{"gang-a", "gang-b"}, Size: 3, GraceSec: 5}
		tl := verdict.Build(fixture("gang-control",
			done("a0", "gang-a", "n1", 0, 1, 2, 22), done("a1", "gang-a", "n2", 0, 1, 2, 22),
			done("a2", "gang-a", "n1", 0, 22.5, 23, 43),
			done("b0", "gang-b", "n3", 0, 1, 2, 22), done("b1", "gang-b", "n4", 0, 1, 2, 22),
			done("b2", "gang-b", "n2", 0, 22.5, 23, 43),
		), "gang-control", at(60))
		v, err := verdict.Gang(tl, three)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Partial))
		Expect(v.Reason).To(Equal("gang gang-a was partly placed for 21.5 s; all 2 gangs completed"))
		Expect(v.Measurements["max_partial_s"]).To(Equal(21.5))
	})

	It("counts one millisecond past the grace as partly placed", func() {
		tl := verdict.Build(fixture("gang",
			done("a0", "gang-a", "n1", 0, 1, 7.001, 27), done("a1", "gang-a", "n2", 0, 6.001, 7.001, 27),
			done("b0", "gang-b", "n1", 0, 28, 29, 49), done("b1", "gang-b", "n2", 0, 28, 29, 49),
		), "gang", at(60))
		v, err := verdict.Gang(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Partial))
		Expect(v.Measurements["max_partial_s"]).To(BeNumerically("~", 5.001, 1e-9))
	})

	It("is not supported when a partial placement lasts until the probe ends", func() {
		tl := verdict.Build(fixture("gang",
			pod{name: "a0", job: "gang-a", tenant: "t1", gpus: 8, node: "n1", created: 0, scheduled: 1, running: 2, end: -1},
			pending("a1", "gang-a", 0),
			done("b0", "gang-b", "n2", 0, 1, 2, 22), done("b1", "gang-b", "n3", 0, 1, 2, 22),
		), "gang", at(120))
		v, err := verdict.Gang(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.NotSupported))
		Expect(v.Reason).To(Equal("gang gang-a was partly placed for 119 s, and gang gang-a did not complete"))
		Expect(v.Measurements["gangs_completed"]).To(Equal(1.0))
	})

	It("is not supported when a gang never gets a pod placed", func() {
		tl := verdict.Build(fixture("gang",
			done("a0", "gang-a", "n1", 0, 1, 2, 22), done("a1", "gang-a", "n2", 0, 1, 2, 22),
			pending("b0", "gang-b", 0), pending("b1", "gang-b", 0),
		), "gang", at(120))
		v, err := verdict.Gang(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.NotSupported))
		Expect(v.Reason).To(Equal("gang gang-b did not complete before the probe ended"))
		Expect(v.Measurements["max_partial_s"]).To(Equal(0.0))
	})

	It("does not count a deleted pod as completed", func() {
		tl := verdict.Build(fixture("gang",
			done("a0", "gang-a", "n1", 0, 1, 2, 22),
			pod{name: "a1", job: "gang-a", tenant: "t1", gpus: 8, node: "n2", created: 0, scheduled: 1, running: 2, end: 22, endBy: events.Deleted},
			done("b0", "gang-b", "n1", 0, 23, 24, 44), done("b1", "gang-b", "n2", 0, 23, 24, 44),
		), "gang", at(50))
		v, err := verdict.Gang(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.NotSupported))
		Expect(v.Reason).To(Equal("gang gang-a did not complete before the probe ended"))
	})

	It("is not supported when a queue never admits a gang, so it has a Job and no pods", func() {
		tl := verdict.Build(fixtureJobs("gang", []job{{name: "gang-b", tenant: "t1", gpus: 16, created: 0, end: -1}},
			done("a0", "gang-a", "n1", 0, 1, 2, 22), done("a1", "gang-a", "n2", 0, 1, 2, 22),
		), "gang", at(120))
		v, err := verdict.Gang(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.NotSupported))
		Expect(v.Reason).To(Equal("gang gang-b did not complete before the probe ended"))
		Expect(v.EvidenceSeq).To(ContainElement(int64(1)))
	})

	It("fails, as a harness error, when a gang has no recorded event at all", func() {
		tl := verdict.Build(fixture("gang", done("a0", "gang-a", "n1", 0, 1, 2, 22)), "gang", at(50))
		_, err := verdict.Gang(tl, params)
		Expect(err).To(MatchError("no events recorded for job gang-b"))
	})
})
```

- [ ] **Step 2: Run and watch it fail**

Run: `cd "$W" && go test ./pkg/verdict/`
Expected: FAIL, build failure naming `undefined: verdict.Pods`, `undefined: verdict.Build` and `undefined: verdict.GangParams`.

- [ ] **Step 3: Commit the failing specs**

```bash
git -C "$W" add pkg/verdict/fixture_test.go pkg/verdict/gang_test.go
git -C "$W" commit -s -S -m "test(verdict): specify the gang rule on hand-built timelines" -m "Each outcome, the exact grace boundary, a gang that never starts, a gang a queue never admits, and a deleted pod that must not count as completed."
```

- [ ] **Step 4: Implement**

`pkg/verdict/verdict.go`:

```go
// Package verdict turns recorded events into observed verdicts. Every
// function here is pure: the same events and parameters give the same
// verdict, which is what lets cmd/scs-report rebuild a run's results.
package verdict

import (
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
)

// Outcome is the observed verdict for one capability.
type Outcome string

// The three outcomes a probe can record.
const (
	Supported    Outcome = "supported"
	Partial      Outcome = "partial"
	NotSupported Outcome = "not_supported"
)

// Valid reports whether o is one of the three outcomes.
func (o Outcome) Valid() bool {
	return o == Supported || o == Partial || o == NotSupported
}

// Verdict is what one probe observed, with the numbers and the event
// sequence numbers it rests on.
type Verdict struct {
	Capability   string             `json:"capability"`
	Outcome      Outcome            `json:"outcome"`
	Reason       string             `json:"reason"`
	Measurements map[string]float64 `json:"measurements"`
	EvidenceSeq  []int64            `json:"evidence_seq"`
}

// Pod is one pod's life as the recorder saw it. A zero time means the
// recorder never saw that transition within the evidence window.
type Pod struct {
	UID       string
	Name      string
	Job       string
	Tenant    string
	Priority  int32
	GPUs      int64
	Node      string
	Created   time.Time
	Scheduled time.Time
	Running   time.Time
	End       time.Time
	EndBy     events.Transition
	Seqs      []int64
}

var terminal = map[events.Transition]bool{
	events.Succeeded: true, events.Failed: true, events.Deleted: true,
	events.Preempted: true, events.Evicted: true,
}

// Pods folds the Pod events of one probe, observed no later than until,
// into one Pod per UID, in order of first appearance. The first time a
// transition is seen wins, and the first terminal transition sets End.
func Pods(evs []events.Event, probe string, until time.Time) []*Pod {
	byUID := map[string]*Pod{}
	var order []*Pod
	for _, e := range evs {
		if e.Kind != "Pod" || e.Probe != probe || e.ObservedAt.After(until) {
			continue
		}
		p, ok := byUID[e.UID]
		if !ok {
			p = &Pod{UID: e.UID, Name: e.Name, Job: e.Job, Tenant: e.Tenant, Priority: e.Priority, GPUs: e.GPUs}
			byUID[e.UID] = p
			order = append(order, p)
		}
		p.Seqs = append(p.Seqs, e.Seq)
		switch {
		case e.Transition == events.Created:
			setOnce(&p.Created, e.ObservedAt)
		case e.Transition == events.Scheduled:
			if p.Scheduled.IsZero() {
				p.Scheduled = e.ObservedAt
				p.Node = e.Node
			}
		case e.Transition == events.Running:
			setOnce(&p.Running, e.ObservedAt)
		case terminal[e.Transition]:
			if p.End.IsZero() {
				p.End = e.ObservedAt
				p.EndBy = e.Transition
			}
		}
	}
	return order
}

func setOnce(t *time.Time, v time.Time) {
	if t.IsZero() {
		*t = v
	}
}

// ofJob returns the pods of job, in order of first appearance.
func ofJob(pods []*Pod, job string) []*Pod {
	var out []*Pod
	for _, p := range pods {
		if p.Job == job {
			out = append(out, p)
		}
	}
	return out
}

// ofJobs returns the pods of every job in jobs, and the first job that has
// no recorded pod.
func ofJobs(pods []*Pod, jobs []string) ([]*Pod, string) {
	var out []*Pod
	for _, j := range jobs {
		ps := ofJob(pods, j)
		if len(ps) == 0 {
			return nil, j
		}
		out = append(out, ps...)
	}
	return out, ""
}

// nthScheduled returns the n-th earliest scheduling time among pods (n
// counts from 1), or false when fewer than n pods were scheduled.
func nthScheduled(pods []*Pod, n int) (time.Time, bool) {
	var ts []time.Time
	for _, p := range pods {
		if !p.Scheduled.IsZero() {
			ts = append(ts, p.Scheduled)
		}
	}
	if n < 1 || len(ts) < n {
		return time.Time{}, false
	}
	sort.Slice(ts, func(i, j int) bool { return ts[i].Before(ts[j]) })
	return ts[n-1], true
}

// earliestCreated returns the earliest creation time among pods, or the
// zero time when none was seen.
func earliestCreated(pods []*Pod) time.Time {
	var t time.Time
	for _, p := range pods {
		if !p.Created.IsZero() && (t.IsZero() || p.Created.Before(t)) {
			t = p.Created
		}
	}
	return t
}

// evidence returns the sorted, de-duplicated event sequence numbers of pods.
func evidence(groups ...[]*Pod) []int64 {
	seen := map[int64]bool{}
	out := []int64{}
	for _, g := range groups {
		for _, p := range g {
			for _, s := range p.Seqs {
				if !seen[s] {
					seen[s] = true
					out = append(out, s)
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// seconds converts s seconds to a Duration, rounded to the nanosecond.
func seconds(s float64) time.Duration {
	return time.Duration(math.Round(s * float64(time.Second)))
}

// fmtSec renders seconds rounded to the millisecond, without trailing zeros.
func fmtSec(s float64) string {
	return strconv.FormatFloat(math.Round(s*1000)/1000, 'f', -1, 64)
}
```

`pkg/verdict/timeline.go`:

```go
package verdict

import (
	"time"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
)

// Job is one job's life as recorded from its batch/v1 Job object. Under
// Kueue a queued job has no pods (Kueue suspends it), so a job's submission
// is its Job's creation, not its first pod's.
type Job struct {
	Name    string
	Tenant  string
	GPUs    int64
	Created time.Time
	End     time.Time
	Seqs    []int64
}

// Timeline is one probe's evidence up to the end of its window.
type Timeline struct {
	Pods []*Pod
	Jobs map[string]*Job
	End  time.Time
}

// Build folds a probe's events, observed no later than until, into its
// pods and jobs. A job seen only through its pods (no Job events, as in
// hand-built fixtures) gets a Job derived from them: created with its first
// pod, its pods' GPUs, and ended with its last pod when all of them ended.
func Build(evs []events.Event, probe string, until time.Time) Timeline {
	t := Timeline{Pods: Pods(evs, probe, until), Jobs: map[string]*Job{}, End: until}
	for _, e := range evs {
		if e.Kind != "Job" || e.Probe != probe || e.ObservedAt.After(until) {
			continue
		}
		j, ok := t.Jobs[e.Name]
		if !ok {
			j = &Job{Name: e.Name, Tenant: e.Tenant, GPUs: e.GPUs}
			t.Jobs[e.Name] = j
		}
		j.Seqs = append(j.Seqs, e.Seq)
		switch e.Transition {
		case events.Created:
			setOnce(&j.Created, e.ObservedAt)
		case events.Succeeded, events.Failed, events.Deleted:
			setOnce(&j.End, e.ObservedAt)
		}
	}
	derived := map[string]*Job{}
	for _, p := range t.Pods {
		if _, recorded := t.Jobs[p.Job]; recorded {
			continue
		}
		j, ok := derived[p.Job]
		if !ok {
			j = &Job{Name: p.Job, Tenant: p.Tenant}
			derived[p.Job] = j
		}
		j.GPUs += p.GPUs
		if !p.Created.IsZero() && (j.Created.IsZero() || p.Created.Before(j.Created)) {
			j.Created = p.Created
		}
	}
	for name, j := range derived {
		allEnded := true
		for _, p := range ofJob(t.Pods, name) {
			if p.End.IsZero() {
				allEnded = false
			} else if p.End.After(j.End) {
				j.End = p.End
			}
		}
		if !allEnded {
			j.End = time.Time{}
		}
		t.Jobs[name] = j
	}
	return t
}

// job returns a job's pods, and whether the job was seen at all (as a Job
// or through its pods). A job seen without pods was never admitted.
func (t Timeline) job(name string) ([]*Pod, bool) {
	ps := ofJob(t.Pods, name)
	_, seen := t.Jobs[name]
	return ps, seen || len(ps) > 0
}

// submitted is when a job was submitted: its Job's creation.
func (t Timeline) submitted(name string) time.Time {
	if j, ok := t.Jobs[name]; ok {
		return j.Created
	}
	return time.Time{}
}

// jobSeqs returns the Job events' sequence numbers of the named jobs, as
// pseudo-pods so evidence() can merge them with pod evidence.
func (t Timeline) jobSeqs(names ...string) []*Pod {
	var out []*Pod
	for _, n := range names {
		if j, ok := t.Jobs[n]; ok && len(j.Seqs) > 0 {
			out = append(out, &Pod{Seqs: j.Seqs})
		}
	}
	return out
}
```

`pkg/verdict/gang.go`:

```go
package verdict

import (
	"errors"
	"fmt"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
)

// GangParams are the gang probe's parameters: the Jobs that are gangs, the
// pods per gang, and how long a gang may be partly placed before it counts.
type GangParams struct {
	Jobs     []string `json:"jobs"`
	Size     int      `json:"size"`
	GraceSec float64  `json:"grace_sec"`
}

func (p GangParams) validate() error {
	if len(p.Jobs) == 0 {
		return errors.New("jobs is empty")
	}
	if p.Size < 2 {
		return fmt.Errorf("size %d, want 2 or more", p.Size)
	}
	if p.GraceSec < 0 {
		return fmt.Errorf("grace_sec %s is negative", fmtSec(p.GraceSec))
	}
	return nil
}

// Gang judges whether gangs were placed all at once. A gang is partly
// placed from its first pod's scheduling until its Size-th pod's or, if it
// never has Size pods scheduled, until the window ends. A gang completed
// when at least Size of its pods succeeded. A gang seen only as a Job (a
// queue never admitted it) was neither placed nor completed.
func Gang(t Timeline, p GangParams) (Verdict, error) {
	if err := p.validate(); err != nil {
		return Verdict{}, err
	}
	maxPartial, worst := 0.0, ""
	completed, firstIncomplete := 0, ""
	groups := [][]*Pod{t.jobSeqs(p.Jobs...)}
	for _, job := range p.Jobs {
		ps, seen := t.job(job)
		if !seen {
			return Verdict{}, fmt.Errorf("no events recorded for job %s", job)
		}
		groups = append(groups, ps)
		if first, placed := nthScheduled(ps, 1); placed {
			last, full := nthScheduled(ps, p.Size)
			if !full {
				last = t.End
			}
			if d := last.Sub(first).Seconds(); d > maxPartial {
				maxPartial, worst = d, job
			}
		}
		succeeded := 0
		for _, q := range ps {
			if q.EndBy == events.Succeeded {
				succeeded++
			}
		}
		if succeeded >= p.Size {
			completed++
		} else if firstIncomplete == "" {
			firstIncomplete = job
		}
	}
	v := Verdict{
		Measurements: map[string]float64{
			"gangs":           float64(len(p.Jobs)),
			"gangs_completed": float64(completed),
			"max_partial_s":   maxPartial,
		},
		EvidenceSeq: evidence(groups...),
	}
	switch {
	case maxPartial > p.GraceSec && firstIncomplete != "":
		v.Outcome = NotSupported
		v.Reason = fmt.Sprintf("gang %s was partly placed for %s s, and gang %s did not complete", worst, fmtSec(maxPartial), firstIncomplete)
	case firstIncomplete != "":
		v.Outcome = NotSupported
		v.Reason = fmt.Sprintf("gang %s did not complete before the probe ended", firstIncomplete)
	case maxPartial > p.GraceSec:
		v.Outcome = Partial
		v.Reason = fmt.Sprintf("gang %s was partly placed for %s s; all %d gangs completed", worst, fmtSec(maxPartial), len(p.Jobs))
	default:
		v.Outcome = Supported
		v.Reason = fmt.Sprintf("no gang was partly placed for more than %s s, and all %d gangs completed", fmtSec(p.GraceSec), len(p.Jobs))
	}
	return v, nil
}
```

- [ ] **Step 5: Run the specs**

Run: `cd "$W" && go tool ginkgo --no-color ./pkg/verdict && make lint`
Expected: `Ran 11 of 11 Specs`, `SUCCESS!`; lint exits 0.

- [ ] **Step 6: Commit**

```bash
git -C "$W" add pkg/verdict/verdict.go pkg/verdict/timeline.go pkg/verdict/gang.go
git -C "$W" commit -s -S -m "feat(verdict): fold pod events into timelines and judge gang placement" -m "A gang counts as partly placed from its first pod's binding to its last, so a scheduler that places part of a gang and waits is visible in the data."
```

---

### Task 3: Priority and preemption rules

**Files:**
- Create: `pkg/verdict/priority.go`, `pkg/verdict/preemption.go`
- Test: `pkg/verdict/priority_test.go`, `pkg/verdict/preemption_test.go`

**Interfaces:**
- Consumes: Task 2's `Pod`, `Verdict`, helpers and test helpers.
- Produces: `type PriorityParams struct{HighJob string; QueuedJobs []string}` (JSON `high_job`, `queued_jobs`); `func Priority(t Timeline, p PriorityParams) (Verdict, error)` (measurements `queued_jobs`, `queued_before_high`, `high_time_to_schedule_s` from the high Job's submission); `type PreemptionParams struct{HighJob string; HighPods int; VictimJob string; GraceSec float64}` (JSON `high_job`, `high_pods`, `victim_job`, `grace_sec`); `func Preemption(t Timeline, p PreemptionParams) (Verdict, error)`.

- [ ] **Step 1: Write the failing specs**

`pkg/verdict/priority_test.go`:

```go
package verdict_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
)

var _ = Describe("Priority", func() {
	params := verdict.PriorityParams{HighJob: "high", QueuedJobs: []string{"q1", "q2", "q3", "q4"}}

	// queued returns the four queued low-priority pods, scheduled at the
	// given seconds (negative: never).
	queued := func(s1, s2, s3, s4 float64) []pod {
		var out []pod
		for i, s := range []float64{s1, s2, s3, s4} {
			name := []string{"q1", "q2", "q3", "q4"}[i]
			if s < 0 {
				out = append(out, pending(name, name, 1))
				continue
			}
			out = append(out, done(name, name, "n1", 1, s, s+0.5, s+20))
		}
		return out
	}

	It("is supported when the high-priority pod takes the first freed slot", func() {
		tl := verdict.Build(fixture("priority", append(queued(40, 50, 60, 70),
			done("h0", "high", "n1", 3, 30, 30.5, 50))...), "priority", at(100))
		v, err := verdict.Priority(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Supported))
		Expect(v.Reason).To(Equal("the high-priority pod was scheduled before every queued lower-priority pod"))
		Expect(v.Measurements).To(Equal(map[string]float64{"queued_jobs": 4, "queued_before_high": 0, "high_time_to_schedule_s": 27}))
	})

	It("does not count a queued pod scheduled at the same instant as ahead", func() {
		tl := verdict.Build(fixture("priority", append(queued(30, 50, 60, 70),
			done("h0", "high", "n2", 3, 30, 30.5, 50))...), "priority", at(100))
		v, err := verdict.Priority(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Supported))
	})

	It("is partial when some queued pods go first", func() {
		tl := verdict.Build(fixture("priority", append(queued(30, 40, 60, 70),
			done("h0", "high", "n1", 3, 50, 50.5, 70))...), "priority", at(100))
		v, err := verdict.Priority(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Partial))
		Expect(v.Reason).To(Equal("2 of 4 queued lower-priority pods were scheduled before the high-priority pod"))
	})

	It("is not supported when the pods run in arrival order", func() {
		tl := verdict.Build(fixture("priority", append(queued(30, 40, 50, 60),
			done("h0", "high", "n1", 3, 70, 70.5, 90))...), "priority", at(100))
		v, err := verdict.Priority(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.NotSupported))
		Expect(v.Reason).To(Equal("all 4 queued lower-priority pods were scheduled before the high-priority pod"))
	})

	It("is not supported when the high-priority pod is never scheduled", func() {
		tl := verdict.Build(fixture("priority", append(queued(30, 40, -1, -1),
			pending("h0", "high", 3))...), "priority", at(100))
		v, err := verdict.Priority(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.NotSupported))
		Expect(v.Reason).To(Equal("the high-priority job was never scheduled"))
		Expect(v.Measurements).To(Equal(map[string]float64{"queued_jobs": 4, "queued_before_high": 2}))
	})

	It("measures time to schedule from the Job's submission, not its pod's creation", func() {
		// Kueue holds a queued Job without pods: the Job is created at 2.2 s,
		// its pod only at admission (33.24 s), scheduled at 34.26 s.
		tl := verdict.Build(fixtureJobs("priority", []job{{name: "high", tenant: "t1", gpus: 8, created: 2.2, end: -1}},
			append(queued(44, 54, 64, 67),
				pod{name: "h0", job: "high", tenant: "t1", gpus: 8, node: "n1", created: 33.24, scheduled: 34.26, running: 34.3, end: -1})...),
			"priority", at(100))
		v, err := verdict.Priority(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Supported))
		Expect(v.Measurements["high_time_to_schedule_s"]).To(BeNumerically("~", 32.06, 1e-9))
	})

	It("is not supported when the high-priority job is held as a Job and never gets a pod", func() {
		tl := verdict.Build(fixtureJobs("priority", []job{{name: "high", tenant: "t1", gpus: 8, created: 3, end: -1}},
			queued(30, 40, 50, 60)...), "priority", at(100))
		v, err := verdict.Priority(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.NotSupported))
		Expect(v.Reason).To(Equal("the high-priority job was never scheduled"))
	})
})
```

`pkg/verdict/preemption_test.go`:

```go
package verdict_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
)

var _ = Describe("Preemption", func() {
	params := verdict.PreemptionParams{HighJob: "high", HighPods: 2, VictimJob: "victim", GraceSec: 5}

	victim := func(name, node string, end float64, by events.Transition) pod {
		return pod{name: name, job: "victim", tenant: "t1", gpus: 8, node: node,
			created: 0, scheduled: 1, running: 2, end: end, endBy: by}
	}
	high := func(name, node string, scheduled float64) pod {
		return pod{name: name, job: "high", tenant: "t1", gpus: 8, node: node,
			created: 10, scheduled: scheduled, running: scheduled + 0.5, end: -1}
	}

	It("is supported when the whole victim workload is evicted", func() {
		tl := verdict.Build(fixture("preemption",
			victim("v0", "n1", 10.5, events.Deleted), victim("v1", "n2", 10.5, events.Deleted),
			victim("v2", "n3", 10.5, events.Deleted), victim("v3", "n4", 10.5, events.Deleted),
			high("h0", "n1", 11.0), high("h1", "n2", 11.2),
		), "preemption", at(60))
		v, err := verdict.Preemption(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Supported))
		Expect(v.Reason).To(Equal("no victim pod was left running 5 s after the high-priority job was scheduled"))
		Expect(v.Measurements["victim_pods_before"]).To(Equal(4.0))
		Expect(v.Measurements["victim_pods_evicted"]).To(Equal(4.0))
		Expect(v.Measurements["victim_pods_remaining"]).To(Equal(0.0))
		Expect(v.Measurements["gpu_seconds_lost"]).To(BeNumerically("~", 272, 1e-9))
		Expect(v.Measurements["high_time_to_schedule_s"]).To(BeNumerically("~", 1.2, 1e-9))
	})

	It("is partial when only some victim pods are evicted, ignoring recreated ones", func() {
		tl := verdict.Build(fixture("preemption",
			victim("v0", "n1", 10.3, events.Preempted), victim("v1", "n2", 10.3, events.Preempted),
			victim("v2", "n3", -1, ""), victim("v3", "n4", -1, ""),
			pod{name: "v4", job: "victim", tenant: "t1", gpus: 8, node: "n3", created: 10.4, scheduled: 30, running: 30.5, end: -1},
			high("h0", "n1", 11.0), high("h1", "n2", 11.2),
		), "preemption", at(60))
		v, err := verdict.Preemption(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Partial))
		Expect(v.Reason).To(Equal("2 of 4 victim pods were left running 5 s after the high-priority job was scheduled"))
		Expect(v.Measurements["gpu_seconds_lost"]).To(BeNumerically("~", 132.8, 1e-9))
	})

	It("counts a victim that ends exactly at the grace as evicted", func() {
		tl := verdict.Build(fixture("preemption",
			victim("v0", "n1", 16.2, events.Deleted), victim("v1", "n2", 10.5, events.Deleted),
			victim("v2", "n3", 10.5, events.Deleted), victim("v3", "n4", 10.5, events.Deleted),
			high("h0", "n3", 11.0), high("h1", "n4", 11.2),
		), "preemption", at(60))
		v, err := verdict.Preemption(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Supported))
	})

	It("is not supported when the high-priority job never gets all its pods", func() {
		tl := verdict.Build(fixture("preemption",
			victim("v0", "n1", -1, ""), victim("v1", "n2", -1, ""),
			victim("v2", "n3", -1, ""), victim("v3", "n4", -1, ""),
			pending("h0", "high", 10), pending("h1", "high", 10),
		), "preemption", at(60))
		v, err := verdict.Preemption(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.NotSupported))
		Expect(v.Reason).To(Equal("the high-priority job did not get all 2 pods scheduled"))
	})

	It("fails, as a harness error, when no victim was running first", func() {
		tl := verdict.Build(fixture("preemption",
			pending("v0", "victim", 0), high("h0", "n1", 11.0), high("h1", "n2", 11.2),
		), "preemption", at(60))
		_, err := verdict.Preemption(tl, params)
		Expect(err).To(MatchError("no victim pod was running when the high-priority job was created"))
	})
})
```

- [ ] **Step 2: Run and watch it fail**

Run: `cd "$W" && go test ./pkg/verdict/`
Expected: FAIL, build failure naming `undefined: verdict.PriorityParams` and `undefined: verdict.PreemptionParams`.

- [ ] **Step 3: Commit the failing specs**

```bash
git -C "$W" add pkg/verdict/priority_test.go pkg/verdict/preemption_test.go
git -C "$W" commit -s -S -m "test(verdict): specify the priority and preemption rules" -m "Ties, a high-priority pod that never runs, time to schedule measured from the Job's submission (a queue may create the pod much later), a victim recreated after the high-priority job arrives, and a victim ending exactly at the grace boundary."
```

- [ ] **Step 4: Implement**

`pkg/verdict/priority.go`:

```go
package verdict

import (
	"errors"
	"fmt"
)

// PriorityParams are the priority probe's parameters: the high-priority
// Job, and the lower-priority Jobs queued before it arrived.
type PriorityParams struct {
	HighJob    string   `json:"high_job"`
	QueuedJobs []string `json:"queued_jobs"`
}

func (p PriorityParams) validate() error {
	if p.HighJob == "" {
		return errors.New("high_job is empty")
	}
	if len(p.QueuedJobs) == 0 {
		return errors.New("queued_jobs is empty")
	}
	return nil
}

// Priority judges whether the high-priority pod was scheduled ahead of the
// queued lower-priority pods. A queued pod counts as ahead only when it was
// scheduled strictly before the high-priority pod. Time to schedule runs
// from the high-priority Job's submission, so a queue that holds the job
// before creating its pods is measured too.
func Priority(t Timeline, p PriorityParams) (Verdict, error) {
	if err := p.validate(); err != nil {
		return Verdict{}, err
	}
	high, seen := t.job(p.HighJob)
	if !seen {
		return Verdict{}, fmt.Errorf("no events recorded for job %s", p.HighJob)
	}
	var queued []*Pod
	for _, j := range p.QueuedJobs {
		ps, seen := t.job(j)
		if !seen {
			return Verdict{}, fmt.Errorf("no events recorded for job %s", j)
		}
		queued = append(queued, ps...)
	}
	submitted := t.submitted(p.HighJob)
	if submitted.IsZero() {
		return Verdict{}, fmt.Errorf("no submission recorded for job %s", p.HighJob)
	}
	v := Verdict{
		Measurements: map[string]float64{"queued_jobs": float64(len(p.QueuedJobs))},
		EvidenceSeq:  evidence(high, queued, t.jobSeqs(append([]string{p.HighJob}, p.QueuedJobs...)...)),
	}
	hs, ok := nthScheduled(high, 1)
	before := 0
	for _, q := range queued {
		if !q.Scheduled.IsZero() && (!ok || q.Scheduled.Before(hs)) {
			before++
		}
	}
	v.Measurements["queued_before_high"] = float64(before)
	if !ok {
		v.Outcome = NotSupported
		v.Reason = "the high-priority job was never scheduled"
		return v, nil
	}
	v.Measurements["high_time_to_schedule_s"] = hs.Sub(submitted).Seconds()
	switch before {
	case 0:
		v.Outcome = Supported
		v.Reason = "the high-priority pod was scheduled before every queued lower-priority pod"
	case len(p.QueuedJobs):
		v.Outcome = NotSupported
		v.Reason = fmt.Sprintf("all %d queued lower-priority pods were scheduled before the high-priority pod", len(p.QueuedJobs))
	default:
		v.Outcome = Partial
		v.Reason = fmt.Sprintf("%d of %d queued lower-priority pods were scheduled before the high-priority pod", before, len(p.QueuedJobs))
	}
	return v, nil
}
```

`pkg/verdict/preemption.go`:

```go
package verdict

import (
	"errors"
	"fmt"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
)

// PreemptionParams are the preemption probe's parameters: the
// high-priority Job and its pod count, the low-priority victim Job, and the
// grace after the high-priority job is scheduled.
type PreemptionParams struct {
	HighJob   string  `json:"high_job"`
	HighPods  int     `json:"high_pods"`
	VictimJob string  `json:"victim_job"`
	GraceSec  float64 `json:"grace_sec"`
}

func (p PreemptionParams) validate() error {
	if p.HighJob == "" || p.VictimJob == "" {
		return errors.New("high_job and victim_job must both be set")
	}
	if p.HighPods < 1 {
		return fmt.Errorf("high_pods %d, want 1 or more", p.HighPods)
	}
	if p.GraceSec < 0 {
		return fmt.Errorf("grace_sec %s is negative", fmtSec(p.GraceSec))
	}
	return nil
}

// Preemption judges whether making room for the high-priority job evicted
// the victim workload as a whole. The victim pods considered are those
// scheduled before the high-priority Job was submitted; pods the victim's
// controller recreates later are not. A victim pod is left running when it
// had not ended GraceSec after the high-priority job's last pod was
// scheduled.
func Preemption(t Timeline, p PreemptionParams) (Verdict, error) {
	if err := p.validate(); err != nil {
		return Verdict{}, err
	}
	high, seen := t.job(p.HighJob)
	if !seen {
		return Verdict{}, fmt.Errorf("no events recorded for job %s", p.HighJob)
	}
	victims := ofJob(t.Pods, p.VictimJob)
	highCreated := t.submitted(p.HighJob)
	if highCreated.IsZero() {
		return Verdict{}, fmt.Errorf("no submission recorded for job %s", p.HighJob)
	}
	var before []*Pod
	for _, q := range victims {
		if !q.Scheduled.IsZero() && q.Scheduled.Before(highCreated) {
			before = append(before, q)
		}
	}
	if len(before) == 0 {
		return Verdict{}, errors.New("no victim pod was running when the high-priority job was created")
	}
	v := Verdict{
		Measurements: map[string]float64{"victim_pods_before": float64(len(before))},
		EvidenceSeq:  evidence(high, victims, t.jobSeqs(p.HighJob, p.VictimJob)),
	}
	th, ok := nthScheduled(high, p.HighPods)
	if !ok {
		v.Outcome = NotSupported
		v.Reason = fmt.Sprintf("the high-priority job did not get all %d pods scheduled", p.HighPods)
		return v, nil
	}
	cut := th.Add(seconds(p.GraceSec))
	remaining, evicted, lost := 0, 0, 0.0
	for _, q := range before {
		if !q.End.IsZero() && !q.End.After(cut) {
			if q.EndBy != events.Succeeded {
				evicted++
				if !q.Running.IsZero() {
					lost += float64(q.GPUs) * q.End.Sub(q.Running).Seconds()
				}
			}
			continue
		}
		remaining++
	}
	v.Measurements["high_time_to_schedule_s"] = th.Sub(highCreated).Seconds()
	v.Measurements["victim_pods_evicted"] = float64(evicted)
	v.Measurements["victim_pods_remaining"] = float64(remaining)
	v.Measurements["gpu_seconds_lost"] = lost
	if remaining == 0 {
		v.Outcome = Supported
		v.Reason = fmt.Sprintf("no victim pod was left running %s s after the high-priority job was scheduled", fmtSec(p.GraceSec))
	} else {
		v.Outcome = Partial
		v.Reason = fmt.Sprintf("%d of %d victim pods were left running %s s after the high-priority job was scheduled", remaining, len(before), fmtSec(p.GraceSec))
	}
	return v, nil
}
```

- [ ] **Step 5: Run the specs**

Run: `cd "$W" && go tool ginkgo --no-color ./pkg/verdict && make lint`
Expected: `Ran 23 of 23 Specs`, `SUCCESS!`; lint exits 0.

- [ ] **Step 6: Commit**

```bash
git -C "$W" add pkg/verdict/priority.go pkg/verdict/preemption.go
git -C "$W" commit -s -S -m "feat(verdict): judge priority ordering and workload-aware preemption" -m "Preemption is judged on the victims running before the high-priority job arrived, so a controller recreating evicted pods does not hide a partial eviction."
```

---

### Task 4: Fair-share rule and fairness over time

**Files:**
- Create: `pkg/verdict/fairshare.go`
- Test: `pkg/verdict/fairshare_test.go`

**Interfaces:**
- Consumes: Task 2's `Pod`, `Verdict`, helpers.
- Produces: `type FairShareParams struct{TenantB string; EntitlementGPUs int64; JobLengthSec, ToleranceSec float64; CapacityGPUs int64; WindowSec float64}` (JSON `tenant_b`, `entitlement_gpus`, `job_length_sec`, `tolerance_sec`, `capacity_gpus`, `window_sec`); `func FairShare(t Timeline, p FairShareParams) (Verdict, error)`. Measurements: `b_max_gpus_by_check` (the most GPUs B had running at any moment of [B's submission, submission + length + tolerance], which is what the spec's "within 60 s of its submission" says: R1 saw Kueue give B its 16 GPUs at t0 + 13 s and B sit between waves at the t0 + 75 s instant), `check_offset_s`, `windows_used`, `windows_excluded`, `worst_shortfall_gpu_s`, and when any window was used `jain_median`, `jain_p5`.

- [ ] **Step 1: Write the failing specs**

`pkg/verdict/fairshare_test.go`:

```go
package verdict_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
)

// tenantPod is a pod of tenant t with g GPUs; negative seconds mean never.
func tenantPod(name, t string, g int64, created, running, end float64) pod {
	p := pod{name: name, job: name, tenant: t, gpus: g, node: "n1", created: created,
		scheduled: running, running: running, end: end, endBy: events.Succeeded}
	if end < 0 {
		p.endBy = ""
	}
	return p
}

var _ = Describe("FairShare", func() {
	params := verdict.FairShareParams{TenantB: "b", EntitlementGPUs: 16, JobLengthSec: 60,
		ToleranceSec: 5, CapacityGPUs: 32, WindowSec: 10}

	// aPods: tenant A's four running pods (0 to 60 s) and four queued pods
	// that start at the given second (negative: never).
	aPods := func(queuedStart ...float64) []pod {
		var out []pod
		for _, n := range []string{"a0", "a1", "a2", "a3"} {
			out = append(out, tenantPod(n, "a", 8, 0, 0.5, 60))
		}
		for i, s := range queuedStart {
			n := []string{"a4", "a5", "a6", "a7"}[i]
			end := -1.0
			if s >= 0 {
				end = s + 60
			}
			out = append(out, tenantPod(n, "a", 8, 0, s, end))
		}
		return out
	}
	bPods := func(starts ...float64) []pod {
		var out []pod
		for i, s := range starts {
			n := []string{"b0", "b1", "b2", "b3"}[i]
			end := -1.0
			if s >= 0 {
				end = s + 60
			}
			out = append(out, tenantPod(n, "b", 8, 10, s, end))
		}
		return out
	}

	It("is supported when B reaches its entitlement within one job length", func() {
		tl := verdict.Build(fixture("fair-share", append(aPods(60.5, 60.5, 120.5, 120.5),
			bPods(60.5, 60.5, 120.5, 120.5)...)...), "fair-share", at(200))
		v, err := verdict.FairShare(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Supported))
		Expect(v.Reason).To(Equal("tenant b had 16 GPUs running within 65 s of submitting; its entitlement is 16"))
		Expect(v.Measurements["b_max_gpus_by_check"]).To(Equal(16.0))
		Expect(v.Measurements["check_offset_s"]).To(Equal(65.0))
	})

	It("is partial when B gets some but not all of its entitlement", func() {
		tl := verdict.Build(fixture("fair-share", append(aPods(60.5, 60.5, 60.5, 120.5),
			bPods(60.5, 120.5, 120.5, 180.5)...)...), "fair-share", at(250))
		v, err := verdict.FairShare(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Partial))
		Expect(v.Reason).To(Equal("tenant b had at most 8 of its 16 entitled GPUs running within 65 s of submitting"))
	})

	It("is not supported when B waits for A's whole backlog", func() {
		tl := verdict.Build(fixture("fair-share", append(aPods(60.5, 60.5, 60.5, 60.5),
			bPods(120.5, 120.5, 120.5, 120.5)...)...), "fair-share", at(200))
		v, err := verdict.FairShare(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.NotSupported))
		Expect(v.Reason).To(Equal("tenant b had no GPUs running within 65 s of submitting"))
	})

	It("counts the entitlement reached early, even if B is between waves at the check", func() {
		// Kueue reclaimed for B at 12 s: b0 and b1 ran 12 to 72 s; the next
		// wave starts at 75.1 s, after the check at 10 + 65 = 75 s. At the
		// instant of the check B runs nothing; within the window it had 16.
		pods := []pod{tenantPod("a0", "a", 8, 0, 0.5, 60), tenantPod("a1", "a", 8, 0, 0.5, 60),
			tenantPod("b0", "b", 8, 10, 12, 72), tenantPod("b1", "b", 8, 10, 12, 72),
			tenantPod("b2", "b", 8, 10, 75.1, 135.1), tenantPod("b3", "b", 8, 10, 75.1, 135.1)}
		tl := verdict.Build(fixture("fair-share", pods...), "fair-share", at(140))
		v, err := verdict.FairShare(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Supported))
		Expect(v.Measurements["b_max_gpus_by_check"]).To(Equal(16.0))
	})

	It("counts a B pod that starts exactly at the check", func() {
		tl := verdict.Build(fixture("fair-share", append(aPods(60.5, 60.5, 120.5, 120.5),
			bPods(75, 75, 120.5, 120.5)...)...), "fair-share", at(200))
		v, err := verdict.FairShare(tl, params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Measurements["b_max_gpus_by_check"]).To(Equal(16.0))
	})
})

var _ = Describe("fairness over time", func() {
	// capacity 4 GPUs, 10 s windows. Tenant a: 4 GPUs from 0 to 20 s.
	// Tenant b: 4 GPUs, submitted at 5 s, running 20 to 30 s.
	// Windows [0,10) and [10,20): entitlement 20 each, a delivers 40, b 0:
	// x = (2, 0), J = 0.5. Window [20,30): only b has demand, excluded from
	// J; b's shortfall is 20 + 20 + 0 = 40 GPU-seconds.
	It("computes per-window Jain's index and the worst shortfall by hand", func() {
		p := verdict.FairShareParams{TenantB: "b", EntitlementGPUs: 4, JobLengthSec: 10,
			ToleranceSec: 0, CapacityGPUs: 4, WindowSec: 10}
		tl := verdict.Build(fixture("fair-share",
			tenantPod("a1", "a", 4, 0, 0, 20), tenantPod("b1", "b", 4, 5, 20, 30),
		), "fair-share", at(30))
		v, err := verdict.FairShare(tl, p)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Measurements["jain_median"]).To(Equal(0.5))
		Expect(v.Measurements["jain_p5"]).To(Equal(0.5))
		Expect(v.Measurements["windows_used"]).To(Equal(2.0))
		Expect(v.Measurements["windows_excluded"]).To(Equal(1.0))
		Expect(v.Measurements["worst_shortfall_gpu_s"]).To(Equal(40.0))
	})

	// capacity 8, 10 s windows, horizon 30 s. Tenant a: one 4-GPU pod running
	// 0 to 30 s. Tenant b: a 4-GPU Job submitted at 0 that a queue holds
	// without a pod until 20 s, then runs to 30 s. Demand starts at the
	// Job's submission, so b wants 40 GPU-s in every window: windows 1 and 2
	// have entitlements 40 and 40, deliveries 40 and 0, x = (1, 0), J = 0.5;
	// window 3 has x = (1, 1), J = 1. Median 0.5, three windows used, and
	// b's shortfall is 40 + 40 = 80 GPU-s. Counting demand from the pod's
	// creation instead would leave b without demand in windows 1 and 2.
	It("counts demand from the Job's submission, while a queue holds it without pods", func() {
		p := verdict.FairShareParams{TenantB: "b", EntitlementGPUs: 4, JobLengthSec: 30,
			ToleranceSec: 0, CapacityGPUs: 8, WindowSec: 10}
		tl := verdict.Build(fixtureJobs("fair-share",
			[]job{{name: "a1", tenant: "a", gpus: 4, created: 0, end: 30}, {name: "b1", tenant: "b", gpus: 4, created: 0, end: 30}},
			tenantPod("a1", "a", 4, 0, 0, 30), tenantPod("b1", "b", 4, 20, 20, 30),
		), "fair-share", at(30))
		v, err := verdict.FairShare(tl, p)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Measurements["jain_median"]).To(Equal(0.5))
		Expect(v.Measurements["windows_used"]).To(Equal(3.0))
		Expect(v.Measurements["worst_shortfall_gpu_s"]).To(Equal(80.0))
	})

	// capacity 4, one 10 s window; a runs 3 GPUs, b runs 1 GPU, both the
	// whole window. Capped water-filling gives entitlements 30 and 10, so
	// x = (1, 1) and J = 1. An uncapped equal split (20, 20) would give 0.8.
	It("caps each tenant's entitlement at its demand", func() {
		p := verdict.FairShareParams{TenantB: "b", EntitlementGPUs: 1, JobLengthSec: 10,
			ToleranceSec: 0, CapacityGPUs: 4, WindowSec: 10}
		tl := verdict.Build(fixture("fair-share",
			tenantPod("a1", "a", 3, 0, 0, 10), tenantPod("b1", "b", 1, 0, 0, 10),
		), "fair-share", at(10))
		v, err := verdict.FairShare(tl, p)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Measurements["jain_median"]).To(Equal(1.0))
		Expect(v.Measurements["worst_shortfall_gpu_s"]).To(Equal(0.0))
	})
})
```

- [ ] **Step 2: Run and watch it fail**

Run: `cd "$W" && go test ./pkg/verdict/`
Expected: FAIL, build failure naming `undefined: verdict.FairShareParams`.

- [ ] **Step 3: Commit the failing specs**

```bash
git -C "$W" add pkg/verdict/fairshare_test.go
git -C "$W" commit -s -S -m "test(verdict): specify the fair-share rule and hand-computed fairness windows" -m "The fairness fixtures are small enough to compute by hand (comments show the arithmetic); one fails if entitlement is not capped by demand, one if demand starts at a pod rather than its Job, and one if B is read at a single instant."
```

- [ ] **Step 4: Implement**

`pkg/verdict/fairshare.go`:

```go
package verdict

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"
)

// FairShareParams are the fair-share probe's parameters.
type FairShareParams struct {
	TenantB         string  `json:"tenant_b"`
	EntitlementGPUs int64   `json:"entitlement_gpus"`
	JobLengthSec    float64 `json:"job_length_sec"`
	ToleranceSec    float64 `json:"tolerance_sec"`
	CapacityGPUs    int64   `json:"capacity_gpus"`
	WindowSec       float64 `json:"window_sec"`
}

func (p FairShareParams) validate() error {
	if p.TenantB == "" {
		return errors.New("tenant_b is empty")
	}
	if p.EntitlementGPUs < 1 || p.CapacityGPUs < 1 {
		return errors.New("entitlement_gpus and capacity_gpus must be 1 or more")
	}
	if p.JobLengthSec <= 0 || p.WindowSec <= 0 || p.ToleranceSec < 0 {
		return errors.New("job_length_sec and window_sec must be positive, tolerance_sec not negative")
	}
	return nil
}

// FairShare judges whether tenant B received its entitlement within one
// job length (plus tolerance) of its first submission: the most GPUs B had
// running at any moment of [submission, submission + length + tolerance].
// It also records the fairness of capacity over time across the probe's
// tenants.
func FairShare(t Timeline, p FairShareParams) (Verdict, error) {
	if err := p.validate(); err != nil {
		return Verdict{}, err
	}
	var bSubmit time.Time
	for _, j := range t.Jobs {
		if j.Tenant == p.TenantB && !j.Created.IsZero() && (bSubmit.IsZero() || j.Created.Before(bSubmit)) {
			bSubmit = j.Created
		}
	}
	if bSubmit.IsZero() {
		return Verdict{}, fmt.Errorf("no submission recorded for tenant %s", p.TenantB)
	}
	var bPods []*Pod
	for _, q := range t.Pods {
		if q.Tenant == p.TenantB {
			bPods = append(bPods, q)
		}
	}
	offset := p.JobLengthSec + p.ToleranceSec
	check := bSubmit.Add(seconds(offset))
	bMax := maxRunningGPUs(bPods, check)
	f := fairnessOverTime(t, p.CapacityGPUs, p.WindowSec)
	v := Verdict{
		Measurements: map[string]float64{
			"b_max_gpus_by_check":   float64(bMax),
			"check_offset_s":        offset,
			"windows_used":          float64(f.Used),
			"windows_excluded":      float64(f.Excluded),
			"worst_shortfall_gpu_s": f.WorstShortfall,
		},
		EvidenceSeq: evidence(t.Pods, t.jobSeqs(jobNames(t)...)),
	}
	if f.Used > 0 {
		v.Measurements["jain_median"] = f.Median
		v.Measurements["jain_p5"] = f.P5
	}
	switch {
	case bMax >= p.EntitlementGPUs:
		v.Outcome = Supported
		v.Reason = fmt.Sprintf("tenant %s had %d GPUs running within %s s of submitting; its entitlement is %d", p.TenantB, bMax, fmtSec(offset), p.EntitlementGPUs)
	case bMax > 0:
		v.Outcome = Partial
		v.Reason = fmt.Sprintf("tenant %s had at most %d of its %d entitled GPUs running within %s s of submitting", p.TenantB, bMax, p.EntitlementGPUs, fmtSec(offset))
	default:
		v.Outcome = NotSupported
		v.Reason = fmt.Sprintf("tenant %s had no GPUs running within %s s of submitting", p.TenantB, fmtSec(offset))
	}
	return v, nil
}

// maxRunningGPUs is the most GPUs the pods had running at one moment, over
// every moment up to and including check. A pod runs over [Running, End).
func maxRunningGPUs(pods []*Pod, check time.Time) int64 {
	type edge struct {
		at    time.Time
		delta int64
	}
	var edges []edge
	for _, q := range pods {
		if q.Running.IsZero() || q.Running.After(check) {
			continue
		}
		edges = append(edges, edge{q.Running, q.GPUs})
		if !q.End.IsZero() {
			edges = append(edges, edge{q.End, -q.GPUs})
		}
	}
	// at equal times, apply ends before starts: a pod ending as another
	// starts does not count both
	sort.Slice(edges, func(i, j int) bool {
		if !edges[i].at.Equal(edges[j].at) {
			return edges[i].at.Before(edges[j].at)
		}
		return edges[i].delta < edges[j].delta
	})
	var cur, best int64
	for _, e := range edges {
		cur += e.delta
		if cur > best {
			best = cur
		}
	}
	return best
}

func jobNames(t Timeline) []string {
	out := make([]string, 0, len(t.Jobs))
	for n := range t.Jobs {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// fairness summarizes Jain's index of delivered over entitled GPU-time per
// window, as the Methods section's fairness-over-time metric defines it.
type fairness struct {
	Median, P5     float64
	Used, Excluded int
	WorstShortfall float64
}

// fairnessOverTime splits [first submission, window end) into windows of
// windowSec (the last may be shorter). Per window and tenant: demand is the
// GPU-seconds a job wanted from its submission until it ended (so a job a
// queue holds without pods still counts), delivered is the GPU-seconds its
// pods ran, and entitlement is the max-min fair split of capacity capped by
// demand. Jain's index of delivered/entitlement uses windows with at least
// two tenants with demand and some delivery; the shortfall sums
// max(0, entitlement - delivered) over every window with demand.
func fairnessOverTime(t Timeline, capacityGPUs int64, windowSec float64) fairness {
	var start time.Time
	for _, j := range t.Jobs {
		if !j.Created.IsZero() && (start.IsZero() || j.Created.Before(start)) {
			start = j.Created
		}
	}
	if start.IsZero() {
		return fairness{}
	}
	horizon := t.End.Sub(start).Seconds()
	rel := func(x time.Time, dflt float64) float64 {
		if x.IsZero() {
			return dflt
		}
		return x.Sub(start).Seconds()
	}
	type span struct {
		tenant   string
		gpus     float64
		from, to float64
	}
	var want, ran []span
	for _, n := range jobNames(t) {
		j := t.Jobs[n]
		if !j.Created.IsZero() {
			want = append(want, span{j.Tenant, float64(j.GPUs), rel(j.Created, 0), rel(j.End, horizon)})
		}
	}
	for _, q := range t.Pods {
		if !q.Running.IsZero() {
			ran = append(ran, span{q.Tenant, float64(q.GPUs), rel(q.Running, 0), rel(q.End, horizon)})
		}
	}
	var js []float64
	f := fairness{}
	shortfall := map[string]float64{}
	for k := 0; float64(k)*windowSec < horizon; k++ {
		w0 := float64(k) * windowSec
		w1 := math.Min(w0+windowSec, horizon)
		demand, delivered := map[string]float64{}, map[string]float64{}
		for _, s := range want {
			if d := s.gpus * overlap(s.from, s.to, w0, w1); d > 0 {
				demand[s.tenant] += d
			}
		}
		for _, s := range ran {
			delivered[s.tenant] += s.gpus * overlap(s.from, s.to, w0, w1)
		}
		if len(demand) == 0 {
			f.Excluded++
			continue
		}
		ent := waterFill(float64(capacityGPUs)*(w1-w0), demand)
		for tn, e := range ent {
			shortfall[tn] += math.Max(0, e-delivered[tn])
		}
		if len(demand) < 2 {
			f.Excluded++
			continue
		}
		var sum, sq float64
		n := 0
		for tn, e := range ent {
			if e > 0 {
				x := delivered[tn] / e
				sum += x
				sq += x * x
				n++
			}
		}
		if sq == 0 {
			f.Excluded++
			continue
		}
		js = append(js, sum*sum/(float64(n)*sq))
	}
	for _, s := range shortfall {
		f.WorstShortfall = math.Max(f.WorstShortfall, s)
	}
	f.Used = len(js)
	if f.Used == 0 {
		return f
	}
	sort.Float64s(js)
	if f.Used%2 == 1 {
		f.Median = js[f.Used/2]
	} else {
		f.Median = (js[f.Used/2-1] + js[f.Used/2]) / 2
	}
	f.P5 = js[int(math.Ceil(0.05*float64(f.Used)))-1]
	return f
}

// overlap returns the length of [a, b) intersected with [w0, w1).
func overlap(a, b, w0, w1 float64) float64 {
	return math.Max(0, math.Min(b, w1)-math.Max(a, w0))
}

// waterFill splits capacity max-min fairly among the tenants in demand,
// never giving a tenant more than its demand.
func waterFill(capacity float64, demand map[string]float64) map[string]float64 {
	names := make([]string, 0, len(demand))
	for t := range demand {
		names = append(names, t)
	}
	sort.Slice(names, func(i, j int) bool {
		if demand[names[i]] != demand[names[j]] {
			return demand[names[i]] < demand[names[j]]
		}
		return names[i] < names[j]
	})
	out := map[string]float64{}
	remaining := capacity
	for i, t := range names {
		give := math.Min(demand[t], remaining/float64(len(names)-i))
		out[t] = give
		remaining -= give
	}
	return out
}
```

- [ ] **Step 5: Run the specs**

Run: `cd "$W" && go tool ginkgo --no-color ./pkg/verdict && make lint`
Expected: `Ran 31 of 31 Specs`, `SUCCESS!`; lint exits 0.

- [ ] **Step 6: Commit**

```bash
git -C "$W" add pkg/verdict/fairshare.go
git -C "$W" commit -s -S -m "feat(verdict): judge fair share and measure fairness over time" -m "Implements the Methods section's fairness-over-time metric with probe-scale windows, so vanilla's lack of a tenant policy shows up as a number, not only as a verdict."
```

---

### Task 5: Backfill and topology rules, and the probe dispatcher

**Files:**
- Create: `pkg/verdict/backfill.go`, `pkg/verdict/topology.go`, `pkg/verdict/evaluate.go`
- Test: `pkg/verdict/backfill_test.go`, `pkg/verdict/topology_test.go`, `pkg/verdict/evaluate_test.go`

**Interfaces:**
- Consumes: Tasks 2 to 4.
- Produces: `type BackfillParams struct{BigJob string; BigPods int; BlockerJobs, SmallJobs []string; ToleranceSec float64}` (JSON `big_job`, `big_pods`, `blocker_jobs`, `small_jobs`, `tolerance_sec`); `func Backfill(t Timeline, p BackfillParams) (Verdict, error)`; `type TopologyParams struct{AttemptJobs []string; PodsPerJob int; RackOf map[string]string}` (JSON `attempt_jobs`, `pods_per_job`, `rack_of`); `func Topology(t Timeline, p TopologyParams) (Verdict, error)`; capability constants `CapGang = "gang"`, `CapGangControl = "gang-control"`, `CapPriority = "priority"`, `CapPreemption = "preemption"`, `CapFairShare = "fair-share"`, `CapBackfill = "backfill"`, `CapTopology = "topology"`; `var Capabilities = []string{gang, priority, preemption, fair-share, backfill, topology}`; `type Probe struct{Capability string; Params json.RawMessage; WindowEnd time.Time}` (JSON `capability`, `params`, `window_end`); `func Evaluate(evs []events.Event, pr Probe) (Verdict, error)`.

- [ ] **Step 1: Write the failing specs**

`pkg/verdict/backfill_test.go`:

```go
package verdict_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
)

var _ = Describe("Backfill", func() {
	params := verdict.BackfillParams{BigJob: "big", BigPods: 4,
		BlockerJobs: []string{"blk0", "blk1", "blk2"}, SmallJobs: []string{"s0", "s1", "s2", "s3"}, ToleranceSec: 5}

	// blockers run on n1..n3 and end at 50, 55 and 60 s, so the gang's
	// earliest possible start is 60 s (the LAST blocker's end); its pods are scheduled
	// at bigAt; short jobs are scheduled at the given seconds (negative:
	// never) and run 25 s.
	scenario := func(bigAt float64, smallAt ...float64) verdict.Timeline {
		ps := []pod{
			done("blk0", "blk0", "n1", 0, 0, 0.5, 50), done("blk1", "blk1", "n2", 0, 0, 0.5, 55),
			done("blk2", "blk2", "n3", 0, 0, 0.5, 60),
		}
		for i, n := range []string{"b0", "b1", "b2", "b3"} {
			node := []string{"n1", "n2", "n3", "n4"}[i]
			if bigAt < 0 {
				ps = append(ps, pending(n, "big", 1))
				continue
			}
			ps = append(ps, pod{name: n, job: "big", tenant: "t1", gpus: 8, node: node,
				created: 1, scheduled: bigAt, running: bigAt + 0.5, end: -1})
		}
		for i, s := range smallAt {
			n := []string{"s0", "s1", "s2", "s3"}[i]
			if s < 0 {
				ps = append(ps, pending(n, n, 2))
				continue
			}
			ps = append(ps, done(n, n, "n4", 2, s, s+0.5, s+25))
		}
		return verdict.Build(fixture("backfill", ps...), "backfill", at(200))
	}

	It("is supported when short jobs fill the gap and the gang starts on time", func() {
		v, err := verdict.Backfill(scenario(61, 2, 27.5, 61.5, 86.5), params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Supported))
		Expect(v.Reason).To(Equal("2 short jobs ran before the gang, which started 1 s after its earliest possible start"))
		Expect(v.Measurements).To(Equal(map[string]float64{"small_before_big": 2, "big_start_delay_s": 1}))
	})

	It("still counts a start exactly at the tolerance as on time", func() {
		v, err := verdict.Backfill(scenario(65, 2, 27.5, 66, 91), params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Supported))
	})

	It("is partial when short jobs delay the gang past the tolerance", func() {
		v, err := verdict.Backfill(scenario(78, 2, 27.5, 52.5, 78.5), params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Partial))
		Expect(v.Reason).To(Equal("3 short jobs ran before the gang, which started 18 s after its earliest possible start"))
	})

	It("is partial when short jobs ran and the gang never started", func() {
		v, err := verdict.Backfill(scenario(-1, 2, 27.5, 52.5, 78), params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Partial))
		Expect(v.Reason).To(Equal("4 short jobs ran, and the gang never started"))
	})

	It("is not supported when the short jobs wait behind the gang", func() {
		v, err := verdict.Backfill(scenario(60.5, 61, 61, 86, 86), params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.NotSupported))
		Expect(v.Reason).To(Equal("no short job ran before the gang started"))
	})
})
```

`pkg/verdict/topology_test.go`:

```go
package verdict_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
)

var _ = Describe("Topology", func() {
	racks := map[string]string{"n1": "rack-a", "n2": "rack-a", "n3": "rack-b", "n4": "rack-b"}
	params := verdict.TopologyParams{AttemptJobs: []string{"t1", "t2", "t3"}, PodsPerJob: 2, RackOf: racks}

	attempt := func(job, nodeA, nodeB string, start float64) []pod {
		return []pod{
			done(job+"-0", job, nodeA, start, start+1, start+1.5, start+20),
			done(job+"-1", job, nodeB, start, start+1, start+1.5, start+20),
		}
	}
	run := func(ps ...[]pod) verdict.Timeline {
		var all []pod
		for _, p := range ps {
			all = append(all, p...)
		}
		return verdict.Build(fixture("topology", all...), "topology", at(200))
	}

	It("is supported when every attempt lands in one rack", func() {
		v, err := verdict.Topology(run(attempt("t1", "n3", "n4", 0), attempt("t2", "n4", "n3", 30),
			attempt("t3", "n3", "n4", 60)), params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Supported))
		Expect(v.Reason).To(Equal("all 3 attempts placed every pod in one rack"))
		Expect(v.Measurements).To(Equal(map[string]float64{"attempts": 3, "attempts_one_rack": 3}))
	})

	It("is partial when some attempts split across racks", func() {
		v, err := verdict.Topology(run(attempt("t1", "n3", "n4", 0), attempt("t2", "n2", "n3", 30),
			attempt("t3", "n3", "n4", 60)), params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.Partial))
		Expect(v.Reason).To(Equal("2 of 3 attempts placed every pod in one rack"))
	})

	It("is not supported when no attempt lands in one rack", func() {
		v, err := verdict.Topology(run(attempt("t1", "n2", "n3", 0), attempt("t2", "n2", "n4", 30),
			attempt("t3", "n2", "n3", 60)), params)
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Outcome).To(Equal(verdict.NotSupported))
	})

	It("fails, as a harness error, on a node outside the fleet", func() {
		_, err := verdict.Topology(run(attempt("t1", "n3", "n9", 0), attempt("t2", "n3", "n4", 30),
			attempt("t3", "n3", "n4", 60)), params)
		Expect(err).To(MatchError(`node "n9" is not in rack_of`))
	})
})
```

`pkg/verdict/evaluate_test.go`:

```go
package verdict_test

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
)

var _ = Describe("Evaluate", func() {
	evs := fixture("gang",
		done("a0", "gang-a", "n1", 0, 1.0, 2, 22), done("a1", "gang-a", "n2", 0, 1.5, 2, 22),
		done("b0", "gang-b", "n1", 0, 22.5, 23.5, 44), done("b1", "gang-b", "n2", 0, 23.0, 24, 44),
		done("late", "gang-b", "n3", 55, 56, 57, 58),
	)

	It("decodes the parameters, applies the window and names the capability", func() {
		v, err := verdict.Evaluate(evs, verdict.Probe{
			Capability: "gang",
			Params:     json.RawMessage(`{"jobs":["gang-a","gang-b"],"size":2,"grace_sec":5}`),
			WindowEnd:  at(50),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(v.Capability).To(Equal("gang"))
		Expect(v.Outcome).To(Equal(verdict.Supported))
		Expect(v.EvidenceSeq).To(HaveLen(16))
	})

	It("rejects a parameter the probe does not define", func() {
		_, err := verdict.Evaluate(evs, verdict.Probe{
			Capability: "gang",
			Params:     json.RawMessage(`{"jobs":["gang-a"],"size":2,"grace_sec":5,"colour":"red"}`),
			WindowEnd:  at(50),
		})
		Expect(err).To(MatchError(`gang: params: json: unknown field "colour"`))
	})

	It("rejects an unknown capability", func() {
		_, err := verdict.Evaluate(evs, verdict.Probe{Capability: "teleport", Params: json.RawMessage(`{}`), WindowEnd: at(50)})
		Expect(err).To(MatchError("teleport: unknown capability"))
	})

	It("rejects a probe without a window end", func() {
		_, err := verdict.Evaluate(evs, verdict.Probe{Capability: "gang", Params: json.RawMessage(`{}`)})
		Expect(err).To(MatchError("gang: window_end is missing"))
	})
})
```

- [ ] **Step 2: Run and watch it fail**

Run: `cd "$W" && go test ./pkg/verdict/`
Expected: FAIL, build failure naming `undefined: verdict.BackfillParams`, `undefined: verdict.TopologyParams` and `undefined: verdict.Evaluate`.

- [ ] **Step 3: Commit the failing specs**

```bash
git -C "$W" add pkg/verdict/backfill_test.go pkg/verdict/topology_test.go pkg/verdict/evaluate_test.go
git -C "$W" commit -s -S -m "test(verdict): specify backfill, topology and the probe dispatcher" -m "Blockers end at different times so the earliest start must be the last blocker's end; the dispatcher must apply the evidence window and refuse unknown parameters."
```

- [ ] **Step 4: Implement**

`pkg/verdict/backfill.go`:

```go
package verdict

import (
	"errors"
	"fmt"
	"time"
)

// BackfillParams are the backfill probe's parameters: the large gang, the
// blocker Jobs whose end sets the gang's earliest possible start, the short
// Jobs that may run in the gap, and the start tolerance.
type BackfillParams struct {
	BigJob       string   `json:"big_job"`
	BigPods      int      `json:"big_pods"`
	BlockerJobs  []string `json:"blocker_jobs"`
	SmallJobs    []string `json:"small_jobs"`
	ToleranceSec float64  `json:"tolerance_sec"`
}

func (p BackfillParams) validate() error {
	if p.BigJob == "" || len(p.BlockerJobs) == 0 || len(p.SmallJobs) == 0 {
		return errors.New("big_job, blocker_jobs and small_jobs must all be set")
	}
	if p.BigPods < 1 {
		return fmt.Errorf("big_pods %d, want 1 or more", p.BigPods)
	}
	if p.ToleranceSec < 0 {
		return fmt.Errorf("tolerance_sec %s is negative", fmtSec(p.ToleranceSec))
	}
	return nil
}

// Backfill judges whether short jobs ran in the gap before the large gang
// without delaying it. The gang starts when its BigPods-th pod is
// scheduled; its earliest possible start is the last blocker pod's end. A
// short job or gang seen only as a Job (a queue still held it) never ran.
func Backfill(t Timeline, p BackfillParams) (Verdict, error) {
	if err := p.validate(); err != nil {
		return Verdict{}, err
	}
	big, seen := t.job(p.BigJob)
	if !seen {
		return Verdict{}, fmt.Errorf("no events recorded for job %s", p.BigJob)
	}
	blockers, missing := ofJobs(t.Pods, p.BlockerJobs)
	if missing != "" {
		return Verdict{}, fmt.Errorf("no pods recorded for blocker job %s", missing)
	}
	var smalls []*Pod
	for _, j := range p.SmallJobs {
		ps, seen := t.job(j)
		if !seen {
			return Verdict{}, fmt.Errorf("no events recorded for job %s", j)
		}
		smalls = append(smalls, ps...)
	}
	var earliest time.Time
	for _, b := range blockers {
		if b.End.IsZero() {
			return Verdict{}, fmt.Errorf("blocker job %s never finished", b.Job)
		}
		if b.End.After(earliest) {
			earliest = b.End
		}
	}
	tbig, started := nthScheduled(big, p.BigPods)
	before := 0
	for _, s := range smalls {
		if !s.Scheduled.IsZero() && (!started || s.Scheduled.Before(tbig)) {
			before++
		}
	}
	v := Verdict{
		Measurements: map[string]float64{"small_before_big": float64(before)},
		EvidenceSeq:  evidence(big, blockers, smalls, t.jobSeqs(append(append([]string{p.BigJob}, p.BlockerJobs...), p.SmallJobs...)...)),
	}
	delay := 0.0
	if started {
		delay = tbig.Sub(earliest).Seconds()
		v.Measurements["big_start_delay_s"] = delay
	}
	switch {
	case before == 0 && started:
		v.Outcome = NotSupported
		v.Reason = "no short job ran before the gang started"
	case before == 0:
		v.Outcome = NotSupported
		v.Reason = "no short job ran, and the gang never started"
	case !started:
		v.Outcome = Partial
		v.Reason = fmt.Sprintf("%d short jobs ran, and the gang never started", before)
	case delay > p.ToleranceSec:
		v.Outcome = Partial
		v.Reason = fmt.Sprintf("%d short jobs ran before the gang, which started %s s after its earliest possible start", before, fmtSec(delay))
	default:
		v.Outcome = Supported
		v.Reason = fmt.Sprintf("%d short jobs ran before the gang, which started %s s after its earliest possible start", before, fmtSec(delay))
	}
	return v, nil
}
```

`pkg/verdict/topology.go`:

```go
package verdict

import (
	"errors"
	"fmt"
	"sort"
)

// TopologyParams are the topology probe's parameters: one Job per attempt,
// its pod count, and the rack of every node in the fleet.
type TopologyParams struct {
	AttemptJobs []string          `json:"attempt_jobs"`
	PodsPerJob  int               `json:"pods_per_job"`
	RackOf      map[string]string `json:"rack_of"`
}

func (p TopologyParams) validate() error {
	if len(p.AttemptJobs) == 0 || len(p.RackOf) == 0 {
		return errors.New("attempt_jobs and rack_of must both be set")
	}
	if p.PodsPerJob < 1 {
		return fmt.Errorf("pods_per_job %d, want 1 or more", p.PodsPerJob)
	}
	return nil
}

// Topology judges whether each attempt placed its first PodsPerJob
// scheduled pods in a single rack. An attempt with fewer scheduled pods
// (stuck, or held by a queue) does not count as one rack.
func Topology(t Timeline, p TopologyParams) (Verdict, error) {
	if err := p.validate(); err != nil {
		return Verdict{}, err
	}
	oneRack := 0
	groups := [][]*Pod{t.jobSeqs(p.AttemptJobs...)}
	for _, job := range p.AttemptJobs {
		ps, seen := t.job(job)
		if !seen {
			return Verdict{}, fmt.Errorf("no events recorded for job %s", job)
		}
		groups = append(groups, ps)
		var sched []*Pod
		for _, q := range ps {
			if !q.Scheduled.IsZero() {
				sched = append(sched, q)
			}
		}
		if len(sched) < p.PodsPerJob {
			continue
		}
		sort.SliceStable(sched, func(i, j int) bool { return sched[i].Scheduled.Before(sched[j].Scheduled) })
		racks := map[string]bool{}
		for _, q := range sched[:p.PodsPerJob] {
			r, ok := p.RackOf[q.Node]
			if !ok {
				return Verdict{}, fmt.Errorf("node %q is not in rack_of", q.Node)
			}
			racks[r] = true
		}
		if len(racks) == 1 {
			oneRack++
		}
	}
	n := len(p.AttemptJobs)
	v := Verdict{
		Measurements: map[string]float64{"attempts": float64(n), "attempts_one_rack": float64(oneRack)},
		EvidenceSeq:  evidence(groups...),
	}
	switch oneRack {
	case n:
		v.Outcome = Supported
		v.Reason = fmt.Sprintf("all %d attempts placed every pod in one rack", n)
	case 0:
		v.Outcome = NotSupported
		v.Reason = "no attempt placed its pods in one rack"
	default:
		v.Outcome = Partial
		v.Reason = fmt.Sprintf("%d of %d attempts placed every pod in one rack", oneRack, n)
	}
	return v, nil
}
```

`pkg/verdict/evaluate.go`:

```go
package verdict

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
)

// Capability names, as used in probe labels, probes.json and claims files.
const (
	CapGang        = "gang"
	CapGangControl = "gang-control"
	CapPriority    = "priority"
	CapPreemption  = "preemption"
	CapFairShare   = "fair-share"
	CapBackfill    = "backfill"
	CapTopology    = "topology"
)

// Capabilities lists the six capabilities, in report order. Controls such
// as CapGangControl are not capabilities and have no documented claim.
var Capabilities = []string{CapGang, CapPriority, CapPreemption, CapFairShare, CapBackfill, CapTopology}

// Probe is one entry of probes.json: which probe ran, its parameters, and
// the end of its evidence window. Events observed after WindowEnd are
// ignored, so a verdict does not depend on the probe's cleanup.
type Probe struct {
	Capability string          `json:"capability"`
	Params     json.RawMessage `json:"params"`
	WindowEnd  time.Time       `json:"window_end"`
}

// Evaluate computes the verdict of one probe from a run's events.
func Evaluate(evs []events.Event, pr Probe) (Verdict, error) {
	v, err := evaluate(evs, pr)
	if err != nil {
		return Verdict{}, fmt.Errorf("%s: %w", pr.Capability, err)
	}
	v.Capability = pr.Capability
	return v, nil
}

func evaluate(evs []events.Event, pr Probe) (Verdict, error) {
	if pr.WindowEnd.IsZero() {
		return Verdict{}, fmt.Errorf("window_end is missing")
	}
	t := Build(evs, pr.Capability, pr.WindowEnd)
	switch pr.Capability {
	case CapGang, CapGangControl:
		var p GangParams
		if err := decode(pr.Params, &p); err != nil {
			return Verdict{}, err
		}
		return Gang(t, p)
	case CapPriority:
		var p PriorityParams
		if err := decode(pr.Params, &p); err != nil {
			return Verdict{}, err
		}
		return Priority(t, p)
	case CapPreemption:
		var p PreemptionParams
		if err := decode(pr.Params, &p); err != nil {
			return Verdict{}, err
		}
		return Preemption(t, p)
	case CapFairShare:
		var p FairShareParams
		if err := decode(pr.Params, &p); err != nil {
			return Verdict{}, err
		}
		return FairShare(t, p)
	case CapBackfill:
		var p BackfillParams
		if err := decode(pr.Params, &p); err != nil {
			return Verdict{}, err
		}
		return Backfill(t, p)
	case CapTopology:
		var p TopologyParams
		if err := decode(pr.Params, &p); err != nil {
			return Verdict{}, err
		}
		return Topology(t, p)
	}
	return Verdict{}, fmt.Errorf("unknown capability")
}

func decode(raw json.RawMessage, into any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		return fmt.Errorf("params: %w", err)
	}
	return nil
}
```

- [ ] **Step 5: Run the specs**

Run: `cd "$W" && go tool ginkgo --no-color ./pkg/verdict && make lint`
Expected: `Ran 44 of 44 Specs`, `SUCCESS!`; lint exits 0.

- [ ] **Step 6: Commit**

```bash
git -C "$W" add pkg/verdict/backfill.go pkg/verdict/topology.go pkg/verdict/evaluate.go
git -C "$W" commit -s -S -m "feat(verdict): judge backfill and topology, and dispatch probes by capability" -m "Evaluate is the single entry point the e2e suite and the offline report share, so both read the same parameters and the same evidence window."
```

---

### Task 6: Claims loader

**Files:**
- Create: `pkg/claims/claims.go`
- Test: `pkg/claims/claims_test.go`

**Interfaces:**
- Consumes: `verdict.Outcome`, `verdict.Capabilities`.
- Produces: `type Claim struct{Claim, Citation string; Expected verdict.Outcome; Mechanism string}` (JSON/YAML `claim`, `citation`, `expected`, `mechanism`); `type File struct{Scheduler, Version string; Claims map[string]Claim}`; `func Parse([]byte) (File, error)`; `func Load(path string) (File, error)`.

- [ ] **Step 1: Write the failing specs**

`pkg/claims/claims_test.go`:

```go
package claims_test

import (
	"strings"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/claims"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
)

func TestClaims(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "claims") {
		t.Fatal("claims specs failed")
	}
}

const valid = `scheduler: vanilla
version: Kubernetes v1.37.0
claims:
  gang:
    claim: Pods of a group are scheduled together or not at all.
    citation: https://example.org/gang
    expected: supported
    mechanism: Workload API gang
  priority:
    claim: Higher-priority pods are ordered ahead in the scheduling queue.
    citation: https://example.org/priority
    expected: supported
    mechanism: PriorityClass
  preemption:
    claim: The scheduler evicts lower-priority pods to make room.
    citation: https://example.org/preemption
    expected: partial
    mechanism: PriorityClass with PreemptLowerPriority
  fair-share:
    claim: No tenant fair-share mechanism is documented.
    citation: https://example.org/fair-share
    expected: not_supported
    mechanism: namespaces as tenants, no policy
  backfill:
    claim: No backfill or reservation mechanism is documented.
    citation: https://example.org/backfill
    expected: partial
    mechanism: plain Jobs
  topology:
    claim: Required pod affinity keeps pods in one topology domain.
    citation: https://example.org/topology
    expected: supported
    mechanism: required pod affinity on the rack label
`

var _ = Describe("Parse", func() {
	It("reads every capability's claim", func() {
		f, err := claims.Parse([]byte(valid))
		Expect(err).NotTo(HaveOccurred())
		Expect(f.Scheduler).To(Equal("vanilla"))
		Expect(f.Version).To(Equal("Kubernetes v1.37.0"))
		Expect(f.Claims).To(HaveLen(6))
		Expect(f.Claims["fair-share"]).To(Equal(claims.Claim{
			Claim:     "No tenant fair-share mechanism is documented.",
			Citation:  "https://example.org/fair-share",
			Expected:  verdict.NotSupported,
			Mechanism: "namespaces as tenants, no policy",
		}))
	})

	It("rejects a capability the suite does not define", func() {
		in := strings.Replace(valid, "  backfill:", "  removed:", 1)
		_, err := claims.Parse([]byte(in))
		Expect(err).To(MatchError(`claims: unknown capability "removed"`))
	})

	It("rejects a file that leaves a capability out", func() {
		cut := valid[:strings.Index(valid, "  topology:")]
		_, err := claims.Parse([]byte(cut))
		Expect(err).To(MatchError(`claims: missing capability "topology"`))
	})

	It("rejects a citation that is not https", func() {
		in := strings.Replace(valid, "https://example.org/priority", "http://example.org/priority", 1)
		_, err := claims.Parse([]byte(in))
		Expect(err).To(MatchError(`claims: capability "priority": citation "http://example.org/priority" is not an https URL`))
	})

	It("rejects an expected outcome outside the three", func() {
		in := strings.Replace(valid, "expected: partial\n    mechanism: plain Jobs", "expected: maybe\n    mechanism: plain Jobs", 1)
		_, err := claims.Parse([]byte(in))
		Expect(err).To(MatchError(`claims: capability "backfill": expected "maybe", want supported, partial or not_supported`))
	})

	It("rejects a field the format does not define", func() {
		in := strings.Replace(valid, "version: Kubernetes v1.37.0", "version: Kubernetes v1.37.0\nvendor: someone", 1)
		_, err := claims.Parse([]byte(in))
		Expect(err).To(MatchError(ContainSubstring(`unknown field "vendor"`)))
	})
})
```

- [ ] **Step 2: Run and watch it fail**

Run: `cd "$W" && go test ./pkg/claims/`
Expected: FAIL, build failure naming `undefined: claims.Parse`.

- [ ] **Step 3: Commit the failing specs**

```bash
git -C "$W" add pkg/claims/claims_test.go
git -C "$W" commit -s -S -m "test(claims): specify the claims file and its validation"
```

- [ ] **Step 4: Implement**

`pkg/claims/claims.go`:

```go
// Package claims loads a scheduler's documented claims: for each
// capability, what the scheduler's documentation says, where, the outcome
// that claim implies, and how the suite's profile exercises it.
package claims

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"sigs.k8s.io/yaml"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
)

// Claim is one capability's documented claim.
type Claim struct {
	Claim     string          `json:"claim"`
	Citation  string          `json:"citation"`
	Expected  verdict.Outcome `json:"expected"`
	Mechanism string          `json:"mechanism"`
}

// File is claims/<scheduler>.yaml.
type File struct {
	Scheduler string           `json:"scheduler"`
	Version   string           `json:"version"`
	Claims    map[string]Claim `json:"claims"`
}

// Parse decodes and validates a claims file: every capability in
// verdict.Capabilities must have a non-empty claim and mechanism, an https
// citation and a valid expected outcome, and no other capability may appear.
func Parse(data []byte) (File, error) {
	var f File
	if err := yaml.UnmarshalStrict(data, &f); err != nil {
		return File{}, fmt.Errorf("claims: %w", err)
	}
	if f.Scheduler == "" || f.Version == "" {
		return File{}, errors.New("claims: scheduler and version must both be set")
	}
	known := map[string]bool{}
	for _, c := range verdict.Capabilities {
		known[c] = true
	}
	names := make([]string, 0, len(f.Claims))
	for name := range f.Claims {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if !known[name] {
			return File{}, fmt.Errorf("claims: unknown capability %q", name)
		}
	}
	for _, c := range verdict.Capabilities {
		cl, ok := f.Claims[c]
		switch {
		case !ok:
			return File{}, fmt.Errorf("claims: missing capability %q", c)
		case strings.TrimSpace(cl.Claim) == "":
			return File{}, fmt.Errorf("claims: capability %q: claim is empty", c)
		case !strings.HasPrefix(cl.Citation, "https://"):
			return File{}, fmt.Errorf("claims: capability %q: citation %q is not an https URL", c, cl.Citation)
		case !cl.Expected.Valid():
			return File{}, fmt.Errorf("claims: capability %q: expected %q, want supported, partial or not_supported", c, cl.Expected)
		case strings.TrimSpace(cl.Mechanism) == "":
			return File{}, fmt.Errorf("claims: capability %q: mechanism is empty", c)
		}
	}
	return f, nil
}

// Load reads and validates the claims file at path.
func Load(path string) (File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return File{}, err
	}
	f, err := Parse(data)
	if err != nil {
		return File{}, fmt.Errorf("%s: %w", path, err)
	}
	return f, nil
}
```

- [ ] **Step 5: Run the specs**

Run: `cd "$W" && go tool ginkgo --no-color ./pkg/claims && make lint`
Expected: `Ran 6 of 6 Specs`, `SUCCESS!`; lint exits 0.

- [ ] **Step 6: Commit**

```bash
git -C "$W" add pkg/claims/claims.go
git -C "$W" commit -s -S -m "feat(claims): load documented claims with citations for all six capabilities" -m "A claim without an https citation or an expected outcome cannot be compared with an observation, so the loader refuses it."
```

---

### Task 7: Report package and cmd/scs-report

**Files:**
- Create: `pkg/report/report.go`, `pkg/report/manifest.go`, `cmd/scs-report/main.go`
- Test: `pkg/report/report_test.go`, `pkg/report/manifest_test.go`, `cmd/scs-report/main_test.go`

**Interfaces:**
- Consumes: `events.ReadFile`, `verdict.Evaluate`, `verdict.Probe`, `verdict.Capabilities`, `claims.Load`.
- Produces: `var Profiles = []string{"vanilla", "kueue"}`; file-name constants `ManifestFile = "manifest.json"`, `SummaryFile = "summary.md"`, `EventsFile = "events.jsonl"`, `ProbesFile = "probes.json"`, `ClaimsFile = "claims.yaml"`, `VerdictsFile = "verdicts.json"`; `type Entry struct{Capability, Profile string; Documented *claims.Claim; Observed verdict.Verdict; Agree *bool}`; `func ReadProbes(path string) ([]verdict.Probe, error)`; `func ProfileVerdicts(profileDir, profile string) ([]Entry, error)`; `func Write(runDir, outDir string) error`; `func Summary(runID string, entries []Entry) []byte`; `type Manifest struct{Schema int; RunID, GitCommit string; GitDirty bool; Host, Tools map[string]string; Fleet json.RawMessage; Profiles map[string]ProfileRun}`; `type ProfileRun struct{Start, End time.Time; Cluster string; FeatureGates map[string]bool; RuntimeConfig, Images, Manifests, Scheduler map[string]string; ClockOffsetStartS, ClockOffsetEndS float64; WatchErrors int}`; `func MergeManifest(runDir string, base Manifest, profile string, pr ProfileRun) error`; `func PrepareProfileDir(runDir, profile string) (string, error)`. Binary `scs-report [-out DIR] RUN_DIR` (exit 0 ok, 1 failure, 2 usage).

- [ ] **Step 1: Write the failing specs**

`pkg/report/report_test.go`:

```go
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

	It("fails when no profile directory has events", func() {
		run := GinkgoT().TempDir()
		Expect(os.WriteFile(filepath.Join(run, report.ManifestFile), []byte(`{"run_id":"x"}`), 0o644)).To(Succeed())
		Expect(report.Write(run, run)).To(MatchError(run + ": no profile directory has an events.jsonl"))
	})

	It("fails when a probe names a job the run never recorded, instead of guessing", func() {
		run := fixtureRun()
		p := filepath.Join(run, "vanilla", report.ProbesFile)
		Expect(os.WriteFile(p, []byte(`[{"capability":"priority","params":{"high_job":"high","queued_jobs":["q9"]},"window_end":"2026-10-10T10:01:40Z"}]`), 0o644)).To(Succeed())
		Expect(report.Write(run, run)).To(MatchError("vanilla: priority: no events recorded for job q9"))
	})
})
```

`pkg/report/manifest_test.go`:

```go
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
```

`cmd/scs-report/main_test.go`:

```go
package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestScsReport(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "scs-report") {
		t.Fatal("scs-report specs failed")
	}
}

var _ = Describe("run", func() {
	It("exits 2 with a usage line when RUN_DIR is missing", func() {
		var stderr bytes.Buffer
		Expect(run(nil, &stderr)).To(Equal(2))
		Expect(stderr.String()).To(Equal("usage: scs-report [-out DIR] RUN_DIR\n"))
	})

	It("exits 1 and names the problem when the run directory has no manifest", func() {
		dir := GinkgoT().TempDir()
		var stderr bytes.Buffer
		Expect(run([]string{dir}, &stderr)).To(Equal(1))
		Expect(stderr.String()).To(Equal("scs-report: open " + filepath.Join(dir, "manifest.json") + ": no such file or directory\n"))
	})

	It("with -out, fails on a run without events and writes nothing into the run directory", func() {
		run0 := GinkgoT().TempDir()
		Expect(os.WriteFile(filepath.Join(run0, "manifest.json"), []byte(`{"run_id":"empty"}`), 0o644)).To(Succeed())
		var stderr bytes.Buffer
		Expect(run([]string{"-out", GinkgoT().TempDir(), run0}, &stderr)).To(Equal(1))
		Expect(stderr.String()).To(Equal("scs-report: " + run0 + ": no profile directory has an events.jsonl\n"))
		entries, err := os.ReadDir(run0)
		Expect(err).NotTo(HaveOccurred())
		Expect(entries).To(HaveLen(1))
	})
})
```

- [ ] **Step 2: Run and watch it fail**

Run: `cd "$W" && go test ./pkg/report/ ./cmd/scs-report/`
Expected: FAIL, build failures naming `undefined: report.Write`, `undefined: report.MergeManifest` and `undefined: run`.

- [ ] **Step 3: Commit the failing specs**

```bash
git -C "$W" add pkg/report/report_test.go pkg/report/manifest_test.go cmd/scs-report/main_test.go
git -C "$W" commit -s -S -m "test(report): specify verdicts.json, summary.md and the offline rebuild" -m "The summary is pinned byte for byte, and a rebuild into another directory must give identical bytes: that is acceptance item 3 in miniature."
```

- [ ] **Step 4: Implement**

`pkg/report/report.go`:

```go
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
// and writes <outDir>/<profile>/verdicts.json and <outDir>/summary.md.
func Write(runDir, outDir string) error {
	runID, err := readRunID(filepath.Join(runDir, ManifestFile))
	if err != nil {
		return err
	}
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
		b, err := json.MarshalIndent(entries, "", "  ")
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Join(outDir, prof), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(outDir, prof, VerdictsFile), append(b, '\n'), 0o644); err != nil {
			return err
		}
		all = append(all, entries...)
	}
	if len(all) == 0 {
		return fmt.Errorf("%s: no profile directory has an %s", runDir, EventsFile)
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
```

`pkg/report/manifest.go`:

```go
package report

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// Manifest is manifest.json: what ran, where, and with which pins. Each
// profile's run merges its own entry under Profiles.
type Manifest struct {
	Schema    int                   `json:"schema"`
	RunID     string                `json:"run_id"`
	GitCommit string                `json:"git_commit"`
	GitDirty  bool                  `json:"git_dirty"`
	Host      map[string]string     `json:"host"`
	Tools     map[string]string     `json:"tools"`
	Fleet     json.RawMessage       `json:"fleet"`
	Profiles  map[string]ProfileRun `json:"profiles"`
}

// ProfileRun is one profile's part of the manifest.
type ProfileRun struct {
	Start         time.Time         `json:"start"`
	End           time.Time         `json:"end"`
	Cluster       string            `json:"cluster"`
	FeatureGates  map[string]bool   `json:"feature_gates"`
	RuntimeConfig map[string]string `json:"runtime_config"`
	Images        map[string]string `json:"images"`
	Manifests     map[string]string `json:"manifests"`
	Scheduler     map[string]string `json:"scheduler"`
	// ClockOffset*S is the API server's creation timestamp minus the host's
	// clock, in seconds; the server stamps whole seconds, so |offset| < 1 is
	// as close as this can tell.
	ClockOffsetStartS float64 `json:"clock_offset_start_s"`
	ClockOffsetEndS   float64 `json:"clock_offset_end_s"`
	WatchErrors       int     `json:"watch_errors"`
}

// MergeManifest writes runDir/manifest.json with base's run-level fields
// and pr under Profiles[profile], keeping any other profile already there.
// It refuses a manifest that belongs to another run ID.
func MergeManifest(runDir string, base Manifest, profile string, pr ProfileRun) error {
	path := filepath.Join(runDir, ManifestFile)
	profiles := map[string]ProfileRun{}
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		var old Manifest
		if err := json.Unmarshal(data, &old); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		if old.RunID != base.RunID {
			return fmt.Errorf("%s belongs to run %s, not %s", path, old.RunID, base.RunID)
		}
		for k, v := range old.Profiles {
			profiles[k] = v
		}
	case !errors.Is(err, fs.ErrNotExist):
		return err
	}
	profiles[profile] = pr
	base.Schema = 1
	base.Profiles = profiles
	out, err := json.MarshalIndent(base, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(out, '\n'), 0o644)
}

// PrepareProfileDir creates runDir/profile and refuses one that already
// holds an events file, so a repeated run ID never overwrites recorded data.
func PrepareProfileDir(runDir, profile string) (string, error) {
	dir := filepath.Join(runDir, profile)
	if _, err := os.Stat(filepath.Join(dir, EventsFile)); err == nil {
		return "", fmt.Errorf("%s already holds %s from an earlier run; choose another run ID", dir, EventsFile)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	return dir, os.MkdirAll(dir, 0o755)
}
```

`cmd/scs-report/main.go`:

```go
// Command scs-report rebuilds a run's verdicts.json files and summary.md
// from the run's recorded events, probes and claims.
//
//	scs-report [-out DIR] RUN_DIR
//
// Without -out it rewrites the files inside RUN_DIR.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/report"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stderr))
}

func run(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("scs-report", flag.ContinueOnError)
	fs.SetOutput(stderr)
	out := fs.String("out", "", "directory to write into (default: RUN_DIR)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 1 {
		fmt.Fprintln(stderr, "usage: scs-report [-out DIR] RUN_DIR")
		return 2
	}
	runDir := fs.Arg(0)
	dst := *out
	if dst == "" {
		dst = runDir
	}
	if err := report.Write(runDir, dst); err != nil {
		fmt.Fprintln(stderr, "scs-report:", err)
		return 1
	}
	return 0
}
```

- [ ] **Step 5: Run the specs**

Run: `cd "$W" && go tool ginkgo --no-color ./pkg/report ./cmd/scs-report && make lint`
Expected: `report - 9/9 specs` and `scs-report - 3/3 specs`, both `SUCCESS!`; lint exits 0.

- [ ] **Step 6: Commit**

```bash
git -C "$W" add pkg/report/report.go pkg/report/manifest.go cmd/scs-report/main.go
git -C "$W" commit -s -S -m "feat(report): rebuild verdicts and the summary from a run's recorded files" -m "The e2e suite and scs-report call the same Write, so a run's own output and an offline rebuild can be compared with cmp."
```

---

### Task 8: Labels and the recorder

**Files:**
- Create: `pkg/labels/labels.go`, `pkg/recorder/changes.go`, `pkg/recorder/workload.go`, `pkg/recorder/recorder.go`
- Test: `pkg/recorder/recorder_test.go`

**Interfaces:**
- Consumes: `events` (Task 1).
- Produces: package `labels` constants `Prefix`, `PartOf`, `PartOfValue`, `Probe`, `Tenant`, `Job`, `RunID`, `Rack`, `Block`, `Selector`; package `recorder`: `type Options struct{RunID string; WorkloadGVR *schema.GroupVersionResource; Now func() time.Time}`; `const SyncTimeout = 2 * time.Minute`; `func New(cs kubernetes.Interface, dyn dynamic.Interface, w io.Writer, opt Options) *Recorder`; `func (*Recorder) Start(ctx) error` (watches Namespaces, Pods and Jobs with the suite's label, `events.k8s.io` Events, and the optional Workload resource, for as long as ctx lives); `Flush() error`; `Stop() error`; `WatchErrors() []time.Time`. Namespaces the suite creates must carry `labels.PartOf`, `labels.Probe` and (per tenant) `labels.Tenant`; pods must carry `labels.PartOf`, `labels.Probe`, `labels.Job`, `labels.Tenant`.

Recorder facts this code relies on (R1, VERIFIED-BY-RUN on kind 1.37 and kwok v0.8.0): kwok sets phase `Running` at its pod-ready stage and `Succeeded` at pod-complete; Ready stays True after completion; `terminated.startedAt` equals `finishedAt` under kwok, so the run start is taken from the Running update; all status timestamps have one-second resolution, and the watch receive time resolves the 13 ms spread observed.

- [ ] **Step 1: Write the failing specs**

`pkg/recorder/recorder_test.go`:

```go
package recorder

import (
	"bytes"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/labels"
)

func TestRecorder(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "recorder") {
		t.Fatal("recorder specs failed")
	}
}

var t0 = time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

func mt(s int) metav1.Time { return metav1.NewTime(t0.Add(time.Duration(s) * time.Second)) }

func basePod() *corev1.Pod {
	prio := int32(1000)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p0", Namespace: "scs-gang", UID: "uid-p0", CreationTimestamp: mt(0),
			Labels: map[string]string{labels.PartOf: labels.PartOfValue, labels.Probe: "gang", labels.Job: "gang-a", labels.Tenant: "t1"}},
		Spec: corev1.PodSpec{Priority: &prio, Containers: []corev1.Container{{Name: "main",
			Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("8")}}}}},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}
}

func bound(p *corev1.Pod) *corev1.Pod {
	q := p.DeepCopy()
	q.Spec.NodeName = "n2"
	q.Status.Conditions = append(q.Status.Conditions, corev1.PodCondition{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: mt(1)})
	return q
}

func running(p *corev1.Pod) *corev1.Pod {
	q := p.DeepCopy()
	q.Status.Phase = corev1.PodRunning
	q.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "main", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: mt(2)}}}}
	return q
}

func succeeded(p *corev1.Pod) *corev1.Pod {
	q := p.DeepCopy()
	q.Status.Phase = corev1.PodSucceeded
	q.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "main", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{FinishedAt: mt(22), StartedAt: mt(22)}}}}
	return q
}

func trs(cs []change) []events.Transition {
	out := []events.Transition{}
	for _, c := range cs {
		out = append(out, c.tr)
	}
	return out
}

var _ = Describe("podChanges", func() {
	It("records creation with the creation timestamp", func() {
		cs := podChanges(nil, basePod())
		Expect(trs(cs)).To(Equal([]events.Transition{events.Created}))
		Expect(*cs[0].apiTime).To(Equal(t0))
		Expect(cs[0].precision).To(Equal("s"))
	})

	It("records binding with the node and the PodScheduled time", func() {
		cs := podChanges(basePod(), bound(basePod()))
		Expect(trs(cs)).To(Equal([]events.Transition{events.Scheduled}))
		Expect(cs[0].node).To(Equal("n2"))
		Expect(*cs[0].apiTime).To(Equal(t0.Add(time.Second)))
	})

	It("records running from the container's start, and success from its finish", func() {
		cs := podChanges(bound(basePod()), running(bound(basePod())))
		Expect(trs(cs)).To(Equal([]events.Transition{events.Running}))
		Expect(*cs[0].apiTime).To(Equal(t0.Add(2 * time.Second)))
		cs = podChanges(running(bound(basePod())), succeeded(running(bound(basePod()))))
		Expect(trs(cs)).To(Equal([]events.Transition{events.Succeeded}))
		Expect(*cs[0].apiTime).To(Equal(t0.Add(22 * time.Second)))
	})

	It("records every passed transition for a pod first seen already running", func() {
		Expect(trs(podChanges(nil, running(bound(basePod()))))).To(Equal([]events.Transition{events.Created, events.Scheduled, events.Running}))
	})

	It("records nothing for an update that changes no tracked field", func() {
		p := running(bound(basePod()))
		Expect(podChanges(p, p.DeepCopy())).To(BeEmpty())
	})

	It("records preemption by the scheduler and eviction by anyone else", func() {
		p := running(bound(basePod()))
		pre := p.DeepCopy()
		pre.Status.Conditions = append(pre.Status.Conditions, corev1.PodCondition{Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue,
			Reason: corev1.PodReasonPreemptionByScheduler, LastTransitionTime: mt(10)})
		Expect(trs(podChanges(p, pre))).To(Equal([]events.Transition{events.Preempted}))
		ev := p.DeepCopy()
		ev.Status.Conditions = append(ev.Status.Conditions, corev1.PodCondition{Type: corev1.DisruptionTarget, Status: corev1.ConditionTrue,
			Reason: "EvictionByEvictionAPI", LastTransitionTime: mt(10)})
		cs := podChanges(p, ev)
		Expect(trs(cs)).To(Equal([]events.Transition{events.Evicted}))
		Expect(cs[0].reason).To(Equal("EvictionByEvictionAPI"))
	})

	It("records an unschedulable pod once, not on every retry", func() {
		u := basePod()
		u.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable,
			Message: "0/4 nodes are available", LastTransitionTime: mt(1)}}
		cs := podChanges(basePod(), u)
		Expect(trs(cs)).To(Equal([]events.Transition{events.Unschedulable}))
		Expect(cs[0].message).To(Equal("0/4 nodes are available"))
		again := u.DeepCopy()
		again.Status.Conditions[0].Message = "0/4 nodes are available: 4 Insufficient nvidia.com/gpu"
		Expect(podChanges(u, again)).To(BeEmpty())
	})
})

var _ = Describe("jobChanges", func() {
	three := int32(3)
	newJob := func() *batchv1.Job {
		return &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: "gang-a", Namespace: "scs-gang-t1", CreationTimestamp: mt(0),
				Labels: map[string]string{labels.Probe: "gang", labels.Tenant: "t1"}},
			Spec: batchv1.JobSpec{Parallelism: &three, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{
				Name: "main", Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("8")}}}}}}},
		}
	}
	It("records a Job's creation, with parallelism times GPUs per pod", func() {
		cs := jobChanges(nil, newJob())
		Expect(trs(cs)).To(Equal([]events.Transition{events.Created}))
		Expect(*cs[0].apiTime).To(Equal(t0))
		Expect(jobBase(newJob()).GPUs).To(Equal(int64(24)))
		Expect(jobBase(newJob()).Tenant).To(Equal("t1"))
	})
	It("records completion once, when Complete turns True", func() {
		done := newJob()
		done.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: mt(22)}}
		cs := jobChanges(newJob(), done)
		Expect(trs(cs)).To(Equal([]events.Transition{events.Succeeded}))
		Expect(*cs[0].apiTime).To(Equal(t0.Add(22 * time.Second)))
		Expect(jobChanges(done, done.DeepCopy())).To(BeEmpty())
	})
})

var _ = Describe("workloadChanges", func() {
	wl := func(conds ...map[string]any) *unstructured.Unstructured {
		cs := make([]any, 0, len(conds))
		for _, c := range conds {
			cs = append(cs, c)
		}
		return &unstructured.Unstructured{Object: map[string]any{
			"metadata": map[string]any{"name": "job-gang-a-1", "namespace": "scs-gang", "creationTimestamp": "2026-10-10T10:00:00Z"},
			"status":   map[string]any{"conditions": cs},
		}}
	}
	It("records each condition that turns True, once", func() {
		pending := wl()
		admitted := wl(map[string]any{"type": "QuotaReserved", "status": "True", "lastTransitionTime": "2026-10-10T10:00:03Z"},
			map[string]any{"type": "Admitted", "status": "True", "lastTransitionTime": "2026-10-10T10:00:03Z"})
		Expect(trs(workloadChanges(nil, pending))).To(Equal([]events.Transition{events.Created}))
		cs := workloadChanges(pending, admitted)
		Expect(trs(cs)).To(Equal([]events.Transition{events.QuotaReserved, events.Admitted}))
		Expect(*cs[1].apiTime).To(Equal(t0.Add(3 * time.Second)))
		Expect(workloadChanges(admitted, admitted)).To(BeEmpty())
	})
})

// syncBuffer is a bytes.Buffer safe for the recorder's writer and the test's reader.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

var _ = Describe("Recorder", func() {
	It("writes a labeled pod's life and a suite Event, attributed by labels", func(ctx SpecContext) {
		cs := fake.NewClientset()
		var out syncBuffer
		clock := t0
		var clockMu sync.Mutex
		r := New(cs, nil, &out, Options{RunID: "r1", Now: func() time.Time {
			clockMu.Lock()
			defer clockMu.Unlock()
			clock = clock.Add(time.Millisecond)
			return clock
		}})
		Expect(r.Start(ctx)).To(Succeed())
		DeferCleanup(r.Stop)

		_, err := cs.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "scs-gang",
			Labels: map[string]string{labels.PartOf: labels.PartOfValue, labels.Probe: "gang", labels.Tenant: "t1"}}}, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())
		pods := cs.CoreV1().Pods("scs-gang")
		p := basePod()
		_, err = pods.Create(ctx, p, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())
		// let each update reach the informer before the next one
		lines := func() int { Expect(r.Flush()).To(Succeed()); return bytes.Count([]byte(out.String()), []byte("\n")) }
		Eventually(lines).Should(Equal(1))
		_, err = pods.Update(ctx, bound(p), metav1.UpdateOptions{})
		Expect(err).NotTo(HaveOccurred())
		Eventually(lines).Should(Equal(2))
		_, err = pods.Update(ctx, running(bound(p)), metav1.UpdateOptions{})
		Expect(err).NotTo(HaveOccurred())
		Eventually(lines).Should(Equal(3))
		_, err = pods.Update(ctx, succeeded(running(bound(p))), metav1.UpdateOptions{})
		Expect(err).NotTo(HaveOccurred())
		Eventually(lines).Should(Equal(4))
		Expect(pods.Delete(ctx, "p0", metav1.DeleteOptions{})).To(Succeed())
		Eventually(lines).Should(Equal(5))
		_, err = cs.EventsV1().Events("scs-gang").Create(ctx, &eventsv1.Event{
			ObjectMeta: metav1.ObjectMeta{Name: "p0.1", Namespace: "scs-gang"},
			EventTime:  metav1.NewMicroTime(t0.Add(1500 * time.Millisecond)),
			Reason:     "Scheduled", Note: "Successfully assigned scs-gang/p0 to n2",
			Regarding: corev1.ObjectReference{Kind: "Pod", Namespace: "scs-gang", Name: "p0", UID: "uid-p0"},
		}, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())
		Eventually(lines).Should(Equal(6))

		got, err := events.Read(bytes.NewReader([]byte(out.String())))
		Expect(err).NotTo(HaveOccurred())
		var tr []events.Transition
		for i, e := range got {
			tr = append(tr, e.Transition)
			Expect(e.ObservedAt).To(Equal(t0.Add(time.Duration(i+1) * time.Millisecond)))
		}
		Expect(tr).To(Equal([]events.Transition{events.Created, events.Scheduled, events.Running, events.Succeeded, events.Deleted, events.Scheduled}))
		created := t0
		Expect(got[0]).To(Equal(events.Event{Schema: 1, Seq: 1, RunID: "r1", ObservedAt: t0.Add(time.Millisecond),
			APITime: &created, APITimePrecision: "s", Kind: "Pod", Namespace: "scs-gang", Name: "p0", UID: "uid-p0",
			Probe: "gang", Job: "gang-a", Tenant: "t1", Priority: 1000, GPUs: 8, Transition: events.Created}))
		Expect(got[1].Node).To(Equal("n2"))
		Expect(got[5].Kind).To(Equal("Event"))
		Expect(got[5].Probe).To(Equal("gang"))
		Expect(got[5].APITimePrecision).To(Equal("us"))
		Expect(*got[5].APITime).To(Equal(t0.Add(1500 * time.Millisecond)))
		Expect(r.WatchErrors()).To(BeEmpty())
	}, SpecTimeout(30*time.Second))

	It("ignores Events in namespaces the suite did not create", func(ctx SpecContext) {
		cs := fake.NewClientset()
		var out syncBuffer
		r := New(cs, nil, &out, Options{RunID: "r1"})
		Expect(r.Start(ctx)).To(Succeed())
		DeferCleanup(r.Stop)
		_, err := cs.EventsV1().Events("default").Create(ctx, &eventsv1.Event{
			ObjectMeta: metav1.ObjectMeta{Name: "x.1", Namespace: "default"}, Reason: "Scheduled",
			Regarding: corev1.ObjectReference{Kind: "Pod", Namespace: "default", Name: "x"},
		}, metav1.CreateOptions{})
		Expect(err).NotTo(HaveOccurred())
		Consistently(func() string { Expect(r.Flush()).To(Succeed()); return out.String() }, "300ms", "50ms").Should(BeEmpty())
	}, SpecTimeout(30*time.Second))
})
```

- [ ] **Step 2: Run and watch it fail**

Run: `cd "$W" && go test ./pkg/recorder/`
Expected: FAIL, build failure naming `undefined: podChanges`, `undefined: jobChanges`, `undefined: workloadChanges` and `undefined: New`, and the missing `pkg/labels` package.

- [ ] **Step 3: Commit the failing specs**

```bash
git -C "$W" add pkg/recorder/recorder_test.go
git -C "$W" commit -s -S -m "test(recorder): specify pod, Job, Event and Workload transitions and attribution"
```

- [ ] **Step 4: Implement**

`pkg/labels/labels.go`:

```go
// Package labels holds the label keys the suite puts on every object it
// creates, so the recorder can find and attribute them.
package labels

// Prefix is the suite's label prefix, under a domain Carlos controls.
const Prefix = "scs.arangogutierrez.github.io/"

// Keys and values.
const (
	PartOf      = "app.kubernetes.io/part-of"
	PartOfValue = "scheduler-capabilities-suite"
	Probe       = Prefix + "probe"
	Tenant      = Prefix + "tenant"
	Job         = Prefix + "job"
	RunID       = Prefix + "run-id"
	Rack        = Prefix + "rack"
	Block       = Prefix + "block"
)

// Selector selects every object the suite created.
const Selector = PartOf + "=" + PartOfValue
```

`pkg/recorder/changes.go`:

```go
package recorder

import (
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
)

// change is one transition found by comparing two versions of an object.
type change struct {
	tr        events.Transition
	apiTime   *time.Time
	precision string
	node      string
	reason    string
	message   string
}

func secTime(t metav1.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	v := t.UTC()
	return &v
}

func condition(p *corev1.Pod, t corev1.PodConditionType) *corev1.PodCondition {
	for i := range p.Status.Conditions {
		if p.Status.Conditions[i].Type == t {
			return &p.Status.Conditions[i]
		}
	}
	return nil
}

// podChanges returns, in order, the transitions between old and cur. A nil
// old means cur was just added; an already-progressed pod then yields every
// transition it has passed (an informer's initial list).
func podChanges(old, cur *corev1.Pod) []change {
	var out []change
	if old == nil {
		out = append(out, change{tr: events.Created, apiTime: secTime(cur.CreationTimestamp), precision: "s"})
		old = &corev1.Pod{}
	}
	if old.Spec.NodeName == "" && cur.Spec.NodeName != "" {
		c := change{tr: events.Scheduled, node: cur.Spec.NodeName}
		if sc := condition(cur, corev1.PodScheduled); sc != nil && sc.Status == corev1.ConditionTrue {
			c.apiTime, c.precision = secTime(sc.LastTransitionTime), "s"
		}
		out = append(out, c)
	}
	if nc := condition(cur, corev1.PodScheduled); nc != nil && nc.Status == corev1.ConditionFalse && nc.Reason == corev1.PodReasonUnschedulable {
		oc := condition(old, corev1.PodScheduled)
		if oc == nil || oc.Status != nc.Status || oc.Reason != nc.Reason {
			out = append(out, change{tr: events.Unschedulable, apiTime: secTime(nc.LastTransitionTime), precision: "s", reason: nc.Reason, message: nc.Message})
		}
	}
	if old.Status.Phase != corev1.PodRunning && cur.Status.Phase == corev1.PodRunning {
		c := change{tr: events.Running}
		if len(cur.Status.ContainerStatuses) > 0 && cur.Status.ContainerStatuses[0].State.Running != nil {
			c.apiTime, c.precision = secTime(cur.Status.ContainerStatuses[0].State.Running.StartedAt), "s"
		} else if cur.Status.StartTime != nil {
			c.apiTime, c.precision = secTime(*cur.Status.StartTime), "s"
		}
		out = append(out, c)
	}
	if old.Status.Phase != cur.Status.Phase && (cur.Status.Phase == corev1.PodSucceeded || cur.Status.Phase == corev1.PodFailed) {
		c := change{tr: events.Succeeded, reason: cur.Status.Reason}
		if cur.Status.Phase == corev1.PodFailed {
			c.tr = events.Failed
		}
		if len(cur.Status.ContainerStatuses) > 0 && cur.Status.ContainerStatuses[0].State.Terminated != nil {
			c.apiTime, c.precision = secTime(cur.Status.ContainerStatuses[0].State.Terminated.FinishedAt), "s"
		}
		out = append(out, c)
	}
	if nd := condition(cur, corev1.DisruptionTarget); nd != nil && nd.Status == corev1.ConditionTrue {
		if od := condition(old, corev1.DisruptionTarget); od == nil || od.Status != corev1.ConditionTrue {
			c := change{tr: events.Evicted, apiTime: secTime(nd.LastTransitionTime), precision: "s", reason: nd.Reason, message: nd.Message}
			if nd.Reason == corev1.PodReasonPreemptionByScheduler {
				c.tr = events.Preempted
			}
			out = append(out, c)
		}
	}
	for i := range out {
		if out[i].apiTime == nil {
			out[i].precision = ""
		}
	}
	return out
}

// eventReasons maps the Event reasons the recorder keeps to transitions.
// Every other reason is skipped: the cluster logs keep the full record.
var eventReasons = map[string]events.Transition{
	"Scheduled":        events.Scheduled,
	"FailedScheduling": events.Unschedulable,
	"Preempted":        events.Preempted,
	"QuotaReserved":    events.QuotaReserved,
	"Admitted":         events.Admitted,
	"Evicted":          events.Evicted,
}

// eventChange returns the transition an events.k8s.io Event records, or
// false when the recorder skips it. EventTime has microseconds; older
// events fall back to the one-second deprecated timestamps.
func eventChange(ev *eventsv1.Event) (change, bool) {
	tr, ok := eventReasons[ev.Reason]
	if !ok {
		return change{}, false
	}
	c := change{tr: tr, reason: ev.Reason, message: ev.Note}
	switch {
	case !ev.EventTime.IsZero():
		t := ev.EventTime.UTC()
		c.apiTime, c.precision = &t, "us"
	case !ev.DeprecatedLastTimestamp.IsZero():
		c.apiTime, c.precision = secTime(ev.DeprecatedLastTimestamp), "s"
	}
	return c, true
}

// jobCondition reports whether condition t is True on j, with its time.
func jobCondition(j *batchv1.Job, t batchv1.JobConditionType) (bool, metav1.Time) {
	if j == nil {
		return false, metav1.Time{}
	}
	for _, c := range j.Status.Conditions {
		if c.Type == t && c.Status == corev1.ConditionTrue {
			return true, c.LastTransitionTime
		}
	}
	return false, metav1.Time{}
}

// jobChanges returns a Job's transitions between old and cur (old nil on
// add): created, then succeeded or failed when the Complete or Failed
// condition turns True. A Job's creation is its submission: under Kueue a
// queued Job has no pods yet.
func jobChanges(old, cur *batchv1.Job) []change {
	var out []change
	if old == nil {
		out = append(out, change{tr: events.Created, apiTime: secTime(cur.CreationTimestamp), precision: "s"})
	}
	for _, x := range []struct {
		t  batchv1.JobConditionType
		tr events.Transition
	}{{batchv1.JobComplete, events.Succeeded}, {batchv1.JobFailed, events.Failed}} {
		now, at := jobCondition(cur, x.t)
		was, _ := jobCondition(old, x.t)
		if now && !was {
			c := change{tr: x.tr, apiTime: secTime(at)}
			if c.apiTime != nil {
				c.precision = "s"
			}
			out = append(out, c)
		}
	}
	return out
}
```

`pkg/recorder/workload.go`:

```go
package recorder

import (
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
)

// workloadConditions maps the Kueue Workload conditions the recorder keeps
// to transitions, in the order they are reported when several turn True in
// one update.
var workloadConditions = []struct {
	typ string
	tr  events.Transition
}{
	{"QuotaReserved", events.QuotaReserved},
	{"Admitted", events.Admitted},
	{"Preempted", events.Preempted},
	{"Evicted", events.Evicted},
	{"Requeued", events.Requeued},
}

// condTrue reports whether condition typ is True on u, with its transition
// time, reason and message.
func condTrue(u *unstructured.Unstructured, typ string) (bool, *time.Time, string, string) {
	if u == nil {
		return false, nil, "", ""
	}
	conds, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	for _, c := range conds {
		m, ok := c.(map[string]any)
		if !ok || m["type"] != typ {
			continue
		}
		if m["status"] != "True" {
			return false, nil, "", ""
		}
		var at *time.Time
		if s, ok := m["lastTransitionTime"].(string); ok {
			if t, err := time.Parse(time.RFC3339, s); err == nil {
				t = t.UTC()
				at = &t
			}
		}
		reason, _ := m["reason"].(string)
		msg, _ := m["message"].(string)
		return true, at, reason, msg
	}
	return false, nil, "", ""
}

// workloadChanges returns the transitions between old and cur; a nil old
// means cur was just added.
func workloadChanges(old, cur *unstructured.Unstructured) []change {
	var out []change
	if old == nil {
		ct := cur.GetCreationTimestamp()
		out = append(out, change{tr: events.Created, apiTime: secTime(ct), precision: "s"})
	}
	for _, wc := range workloadConditions {
		now, at, reason, msg := condTrue(cur, wc.typ)
		was, _, _, _ := condTrue(old, wc.typ)
		if now && !was {
			c := change{tr: wc.tr, apiTime: at, reason: reason, message: msg}
			if at != nil {
				c.precision = "s"
			}
			out = append(out, c)
		}
	}
	return out
}

func (r *Recorder) workloadBase(u *unstructured.Unstructured) (events.Event, bool) {
	info, ok := r.nsInfo(u.GetNamespace())
	if !ok {
		return events.Event{}, false
	}
	e := events.Event{Kind: "Workload", Namespace: u.GetNamespace(), Name: u.GetName(), UID: string(u.GetUID()),
		Probe: info.probe, Tenant: info.tenant}
	if refs := u.GetOwnerReferences(); len(refs) > 0 {
		e.Job = refs[0].Name
	}
	if p, ok, _ := unstructured.NestedInt64(u.Object, "spec", "priority"); ok {
		e.Priority = int32(p)
	}
	return e, true
}

func (r *Recorder) workload(old, cur *unstructured.Unstructured) {
	base, ok := r.workloadBase(cur)
	if !ok {
		return
	}
	for _, c := range workloadChanges(old, cur) {
		r.emit(base, c)
	}
}

func (r *Recorder) workloadDeleted(o any) {
	if d, ok := o.(cache.DeletedFinalStateUnknown); ok {
		o = d.Obj
	}
	u, ok := o.(*unstructured.Unstructured)
	if !ok {
		return
	}
	if base, ok := r.workloadBase(u); ok {
		r.emit(base, change{tr: events.Deleted})
	}
}
```

`pkg/recorder/recorder.go`:

```go
// Package recorder watches the suite's Pods, Events and (optionally) Kueue
// Workloads and writes every transition to events.jsonl. One mutex orders
// the writes, so seq and observed_at increase together.
package recorder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/labels"
)

// Options configure a Recorder.
type Options struct {
	RunID string
	// WorkloadGVR is Kueue's Workload resource; nil means not watched.
	WorkloadGVR *schema.GroupVersionResource
	// Now returns the observation time; nil means time.Now.
	Now func() time.Time
}

// Recorder writes the suite's object transitions as events.
type Recorder struct {
	cs   kubernetes.Interface
	dyn  dynamic.Interface
	opt  Options
	mu   sync.Mutex
	enc  *events.Encoder
	seq  int64
	err  error
	ns   map[string]nsInfo
	werr []time.Time
	stop context.CancelFunc
}

type nsInfo struct{ probe, tenant string }

// New returns a Recorder writing to w. dyn may be nil when opt.WorkloadGVR is nil.
func New(cs kubernetes.Interface, dyn dynamic.Interface, w io.Writer, opt Options) *Recorder {
	if opt.Now == nil {
		opt.Now = time.Now
	}
	return &Recorder{cs: cs, dyn: dyn, opt: opt, enc: events.NewEncoder(w), ns: map[string]nsInfo{}}
}

// SyncTimeout bounds how long Start waits for the watches to sync.
const SyncTimeout = 2 * time.Minute

// Start begins watching for as long as ctx lives (or until Stop) and returns
// once every watch has synced, or with an error after SyncTimeout.
func (r *Recorder) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	r.stop = cancel
	labeled := informers.NewSharedInformerFactoryWithOptions(r.cs, 0,
		informers.WithTweakListOptions(func(o *metav1.ListOptions) { o.LabelSelector = labels.Selector }))
	all := informers.NewSharedInformerFactory(r.cs, 0)

	nsInf := labeled.Core().V1().Namespaces().Informer()
	podInf := labeled.Core().V1().Pods().Informer()
	jobInf := labeled.Batch().V1().Jobs().Informer()
	evInf := all.Events().V1().Events().Informer()
	infs := []cache.SharedIndexInformer{nsInf, podInf, jobInf, evInf}

	if _, err := nsInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(o any) { r.namespace(o) },
		UpdateFunc: func(_, o any) { r.namespace(o) },
	}); err != nil {
		return err
	}
	if _, err := podInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(o any) { r.pod(nil, o.(*corev1.Pod)) },
		UpdateFunc: func(old, o any) { r.pod(old.(*corev1.Pod), o.(*corev1.Pod)) },
		DeleteFunc: func(o any) { r.podDeleted(o) },
	}); err != nil {
		return err
	}
	if _, err := jobInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(o any) { r.job(nil, o.(*batchv1.Job)) },
		UpdateFunc: func(old, o any) { r.job(old.(*batchv1.Job), o.(*batchv1.Job)) },
		DeleteFunc: func(o any) { r.jobDeleted(o) },
	}); err != nil {
		return err
	}
	if _, err := evInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(o any) { r.event(o.(*eventsv1.Event)) },
	}); err != nil {
		return err
	}
	var dynFactory dynamicinformer.DynamicSharedInformerFactory
	if r.opt.WorkloadGVR != nil {
		dynFactory = dynamicinformer.NewDynamicSharedInformerFactory(r.dyn, 0)
		wlInf := dynFactory.ForResource(*r.opt.WorkloadGVR).Informer()
		if _, err := wlInf.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    func(o any) { r.workload(nil, o.(*unstructured.Unstructured)) },
			UpdateFunc: func(old, o any) { r.workload(old.(*unstructured.Unstructured), o.(*unstructured.Unstructured)) },
			DeleteFunc: func(o any) { r.workloadDeleted(o) },
		}); err != nil {
			return err
		}
		infs = append(infs, wlInf)
	}
	for _, inf := range infs {
		if err := inf.SetWatchErrorHandlerWithContext(func(_ context.Context, _ *cache.Reflector, _ error) {
			r.mu.Lock()
			r.werr = append(r.werr, r.opt.Now())
			r.mu.Unlock()
		}); err != nil {
			return err
		}
	}
	labeled.Start(ctx.Done())
	all.Start(ctx.Done())
	if dynFactory != nil {
		dynFactory.Start(ctx.Done())
	}
	synced := make([]cache.InformerSynced, 0, len(infs))
	for _, inf := range infs {
		synced = append(synced, inf.HasSynced)
	}
	syncCtx, cancelSync := context.WithTimeout(ctx, SyncTimeout)
	defer cancelSync()
	if !cache.WaitForCacheSync(syncCtx.Done(), synced...) {
		return errors.New("recorder: watches did not sync")
	}
	return nil
}

// Flush writes buffered events and returns the first write error, if any.
func (r *Recorder) Flush() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.enc.Flush(); err != nil && r.err == nil {
		r.err = err
	}
	return r.err
}

// Stop ends every watch and flushes.
func (r *Recorder) Stop() error {
	if r.stop != nil {
		r.stop()
	}
	return r.Flush()
}

// WatchErrors returns when each watch error happened. A watch error means
// the informer relisted and the recorder may have missed a transition.
func (r *Recorder) WatchErrors() []time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Time(nil), r.werr...)
}

func (r *Recorder) emit(e events.Event, c change) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return
	}
	r.seq++
	e.Schema, e.Seq, e.RunID, e.ObservedAt = events.SchemaVersion, r.seq, r.opt.RunID, r.opt.Now().UTC()
	e.Transition, e.APITime, e.APITimePrecision = c.tr, c.apiTime, c.precision
	e.Node, e.Reason, e.Message = c.node, c.reason, c.message
	if err := r.enc.Encode(e); err != nil {
		r.err = fmt.Errorf("recorder: %w", err)
	}
}

func (r *Recorder) namespace(o any) {
	ns, ok := o.(*corev1.Namespace)
	if !ok {
		return
	}
	r.mu.Lock()
	r.ns[ns.Name] = nsInfo{probe: ns.Labels[labels.Probe], tenant: ns.Labels[labels.Tenant]}
	r.mu.Unlock()
}

func (r *Recorder) nsInfo(name string) (nsInfo, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	i, ok := r.ns[name]
	return i, ok
}

func podBase(p *corev1.Pod) events.Event {
	e := events.Event{
		Kind: "Pod", Namespace: p.Namespace, Name: p.Name, UID: string(p.UID),
		Probe: p.Labels[labels.Probe], Job: p.Labels[labels.Job], Tenant: p.Labels[labels.Tenant],
	}
	if p.Spec.Priority != nil {
		e.Priority = *p.Spec.Priority
	}
	for _, c := range p.Spec.Containers {
		if q, ok := c.Resources.Limits["nvidia.com/gpu"]; ok {
			e.GPUs += q.Value()
		}
	}
	return e
}

func (r *Recorder) pod(old, cur *corev1.Pod) {
	for _, c := range podChanges(old, cur) {
		r.emit(podBase(cur), c)
	}
}

func (r *Recorder) podDeleted(o any) {
	if d, ok := o.(cache.DeletedFinalStateUnknown); ok {
		o = d.Obj
	}
	p, ok := o.(*corev1.Pod)
	if !ok {
		return
	}
	c := change{tr: events.Deleted}
	if p.DeletionTimestamp != nil {
		c.apiTime, c.precision = secTime(*p.DeletionTimestamp), "s"
	}
	r.emit(podBase(p), c)
}

func (r *Recorder) event(ev *eventsv1.Event) {
	if ev.Regarding.Kind != "Pod" && ev.Regarding.Kind != "Workload" {
		return
	}
	info, ok := r.nsInfo(ev.Regarding.Namespace)
	if !ok {
		return
	}
	c, keep := eventChange(ev)
	if !keep {
		return
	}
	r.emit(events.Event{
		Kind: "Event", Namespace: ev.Regarding.Namespace, Name: ev.Regarding.Name, UID: string(ev.Regarding.UID),
		Probe: info.probe, Tenant: info.tenant,
	}, c)
}

// jobBase is a Job's event fields: its labels, and its GPUs as parallelism
// times the pod template's GPU limit.
func jobBase(j *batchv1.Job) events.Event {
	e := events.Event{
		Kind: "Job", Namespace: j.Namespace, Name: j.Name, UID: string(j.UID),
		Probe: j.Labels[labels.Probe], Job: j.Name, Tenant: j.Labels[labels.Tenant],
	}
	per := int64(0)
	for _, c := range j.Spec.Template.Spec.Containers {
		if q, ok := c.Resources.Limits["nvidia.com/gpu"]; ok {
			per += q.Value()
		}
	}
	n := int64(1)
	if j.Spec.Parallelism != nil {
		n = int64(*j.Spec.Parallelism)
	}
	e.GPUs = per * n
	return e
}

func (r *Recorder) job(old, cur *batchv1.Job) {
	for _, c := range jobChanges(old, cur) {
		r.emit(jobBase(cur), c)
	}
}

func (r *Recorder) jobDeleted(o any) {
	if d, ok := o.(cache.DeletedFinalStateUnknown); ok {
		o = d.Obj
	}
	j, ok := o.(*batchv1.Job)
	if !ok {
		return
	}
	c := change{tr: events.Deleted}
	if j.DeletionTimestamp != nil {
		c.apiTime, c.precision = secTime(*j.DeletionTimestamp), "s"
	}
	r.emit(jobBase(j), c)
}
```

- [ ] **Step 5: Run the specs**

Run: `cd "$W" && go tool ginkgo --no-color ./pkg/recorder && make lint`
Expected: `Ran 12 of 12 Specs`, `SUCCESS!`; lint exits 0.

- [ ] **Step 6: Commit**

```bash
git -C "$W" add pkg/labels/labels.go pkg/recorder/changes.go pkg/recorder/workload.go pkg/recorder/recorder.go
git -C "$W" commit -s -S -m "feat(recorder): write every suite pod, Job, Event and Kueue Workload transition" -m "One mutex orders the writes so seq and observed_at rise together; watch errors are kept so a probe whose window saw one can be marked invalid instead of judged."
```

---

### Task 9: Cluster, kwok and fleet

**Files:**
- Create: `pkg/apply/apply.go`, `pkg/kwok/kwok.go`, `pkg/kwok/manifests/` (nine files and `SHA256SUMS`), `pkg/cluster/cluster.go`, `pkg/fleet/fleet.go`
- Test: `pkg/apply/apply_test.go`, `pkg/kwok/kwok_test.go`, `pkg/cluster/cluster_test.go`, `pkg/fleet/fleet_test.go`

**Interfaces:**
- Consumes: `labels` (Task 8).
- Produces: `apply.FieldManager = "scs"`; `apply.New(*rest.Config) (*Applier, error)`; `apply.Decode([]byte) ([]*unstructured.Unstructured, error)`; `(*Applier).Apply(ctx, data []byte, timeout time.Duration) ([]string, error)`; `(*Applier).ApplyObjects(ctx, objs []*unstructured.Unstructured, timeout) ([]string, error)` (the same ordering, for objects a caller edited, such as Kueue's manager ConfigMap in Task 12). `kwok.Order []string`; `kwok.Manifest(name) ([]byte, string, error)`; `kwok.Install(ctx, *apply.Applier, kubernetes.Interface, timeout) (map[string]string, error)`. `cluster.NodeImage`; `type cluster.Config struct{Name, Kubeconfig string; FeatureGates map[string]bool; RuntimeConfig map[string]string}`; `cluster.SuiteConfig(name, kubeconfig) Config`; `type cluster.Provider interface{List; Create; Delete; CollectLogs}`; `cluster.KindProvider() Provider`; `type cluster.Cluster struct{Name, Kubeconfig string; REST *rest.Config; Clientset kubernetes.Interface; Dynamic dynamic.Interface; Applier *apply.Applier}`; `cluster.Create(Provider, Config) (*Cluster, error)`; `(*Cluster).Delete() error`; `(*Cluster).ExportLogs(dir) error`; `cluster.KeepSystemPodsOffFleet(ctx, kubernetes.Interface, timeout) error`. `fleet.GPUResource`; `type fleet.Node`, `type fleet.Spec`; `fleet.Default() Spec`; `(Spec).GPUs() int`; `(Spec).RackOf() map[string]string`; `(Spec).Manifest() ([]byte, error)`; `fleet.Create(ctx, *apply.Applier, Spec) error`; `fleet.WaitReady(ctx, kubernetes.Interface, Spec, timeout) error`.

- [ ] **Step 1: Fetch the nine kwok manifests and pin them**

```bash
cd "$W" && mkdir -p pkg/kwok/manifests && cd pkg/kwok/manifests && \
REF=156033d7df7ea0e09cea82b715fe566ea68aeeb4 && \
curl -sSfL -o kwok.yaml https://github.com/kubernetes-sigs/kwok/releases/download/v0.8.0/kwok.yaml && \
for f in node/fast/node-initialize node/heartbeat-with-lease/node-heartbeat-with-lease pod/general/pod-create pod/general/pod-init-container-running pod/general/pod-init-container-completed pod/general/pod-ready pod/general/pod-complete pod/general/pod-delete; do curl -sSfL -o "$(basename $f).yaml" "https://raw.githubusercontent.com/kubernetes-sigs/kwok/$REF/kustomize/stage/$f.yaml"; done && \
shasum -a 256 *.yaml > SHA256SUMS && cat SHA256SUMS
```

Expected, exactly (the bytes R1 applied to a live cluster):

```text
a4c16e6431e382dcb5c1903139344b7a68652f16a6460337fe17a678a426f405  kwok.yaml
9becfc93321222915bd1ec8ab103fe769afe4b23e1386bf40553deb02e332c9c  node-heartbeat-with-lease.yaml
bb38f752d90248528b579a0ae8f0f660ff3428457eb5c1ebe6345c3246cc7ee5  node-initialize.yaml
7c24515ce76b597901e4e2f39fc372788c97cb2c40c580623450d0a2a13a9fe7  pod-complete.yaml
e77798d07e6cd29b279a6edcd817bf8854291db07003b9e72d7a782019ccc49e  pod-create.yaml
f4141c2a4e7c9aac18e6eedac238912b12935a597ec1267b59c0089ed9862f7a  pod-delete.yaml
48c96914db2186ef94d876c22161ec09c12a14a51b665088347be65467153a9c  pod-init-container-completed.yaml
cb26a694e43b3102703f891b56c5869f8e1da4e17d27c0a3ee9b7c3d48617e85  pod-init-container-running.yaml
fd8335e528adc853303cfdfc1a86f4c669ef0b7f30170dcfb560c799c030369f  pod-ready.yaml
```

If any line differs, stop and report: upstream changed a file behind a pinned tag.

- [ ] **Step 2: Write the failing specs**

`pkg/apply/apply_test.go`:

```go
package apply

import (
	"context"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/clientcmd"
)

func TestApply(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "apply") {
		t.Fatal("apply specs failed")
	}
}

// crBeforeCRD puts a custom resource BEFORE its CRD and Namespace: Apply
// must still create it, which needs the Established wait and the mapper reset.
const crBeforeCRD = `
apiVersion: spike.scs.io/v1
kind: Widget
metadata:
  name: w1
  namespace: spike-apply
spec:
  size: 3
---
apiVersion: v1
kind: Namespace
metadata:
  name: spike-apply
---
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: widgets.spike.scs.io
spec:
  group: spike.scs.io
  names: {kind: Widget, plural: widgets, singular: widget, listKind: WidgetList}
  scope: Namespaced
  versions:
  - name: v1
    served: true
    storage: true
    schema:
      openAPIV3Schema:
        type: object
        properties:
          spec:
            type: object
            properties:
              size: {type: integer}
`

var _ = Describe("Decode", func() {
	It("skips empty documents", func() {
		objs, err := Decode([]byte("---\n---\napiVersion: v1\nkind: Namespace\nmetadata: {name: a}\n---\n\n"))
		Expect(err).NotTo(HaveOccurred())
		Expect(objs).To(HaveLen(1))
		Expect(objs[0].GetName()).To(Equal("a"))
	})

	It("rejects a document without kind", func() {
		_, err := Decode([]byte("apiVersion: v1\nkind: Namespace\nmetadata: {name: a}\n---\napiVersion: v1\nmetadata: {name: b}\n"))
		Expect(err).To(MatchError("document 2 has no kind or apiVersion"))
	})
})

// This spec needs a live cluster: SCS_TEST_KUBECONFIG names its kubeconfig.
var _ = Describe("Apply against a live cluster", func() {
	It("creates a custom resource that precedes its CRD, and re-applies without change", func(ctx SpecContext) {
		kc := os.Getenv("SCS_TEST_KUBECONFIG")
		if kc == "" {
			Skip("SCS_TEST_KUBECONFIG is not set")
		}
		cfg, err := clientcmd.BuildConfigFromFlags("", kc)
		Expect(err).NotTo(HaveOccurred())
		a, err := New(cfg)
		Expect(err).NotTo(HaveOccurred())
		got, err := a.Apply(ctx, []byte(crBeforeCRD), 60*time.Second)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal([]string{"namespaces/ spike-apply", "customresourcedefinitions/ widgets.spike.scs.io", "widgets/spike-apply w1"}))
		gvr := schema.GroupVersionResource{Group: "spike.scs.io", Version: "v1", Resource: "widgets"}
		w, err := a.dyn.Resource(gvr).Namespace("spike-apply").Get(ctx, "w1", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		m := w.GetManagedFields()
		Expect(m).To(HaveLen(1))
		Expect(m[0].Manager).To(Equal(FieldManager))
		Expect(m[0].Operation).To(Equal(metav1.ManagedFieldsOperationApply))
		rv := w.GetResourceVersion()
		_, err = a.Apply(context.Background(), []byte(crBeforeCRD), 60*time.Second)
		Expect(err).NotTo(HaveOccurred())
		w2, err := a.dyn.Resource(gvr).Namespace("spike-apply").Get(ctx, "w1", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(w2.GetResourceVersion()).To(Equal(rv))
	}, SpecTimeout(3*time.Minute))
})
```

`pkg/kwok/kwok_test.go`:

```go
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
```

`pkg/cluster/cluster_test.go`:

```go
package cluster

import (
	"errors"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	kindcluster "sigs.k8s.io/kind/pkg/cluster"
)

func TestCluster(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "cluster") {
		t.Fatal("cluster specs failed")
	}
}

// fakeProvider records calls; it never touches Docker.
type fakeProvider struct {
	existing []string
	created  []string
	deleted  []string
}

func (f *fakeProvider) List() ([]string, error) { return f.existing, nil }
func (f *fakeProvider) Create(name string, _ ...kindcluster.CreateOption) error {
	f.created = append(f.created, name)
	return errors.New("fake: no docker")
}
func (f *fakeProvider) Delete(name, _ string) error   { f.deleted = append(f.deleted, name); return nil }
func (f *fakeProvider) CollectLogs(_, _ string) error { return nil }

var _ = Describe("Create", func() {
	It("refuses a cluster name that already exists, and creates or deletes nothing", func() {
		p := &fakeProvider{existing: []string{"bench-mokka", "scs-vanilla"}}
		_, err := Create(p, SuiteConfig("scs-vanilla", "/tmp/kc"))
		Expect(err).To(MatchError(`kind cluster "scs-vanilla" already exists; the suite does not reuse or delete it. Remove it with: kind delete cluster --name scs-vanilla`))
		Expect(p.created).To(BeEmpty())
		Expect(p.deleted).To(BeEmpty())
	})

	It("asks kind to create a new name", func() {
		p := &fakeProvider{existing: []string{"bench-mokka"}}
		_, err := Create(p, SuiteConfig("scs-kueue", "/tmp/kc"))
		Expect(err).To(MatchError(`create kind cluster "scs-kueue": fake: no docker`))
		Expect(p.created).To(Equal([]string{"scs-kueue"}))
	})
})

var _ = Describe("SuiteConfig", func() {
	It("pins the node image and turns on the Workload API in every component", func() {
		k := SuiteConfig("scs-vanilla", "/tmp/kc").kind()
		Expect(k.Nodes).To(HaveLen(1))
		Expect(k.Nodes[0].Image).To(Equal("kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5"))
		Expect(k.FeatureGates).To(Equal(map[string]bool{"GenericWorkload": true, "TopologyAwareWorkloadScheduling": true}))
		Expect(k.RuntimeConfig).To(Equal(map[string]string{"scheduling.k8s.io/v1beta1": "true"}))
	})
})

var _ = Describe("KeepSystemPodsOffFleet", func() {
	It("adds a type NotIn [kwok] node affinity to every kube-system DaemonSet", func(ctx SpecContext) {
		ready := appsv1.DaemonSetStatus{DesiredNumberScheduled: 1, UpdatedNumberScheduled: 1, NumberAvailable: 1}
		cs := fake.NewClientset(
			&appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "kindnet", Namespace: "kube-system"}, Status: ready},
			&appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: "kube-proxy", Namespace: "kube-system"}, Status: ready},
		)
		Expect(KeepSystemPodsOffFleet(ctx, cs, 5*time.Second)).To(Succeed())
		for _, name := range []string{"kindnet", "kube-proxy"} {
			d, err := cs.AppsV1().DaemonSets("kube-system").Get(ctx, name, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			terms := d.Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
			Expect(terms).To(HaveLen(1))
			Expect(terms[0].MatchExpressions).To(HaveLen(1))
			Expect(terms[0].MatchExpressions[0].Key).To(Equal("type"))
			Expect(string(terms[0].MatchExpressions[0].Operator)).To(Equal("NotIn"))
			Expect(terms[0].MatchExpressions[0].Values).To(Equal([]string{"kwok"}))
		}
	}, SpecTimeout(30*time.Second))
})
```

`pkg/fleet/fleet_test.go`:

```go
package fleet_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/apply"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/fleet"
)

func TestFleet(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "fleet") {
		t.Fatal("fleet specs failed")
	}
}

var _ = Describe("Default", func() {
	It("is four 8-GPU nodes in two racks of two", func() {
		s := fleet.Default()
		Expect(s.GPUs()).To(Equal(32))
		Expect(s.RackOf()).To(Equal(map[string]string{"n1": "rack-a", "n2": "rack-a", "n3": "rack-b", "n4": "rack-b"}))
	})

	It("renders nodes kwok manages, tainted, labeled with rack and block, with GPUs allocatable", func() {
		m, err := fleet.Default().Manifest()
		Expect(err).NotTo(HaveOccurred())
		objs, err := apply.Decode(m)
		Expect(err).NotTo(HaveOccurred())
		Expect(objs).To(HaveLen(4))
		n3 := objs[2]
		Expect(n3.GetName()).To(Equal("n3"))
		Expect(n3.GetAnnotations()).To(HaveKeyWithValue("kwok.x-k8s.io/node", "fake"))
		Expect(n3.GetLabels()).To(Equal(map[string]string{
			"type": "kwok", "kubernetes.io/hostname": "n3", "kubernetes.io/os": "linux", "kubernetes.io/arch": "amd64",
			"app.kubernetes.io/part-of":           "scheduler-capabilities-suite",
			"scs.arangogutierrez.github.io/rack":  "rack-b",
			"scs.arangogutierrez.github.io/block": "block-1",
		}))
		gpu, _, _ := unstructured.NestedString(n3.Object, "status", "allocatable", "nvidia.com/gpu")
		Expect(gpu).To(Equal("8"))
		taints, _, _ := unstructured.NestedSlice(n3.Object, "spec", "taints")
		Expect(taints).To(Equal([]any{map[string]any{"key": "kwok.x-k8s.io/node", "value": "fake", "effect": "NoSchedule"}}))
	})
})
```

- [ ] **Step 3: Run and watch it fail**

Run: `cd "$W" && go test ./pkg/apply/ ./pkg/kwok/ ./pkg/cluster/ ./pkg/fleet/`
Expected: FAIL, build failures naming `undefined: Decode`, `undefined: Manifest`, `undefined: Create` and `undefined: fleet.Default`.

- [ ] **Step 4: Commit the failing specs and the pinned manifests**

```bash
git -C "$W" add pkg/kwok/manifests pkg/apply/apply_test.go pkg/kwok/kwok_test.go pkg/cluster/cluster_test.go pkg/fleet/fleet_test.go
git -C "$W" commit -s -S -m "test(cluster): specify cluster creation, kwok install and the GPU fleet" -m "Pins the nine kwok v0.8.0 manifests by sha256, and pins the refusal to reuse or delete a kind cluster the run did not create."
```

- [ ] **Step 5: Implement**

`pkg/apply/apply.go` (R1's verified applier; `Apply` now decodes and hands the objects to `ApplyObjects`, the ordering logic unchanged):

```go
// Package apply server-side applies multi-document YAML through client-go:
// discovery-backed RESTMapper, dynamic client, field manager "scs", force.
package apply

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
)

const FieldManager = "scs"

var crdGVR = schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}

// Applier holds one dynamic client and one resettable RESTMapper.
type Applier struct {
	dyn    dynamic.Interface
	mapper *restmapper.DeferredDiscoveryRESTMapper
}

func New(cfg *rest.Config) (*Applier, error) {
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &Applier{dyn: dyn, mapper: restmapper.NewDeferredDiscoveryRESTMapper(memory.NewMemCacheClient(dc))}, nil
}

// Decode splits multi-document YAML or JSON into objects, skipping empty documents.
func Decode(data []byte) ([]*unstructured.Unstructured, error) {
	dec := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	var out []*unstructured.Unstructured
	for {
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, err
		}
		if len(m) == 0 {
			continue
		}
		u := &unstructured.Unstructured{Object: m}
		if u.GetKind() == "" || u.GetAPIVersion() == "" {
			return nil, fmt.Errorf("document %d has no kind or apiVersion", len(out)+1)
		}
		out = append(out, u)
	}
}

// Apply applies every document in data. Namespaces and CRDs go first; after
// them it waits for each CRD to be Established and resets the RESTMapper, so
// a custom resource may appear anywhere in data. The rest keep file order.
func (a *Applier) Apply(ctx context.Context, data []byte, timeout time.Duration) ([]string, error) {
	objs, err := Decode(data)
	if err != nil {
		return nil, err
	}
	return a.ApplyObjects(ctx, objs, timeout)
}

// ApplyObjects applies decoded objects in the order Apply describes, so a
// caller may edit a manifest's objects (a ConfigMap's data, say) first.
func (a *Applier) ApplyObjects(ctx context.Context, objs []*unstructured.Unstructured, timeout time.Duration) ([]string, error) {
	var first, rest []*unstructured.Unstructured
	for _, o := range objs {
		gk := o.GroupVersionKind().GroupKind()
		if gk == (schema.GroupKind{Kind: "Namespace"}) || gk == (schema.GroupKind{Group: "apiextensions.k8s.io", Kind: "CustomResourceDefinition"}) {
			first = append(first, o)
		} else {
			rest = append(rest, o)
		}
	}
	var applied []string
	var crds []string
	for _, o := range first {
		id, err := a.applyOne(ctx, o, timeout)
		if err != nil {
			return applied, err
		}
		applied = append(applied, id)
		if o.GetKind() == "CustomResourceDefinition" {
			crds = append(crds, o.GetName())
		}
	}
	for _, name := range crds {
		if err := a.waitEstablished(ctx, name, timeout); err != nil {
			return applied, err
		}
	}
	if len(crds) > 0 {
		a.mapper.Reset()
	}
	for _, o := range rest {
		id, err := a.applyOne(ctx, o, timeout)
		if err != nil {
			return applied, err
		}
		applied = append(applied, id)
	}
	return applied, nil
}

func (a *Applier) applyOne(ctx context.Context, o *unstructured.Unstructured, timeout time.Duration) (string, error) {
	gvk := o.GroupVersionKind()
	var mapping *meta.RESTMapping
	// A NoMatch right after a CRD is Established is a stale discovery cache:
	// reset and retry until the timeout.
	err := wait.PollUntilContextTimeout(ctx, 500*time.Millisecond, timeout, true, func(ctx context.Context) (bool, error) {
		m, err := a.mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if meta.IsNoMatchError(err) {
			a.mapper.Reset()
			return false, nil
		}
		if err != nil {
			return false, err
		}
		mapping = m
		return true, nil
	})
	if err != nil {
		return "", fmt.Errorf("mapping %s: %w", gvk, err)
	}
	var ri dynamic.ResourceInterface
	if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		ns := o.GetNamespace()
		if ns == "" {
			ns = "default"
			o.SetNamespace(ns)
		}
		ri = a.dyn.Resource(mapping.Resource).Namespace(ns)
	} else {
		// kwok.yaml's FlowSchema carries metadata.namespace although
		// FlowSchema is cluster-scoped.
		o.SetNamespace("")
		ri = a.dyn.Resource(mapping.Resource)
	}
	id := fmt.Sprintf("%s/%s %s", mapping.Resource.Resource, o.GetNamespace(), o.GetName())
	if _, err := ri.Apply(ctx, o.GetName(), o, metav1.ApplyOptions{FieldManager: FieldManager, Force: true}); err != nil {
		return "", fmt.Errorf("apply %s: %w", id, err)
	}
	return id, nil
}

func (a *Applier) waitEstablished(ctx context.Context, name string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, 250*time.Millisecond, timeout, true, func(ctx context.Context) (bool, error) {
		crd, err := a.dyn.Resource(crdGVR).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		conds, _, _ := unstructured.NestedSlice(crd.Object, "status", "conditions")
		for _, c := range conds {
			cm, _ := c.(map[string]any)
			if cm["type"] == "Established" && cm["status"] == "True" {
				return true, nil
			}
		}
		return false, nil
	})
}
```

`pkg/kwok/kwok.go`:

```go
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
```

`pkg/cluster/cluster.go`:

```go
// Package cluster creates and deletes the suite's kind clusters through
// kind's Go library, and keeps kind's own DaemonSets off the kwok fleet.
package cluster

import (
	"context"
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
```

`pkg/fleet/fleet.go`:

```go
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
```

- [ ] **Step 6: Run every unit suite**

Run: `cd "$W" && make test && make lint`
Expected: ten suites, each `SUCCESS!`: apply 3 (1 skipped: live cluster), claims 6, cluster 4, events 8, fleet 2, kwok 5, recorder 12, report 9, verdict 44, scs-report 3; lint exits 0.

- [ ] **Step 7: Commit**

```bash
git -C "$W" add pkg/apply/apply.go pkg/kwok/kwok.go pkg/cluster/cluster.go pkg/fleet/fleet.go
git -C "$W" commit -s -S -m "feat(cluster): create kind clusters with a pinned kwok GPU fleet" -m "kind's own DaemonSets tolerate every taint and would sit on the fake nodes; they are moved off before the fleet exists so only probe pods run there."
```

---

### Task 10: Workload builder, profile interface, vanilla profile and its claims

**Files:**
- Create: `pkg/workload/workload.go`, `pkg/scheduler/scheduler.go`, `pkg/scheduler/vanilla/vanilla.go`, `claims/vanilla.yaml`
- Test: `pkg/workload/workload_test.go`, `pkg/scheduler/vanilla/vanilla_test.go`

**Interfaces:**
- Consumes: `labels`, `fleet.GPUResource`, `cluster.Cluster`, `cluster.NodeImage`, `apply.Applier`, `claims.Load`.
- Produces: `workload.Priority` (`Low`, `High`); `workload.Job{Name, Namespace, Probe, Tenant string; Priority Priority; Preempting bool; Pods, GPUsPerPod int; RunFor time.Duration; Gang, EvictWhole, SameRack bool; NodeName string}` with `Validate() error`, `Labels() map[string]string`, `BatchJob() *batchv1.Job`; `workload.FakeImage`, `workload.BackoffLimit = 10`. `scheduler.Tenant{Name, Namespace string; EntitlementGPUs int}`, `scheduler.Scenario{Probe string; Tenants []Tenant}`, `scheduler.Profile` (`Name`, `ClaimsFile`, `Install`, `Prepare`, `Submit`, `Remove`, `Cleanup`, `WorkloadGVR`), `scheduler.CreateNamespaces`. `vanilla.New() *Profile`, `vanilla.PriorityClassName(Job) string`, `vanilla.PodGroupName(Job) string`, `vanilla.PodGroup(Job) map[string]any`, `vanilla.Job(Job) *batchv1.Job`.

Facts this task relies on (R1, VERIFIED-BY-RUN on kind 1.37): a standalone PodGroup gangs a Job's pods through `spec.schedulingGroup.podGroupName` and must exist before the pods; `disruptionMode: {all: {}}` makes 1.37 evict a victim gang whole; `schedulingConstraints.topology` with the rack label placed 2-pod gangs in one rack 3 of 3 times on the first scheduling cycle; `preemptionPolicy: Never` keeps a high-priority pod queued ahead without evicting.

- [ ] **Step 1: Write the failing specs**

`pkg/workload/workload_test.go`:

```go
package workload_test

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

func TestWorkload(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "workload") {
		t.Fatal("workload specs failed")
	}
}

var gang = workload.Job{Name: "gang-a", Namespace: "scs-gang", Probe: "gang", Tenant: "t1",
	Priority: workload.Low, Pods: 3, GPUsPerPod: 8, RunFor: 20 * time.Second, Gang: true}

var _ = Describe("BatchJob", func() {
	It("runs Pods pods at once, survives evictions, and carries exact kwok timing", func() {
		j := gang.BatchJob()
		Expect(j.Name).To(Equal("gang-a"))
		Expect(j.Namespace).To(Equal("scs-gang"))
		Expect(*j.Spec.Parallelism).To(Equal(int32(3)))
		Expect(*j.Spec.Completions).To(Equal(int32(3)))
		Expect(*j.Spec.BackoffLimit).To(Equal(int32(10)))
		Expect(j.Spec.Template.Annotations).To(Equal(map[string]string{
			"pod-create.stage.kwok.x-k8s.io/delay":          "0s",
			"pod-create.stage.kwok.x-k8s.io/jitter-delay":   "0s",
			"pod-ready.stage.kwok.x-k8s.io/delay":           "0s",
			"pod-ready.stage.kwok.x-k8s.io/jitter-delay":    "0s",
			"pod-complete.stage.kwok.x-k8s.io/delay":        "20s",
			"pod-complete.stage.kwok.x-k8s.io/jitter-delay": "20s",
		}))
		Expect(j.Spec.Template.Labels).To(Equal(map[string]string{
			"app.kubernetes.io/part-of":            "scheduler-capabilities-suite",
			"scs.arangogutierrez.github.io/probe":  "gang",
			"scs.arangogutierrez.github.io/tenant": "t1",
			"scs.arangogutierrez.github.io/job":    "gang-a",
		}))
	})

	It("selects and tolerates only the kwok fleet, and asks for the GPUs", func() {
		spec := gang.BatchJob().Spec.Template.Spec
		Expect(spec.NodeSelector).To(Equal(map[string]string{"type": "kwok"}))
		Expect(spec.Tolerations).To(Equal([]corev1.Toleration{{Key: "kwok.x-k8s.io/node", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}}))
		Expect(spec.RestartPolicy).To(Equal(corev1.RestartPolicyNever))
		q := spec.Containers[0].Resources.Limits["nvidia.com/gpu"]
		Expect(q.Value()).To(Equal(int64(8)))
		Expect(spec.Containers[0].Image).To(Equal("registry.invalid/scs/fake:0"))
	})

	It("writes a minute as kwok parses it, and pins a node when asked", func() {
		j := gang
		j.RunFor = 5 * time.Minute
		j.NodeName = "n1"
		b := j.BatchJob()
		Expect(b.Spec.Template.Annotations["pod-complete.stage.kwok.x-k8s.io/delay"]).To(Equal("5m0s"))
		Expect(b.Spec.Template.Spec.NodeSelector).To(Equal(map[string]string{"type": "kwok", "kubernetes.io/hostname": "n1"}))
	})
})

var _ = Describe("Validate", func() {
	It("accepts a well-formed gang", func() {
		Expect(gang.Validate()).To(Succeed())
	})
	It("rejects a low-priority job that preempts", func() {
		j := gang
		j.Preempting = true
		Expect(j.Validate()).To(MatchError("workload: gang-a: only a high-priority job may preempt"))
	})
	It("rejects same-rack on a job that is not a gang", func() {
		j := gang
		j.Gang, j.SameRack = false, true
		Expect(j.Validate()).To(MatchError("workload: gang-a: evict-whole and same-rack apply to gangs only"))
	})
})
```

`pkg/scheduler/vanilla/vanilla_test.go`:

```go
package vanilla_test

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/claims"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler/vanilla"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

func TestVanilla(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "vanilla") {
		t.Fatal("vanilla specs failed")
	}
}

var victim = workload.Job{Name: "victim", Namespace: "scs-preemption-t1", Probe: "preemption", Tenant: "t1",
	Priority: workload.Low, Pods: 4, GPUsPerPod: 8, RunFor: 300 * time.Second, Gang: true, EvictWhole: true}

var _ = Describe("PodGroup", func() {
	It("makes an evict-whole gang a PodGroup with minCount, priority and disruption mode all", func() {
		Expect(vanilla.PodGroup(victim)).To(Equal(map[string]any{
			"apiVersion": "scheduling.k8s.io/v1beta1",
			"kind":       "PodGroup",
			"metadata": map[string]any{"name": "pg-victim", "namespace": "scs-preemption-t1", "labels": map[string]any{
				"app.kubernetes.io/part-of":            "scheduler-capabilities-suite",
				"scs.arangogutierrez.github.io/probe":  "preemption",
				"scs.arangogutierrez.github.io/tenant": "t1",
				"scs.arangogutierrez.github.io/job":    "victim",
			}},
			"spec": map[string]any{
				"schedulingPolicy":  map[string]any{"gang": map[string]any{"minCount": int64(4)}},
				"priorityClassName": "scs-low",
				"disruptionMode":    map[string]any{"all": map[string]any{}},
			},
		}))
	})

	It("adds a rack topology constraint for a same-rack gang", func() {
		j := workload.Job{Name: "topo-1", Namespace: "scs-topology-t1", Probe: "topology", Tenant: "t1",
			Priority: workload.Low, Pods: 2, GPUsPerPod: 8, RunFor: 20 * time.Second, Gang: true, SameRack: true}
		spec := vanilla.PodGroup(j)["spec"].(map[string]any)
		Expect(spec["schedulingConstraints"]).To(Equal(map[string]any{"topology": []any{map[string]any{"key": "scs.arangogutierrez.github.io/rack"}}}))
		Expect(spec).NotTo(HaveKey("disruptionMode"))
	})
})

var _ = Describe("Job", func() {
	It("names the gang's PodGroup and sets the priority class", func() {
		b := vanilla.Job(victim)
		Expect(b.Spec.Template.Spec.PriorityClassName).To(Equal("scs-low"))
		Expect(*b.Spec.Template.Spec.SchedulingGroup.PodGroupName).To(Equal("pg-victim"))
	})

	It("leaves a job that is not a gang ungrouped, and maps a preempting high job", func() {
		j := workload.Job{Name: "high", Namespace: "n", Probe: "preemption", Tenant: "t1", Priority: workload.High,
			Preempting: true, Pods: 2, GPUsPerPod: 8, RunFor: 30 * time.Second}
		b := vanilla.Job(j)
		Expect(b.Spec.Template.Spec.SchedulingGroup).To(BeNil())
		Expect(b.Spec.Template.Spec.PriorityClassName).To(Equal("scs-high-preempt"))
		j.Preempting = false
		Expect(vanilla.PriorityClassName(j)).To(Equal("scs-high"))
	})
})

var _ = Describe("claims", func() {
	It("ships a claims file that loads, with a citation for every capability", func() {
		f, err := claims.Load("../../../claims/vanilla.yaml")
		Expect(err).NotTo(HaveOccurred())
		Expect(f.Scheduler).To(Equal("vanilla"))
		Expect(f.Claims["preemption"].Expected).To(Equal(verdict.Supported))
	})
})
```

- [ ] **Step 2: Run and watch it fail**

Run: `cd "$W" && go test ./pkg/workload/ ./pkg/scheduler/...`
Expected: FAIL, build failures naming `undefined: workload.Job` and the missing package `pkg/scheduler/vanilla`.

- [ ] **Step 3: Commit the failing specs**

```bash
git -C "$W" add pkg/workload/workload_test.go pkg/scheduler/vanilla/vanilla_test.go
git -C "$W" commit -s -S -m "test(scheduler): specify the neutral job and the vanilla profile's objects"
```

- [ ] **Step 4: Implement, and write the vanilla claims**

`pkg/workload/workload.go`:

```go
// Package workload describes the suite's jobs without naming a scheduler,
// and builds the batch/v1 Job every profile starts from.
package workload

import (
	"errors"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/fleet"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/labels"
)

// Priority is a job's priority level; profiles map it to their own objects.
type Priority string

// The two priority levels.
const (
	Low  Priority = "low"
	High Priority = "high"
)

// FakeImage is never pulled: kwok nodes do not run containers.
const FakeImage = "registry.invalid/scs/fake:0"

// BackoffLimit lets a Job survive evictions. A preempted pod counts as a Job
// failure; with a limit of 0 the Job controller fails the Job on the first
// eviction and deletes the surviving pods, which would make any scheduler
// look as if it evicted the whole workload (R1, V4).
const BackoffLimit = 10

// Job is one batch Job, described without naming a scheduler.
type Job struct {
	Name      string
	Namespace string
	Probe     string
	Tenant    string
	Priority  Priority
	// Preempting lets a High job evict lower-priority work.
	Preempting bool
	Pods       int
	GPUsPerPod int
	RunFor     time.Duration
	// Gang: all pods are placed together or not at all.
	Gang bool
	// EvictWhole: if preempted, the gang is evicted as a whole.
	EvictWhole bool
	// SameRack: all pods sit in one rack.
	SameRack bool
	// NodeName pins every pod to one node.
	NodeName string
}

// Validate returns the first inconsistency in j.
func (j Job) Validate() error {
	switch {
	case j.Name == "" || j.Namespace == "" || j.Probe == "" || j.Tenant == "":
		return errors.New("workload: name, namespace, probe and tenant must all be set")
	case j.Priority != Low && j.Priority != High:
		return fmt.Errorf("workload: %s: priority %q, want low or high", j.Name, j.Priority)
	case j.Pods < 1 || j.GPUsPerPod < 0 || j.RunFor <= 0:
		return fmt.Errorf("workload: %s: pods must be 1 or more, gpus not negative, run time positive", j.Name)
	case j.Preempting && j.Priority != High:
		return fmt.Errorf("workload: %s: only a high-priority job may preempt", j.Name)
	case (j.EvictWhole || j.SameRack) && !j.Gang:
		return fmt.Errorf("workload: %s: evict-whole and same-rack apply to gangs only", j.Name)
	}
	return nil
}

// Labels are the suite's labels for j's Job and pods.
func (j Job) Labels() map[string]string {
	return map[string]string{
		labels.PartOf: labels.PartOfValue,
		labels.Probe:  j.Probe,
		labels.Tenant: j.Tenant,
		labels.Job:    j.Name,
	}
}

// BatchJob builds the Job every profile starts from: parallelism and
// completions equal to Pods, pods that tolerate and select the kwok fleet,
// and kwok stage annotations that make each pod start at once and run
// exactly RunFor. kwok's jitter-delay must equal the delay: kwok draws the
// delay uniformly from [delay, jitter), and a jitter at or below the delay
// replaces it (R1, V1).
func (j Job) BatchJob() *batchv1.Job {
	n := int32(j.Pods)
	backoff := int32(BackoffLimit)
	grace := int64(0)
	run := j.RunFor.String()
	sel := map[string]string{"type": "kwok"}
	if j.NodeName != "" {
		sel["kubernetes.io/hostname"] = j.NodeName
	}
	limits := corev1.ResourceList{}
	if j.GPUsPerPod > 0 {
		limits[fleet.GPUResource] = resource.MustParse(fmt.Sprint(j.GPUsPerPod))
	}
	return &batchv1.Job{
		TypeMeta:   metav1.TypeMeta{APIVersion: "batch/v1", Kind: "Job"},
		ObjectMeta: metav1.ObjectMeta{Name: j.Name, Namespace: j.Namespace, Labels: j.Labels()},
		Spec: batchv1.JobSpec{
			Parallelism:  &n,
			Completions:  &n,
			BackoffLimit: &backoff,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: j.Labels(),
					Annotations: map[string]string{
						"pod-create.stage.kwok.x-k8s.io/delay":          "0s",
						"pod-create.stage.kwok.x-k8s.io/jitter-delay":   "0s",
						"pod-ready.stage.kwok.x-k8s.io/delay":           "0s",
						"pod-ready.stage.kwok.x-k8s.io/jitter-delay":    "0s",
						"pod-complete.stage.kwok.x-k8s.io/delay":        run,
						"pod-complete.stage.kwok.x-k8s.io/jitter-delay": run,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy:                 corev1.RestartPolicyNever,
					TerminationGracePeriodSeconds: &grace,
					NodeSelector:                  sel,
					Tolerations: []corev1.Toleration{{
						Key: "kwok.x-k8s.io/node", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule,
					}},
					Containers: []corev1.Container{{
						Name: "main", Image: FakeImage,
						Resources: corev1.ResourceRequirements{Limits: limits},
					}},
				},
			},
		},
	}
}
```

`pkg/scheduler/scheduler.go`:

```go
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
```

`pkg/scheduler/vanilla/vanilla.go`:

```go
// Package vanilla is the baseline profile: kube-scheduler 1.37 with the
// Workload API. Gangs are standalone PodGroups (scheduling.k8s.io/v1beta1)
// named from the pod template's schedulingGroup; priority is a
// PriorityClass; tenants are namespaces with no policy (R1, V2 to V6).
package vanilla

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/cluster"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/labels"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

// PriorityClasses the profile installs. scs-high may not preempt (the
// priority probe); scs-high-preempt may (the preemption probe).
const priorityClasses = `apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata: {name: scs-low, labels: {app.kubernetes.io/part-of: scheduler-capabilities-suite}}
value: 100
preemptionPolicy: Never
---
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata: {name: scs-high, labels: {app.kubernetes.io/part-of: scheduler-capabilities-suite}}
value: 1000
preemptionPolicy: Never
---
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata: {name: scs-high-preempt, labels: {app.kubernetes.io/part-of: scheduler-capabilities-suite}}
value: 1000
preemptionPolicy: PreemptLowerPriority
`

var podGroupGVR = schema.GroupVersionResource{Group: "scheduling.k8s.io", Version: "v1beta1", Resource: "podgroups"}

// Profile is the vanilla profile.
type Profile struct{}

// New returns the vanilla profile.
func New() *Profile { return &Profile{} }

// Name implements scheduler.Profile.
func (*Profile) Name() string { return "vanilla" }

// ClaimsFile implements scheduler.Profile.
func (*Profile) ClaimsFile() string { return "claims/vanilla.yaml" }

// WorkloadGVR implements scheduler.Profile: vanilla has no Workload to watch.
func (*Profile) WorkloadGVR() *schema.GroupVersionResource { return nil }

// Install creates the PriorityClasses.
func (*Profile) Install(ctx context.Context, c *cluster.Cluster) (map[string]string, error) {
	if _, err := c.Applier.Apply(ctx, []byte(priorityClasses), time.Minute); err != nil {
		return nil, err
	}
	return map[string]string{
		"scheduler":        "kube-scheduler from " + cluster.NodeImage,
		"priority_classes": "scs-low=100 Never, scs-high=1000 Never, scs-high-preempt=1000 PreemptLowerPriority",
		"gangs":            "standalone PodGroup scheduling.k8s.io/v1beta1, pod spec.schedulingGroup",
	}, nil
}

// Prepare creates the probe's namespaces; vanilla has no per-probe objects.
func (*Profile) Prepare(ctx context.Context, c *cluster.Cluster, s scheduler.Scenario) error {
	return scheduler.CreateNamespaces(ctx, c.Clientset, s)
}

// Cleanup implements scheduler.Profile; there is nothing per probe to remove.
func (*Profile) Cleanup(context.Context, *cluster.Cluster, scheduler.Scenario) error { return nil }

// PriorityClassName maps a job's priority to the installed class.
func PriorityClassName(j workload.Job) string {
	switch {
	case j.Priority == workload.High && j.Preempting:
		return "scs-high-preempt"
	case j.Priority == workload.High:
		return "scs-high"
	}
	return "scs-low"
}

// PodGroupName is the PodGroup a gang job's pods name.
func PodGroupName(j workload.Job) string { return "pg-" + j.Name }

// PodGroup is the gang object for j: minCount equal to its pods, the job's
// priority class, disruption mode all when it must be evicted whole, and a
// rack topology constraint when it must sit in one rack.
func PodGroup(j workload.Job) map[string]any {
	spec := map[string]any{
		"schedulingPolicy":  map[string]any{"gang": map[string]any{"minCount": int64(j.Pods)}},
		"priorityClassName": PriorityClassName(j),
	}
	if j.EvictWhole {
		spec["disruptionMode"] = map[string]any{"all": map[string]any{}}
	}
	if j.SameRack {
		spec["schedulingConstraints"] = map[string]any{"topology": []any{map[string]any{"key": labels.Rack}}}
	}
	lbl := map[string]any{}
	for k, v := range j.Labels() {
		lbl[k] = v
	}
	return map[string]any{
		"apiVersion": "scheduling.k8s.io/v1beta1",
		"kind":       "PodGroup",
		"metadata":   map[string]any{"name": PodGroupName(j), "namespace": j.Namespace, "labels": lbl},
		"spec":       spec,
	}
}

// Job is j's batch Job with the priority class and, for a gang, the
// schedulingGroup that names its PodGroup.
func Job(j workload.Job) *batchv1.Job {
	b := j.BatchJob()
	b.Spec.Template.Spec.PriorityClassName = PriorityClassName(j)
	if j.Gang {
		b.Spec.Template.Spec.SchedulingGroup = &corev1.PodSchedulingGroup{PodGroupName: ptr(PodGroupName(j))}
	}
	return b
}

func ptr[T any](v T) *T { return &v }

// Submit creates the PodGroup first (the scheduler holds a gang's pods until
// it exists), then the Job.
func (*Profile) Submit(ctx context.Context, c *cluster.Cluster, j workload.Job) error {
	if err := j.Validate(); err != nil {
		return err
	}
	if j.Gang {
		data, err := json.Marshal(PodGroup(j))
		if err != nil {
			return err
		}
		if _, err := c.Applier.Apply(ctx, data, time.Minute); err != nil {
			return fmt.Errorf("podgroup for %s: %w", j.Name, err)
		}
	}
	_, err := c.Clientset.BatchV1().Jobs(j.Namespace).Create(ctx, Job(j), metav1.CreateOptions{})
	return err
}

// Remove deletes the Job (and its pods) and the PodGroup.
func (*Profile) Remove(ctx context.Context, c *cluster.Cluster, j workload.Job) error {
	bg := metav1.DeletePropagationBackground
	if err := c.Clientset.BatchV1().Jobs(j.Namespace).Delete(ctx, j.Name, metav1.DeleteOptions{PropagationPolicy: &bg}); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if j.Gang {
		if err := c.Dynamic.Resource(podGroupGVR).Namespace(j.Namespace).Delete(ctx, PodGroupName(j), metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
```

`claims/vanilla.yaml` (every quote was fetched from its URL on 2026-10-10 and checked to be on the page; R1, K3):

```yaml
# Documented claims for the vanilla profile: kube-scheduler 1.37 with the
# Workload API. Each claim is quoted from the page cited (fetched and
# checked on 2026-10-10, R1 K3), or states that no mechanism is documented.
# expected is the outcome the documentation implies under the suite's rule.
scheduler: vanilla
version: Kubernetes v1.37.0 (kindest/node v1.37.0)
claims:
  gang:
    claim: 'Gang scheduling ensures that a group of Pods are scheduled on an "all-or-nothing" basis.'
    citation: https://kubernetes.io/docs/concepts/scheduling-eviction/gang-scheduling/
    expected: supported
    mechanism: standalone PodGroup (scheduling.k8s.io/v1beta1, gang minCount = pods) named by the pods' spec.schedulingGroup; GenericWorkload on (Beta since 1.37, off by default)
  priority:
    claim: 'Pods with preemptionPolicy: Never will be placed in the scheduling queue ahead of lower-priority pods, but they cannot preempt other pods.'
    citation: https://kubernetes.io/docs/concepts/scheduling-eviction/pod-priority-preemption/
    expected: supported
    mechanism: PriorityClass scs-high (1000, preemptionPolicy Never) over scs-low (100)
  preemption:
    claim: 'It instructs the scheduler that all pods from the PodGroup have to be disrupted together.'
    citation: https://kubernetes.io/docs/concepts/workloads/workload-api/disruption-and-priority/
    expected: supported
    mechanism: victim gang PodGroup with disruptionMode all; high-priority job PriorityClass scs-high-preempt (PreemptLowerPriority); workload-aware preemption is part of GenericWorkload in 1.37
  fair-share:
    claim: 'No tenant fair-share mechanism is documented for kube-scheduler 1.37. The nearest page treats fair share as a quota cap: "there is a concern that one team could use more than its fair share of resources."'
    citation: https://kubernetes.io/docs/concepts/policy/resource-quotas/
    expected: not_supported
    mechanism: tenants are namespaces with no quota and no policy
  backfill:
    claim: 'No backfill or reservation mechanism is documented for kube-scheduler 1.37 (the scheduling and workload pages were searched on 2026-10-10).'
    citation: https://kubernetes.io/docs/concepts/scheduling-eviction/
    expected: not_supported
    mechanism: plain Jobs; the large job is a PodGroup gang
  topology:
    claim: 'allows finding the optimal placement for the considered PodGroup, guaranteeing that all pods will be collocated within the same topology domain.'
    citation: https://kubernetes.io/docs/concepts/scheduling-eviction/topology-aware-scheduling/
    expected: supported
    mechanism: PodGroup schedulingConstraints.topology with the rack label as key; TopologyAwareWorkloadScheduling on (Alpha since 1.36, off by default)
```

- [ ] **Step 5: Run the specs**

Run: `cd "$W" && make test && make lint`
Expected: every suite `SUCCESS!`, including `workload - 6/6 specs` and `vanilla - 5/5 specs`; lint exits 0.

- [ ] **Step 6: Commit**

```bash
git -C "$W" add pkg/workload/workload.go pkg/scheduler/scheduler.go pkg/scheduler/vanilla/vanilla.go claims/vanilla.yaml
git -C "$W" commit -s -S -m "feat(scheduler): add the vanilla profile and its documented claims" -m "Vanilla expresses each capability with what Kubernetes 1.37 documents: PodGroups for gangs, disruption mode all for whole-workload eviction, a PodGroup topology constraint for one-rack placement, PriorityClasses for priority."
```

---

### Task 11: The e2e harness and the gang probes, first live run

**Files:**
- Create: `test/e2e/suite_test.go`, `test/e2e/harness_test.go`, `test/e2e/gang_test.go`, `hack/e2e.sh`
- Modify: `Makefile` (replace with the version below: adds `e2e` and `report`)

**Interfaces:**
- Consumes: everything above.
- Produces: `register(order int, specs func()) bool` (each probe file calls it once); `startProbe(ctx, name string, tenants ...scheduler.Tenant) *probe`; `(*probe).submit`, `remove`, `pods`, `scheduled`, `running`, `succeeded`, `mustAwait`, `await`, `finish(ctx, capability string, params any) verdict.Verdict`; `names(prefix string, n int) []string`; the `profiles` map (Task 12 adds `kueue`). `make e2e SCHEDULER=vanilla|kueue|all FOCUS=<label>`; `make report RUN=<dir> [OUT=<dir>]`.

How the e2e suite proves itself (this is its RED): the harness is new code that can only be exercised against a cluster, and the probes have no unit-level failing state. The proof that the probes discriminate is the gang pair: the gang probe must record `supported` and the gang control `partial` on the same cluster build. If both read the same, the probe does not discriminate and the task is not done.

Run-host rule (spec D7): on Carlos's Mac only single-probe runs (`FOCUS=<one label>`), one cluster at a time. Never `make e2e` without `FOCUS` there.

- [ ] **Step 1: Write the harness, the gang probes, the run script and the Makefile**

`test/e2e/suite_test.go` (Task 12 adds the Kueue profile to `profiles`):

```go
// Package e2e runs the suite's capability probes against one profile on a
// fresh kind cluster with a kwok GPU fleet. Configure it with SCS_PROFILE
// (vanilla or kueue), SCS_RUN_ID and SCS_RESULTS (an absolute directory);
// hack/e2e.sh sets them.
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/cluster"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/fleet"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/kwok"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/recorder"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/report"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler/vanilla"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
)

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "e2e") {
		t.Fatal("e2e specs failed")
	}
}

// profiles maps SCS_PROFILE to a constructor.
var profiles = map[string]func() scheduler.Profile{
	"vanilla": func() scheduler.Profile { return vanilla.New() },
}

// env is the suite's state, set up in BeforeSuite.
var env struct {
	profile     scheduler.Profile
	cluster     *cluster.Cluster
	rec         *recorder.Recorder
	eventsFile  *os.File
	runID       string
	runDir      string
	profileDir  string
	eventsPath  string
	start       time.Time
	offsetStart float64
	installed   map[string]string
	kwokSums    map[string]string
	probes      []verdict.Probe
	verdicts    []verdict.Verdict
	failed      bool
}

var _ = ReportAfterEach(func(r SpecReport) {
	if r.Failed() {
		env.failed = true
	}
})

var _ = BeforeSuite(func(ctx SpecContext) {
	name := os.Getenv("SCS_PROFILE")
	mk, ok := profiles[name]
	Expect(ok).To(BeTrue(), "SCS_PROFILE=%q is not a known profile", name)
	env.runID = os.Getenv("SCS_RUN_ID")
	Expect(env.runID).NotTo(BeEmpty(), "SCS_RUN_ID is empty")
	results := os.Getenv("SCS_RESULTS")
	Expect(filepath.IsAbs(results)).To(BeTrue(), "SCS_RESULTS=%q must be an absolute path", results)
	env.profile = mk()
	env.runDir = filepath.Join(results, env.runID)
	dir, err := report.PrepareProfileDir(env.runDir, name)
	Expect(err).NotTo(HaveOccurred())
	env.profileDir = dir
	env.start = time.Now().UTC()

	By("creating kind cluster scs-" + name)
	env.cluster, err = cluster.Create(cluster.KindProvider(), cluster.SuiteConfig("scs-"+name, filepath.Join(dir, "scs.kubeconfig")))
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(teardownCluster, NodeTimeout(5*time.Minute))

	By("installing kwok and moving kind's DaemonSets off the fleet")
	env.kwokSums, err = kwok.Install(ctx, env.cluster.Applier, env.cluster.Clientset, 3*time.Minute)
	Expect(err).NotTo(HaveOccurred())
	Expect(cluster.KeepSystemPodsOffFleet(ctx, env.cluster.Clientset, 3*time.Minute)).To(Succeed())

	By("creating the fleet: " + fleet.Default().Describe())
	Expect(fleet.Create(ctx, env.cluster.Applier, fleet.Default())).To(Succeed())
	Expect(fleet.WaitReady(ctx, env.cluster.Clientset, fleet.Default(), 2*time.Minute)).To(Succeed())

	By("installing the " + name + " profile")
	env.installed, err = env.profile.Install(ctx, env.cluster)
	Expect(err).NotTo(HaveOccurred())
	env.offsetStart, err = clockOffset(ctx)
	Expect(err).NotTo(HaveOccurred())

	By("starting the recorder")
	env.eventsPath = filepath.Join(dir, report.EventsFile)
	env.eventsFile, err = os.Create(env.eventsPath)
	Expect(err).NotTo(HaveOccurred())
	env.rec = recorder.New(env.cluster.Clientset, env.cluster.Dynamic, env.eventsFile,
		recorder.Options{RunID: env.runID, WorkloadGVR: env.profile.WorkloadGVR()})
	// The watches must outlive BeforeSuite's context; Stop ends them.
	Expect(env.rec.Start(context.Background())).To(Succeed())
}, NodeTimeout(10*time.Minute))

var _ = AfterSuite(func(ctx SpecContext) {
	if env.rec == nil {
		return
	}
	Expect(env.rec.Stop()).To(Succeed())
	Expect(env.eventsFile.Close()).To(Succeed())
	// The manifest comes first: report.Write reads the run ID from it.
	offsetEnd, err := clockOffset(ctx)
	Expect(err).NotTo(HaveOccurred())
	cfg := cluster.SuiteConfig(env.cluster.Name, env.cluster.Kubeconfig)
	fleetJSON, err := json.Marshal(fleet.Default())
	Expect(err).NotTo(HaveOccurred())
	Expect(report.MergeManifest(env.runDir, report.Manifest{
		RunID: env.runID, GitCommit: os.Getenv("SCS_GIT_COMMIT"), GitDirty: os.Getenv("SCS_GIT_DIRTY") == "true",
		Host: hostInfo(), Tools: toolVersions(), Fleet: fleetJSON,
	}, env.profile.Name(), report.ProfileRun{
		Start: env.start, End: time.Now().UTC(), Cluster: env.cluster.Name,
		FeatureGates: cfg.FeatureGates, RuntimeConfig: cfg.RuntimeConfig,
		Images: images(ctx), Manifests: env.kwokSums, Scheduler: env.installed,
		ClockOffsetStartS: env.offsetStart, ClockOffsetEndS: offsetEnd, WatchErrors: len(env.rec.WatchErrors()),
	})).To(Succeed())
	if len(env.probes) == 0 {
		return
	}
	writeJSON(filepath.Join(env.profileDir, report.ProbesFile), env.probes)
	claims, err := os.ReadFile(filepath.Join("..", "..", env.profile.ClaimsFile()))
	Expect(err).NotTo(HaveOccurred())
	Expect(os.WriteFile(filepath.Join(env.profileDir, report.ClaimsFile), claims, 0o644)).To(Succeed())
	Expect(report.Write(env.runDir, env.runDir)).To(Succeed())
	// The run's verdicts and the offline rebuild must agree.
	entries, err := report.ProfileVerdicts(env.profileDir, env.profile.Name())
	Expect(err).NotTo(HaveOccurred())
	Expect(entries).To(HaveLen(len(env.verdicts)))
	for i, e := range entries {
		Expect(e.Observed).To(Equal(env.verdicts[i]), "probe %s", e.Capability)
	}
}, NodeTimeout(5*time.Minute))

// teardownCluster runs after AfterSuite, even when BeforeSuite failed part way.
func teardownCluster(ctx SpecContext) {
	By("exporting cluster logs")
	if err := env.cluster.ExportLogs(filepath.Join(env.profileDir, "cluster-logs")); err != nil {
		GinkgoWriter.Printf("exporting logs failed: %v\n", err)
	}
	keep := os.Getenv("KEEP_CLUSTER") == "1" || (os.Getenv("KEEP_CLUSTER_ON_FAILURE") == "1" && (env.failed || env.rec == nil))
	if keep {
		GinkgoWriter.Printf("keeping cluster %s; kubeconfig %s\n", env.cluster.Name, env.cluster.Kubeconfig)
		return
	}
	Expect(env.cluster.Delete()).To(Succeed())
}

// clockOffset returns the API server's creation timestamp for a new object
// minus the host time just before creating it, in seconds. The server
// stamps whole seconds, so values in (-1, +1) mean the clocks agree to the
// second.
func clockOffset(ctx context.Context) (float64, error) {
	before := time.Now()
	cm, err := env.cluster.Clientset.CoreV1().ConfigMaps("default").Create(ctx,
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{GenerateName: "scs-clock-"}}, metav1.CreateOptions{})
	if err != nil {
		return 0, err
	}
	_ = env.cluster.Clientset.CoreV1().ConfigMaps("default").Delete(ctx, cm.Name, metav1.DeleteOptions{})
	return cm.CreationTimestamp.Sub(before).Seconds(), nil
}

// images maps each running image reference in the system namespaces to its
// digest-qualified image ID.
func images(ctx context.Context) map[string]string {
	out := map[string]string{}
	for _, ns := range []string{"kube-system", "kueue-system"} {
		pods, err := env.cluster.Clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			continue
		}
		for _, p := range pods.Items {
			for _, s := range p.Status.ContainerStatuses {
				out[s.Image] = s.ImageID
			}
		}
	}
	return out
}

func hostInfo() map[string]string {
	h := map[string]string{"os": runtime.GOOS, "arch": runtime.GOARCH, "cpus": fmt.Sprint(runtime.NumCPU())}
	if out, err := exec.Command("docker", "info", "--format", "{{.ServerVersion}}|{{.NCPU}}|{{.MemTotal}}|{{.OperatingSystem}}").Output(); err == nil {
		f := strings.Split(strings.TrimSpace(string(out)), "|")
		if len(f) == 4 {
			h["docker_version"], h["docker_cpus"], h["docker_memory_bytes"], h["docker_os"] = f[0], f[1], f[2], f[3]
		}
	}
	return h
}

func toolVersions() map[string]string {
	t := map[string]string{"go": runtime.Version()}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, d := range bi.Deps {
			switch d.Path {
			case "github.com/onsi/ginkgo/v2", "github.com/onsi/gomega", "sigs.k8s.io/kind", "k8s.io/client-go":
				t[d.Path] = d.Version
			}
		}
	}
	return t
}

func writeJSON(path string, v any) {
	b, err := json.MarshalIndent(v, "", "  ")
	Expect(err).NotTo(HaveOccurred())
	Expect(os.WriteFile(path, append(b, '\n'), 0o644)).To(Succeed())
}

// registry holds each probe file's registration function by its order.
var registry = map[int]func(){}

// register adds a probe file's specs at position order; each probe file
// calls it once from a package-level var.
func register(order int, specs func()) bool {
	if _, dup := registry[order]; dup {
		panic(fmt.Sprintf("probe order %d registered twice", order))
	}
	registry[order] = specs
	return true
}

// The probes run in registration order, each on an empty fleet.
var _ = Describe("probes", Ordered, func() {
	orders := make([]int, 0, len(registry))
	for o := range registry {
		orders = append(orders, o)
	}
	sort.Ints(orders)
	for _, o := range orders {
		registry[o]()
	}
})
```

`test/e2e/harness_test.go`:

```go
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gmeasure"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/events"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/fleet"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/labels"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

// probe is one probe's run: its scenario, start time, and the restart
// counts of the control-plane containers when it started.
type probe struct {
	name     string
	scenario scheduler.Scenario
	t0       time.Time
	restarts map[string]int32
}

// startProbe starts a probe whose scenario is just its tenants.
func startProbe(ctx context.Context, name string, tenants ...scheduler.Tenant) *probe {
	return startScenario(ctx, scheduler.Scenario{Probe: name, Tenants: tenants})
}

// startScenario checks that the fleet is empty, creates the tenants'
// namespaces (scs-<probe>-<tenant>) and per-probe scheduler objects, and
// registers the probe's cleanup. With no tenants, one tenant "t1" is
// entitled to the whole fleet.
func startScenario(ctx context.Context, s scheduler.Scenario) *probe {
	Expect(suitePods(ctx)).To(BeZero(), "probe %s found suite pods left over", s.Probe)
	if len(s.Tenants) == 0 {
		s.Tenants = []scheduler.Tenant{{Name: "t1", EntitlementGPUs: fleet.Default().GPUs()}}
	}
	for i := range s.Tenants {
		s.Tenants[i].Namespace = "scs-" + s.Probe + "-" + s.Tenants[i].Name
	}
	p := &probe{name: s.Probe, scenario: s, t0: time.Now(), restarts: controlPlaneRestarts(ctx)}
	Expect(env.profile.Prepare(ctx, env.cluster, p.scenario)).To(Succeed())
	DeferCleanup(p.cleanup, NodeTimeout(4*time.Minute))
	return p
}

// controlPlaneRestarts maps each system container to its restart count. A
// restart during a probe (R1 saw etcd stalls make the scheduler, the
// controller-manager and Kueue lose leader election) invalidates the
// probe's evidence.
func controlPlaneRestarts(ctx context.Context) map[string]int32 {
	out := map[string]int32{}
	for _, ns := range []string{"kube-system", "kueue-system"} {
		pods, err := env.cluster.Clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
		Expect(err).NotTo(HaveOccurred())
		for _, q := range pods.Items {
			for _, c := range q.Status.ContainerStatuses {
				out[ns+"/"+q.Name+"/"+c.Name] = c.RestartCount
			}
		}
	}
	return out
}

// fill completes a job with the probe's namespace and labels for tenant.
func (p *probe) fill(j workload.Job) workload.Job {
	if j.Tenant == "" {
		j.Tenant = "t1"
	}
	j.Probe = p.name
	j.Namespace = "scs-" + p.name + "-" + j.Tenant
	return j
}

func (p *probe) submit(ctx context.Context, jobs ...workload.Job) {
	for _, j := range jobs {
		Expect(env.profile.Submit(ctx, env.cluster, p.fill(j))).To(Succeed(), "submit %s", j.Name)
	}
}

// remove deletes a job and waits until its pods are gone.
func (p *probe) remove(ctx context.Context, j workload.Job) {
	j = p.fill(j)
	Expect(env.profile.Remove(ctx, env.cluster, j)).To(Succeed())
	Eventually(func() int { return len(p.pods(ctx, j.Name)) }).WithContext(ctx).
		WithTimeout(time.Minute).WithPolling(250*time.Millisecond).Should(BeZero(), "pods of %s still exist", j.Name)
}

// pods returns the pods of one job across the probe's namespaces.
func (p *probe) pods(ctx context.Context, job string) []corev1.Pod {
	var out []corev1.Pod
	for _, t := range p.scenario.Tenants {
		list, err := env.cluster.Clientset.CoreV1().Pods(t.Namespace).List(ctx, metav1.ListOptions{LabelSelector: labels.Job + "=" + job})
		Expect(err).NotTo(HaveOccurred())
		out = append(out, list.Items...)
	}
	return out
}

func (p *probe) count(ctx context.Context, job string, ok func(corev1.Pod) bool) int {
	n := 0
	for _, q := range p.pods(ctx, job) {
		if ok(q) {
			n++
		}
	}
	return n
}

func (p *probe) scheduled(ctx context.Context, job string) int {
	return p.count(ctx, job, func(q corev1.Pod) bool { return q.Spec.NodeName != "" })
}

func (p *probe) running(ctx context.Context, job string) int {
	return p.count(ctx, job, func(q corev1.Pod) bool { return q.Status.Phase == corev1.PodRunning })
}

func (p *probe) succeeded(ctx context.Context, job string) int {
	return p.count(ctx, job, func(q corev1.Pod) bool { return q.Status.Phase == corev1.PodSucceeded })
}

// mustAwait waits for a scenario precondition; missing it is a harness
// failure (the scenario never took shape), not a verdict.
func (p *probe) mustAwait(ctx context.Context, what string, cond func() bool, timeout time.Duration) {
	Eventually(cond).WithContext(ctx).WithTimeout(timeout).WithPolling(250*time.Millisecond).
		Should(BeTrue(), "probe %s: %s", p.name, what)
}

// await waits for an outcome a verdict rule judges; timing out is evidence,
// not a failure.
func (p *probe) await(ctx context.Context, cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(250 * time.Millisecond):
		}
	}
	return cond()
}

// finish closes the probe's evidence window, evaluates its verdict from
// the recorded file (the same path scs-report takes), and attaches the
// verdict and its measurements to the Ginkgo report.
func (p *probe) finish(ctx context.Context, capability string, params any) verdict.Verdict {
	windowEnd := time.Now().UTC().Round(0)
	Expect(env.rec.Flush()).To(Succeed())
	for _, t := range env.rec.WatchErrors() {
		Expect(t.Before(p.t0)).To(BeTrue(), "probe %s: a watch failed at %s, so its evidence may be incomplete", p.name, t)
	}
	Expect(controlPlaneRestarts(ctx)).To(Equal(p.restarts), "probe %s: a control-plane container restarted or was replaced during the probe, so its evidence is invalid", p.name)
	raw, err := json.Marshal(params)
	Expect(err).NotTo(HaveOccurred())
	pr := verdict.Probe{Capability: capability, Params: raw, WindowEnd: windowEnd}
	evs, err := events.ReadFile(env.eventsPath)
	Expect(err).NotTo(HaveOccurred())
	v, err := verdict.Evaluate(evs, pr)
	Expect(err).NotTo(HaveOccurred())
	env.probes = append(env.probes, pr)
	env.verdicts = append(env.verdicts, v)
	AddReportEntry("verdict", v)
	exp := gmeasure.NewExperiment(capability)
	keys := make([]string, 0, len(v.Measurements))
	for k := range v.Measurements {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		exp.RecordValue(k, v.Measurements[k])
	}
	AddReportEntry(exp.Name, exp)
	GinkgoWriter.Printf("%s on %s: %s (%s)\n", capability, env.profile.Name(), v.Outcome, v.Reason)
	return v
}

// cleanup deletes the probe's namespaces and waits until no suite pod is
// left; a fleet that does not drain would contaminate the next probe.
func (p *probe) cleanup(ctx SpecContext) {
	for _, t := range p.scenario.Tenants {
		err := env.cluster.Clientset.CoreV1().Namespaces().Delete(ctx, t.Namespace, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			Fail(fmt.Sprintf("delete namespace %s: %v", t.Namespace, err))
		}
	}
	Eventually(func() int {
		n := 0
		for _, t := range p.scenario.Tenants {
			if _, err := env.cluster.Clientset.CoreV1().Namespaces().Get(ctx, t.Namespace, metav1.GetOptions{}); err == nil {
				n++
			}
		}
		return n + suitePods(ctx)
	}).WithContext(ctx).WithTimeout(3*time.Minute).WithPolling(time.Second).Should(BeZero(), "probe %s did not drain", p.name)
	Expect(env.profile.Cleanup(ctx, env.cluster, p.scenario)).To(Succeed())
}

// suitePods counts the suite's pods in every namespace.
func suitePods(ctx context.Context) int {
	list, err := env.cluster.Clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{LabelSelector: labels.Selector})
	Expect(err).NotTo(HaveOccurred())
	return len(list.Items)
}

// names returns prefix-1 .. prefix-n.
func names(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s-%d", prefix, i+1)
	}
	return out
}
```

`test/e2e/gang_test.go`:

```go
package e2e

import (
	"time"

	. "github.com/onsi/ginkgo/v2"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

// Scenario numbers are the spec's section 5. Waits that end in a verdict use
// await (a timeout is evidence); waits for a scenario to take shape use
// mustAwait (a timeout is a harness failure). A waiting gang can start up
// to about 17 s after capacity frees, because of the scheduler's gang retry
// backoff (R1, V2), so verdict waits leave room for it.

var _ = register(10, gangProbes)

func gangProbes() {
	gang := func(name string, isGang bool) workload.Job {
		return workload.Job{Name: name, Priority: workload.Low, Pods: 3, GPUsPerPod: 8, RunFor: 20 * time.Second, Gang: isGang}
	}
	params := verdict.GangParams{Jobs: []string{"gang-a", "gang-b"}, Size: 3, GraceSec: 5}

	It("gang", Label("gang"), func(ctx SpecContext) {
		p := startProbe(ctx, verdict.CapGang)
		p.submit(ctx, gang("gang-a", true), gang("gang-b", true))
		p.await(ctx, func() bool { return p.succeeded(ctx, "gang-a") >= 3 && p.succeeded(ctx, "gang-b") >= 3 }, 150*time.Second)
		p.finish(ctx, verdict.CapGang, params)
	}, SpecTimeout(8*time.Minute))

	It("gang-control", Label("gang-control"), func(ctx SpecContext) {
		if env.profile.Name() != "vanilla" {
			Skip("the gang control runs on the vanilla profile only")
		}
		p := startProbe(ctx, verdict.CapGangControl)
		p.submit(ctx, gang("gang-a", false), gang("gang-b", false))
		p.await(ctx, func() bool { return p.succeeded(ctx, "gang-a") >= 3 && p.succeeded(ctx, "gang-b") >= 3 }, 150*time.Second)
		p.finish(ctx, verdict.CapGangControl, params)
	}, SpecTimeout(8*time.Minute))
}
```

`hack/e2e.sh`:

```bash
#!/bin/bash
# Run the e2e suite once per profile, one cluster at a time.
#   hack/e2e.sh PROFILES RUN_ID RESULTS [LABEL_FILTER]
# PROFILES is "vanilla", "kueue" or "all". LABEL_FILTER (a Ginkgo label
# expression such as "gang") runs only matching probes.
set -u
profiles=$1 run_id=$2 results=$3 filter=${4:-}
[ "$profiles" = all ] && profiles="vanilla kueue"
mkdir -p "$results" && results=$(cd "$results" && pwd)
commit=$(git rev-parse --short HEAD)
dirty=false
git diff --quiet HEAD -- || dirty=true
rc=0
for p in $profiles; do
  out="$results/$run_id/$p"
  mkdir -p "$out"
  args=(--no-color -v --timeout=60m --json-report=ginkgo-report.json --output-dir="$out")
  [ -n "$filter" ] && args+=(--label-filter="$filter")
  SCS_PROFILE=$p SCS_RUN_ID=$run_id SCS_RESULTS=$results SCS_GIT_COMMIT=$commit SCS_GIT_DIRTY=$dirty \
    go tool ginkgo "${args[@]}" ./test/e2e 2>&1 | tee "$out/ginkgo.log"
  r=${PIPESTATUS[0]}
  if [ "$r" -ne 0 ]; then
    echo "e2e: profile $p failed (rc $r)" >&2
    rc=$r
  fi
done
echo "results: $results/$run_id"
exit "$rc"
```

`Makefile`:

```make
SHELL := /bin/bash
GO ?= go
SCHEDULER ?= all
FOCUS ?=
RESULTS ?= $(CURDIR)/results
RUN_ID ?= $(shell date -u +%Y%m%dT%H%M%SZ)-$(shell git rev-parse --short HEAD 2>/dev/null)$(shell git diff --quiet HEAD -- 2>/dev/null || echo -dirty)

.PHONY: test lint e2e report

## test: every unit suite (no cluster); the e2e suite needs one and is skipped here
test:
	$(GO) tool ginkgo --no-color -r --skip-package=e2e

## lint: go vet and gofmt
lint:
	$(GO) vet ./...
	@out="$$(gofmt -l .)"; if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

## e2e: SCHEDULER=vanilla|kueue|all, FOCUS=<ginkgo label filter> for one probe
e2e:
	bash hack/e2e.sh "$(SCHEDULER)" "$(RUN_ID)" "$(RESULTS)" "$(FOCUS)"

## report: rebuild verdicts and summary of RUN into OUT (default: in place)
report:
	$(GO) run ./cmd/scs-report $(if $(OUT),-out $(OUT)) $(RUN)
```

- [ ] **Step 2: Compile and dry-run**

Run: `cd "$W" && make lint && go tool ginkgo --dry-run --no-color -v ./test/e2e 2>&1 | grep -E '^probes|Will run'`
Expected: `Will run 2 of 2 specs`, then `probes gang [gang]` and `probes gang-control [gang-control]`.

- [ ] **Step 3: Live run, gang probe (single probe, laptop allowed)**

Run (sandbox disabled; about 3 minutes): `cd "$W" && make e2e SCHEDULER=vanilla FOCUS=gang RUN_ID=dev-gang-1`
Expected: rc 0; the log line `gang on vanilla: supported (no gang was partly placed for more than 5 s, and all 2 gangs completed)`; `results/dev-gang-1/vanilla/` holds `events.jsonl`, `probes.json`, `claims.yaml`, `verdicts.json`, `ginkgo-report.json`, `cluster-logs/`; `results/dev-gang-1/summary.md` has the row `| gang | vanilla | supported | supported | yes | ...`; `kind get clusters` no longer lists `scs-vanilla`.

- [ ] **Step 4: Live run, gang control**

Run: `cd "$W" && make e2e SCHEDULER=vanilla FOCUS=gang-control RUN_ID=dev-gang-2`
Expected: rc 0; a log line starting `gang-control on vanilla: partial (gang ` with a partial placement of about 20 s (validation run: `gang-control on vanilla: partial (gang gang-a was partly placed for 20.064 s; all 2 gangs completed)`); the summary's Controls section lists it.

- [ ] **Step 5: The offline rebuild matches the run**

```bash
cd "$W" && make report RUN=results/dev-gang-1 OUT=/tmp/claude/scs-rebuild-dev-gang-1 && \
cmp results/dev-gang-1/summary.md /tmp/claude/scs-rebuild-dev-gang-1/summary.md && \
cmp results/dev-gang-1/vanilla/verdicts.json /tmp/claude/scs-rebuild-dev-gang-1/vanilla/verdicts.json && echo REBUILD-IDENTICAL
```

Expected: `REBUILD-IDENTICAL`.

- [ ] **Step 6: Commit**

```bash
git -C "$W" add test/e2e/suite_test.go test/e2e/harness_test.go test/e2e/gang_test.go hack/e2e.sh Makefile
git -C "$W" commit -s -S -m "feat(e2e): run the gang probe and its control against vanilla 1.37" -m "First live run: the gang probe records supported and the control, the same jobs without a PodGroup, records partial, so the probe discriminates on a real cluster and not only on fixtures."
```

---

### Task 12: The Kueue profile and its claims, gang on Kueue

**Files:**
- Create: `pkg/scheduler/kueue/kueue.go`, `pkg/scheduler/kueue/manifests/manifests.yaml`, `pkg/scheduler/kueue/manifests/SHA256SUMS`, `claims/kueue.yaml`
- Test: `pkg/scheduler/kueue/kueue_test.go`
- Modify: `test/e2e/suite_test.go` (register the profile; the final file is below)

**Interfaces:**
- Consumes: `apply.Decode`, `(*Applier).ApplyObjects`, `scheduler.Scenario` (with `AllowPreemption`), `workload.Job`, `labels.Rack`, `fleet.GPUResource`.
- Produces: `kueue.Version = "v0.20.1"`, `kueue.FairSharing`, `kueue.Manifest() ([]byte, string, error)`, `kueue.WithFairSharing(cfg string) (string, error)`, `kueue.New() *Profile`, `kueue.ClusterQueueName(scheduler.Tenant) string`, `kueue.ClusterQueue(scheduler.Scenario, scheduler.Tenant) map[string]any`, `kueue.LocalQueue(scheduler.Tenant) map[string]any`, `kueue.PriorityClass(workload.Job) string`, `kueue.Job(workload.Job) *batchv1.Job`. The profile's `WorkloadGVR` is `kueue.x-k8s.io/v1beta2, workloads` (never the short name: `scheduling.k8s.io` also serves `workloads`).

Facts this task relies on (R1, VERIFIED-BY-RUN on kind 1.37 with the same gates): the v0.20.1 release manifest (92 objects) applies with the suite's applier unchanged; a cold install's webhook answered after 239 s in R1 and 2 min 56 s in this plan's validation run; fair sharing is on whenever the manager Configuration has a `fairSharing` block; ClusterQueue `namespaceSelector` must be `{}` (null selects no namespace); the cohort is `spec.cohortName`; Kueue's Job webhook suspends a labeled Job until admission, so a queued job has no pods; TAS gates each admitted pod about 1 s.

- [ ] **Step 1: Fetch and pin Kueue's release manifest**

```bash
cd "$W" && mkdir -p pkg/scheduler/kueue/manifests && cd pkg/scheduler/kueue/manifests && \
curl -sSfL -o manifests.yaml https://github.com/kubernetes-sigs/kueue/releases/download/v0.20.1/manifests.yaml && \
shasum -a 256 manifests.yaml > SHA256SUMS && cat SHA256SUMS && wc -c < manifests.yaml
```

Expected, exactly:

```text
c48514295365aae0c7dc578c70330befdd0015fec57cb4a06ba872bbef8750dd  manifests.yaml
```

and `1798228` bytes. If either differs, stop and report.

- [ ] **Step 2: Write the failing specs**

`pkg/scheduler/kueue/kueue_test.go`:

```go
package kueue_test

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/claims"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler/kueue"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

func TestKueue(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "kueue") {
		t.Fatal("kueue specs failed")
	}
}

var _ = Describe("Manifest", func() {
	It("serves Kueue v0.20.1's release manifest with its pinned sha256", func() {
		data, sum, err := kueue.Manifest()
		Expect(err).NotTo(HaveOccurred())
		Expect(sum).To(Equal("c48514295365aae0c7dc578c70330befdd0015fec57cb4a06ba872bbef8750dd"))
		Expect(len(data)).To(Equal(1798228))
	})
})

var _ = Describe("WithFairSharing", func() {
	It("appends the fairSharing block to a configuration that only mentions it in a comment", func() {
		cfg := "apiVersion: config.kueue.x-k8s.io/v1beta2\nkind: Configuration\n#fairSharing:\n#  preemptionStrategies: []\n"
		got, err := kueue.WithFairSharing(cfg)
		Expect(err).NotTo(HaveOccurred())
		Expect(got).To(Equal(cfg + "fairSharing:\n  preemptionStrategies: [LessThanOrEqualToFinalShare, LessThanInitialShare]\n"))
	})
	It("refuses a configuration that already sets fairSharing", func() {
		_, err := kueue.WithFairSharing("kind: Configuration\nfairSharing:\n  preemptionStrategies: []\n")
		Expect(err).To(MatchError("kueue: the manager configuration already sets fairSharing"))
	})
})

var _ = Describe("ClusterQueue", func() {
	a := scheduler.Tenant{Name: "a", Namespace: "scs-fair-share-a", EntitlementGPUs: 16}
	b := scheduler.Tenant{Name: "b", Namespace: "scs-fair-share-b", EntitlementGPUs: 16}

	It("gives each fair-share tenant its entitlement in a cohort that reclaims lent quota", func() {
		s := scheduler.Scenario{Probe: "fair-share", Tenants: []scheduler.Tenant{a, b}, AllowPreemption: true}
		cq := kueue.ClusterQueue(s, a)
		Expect(cq["metadata"].(map[string]any)["name"]).To(Equal("cq-scs-fair-share-a"))
		spec := cq["spec"].(map[string]any)
		Expect(spec["namespaceSelector"]).To(Equal(map[string]any{}))
		Expect(spec["cohortName"]).To(Equal("scs-fair-share"))
		Expect(spec["preemption"]).To(Equal(map[string]any{"withinClusterQueue": "LowerPriority", "reclaimWithinCohort": "Any"}))
		Expect(spec["resourceGroups"]).To(Equal([]any{map[string]any{
			"coveredResources": []any{"nvidia.com/gpu"},
			"flavors": []any{map[string]any{"name": "gpu", "resources": []any{
				map[string]any{"name": "nvidia.com/gpu", "nominalQuota": int64(16)},
			}}},
		}}))
	})

	It("leaves a single-tenant probe without a cohort, and without preemption unless asked", func() {
		t1 := scheduler.Tenant{Name: "t1", Namespace: "scs-priority-t1", EntitlementGPUs: 32}
		spec := kueue.ClusterQueue(scheduler.Scenario{Probe: "priority", Tenants: []scheduler.Tenant{t1}}, t1)["spec"].(map[string]any)
		Expect(spec).NotTo(HaveKey("cohortName"))
		Expect(spec).NotTo(HaveKey("preemption"))
		spec = kueue.ClusterQueue(scheduler.Scenario{Probe: "preemption", Tenants: []scheduler.Tenant{t1}, AllowPreemption: true}, t1)["spec"].(map[string]any)
		Expect(spec["preemption"]).To(Equal(map[string]any{"withinClusterQueue": "LowerPriority"}))
	})
})

var _ = Describe("Job", func() {
	It("routes the Job to the tenant's LocalQueue with its priority class", func() {
		j := workload.Job{Name: "high", Namespace: "scs-priority-t1", Probe: "priority", Tenant: "t1",
			Priority: workload.High, Pods: 1, GPUsPerPod: 8, RunFor: 30 * time.Second}
		b := kueue.Job(j)
		Expect(b.Labels).To(HaveKeyWithValue("kueue.x-k8s.io/queue-name", "lq"))
		Expect(b.Labels).To(HaveKeyWithValue("kueue.x-k8s.io/priority-class", "scs-high"))
		Expect(b.Spec.Template.Annotations).NotTo(HaveKey("kueue.x-k8s.io/podset-required-topology"))
	})

	It("asks topology-aware scheduling for one rack for a same-rack gang", func() {
		j := workload.Job{Name: "topo-1", Namespace: "scs-topology-t1", Probe: "topology", Tenant: "t1",
			Priority: workload.Low, Pods: 2, GPUsPerPod: 8, RunFor: 20 * time.Second, Gang: true, SameRack: true}
		b := kueue.Job(j)
		Expect(b.Spec.Template.Annotations).To(HaveKeyWithValue("kueue.x-k8s.io/podset-required-topology", "scs.arangogutierrez.github.io/rack"))
		Expect(b.Labels).To(HaveKeyWithValue("kueue.x-k8s.io/priority-class", "scs-low"))
	})
})

var _ = Describe("claims", func() {
	It("ships a claims file that loads, with a citation for every capability", func() {
		f, err := claims.Load("../../../claims/kueue.yaml")
		Expect(err).NotTo(HaveOccurred())
		Expect(f.Scheduler).To(Equal("kueue"))
	})
})
```

- [ ] **Step 3: Run and watch it fail**

Run: `cd "$W" && go test ./pkg/scheduler/kueue/`
Expected: FAIL, build failure naming `undefined: kueue.Manifest` (the package has only its test).

- [ ] **Step 4: Commit the failing specs and the pinned manifest**

```bash
git -C "$W" add pkg/scheduler/kueue/kueue_test.go pkg/scheduler/kueue/manifests
git -C "$W" commit -s -S -m "test(kueue): specify the Kueue profile's objects and pin v0.20.1"
```

- [ ] **Step 5: Implement, write the Kueue claims, register the profile**

`pkg/scheduler/kueue/kueue.go`:

```go
// Package kueue is the Kueue v0.20.1 profile. Kueue admits whole Jobs by
// quota (gangs need nothing extra), orders and preempts by
// WorkloadPriorityClass, shares capacity across a cohort with fair sharing,
// and places a pod set in one rack with topology-aware scheduling (R1, K1
// to K3). kube-scheduler still binds the pods.
package kueue

import (
	"bufio"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/apply"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/cluster"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/fleet"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/labels"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

// Version is the pinned Kueue release.
const Version = "v0.20.1"

//go:embed manifests/manifests.yaml manifests/SHA256SUMS
var files embed.FS

// FairSharing is the block added to Kueue's manager Configuration. Fair
// sharing is on whenever a fairSharing block is present (cmd/kueue/main.go
// at v0.20.1); the strategies are the ones the v0.20 preemption docs show.
const FairSharing = "fairSharing:\n  preemptionStrategies: [LessThanOrEqualToFinalShare, LessThanInitialShare]\n"

var (
	workloadGVR = schema.GroupVersionResource{Group: "kueue.x-k8s.io", Version: "v1beta2", Resource: "workloads"}
	cqGVR       = schema.GroupVersionResource{Group: "kueue.x-k8s.io", Version: "v1beta2", Resource: "clusterqueues"}
	rfGVR       = schema.GroupVersionResource{Group: "kueue.x-k8s.io", Version: "v1beta2", Resource: "resourceflavors"}
)

// base is created once per cluster: the topology (block, rack, host), one
// ResourceFlavor for the kwok fleet, and the two WorkloadPriorityClasses.
const base = `apiVersion: kueue.x-k8s.io/v1beta2
kind: Topology
metadata: {name: scs-topology, labels: {app.kubernetes.io/part-of: scheduler-capabilities-suite}}
spec:
  levels:
  - nodeLabel: scs.arangogutierrez.github.io/block
  - nodeLabel: scs.arangogutierrez.github.io/rack
  - nodeLabel: kubernetes.io/hostname
---
apiVersion: kueue.x-k8s.io/v1beta2
kind: ResourceFlavor
metadata: {name: gpu, labels: {app.kubernetes.io/part-of: scheduler-capabilities-suite}}
spec:
  nodeLabels: {type: kwok}
  topologyName: scs-topology
---
apiVersion: kueue.x-k8s.io/v1beta2
kind: WorkloadPriorityClass
metadata: {name: scs-low, labels: {app.kubernetes.io/part-of: scheduler-capabilities-suite}}
value: 100
---
apiVersion: kueue.x-k8s.io/v1beta2
kind: WorkloadPriorityClass
metadata: {name: scs-high, labels: {app.kubernetes.io/part-of: scheduler-capabilities-suite}}
value: 1000
`

// Manifest returns the embedded release manifest and its verified sha256.
func Manifest() ([]byte, string, error) {
	data, err := files.ReadFile("manifests/manifests.yaml")
	if err != nil {
		return nil, "", err
	}
	sums, err := files.ReadFile("manifests/SHA256SUMS")
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(data)
	got := hex.EncodeToString(sum[:])
	sc := bufio.NewScanner(strings.NewReader(string(sums)))
	for sc.Scan() {
		if f := strings.Fields(sc.Text()); len(f) == 2 && f[1] == "manifests.yaml" {
			if f[0] != got {
				return nil, "", fmt.Errorf("kueue: manifests.yaml has sha256 %s, SHA256SUMS says %s", got, f[0])
			}
			return data, got, nil
		}
	}
	return nil, "", fmt.Errorf("kueue: manifests.yaml is not listed in SHA256SUMS")
}

var fairSharingKey = regexp.MustCompile(`(?m)^fairSharing:`)

// WithFairSharing returns Kueue's manager Configuration with FairSharing
// appended, refusing one that already sets fairSharing.
func WithFairSharing(cfg string) (string, error) {
	if fairSharingKey.MatchString(cfg) {
		return "", fmt.Errorf("kueue: the manager configuration already sets fairSharing")
	}
	if !strings.HasSuffix(cfg, "\n") {
		cfg += "\n"
	}
	return cfg + FairSharing, nil
}

// Profile is the Kueue profile.
type Profile struct{}

// New returns the Kueue profile.
func New() *Profile { return &Profile{} }

// Name implements scheduler.Profile.
func (*Profile) Name() string { return "kueue" }

// ClaimsFile implements scheduler.Profile.
func (*Profile) ClaimsFile() string { return "claims/kueue.yaml" }

// WorkloadGVR implements scheduler.Profile. The resource must be named in
// full: scheduling.k8s.io also serves a "workloads" resource (R1, K1).
func (*Profile) WorkloadGVR() *schema.GroupVersionResource { g := workloadGVR; return &g }

// Install applies Kueue with fair sharing in its configuration, waits until
// its webhook answers (a cold install took 239 s in R1), then creates the
// base objects.
func (*Profile) Install(ctx context.Context, c *cluster.Cluster) (map[string]string, error) {
	data, sum, err := Manifest()
	if err != nil {
		return nil, err
	}
	objs, err := apply.Decode(data)
	if err != nil {
		return nil, err
	}
	edited := false
	for _, o := range objs {
		if o.GetKind() != "ConfigMap" || o.GetName() != "kueue-manager-config" {
			continue
		}
		cfg, _, _ := unstructured.NestedString(o.Object, "data", "controller_manager_config.yaml")
		if cfg, err = WithFairSharing(cfg); err != nil {
			return nil, err
		}
		if err := unstructured.SetNestedField(o.Object, cfg, "data", "controller_manager_config.yaml"); err != nil {
			return nil, err
		}
		edited = true
	}
	if !edited {
		return nil, fmt.Errorf("kueue: manifest has no kueue-manager-config ConfigMap")
	}
	if _, err := c.Applier.ApplyObjects(ctx, objs, 2*time.Minute); err != nil {
		return nil, err
	}
	start := time.Now()
	probe := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kueue.x-k8s.io/v1beta2", "kind": "ResourceFlavor",
		"metadata": map[string]any{"name": "scs-webhook-probe"},
	}}
	err = wait.PollUntilContextTimeout(ctx, 2*time.Second, 8*time.Minute, true, func(ctx context.Context) (bool, error) {
		_, err := c.Dynamic.Resource(rfGVR).Create(ctx, probe, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
		return err == nil, nil
	})
	if err != nil {
		return nil, fmt.Errorf("kueue: webhook did not answer: %w", err)
	}
	webhookWait := time.Since(start).Round(time.Second)
	if _, err := c.Applier.Apply(ctx, []byte(base), time.Minute); err != nil {
		return nil, err
	}
	return map[string]string{
		"kueue_version":           Version,
		"kueue_manifest_sha256":   sum,
		"kueue_config_added":      FairSharing,
		"kueue_webhook_wait":      webhookWait.String(),
		"kueue_queueing_strategy": "BestEffortFIFO",
		"scheduler":               "kube-scheduler from " + cluster.NodeImage + ", behind Kueue admission",
	}, nil
}

// ClusterQueueName is the ClusterQueue of one tenant of a probe.
func ClusterQueueName(t scheduler.Tenant) string { return "cq-" + t.Namespace }

// ClusterQueue is tenant t's ClusterQueue: its entitlement as nominal quota
// of the fleet's GPUs, BestEffortFIFO; with AllowPreemption it preempts
// lower priority work, and with several tenants the queues share a cohort
// and reclaim lent quota.
func ClusterQueue(s scheduler.Scenario, t scheduler.Tenant) map[string]any {
	spec := map[string]any{
		"namespaceSelector": map[string]any{},
		"queueingStrategy":  "BestEffortFIFO",
		"resourceGroups": []any{map[string]any{
			"coveredResources": []any{fleet.GPUResource},
			"flavors": []any{map[string]any{"name": "gpu", "resources": []any{
				map[string]any{"name": fleet.GPUResource, "nominalQuota": int64(t.EntitlementGPUs)},
			}}},
		}},
	}
	if s.AllowPreemption {
		pre := map[string]any{"withinClusterQueue": "LowerPriority"}
		if len(s.Tenants) > 1 {
			pre["reclaimWithinCohort"] = "Any"
		}
		spec["preemption"] = pre
	}
	if len(s.Tenants) > 1 {
		spec["cohortName"] = "scs-" + s.Probe
	}
	return map[string]any{
		"apiVersion": "kueue.x-k8s.io/v1beta2",
		"kind":       "ClusterQueue",
		"metadata":   map[string]any{"name": ClusterQueueName(t), "labels": map[string]any{labels.PartOf: labels.PartOfValue, labels.Probe: s.Probe}},
		"spec":       spec,
	}
}

// LocalQueue is the queue "lq" in tenant t's namespace.
func LocalQueue(t scheduler.Tenant) map[string]any {
	return map[string]any{
		"apiVersion": "kueue.x-k8s.io/v1beta2",
		"kind":       "LocalQueue",
		"metadata":   map[string]any{"name": "lq", "namespace": t.Namespace},
		"spec":       map[string]any{"clusterQueue": ClusterQueueName(t)},
	}
}

// Prepare creates the probe's namespaces, and each tenant's ClusterQueue and LocalQueue.
func (*Profile) Prepare(ctx context.Context, c *cluster.Cluster, s scheduler.Scenario) error {
	if err := scheduler.CreateNamespaces(ctx, c.Clientset, s); err != nil {
		return err
	}
	for _, t := range s.Tenants {
		for _, obj := range []map[string]any{ClusterQueue(s, t), LocalQueue(t)} {
			data, err := json.Marshal(obj)
			if err != nil {
				return err
			}
			if _, err := c.Applier.Apply(ctx, data, time.Minute); err != nil {
				return err
			}
		}
	}
	return nil
}

// Cleanup deletes the probe's ClusterQueues and waits until they are gone
// (Kueue holds one with a finalizer while it still has workloads).
func (*Profile) Cleanup(ctx context.Context, c *cluster.Cluster, s scheduler.Scenario) error {
	for _, t := range s.Tenants {
		if err := c.Dynamic.Resource(cqGVR).Delete(ctx, ClusterQueueName(t), metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return wait.PollUntilContextTimeout(ctx, time.Second, 2*time.Minute, true, func(ctx context.Context) (bool, error) {
		for _, t := range s.Tenants {
			if _, err := c.Dynamic.Resource(cqGVR).Get(ctx, ClusterQueueName(t), metav1.GetOptions{}); err == nil {
				return false, nil
			}
		}
		return true, nil
	})
}

// PriorityClass maps a job's priority to the installed WorkloadPriorityClass.
// Whether a high-priority job may preempt is the ClusterQueue's policy
// (Scenario.AllowPreemption), not the job's.
func PriorityClass(j workload.Job) string {
	if j.Priority == workload.High {
		return "scs-high"
	}
	return "scs-low"
}

// Job is j's batch Job routed to the tenant's LocalQueue with its priority
// class; a same-rack gang asks Kueue's topology-aware scheduling for one
// rack. Kueue's own Job webhook suspends the Job until it is admitted.
func Job(j workload.Job) *batchv1.Job {
	b := j.BatchJob()
	b.Labels["kueue.x-k8s.io/queue-name"] = "lq"
	b.Labels["kueue.x-k8s.io/priority-class"] = PriorityClass(j)
	if j.SameRack {
		b.Spec.Template.Annotations["kueue.x-k8s.io/podset-required-topology"] = labels.Rack
	}
	return b
}

// Submit creates the Job.
func (*Profile) Submit(ctx context.Context, c *cluster.Cluster, j workload.Job) error {
	if err := j.Validate(); err != nil {
		return err
	}
	_, err := c.Clientset.BatchV1().Jobs(j.Namespace).Create(ctx, Job(j), metav1.CreateOptions{})
	return err
}

// Remove deletes the Job and its pods; Kueue removes the Workload.
func (*Profile) Remove(ctx context.Context, c *cluster.Cluster, j workload.Job) error {
	bg := metav1.DeletePropagationBackground
	err := c.Clientset.BatchV1().Jobs(j.Namespace).Delete(ctx, j.Name, metav1.DeleteOptions{PropagationPolicy: &bg})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}
```

`claims/kueue.yaml` (every quote was checked on its page on 2026-10-10; R1, K3):

```yaml
# Documented claims for the Kueue v0.20.1 profile. Each claim is quoted from
# the page cited (fetched and checked on 2026-10-10, R1 K3). expected is the
# outcome the documentation implies under the suite's rule.
scheduler: kueue
version: Kueue v0.20.1 on Kubernetes v1.37.0
claims:
  gang:
    claim: 'a Workload is only unsuspended when the quota for all of its pod sets can be reserved at once.'
    citation: https://kueue.sigs.k8s.io/v0.20/docs/concepts/all_or_nothing/
    expected: supported
    mechanism: a Job is admitted only when quota for all its pods is reserved; nothing gang-specific is set on the Job
  priority:
    claim: 'StrictFIFO: Workloads are ordered first by priority and then by .metadata.creationTimestamp. BestEffortFIFO: Workloads are ordered the same way as StrictFIFO.'
    citation: https://kueue.sigs.k8s.io/v0.20/docs/concepts/cluster_queue/
    expected: supported
    mechanism: WorkloadPriorityClass scs-high (1000) over scs-low (100) through the kueue.x-k8s.io/priority-class label; BestEffortFIFO ClusterQueue without preemption
  preemption:
    claim: 'The preemptee belongs to the same ClusterQueue as the preemptor and the preemptee has a lower priority.'
    citation: https://kueue.sigs.k8s.io/v0.20/docs/concepts/preemption/
    expected: supported
    mechanism: ClusterQueue preemption.withinClusterQueue LowerPriority; Kueue evicts a Workload by suspending its Job
  fair-share:
    claim: 'Fair Sharing uses preemptions to achieve an equal or weighted share of the borrowable resources between the tenants of a cohort.'
    citation: https://kueue.sigs.k8s.io/v0.20/docs/concepts/preemption/
    expected: supported
    mechanism: one 16-GPU ClusterQueue per tenant in one cohort, reclaimWithinCohort Any, fairSharing in the manager configuration
  backfill:
    claim: 'older Workloads that can’t be admitted will not block newer Workloads that fit in the available quota.'
    citation: https://kueue.sigs.k8s.io/v0.20/docs/concepts/cluster_queue/
    expected: partial
    mechanism: BestEffortFIFO ClusterQueue; no documented reservation for a waiting Workload, so short jobs may pass it and delay it
  topology:
    claim: 'requires scheduling all pods on nodes within the same topology domain corresponding to the topology level indicated by the annotation value'
    citation: https://kueue.sigs.k8s.io/v0.20/docs/concepts/topology_aware_scheduling/
    expected: supported
    mechanism: pod template annotation kueue.x-k8s.io/podset-required-topology with the rack label; Topology block, rack, hostname; ResourceFlavor gpu with topologyName
```

`test/e2e/suite_test.go`, final form (adds the `kueue` import and the `"kueue"` entry of `profiles`; nothing else changes):

```go
// Package e2e runs the suite's capability probes against one profile on a
// fresh kind cluster with a kwok GPU fleet. Configure it with SCS_PROFILE
// (vanilla or kueue), SCS_RUN_ID and SCS_RESULTS (an absolute directory);
// hack/e2e.sh sets them.
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/cluster"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/fleet"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/kwok"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/recorder"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/report"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler/kueue"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler/vanilla"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
)

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	if !RunSpecs(t, "e2e") {
		t.Fatal("e2e specs failed")
	}
}

// profiles maps SCS_PROFILE to a constructor.
var profiles = map[string]func() scheduler.Profile{
	"vanilla": func() scheduler.Profile { return vanilla.New() },
	"kueue":   func() scheduler.Profile { return kueue.New() },
}

// env is the suite's state, set up in BeforeSuite.
var env struct {
	profile     scheduler.Profile
	cluster     *cluster.Cluster
	rec         *recorder.Recorder
	eventsFile  *os.File
	runID       string
	runDir      string
	profileDir  string
	eventsPath  string
	start       time.Time
	offsetStart float64
	installed   map[string]string
	kwokSums    map[string]string
	probes      []verdict.Probe
	verdicts    []verdict.Verdict
	failed      bool
}

var _ = ReportAfterEach(func(r SpecReport) {
	if r.Failed() {
		env.failed = true
	}
})

var _ = BeforeSuite(func(ctx SpecContext) {
	name := os.Getenv("SCS_PROFILE")
	mk, ok := profiles[name]
	Expect(ok).To(BeTrue(), "SCS_PROFILE=%q is not a known profile", name)
	env.runID = os.Getenv("SCS_RUN_ID")
	Expect(env.runID).NotTo(BeEmpty(), "SCS_RUN_ID is empty")
	results := os.Getenv("SCS_RESULTS")
	Expect(filepath.IsAbs(results)).To(BeTrue(), "SCS_RESULTS=%q must be an absolute path", results)
	env.profile = mk()
	env.runDir = filepath.Join(results, env.runID)
	dir, err := report.PrepareProfileDir(env.runDir, name)
	Expect(err).NotTo(HaveOccurred())
	env.profileDir = dir
	env.start = time.Now().UTC()

	By("creating kind cluster scs-" + name)
	env.cluster, err = cluster.Create(cluster.KindProvider(), cluster.SuiteConfig("scs-"+name, filepath.Join(dir, "scs.kubeconfig")))
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(teardownCluster, NodeTimeout(5*time.Minute))

	By("installing kwok and moving kind's DaemonSets off the fleet")
	env.kwokSums, err = kwok.Install(ctx, env.cluster.Applier, env.cluster.Clientset, 3*time.Minute)
	Expect(err).NotTo(HaveOccurred())
	Expect(cluster.KeepSystemPodsOffFleet(ctx, env.cluster.Clientset, 3*time.Minute)).To(Succeed())

	By("creating the fleet: " + fleet.Default().Describe())
	Expect(fleet.Create(ctx, env.cluster.Applier, fleet.Default())).To(Succeed())
	Expect(fleet.WaitReady(ctx, env.cluster.Clientset, fleet.Default(), 2*time.Minute)).To(Succeed())

	By("installing the " + name + " profile")
	env.installed, err = env.profile.Install(ctx, env.cluster)
	Expect(err).NotTo(HaveOccurred())
	env.offsetStart, err = clockOffset(ctx)
	Expect(err).NotTo(HaveOccurred())

	By("starting the recorder")
	env.eventsPath = filepath.Join(dir, report.EventsFile)
	env.eventsFile, err = os.Create(env.eventsPath)
	Expect(err).NotTo(HaveOccurred())
	env.rec = recorder.New(env.cluster.Clientset, env.cluster.Dynamic, env.eventsFile,
		recorder.Options{RunID: env.runID, WorkloadGVR: env.profile.WorkloadGVR()})
	// The watches must outlive BeforeSuite's context; Stop ends them.
	Expect(env.rec.Start(context.Background())).To(Succeed())
}, NodeTimeout(10*time.Minute))

var _ = AfterSuite(func(ctx SpecContext) {
	if env.rec == nil {
		return
	}
	Expect(env.rec.Stop()).To(Succeed())
	Expect(env.eventsFile.Close()).To(Succeed())
	// The manifest comes first: report.Write reads the run ID from it.
	offsetEnd, err := clockOffset(ctx)
	Expect(err).NotTo(HaveOccurred())
	cfg := cluster.SuiteConfig(env.cluster.Name, env.cluster.Kubeconfig)
	fleetJSON, err := json.Marshal(fleet.Default())
	Expect(err).NotTo(HaveOccurred())
	Expect(report.MergeManifest(env.runDir, report.Manifest{
		RunID: env.runID, GitCommit: os.Getenv("SCS_GIT_COMMIT"), GitDirty: os.Getenv("SCS_GIT_DIRTY") == "true",
		Host: hostInfo(), Tools: toolVersions(), Fleet: fleetJSON,
	}, env.profile.Name(), report.ProfileRun{
		Start: env.start, End: time.Now().UTC(), Cluster: env.cluster.Name,
		FeatureGates: cfg.FeatureGates, RuntimeConfig: cfg.RuntimeConfig,
		Images: images(ctx), Manifests: env.kwokSums, Scheduler: env.installed,
		ClockOffsetStartS: env.offsetStart, ClockOffsetEndS: offsetEnd, WatchErrors: len(env.rec.WatchErrors()),
	})).To(Succeed())
	if len(env.probes) == 0 {
		return
	}
	writeJSON(filepath.Join(env.profileDir, report.ProbesFile), env.probes)
	claims, err := os.ReadFile(filepath.Join("..", "..", env.profile.ClaimsFile()))
	Expect(err).NotTo(HaveOccurred())
	Expect(os.WriteFile(filepath.Join(env.profileDir, report.ClaimsFile), claims, 0o644)).To(Succeed())
	Expect(report.Write(env.runDir, env.runDir)).To(Succeed())
	// The run's verdicts and the offline rebuild must agree.
	entries, err := report.ProfileVerdicts(env.profileDir, env.profile.Name())
	Expect(err).NotTo(HaveOccurred())
	Expect(entries).To(HaveLen(len(env.verdicts)))
	for i, e := range entries {
		Expect(e.Observed).To(Equal(env.verdicts[i]), "probe %s", e.Capability)
	}
}, NodeTimeout(5*time.Minute))

// teardownCluster runs after AfterSuite, even when BeforeSuite failed part way.
func teardownCluster(ctx SpecContext) {
	By("exporting cluster logs")
	if err := env.cluster.ExportLogs(filepath.Join(env.profileDir, "cluster-logs")); err != nil {
		GinkgoWriter.Printf("exporting logs failed: %v\n", err)
	}
	keep := os.Getenv("KEEP_CLUSTER") == "1" || (os.Getenv("KEEP_CLUSTER_ON_FAILURE") == "1" && (env.failed || env.rec == nil))
	if keep {
		GinkgoWriter.Printf("keeping cluster %s; kubeconfig %s\n", env.cluster.Name, env.cluster.Kubeconfig)
		return
	}
	Expect(env.cluster.Delete()).To(Succeed())
}

// clockOffset returns the API server's creation timestamp for a new object
// minus the host time just before creating it, in seconds. The server
// stamps whole seconds, so values in (-1, +1) mean the clocks agree to the
// second.
func clockOffset(ctx context.Context) (float64, error) {
	before := time.Now()
	cm, err := env.cluster.Clientset.CoreV1().ConfigMaps("default").Create(ctx,
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{GenerateName: "scs-clock-"}}, metav1.CreateOptions{})
	if err != nil {
		return 0, err
	}
	_ = env.cluster.Clientset.CoreV1().ConfigMaps("default").Delete(ctx, cm.Name, metav1.DeleteOptions{})
	return cm.CreationTimestamp.Sub(before).Seconds(), nil
}

// images maps each running image reference in the system namespaces to its
// digest-qualified image ID.
func images(ctx context.Context) map[string]string {
	out := map[string]string{}
	for _, ns := range []string{"kube-system", "kueue-system"} {
		pods, err := env.cluster.Clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			continue
		}
		for _, p := range pods.Items {
			for _, s := range p.Status.ContainerStatuses {
				out[s.Image] = s.ImageID
			}
		}
	}
	return out
}

func hostInfo() map[string]string {
	h := map[string]string{"os": runtime.GOOS, "arch": runtime.GOARCH, "cpus": fmt.Sprint(runtime.NumCPU())}
	if out, err := exec.Command("docker", "info", "--format", "{{.ServerVersion}}|{{.NCPU}}|{{.MemTotal}}|{{.OperatingSystem}}").Output(); err == nil {
		f := strings.Split(strings.TrimSpace(string(out)), "|")
		if len(f) == 4 {
			h["docker_version"], h["docker_cpus"], h["docker_memory_bytes"], h["docker_os"] = f[0], f[1], f[2], f[3]
		}
	}
	return h
}

func toolVersions() map[string]string {
	t := map[string]string{"go": runtime.Version()}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, d := range bi.Deps {
			switch d.Path {
			case "github.com/onsi/ginkgo/v2", "github.com/onsi/gomega", "sigs.k8s.io/kind", "k8s.io/client-go":
				t[d.Path] = d.Version
			}
		}
	}
	return t
}

func writeJSON(path string, v any) {
	b, err := json.MarshalIndent(v, "", "  ")
	Expect(err).NotTo(HaveOccurred())
	Expect(os.WriteFile(path, append(b, '\n'), 0o644)).To(Succeed())
}

// registry holds each probe file's registration function by its order.
var registry = map[int]func(){}

// register adds a probe file's specs at position order; each probe file
// calls it once from a package-level var.
func register(order int, specs func()) bool {
	if _, dup := registry[order]; dup {
		panic(fmt.Sprintf("probe order %d registered twice", order))
	}
	registry[order] = specs
	return true
}

// The probes run in registration order, each on an empty fleet.
var _ = Describe("probes", Ordered, func() {
	orders := make([]int, 0, len(registry))
	for o := range registry {
		orders = append(orders, o)
	}
	sort.Ints(orders)
	for _, o := range orders {
		registry[o]()
	}
})
```

- [ ] **Step 6: Unit specs**

Run: `cd "$W" && make test && make lint`
Expected: every suite `SUCCESS!`, including `kueue - 8/8 specs`; lint exits 0.

- [ ] **Step 7: Live run, gang on Kueue (single probe, laptop allowed; about 7 minutes, most of it Kueue's first webhook wait)**

Run: `cd "$W" && make e2e SCHEDULER=kueue FOCUS=gang RUN_ID=dev-kueue-gang`
Expected: rc 0; the line `gang on kueue: supported (no gang was partly placed for more than 5 s, and all 2 gangs completed)`; `results/dev-kueue-gang/manifest.json` has `profiles.kueue.scheduler.kueue_manifest_sha256` = `c48514295365aae0c7dc578c70330befdd0015fec57cb4a06ba872bbef8750dd` and an image `registry.k8s.io/kueue/kueue@sha256:874ed2a77e2313817ae36570ad831353ae03ab66c1ed96d9392ab84b7be3bde3`; `kueue/events.jsonl` has lines with `"kind":"Workload"`.

- [ ] **Step 8: Commit**

```bash
git -C "$W" add pkg/scheduler/kueue/kueue.go claims/kueue.yaml test/e2e/suite_test.go
git -C "$W" commit -s -S -m "feat(kueue): add the Kueue v0.20.1 profile and its documented claims" -m "Kueue runs in front of the same kube-scheduler and fleet, so the only difference between the profiles is Kueue: quota admission for gangs, WorkloadPriorityClasses, cohort fair sharing and topology-aware scheduling."
```

---

### Task 13: Priority probe

**Files:**
- Create: `test/e2e/priority_test.go`

**Interfaces:**
- Consumes: `register`, `startProbe` or `startScenario`, `(*probe).submit/await/mustAwait/finish`, `names`, the `verdict.{...}Params` of this capability, `workload.Job`.
- Produces: the `priority` spec, labeled `priority`.

Vanilla orders by PriorityClass with `preemptionPolicy: Never`; Kueue by WorkloadPriorityClass in a BestEffortFIFO ClusterQueue without preemption. One run is one observation: the Kubernetes page warns that scheduler back-off can let lower-priority pods go first.

How this probe proves itself: its verdict rule was test-driven in Tasks 2 to 5 against hand-built timelines; the probe file is the scenario, and its test is the live run on both profiles, whose expected verdicts below come from the validation runs this plan was built from. A profile that records a different verdict is a finding to report with its events, not a reason to change the scenario.

- [ ] **Step 1: Write the probe**

`test/e2e/priority_test.go`:

```go
package e2e

import (
	"time"

	. "github.com/onsi/ginkgo/v2"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

var _ = register(20, priorityProbe)

func priorityProbe() {
	It("priority", Label("priority"), func(ctx SpecContext) {
		p := startProbe(ctx, verdict.CapPriority)
		one := func(name string, pr workload.Priority, run time.Duration) workload.Job {
			return workload.Job{Name: name, Priority: pr, Pods: 1, GPUsPerPod: 8, RunFor: run}
		}
		fillers := []workload.Job{one("fill-30", workload.Low, 30*time.Second), one("fill-40", workload.Low, 40*time.Second),
			one("fill-50", workload.Low, 50*time.Second), one("fill-60", workload.Low, 60*time.Second)}
		p.submit(ctx, fillers...)
		p.mustAwait(ctx, "the four fillers run", func() bool {
			for _, f := range fillers {
				if p.running(ctx, f.Name) < 1 {
					return false
				}
			}
			return true
		}, time.Minute)
		queued := names("low", 4)
		for _, n := range queued {
			p.submit(ctx, one(n, workload.Low, 30*time.Second))
		}
		time.Sleep(2 * time.Second) // scenario offset: the high-priority pod arrives 2 s after the queued pods
		p.submit(ctx, one("high", workload.High, 30*time.Second))
		p.await(ctx, func() bool { return p.scheduled(ctx, "high") >= 1 }, 150*time.Second)
		p.finish(ctx, verdict.CapPriority, verdict.PriorityParams{HighJob: "high", QueuedJobs: queued})
	}, SpecTimeout(8*time.Minute))
}
```

- [ ] **Step 2: Dry-run**

Run: `cd "$W" && make lint && go tool ginkgo --dry-run --no-color -v --label-filter=priority ./test/e2e 2>&1 | grep -E '^probes|Will run'`
Expected: `Will run 1 of 3 specs` and `probes priority [priority]`.

- [ ] **Step 3: Live run on vanilla (single probe, laptop allowed)**

Run: `cd "$W" && make e2e SCHEDULER=vanilla FOCUS=priority RUN_ID=dev-priority-vanilla`
Expected: rc 0 and a line starting `priority on vanilla: supported (`. Validation run: `priority on vanilla: supported (the high-priority pod was scheduled before every queued lower-priority pod)`

- [ ] **Step 4: Live run on Kueue (single probe, laptop allowed)**

Run: `cd "$W" && make e2e SCHEDULER=kueue FOCUS=priority RUN_ID=dev-priority-kueue`
Expected: rc 0 and a line starting `priority on kueue: supported (`. Validation run: `priority on kueue: supported (the high-priority pod was scheduled before every queued lower-priority pod)`

- [ ] **Step 5: Commit**

```bash
git -C "$W" add test/e2e/priority_test.go
git -C "$W" commit -s -S -m "feat(e2e): probe priority ordering on both profiles" -m "The high-priority pod arrives behind four queued pods while slots free one at a time, so the first freed slot shows whether priority, not arrival, decides."
```

---

### Task 14: Preemption probe

**Files:**
- Create: `test/e2e/preemption_test.go`

**Interfaces:**
- Consumes: `register`, `startProbe` or `startScenario`, `(*probe).submit/await/mustAwait/finish`, `names`, the `verdict.{...}Params` of this capability, `workload.Job`.
- Produces: the `preemption` spec, labeled `preemption`.

The probe starts with `startScenario(..., AllowPreemption: true)`: Kueue then sets `withinClusterQueue: LowerPriority`. Vanilla's victim gang has `disruptionMode: all` and the high job the `scs-high-preempt` class; every Job has `backoffLimit: 10` so the Job controller cannot fake a whole eviction.

How this probe proves itself: its verdict rule was test-driven in Tasks 2 to 5 against hand-built timelines; the probe file is the scenario, and its test is the live run on both profiles, whose expected verdicts below come from the validation runs this plan was built from. A profile that records a different verdict is a finding to report with its events, not a reason to change the scenario.

- [ ] **Step 1: Write the probe**

`test/e2e/preemption_test.go`:

```go
package e2e

import (
	"time"

	. "github.com/onsi/ginkgo/v2"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

var _ = register(30, preemptionProbe)

func preemptionProbe() {
	It("preemption", Label("preemption"), func(ctx SpecContext) {
		p := startScenario(ctx, scheduler.Scenario{Probe: verdict.CapPreemption, AllowPreemption: true})
		victim := workload.Job{Name: "victim", Priority: workload.Low, Pods: 4, GPUsPerPod: 8, RunFor: 300 * time.Second, Gang: true, EvictWhole: true}
		p.submit(ctx, victim)
		p.mustAwait(ctx, "the victim gang runs on the whole fleet", func() bool { return p.running(ctx, "victim") >= 4 }, 2*time.Minute)
		time.Sleep(2 * time.Second) // scenario offset
		p.submit(ctx, workload.Job{Name: "high", Priority: workload.High, Preempting: true, Pods: 2, GPUsPerPod: 8, RunFor: 30 * time.Second})
		if p.await(ctx, func() bool { return p.scheduled(ctx, "high") >= 2 }, 90*time.Second) {
			// the rule judges the victims 5 s after the high-priority job's last pod is scheduled
			time.Sleep(6 * time.Second)
		}
		p.finish(ctx, verdict.CapPreemption, verdict.PreemptionParams{HighJob: "high", HighPods: 2, VictimJob: "victim", GraceSec: 5})
	}, SpecTimeout(8*time.Minute))
}
```

- [ ] **Step 2: Dry-run**

Run: `cd "$W" && make lint && go tool ginkgo --dry-run --no-color -v --label-filter=preemption ./test/e2e 2>&1 | grep -E '^probes|Will run'`
Expected: `Will run 1 of 4 specs` and `probes preemption [preemption]`.

- [ ] **Step 3: Live run on vanilla (single probe, laptop allowed)**

Run: `cd "$W" && make e2e SCHEDULER=vanilla FOCUS=preemption RUN_ID=dev-preemption-vanilla`
Expected: rc 0 and a line starting `preemption on vanilla: supported (`. Validation run: `preemption on vanilla: supported (no victim pod was left running 5 s after the high-priority job was scheduled)`

- [ ] **Step 4: Live run on Kueue (single probe, laptop allowed)**

Run: `cd "$W" && make e2e SCHEDULER=kueue FOCUS=preemption RUN_ID=dev-preemption-kueue`
Expected: rc 0 and a line starting `preemption on kueue: supported (`. Validation run: `preemption on kueue: supported (no victim pod was left running 5 s after the high-priority job was scheduled)`

- [ ] **Step 5: Commit**

```bash
git -C "$W" add test/e2e/preemption_test.go
git -C "$W" commit -s -S -m "feat(e2e): probe workload-aware preemption on both profiles" -m "A high-priority job needs half the fleet that a low-priority gang holds; the verdict reads whether the whole gang left or part of it kept running."
```

---

### Task 15: Fair-share probe

**Files:**
- Create: `test/e2e/fairshare_test.go`

**Interfaces:**
- Consumes: `register`, `startProbe` or `startScenario`, `(*probe).submit/await/mustAwait/finish`, `names`, the `verdict.{...}Params` of this capability, `workload.Job`.
- Produces: the `fair-share` spec, labeled `fair-share`.

Two tenants entitled to 16 GPUs each; `AllowPreemption: true` gives Kueue a cohort with `reclaimWithinCohort: Any`. Vanilla has no tenant policy (tenants are namespaces). Tenant A submits 8 single-pod Jobs (R1: one 8-pod Job would admit as a unit under Kueue and change the scenario).

How this probe proves itself: its verdict rule was test-driven in Tasks 2 to 5 against hand-built timelines; the probe file is the scenario, and its test is the live run on both profiles, whose expected verdicts below come from the validation runs this plan was built from. A profile that records a different verdict is a finding to report with its events, not a reason to change the scenario.

- [ ] **Step 1: Write the probe**

`test/e2e/fairshare_test.go`:

```go
package e2e

import (
	"time"

	. "github.com/onsi/ginkgo/v2"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/fleet"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/scheduler"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

var _ = register(40, fairShareProbe)

func fairShareProbe() {
	It("fair-share", Label("fair-share"), func(ctx SpecContext) {
		p := startScenario(ctx, scheduler.Scenario{Probe: verdict.CapFairShare, AllowPreemption: true,
			Tenants: []scheduler.Tenant{{Name: "a", EntitlementGPUs: 16}, {Name: "b", EntitlementGPUs: 16}}})
		one := func(name, tenant string) workload.Job {
			return workload.Job{Name: name, Tenant: tenant, Priority: workload.Low, Pods: 1, GPUsPerPod: 8, RunFor: 60 * time.Second}
		}
		for _, n := range names("a", 8) {
			p.submit(ctx, one(n, "a"))
		}
		time.Sleep(10 * time.Second) // scenario offset: B submits 10 s after A
		bSubmit := time.Now()
		for _, n := range names("b", 4) {
			p.submit(ctx, one(n, "b"))
		}
		// the rule reads B's GPUs 65 s after B's first submission; wait 5 s past it
		time.Sleep(time.Until(bSubmit.Add(70 * time.Second)))
		p.finish(ctx, verdict.CapFairShare, verdict.FairShareParams{TenantB: "b", EntitlementGPUs: 16,
			JobLengthSec: 60, ToleranceSec: 5, CapacityGPUs: int64(fleet.Default().GPUs()), WindowSec: 10})
	}, SpecTimeout(8*time.Minute))
}
```

- [ ] **Step 2: Dry-run**

Run: `cd "$W" && make lint && go tool ginkgo --dry-run --no-color -v --label-filter=fair-share ./test/e2e 2>&1 | grep -E '^probes|Will run'`
Expected: `Will run 1 of 5 specs` and `probes fair-share [fair-share]`.

- [ ] **Step 3: Live run on vanilla (single probe, laptop allowed)**

Run: `cd "$W" && make e2e SCHEDULER=vanilla FOCUS=fair-share RUN_ID=dev-fair-share-vanilla`
Expected: rc 0 and a line starting `fair-share on vanilla: not_supported (` or `fair-share on vanilla: partial (`. Both are correct observations: vanilla has no tenant policy, so which tenant's queued pod takes a freed slot is not defined (R1 saw partial, 8 GPUs; validation saw not_supported). Record which one this run gave. Validation run: `fair-share on vanilla: not_supported (tenant b had no GPUs running within 65 s of submitting)`

- [ ] **Step 4: Live run on Kueue (single probe, laptop allowed)**

Run: `cd "$W" && make e2e SCHEDULER=kueue FOCUS=fair-share RUN_ID=dev-fair-share-kueue`
Expected: rc 0 and a line starting `fair-share on kueue: supported (`. Validation run: `fair-share on kueue: supported (tenant b had 16 GPUs running within 65 s of submitting; its entitlement is 16)`

- [ ] **Step 5: Commit**

```bash
git -C "$W" add test/e2e/fairshare_test.go
git -C "$W" commit -s -S -m "feat(e2e): probe fair sharing between two tenants on both profiles" -m "Tenant A fills the fleet first; the verdict reads the most GPUs tenant B held within one job length of arriving, and the fairness index over 10 s windows."
```

---

### Task 16: Backfill probe

**Files:**
- Create: `test/e2e/backfill_test.go`

**Interfaces:**
- Consumes: `register`, `startProbe` or `startScenario`, `(*probe).submit/await/mustAwait/finish`, `names`, the `verdict.{...}Params` of this capability, `workload.Job`.
- Produces: the `backfill` spec, labeled `backfill`.

Neither scheduler documents a reservation for a waiting gang (R1 V6, K2): short jobs may run ahead of it and delay it. Vanilla's gang waits about 15 s more after capacity frees (gang retry backoff, R1 V2); Kueue's hand-offs take about 2 s each.

How this probe proves itself: its verdict rule was test-driven in Tasks 2 to 5 against hand-built timelines; the probe file is the scenario, and its test is the live run on both profiles, whose expected verdicts below come from the validation runs this plan was built from. A profile that records a different verdict is a finding to report with its events, not a reason to change the scenario.

- [ ] **Step 1: Write the probe**

`test/e2e/backfill_test.go`:

```go
package e2e

import (
	"time"

	. "github.com/onsi/ginkgo/v2"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

var _ = register(50, backfillProbe)

func backfillProbe() {
	It("backfill", Label("backfill"), func(ctx SpecContext) {
		p := startProbe(ctx, verdict.CapBackfill)
		blockers := names("blocker", 3)
		for _, n := range blockers {
			p.submit(ctx, workload.Job{Name: n, Priority: workload.Low, Pods: 1, GPUsPerPod: 8, RunFor: 60 * time.Second})
		}
		p.mustAwait(ctx, "the three blockers run", func() bool {
			for _, n := range blockers {
				if p.running(ctx, n) < 1 {
					return false
				}
			}
			return true
		}, time.Minute)
		time.Sleep(time.Second) // scenario offset: the gang arrives at tb + 1 s
		p.submit(ctx, workload.Job{Name: "big", Priority: workload.Low, Pods: 4, GPUsPerPod: 8, RunFor: 20 * time.Second, Gang: true})
		shorts := names("short", 4)
		for _, n := range shorts {
			time.Sleep(time.Second) // scenario offset: short jobs arrive at tb + 2 .. tb + 5 s
			p.submit(ctx, workload.Job{Name: n, Priority: workload.Low, Pods: 1, GPUsPerPod: 8, RunFor: 25 * time.Second})
		}
		p.await(ctx, func() bool { return p.scheduled(ctx, "big") >= 4 }, 3*time.Minute)
		p.finish(ctx, verdict.CapBackfill, verdict.BackfillParams{BigJob: "big", BigPods: 4,
			BlockerJobs: blockers, SmallJobs: shorts, ToleranceSec: 5})
	}, SpecTimeout(10*time.Minute))
}
```

- [ ] **Step 2: Dry-run**

Run: `cd "$W" && make lint && go tool ginkgo --dry-run --no-color -v --label-filter=backfill ./test/e2e 2>&1 | grep -E '^probes|Will run'`
Expected: `Will run 1 of 6 specs` and `probes backfill [backfill]`.

- [ ] **Step 3: Live run on vanilla (single probe, laptop allowed)**

Run: `cd "$W" && make e2e SCHEDULER=vanilla FOCUS=backfill RUN_ID=dev-backfill-vanilla`
Expected: rc 0 and a line starting `backfill on vanilla: partial (`; the summary lists it under Findings (documented not_supported, observed partial). Validation run: `backfill on vanilla: partial (4 short jobs ran before the gang, which started 40.741 s after its earliest possible start)`

- [ ] **Step 4: Live run on Kueue (single probe, laptop allowed)**

Run: `cd "$W" && make e2e SCHEDULER=kueue FOCUS=backfill RUN_ID=dev-backfill-kueue`
Expected: rc 0 and a line starting `backfill on kueue: partial (`. Validation run: `backfill on kueue: partial (4 short jobs ran before the gang, which started 31.196 s after its earliest possible start)`

- [ ] **Step 5: Commit**

```bash
git -C "$W" add test/e2e/backfill_test.go
git -C "$W" commit -s -S -m "feat(e2e): probe backfill around a waiting gang on both profiles" -m "Short jobs arrive while a fleet-wide gang waits for three blockers; the verdict reads whether they ran in the gap and whether the gang started on time."
```

---

### Task 17: Topology probe

**Files:**
- Create: `test/e2e/topology_test.go`

**Interfaces:**
- Consumes: `register`, `startProbe` or `startScenario`, `(*probe).submit/await/mustAwait/finish`, `names`, the `verdict.{...}Params` of this capability, `workload.Job`.
- Produces: the `topology` spec, labeled `topology`.

Vanilla uses the PodGroup's `schedulingConstraints.topology` (gate `TopologyAwareWorkloadScheduling`, on in both clusters); Kueue uses the pod-set annotation `kueue.x-k8s.io/podset-required-topology`. Each attempt is removed (Job and PodGroup) and its pods gone before the next.

How this probe proves itself: its verdict rule was test-driven in Tasks 2 to 5 against hand-built timelines; the probe file is the scenario, and its test is the live run on both profiles, whose expected verdicts below come from the validation runs this plan was built from. A profile that records a different verdict is a finding to report with its events, not a reason to change the scenario.

- [ ] **Step 1: Write the probe**

`test/e2e/topology_test.go`:

```go
package e2e

import (
	"time"

	. "github.com/onsi/ginkgo/v2"

	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/fleet"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/verdict"
	"github.com/ArangoGutierrez/scheduler-capabilities-suite/pkg/workload"
)

var _ = register(60, topologyProbe)

func topologyProbe() {
	It("topology", Label("topology"), func(ctx SpecContext) {
		p := startProbe(ctx, verdict.CapTopology)
		p.submit(ctx, workload.Job{Name: "filler", Priority: workload.Low, Pods: 1, GPUsPerPod: 8, RunFor: 300 * time.Second, NodeName: "n1"})
		p.mustAwait(ctx, "the filler runs on n1", func() bool { return p.running(ctx, "filler") >= 1 }, time.Minute)
		attempts := names("topo", 3)
		for _, n := range attempts {
			j := workload.Job{Name: n, Priority: workload.Low, Pods: 2, GPUsPerPod: 8, RunFor: 20 * time.Second, Gang: true, SameRack: true}
			p.submit(ctx, j)
			p.await(ctx, func() bool { return p.scheduled(ctx, n) >= 2 }, 45*time.Second)
			p.remove(ctx, j)
		}
		p.finish(ctx, verdict.CapTopology, verdict.TopologyParams{AttemptJobs: attempts, PodsPerJob: 2, RackOf: fleet.Default().RackOf()})
	}, SpecTimeout(8*time.Minute))
}
```

- [ ] **Step 2: Dry-run**

Run: `cd "$W" && make lint && go tool ginkgo --dry-run --no-color -v --label-filter=topology ./test/e2e 2>&1 | grep -E '^probes|Will run'`
Expected: `Will run 1 of 7 specs` and `probes topology [topology]`.

- [ ] **Step 3: Live run on vanilla (single probe, laptop allowed)**

Run: `cd "$W" && make e2e SCHEDULER=vanilla FOCUS=topology RUN_ID=dev-topology-vanilla`
Expected: rc 0 and a line starting `topology on vanilla: supported (`. Validation run: `topology on vanilla: supported (all 3 attempts placed every pod in one rack)`

- [ ] **Step 4: Live run on Kueue (single probe, laptop allowed)**

Run: `cd "$W" && make e2e SCHEDULER=kueue FOCUS=topology RUN_ID=dev-topology-kueue`
Expected: rc 0 and a line starting `topology on kueue: supported (`. Validation run: `topology on kueue: supported (all 3 attempts placed every pod in one rack)`

- [ ] **Step 5: Commit**

```bash
git -C "$W" add test/e2e/topology_test.go
git -C "$W" commit -s -S -m "feat(e2e): probe one-rack placement on both profiles" -m "With one node of rack A busy, only rack B can hold a 2-pod gang in one rack; three attempts show whether the scheduler chooses the domain or lands by chance."
```



---

### Task 18: Documentation and a tidy module

**Files:**
- Create: `README.md`, `docs/data-format.md`
- Modify: `go.mod`, `go.sum` (by `go mod tidy` only)

- [ ] **Step 1: Write the README and the data-format document**

`README.md`:

````markdown
# Scheduling Capabilities Suite: proof of concept

Measures what a Kubernetes scheduler does with AI-style workloads, as opposed to what its documentation
claims. Each probe runs one capability scenario on a simulated GPU fleet, records every object change, and
compares the observed behaviour with the scheduler's documented claim.

| capability | scenario (4 nodes x 8 GPUs) |
|---|---|
| gang | two 3-pod gangs that cannot both fit |
| priority | a high-priority pod arrives behind four queued low-priority pods |
| preemption | a high-priority job needs room held by a low-priority gang |
| fair-share | tenant A floods the fleet; tenant B, entitled to half, arrives 10 s later |
| backfill | short jobs arrive while a fleet-wide gang waits for capacity |
| topology | a 2-pod gang must sit in one rack while the other rack has a busy node |

Profiles: **vanilla** (Kubernetes 1.37 with the Workload API: PodGroups, workload-aware preemption,
topology-aware workload scheduling) and **kueue** (Kueue v0.20.1 in front of kube-scheduler). Claims and
their citations are in `claims/`.

A green run means every probe ran cleanly and recorded a verdict (`supported`, `partial`, `not_supported`).
Where the verdict and the documented claim disagree, `summary.md` lists a finding; that is a result, not
a failure. A red run means the harness broke.

## Run it

Needs Docker, Go 1.27 and make. Everything else (kind, kwok, Kueue, Ginkgo) is pinned and fetched by the
suite.

```sh
make test                                     # unit specs, no cluster, under a minute
make e2e SCHEDULER=vanilla FOCUS=gang         # one probe on one profile, about 4 minutes
make e2e                                      # every probe on both profiles, one cluster at a time
make report RUN=results/<run-id> OUT=/tmp/x   # rebuild verdicts and summary from recorded events
```

A full run creates two kind clusters in turn (`scs-vanilla`, `scs-kueue`) and deletes each when done;
`KEEP_CLUSTER=1` keeps it, `KEEP_CLUSTER_ON_FAILURE=1` keeps it only after a failure. The suite refuses to
start if a kind cluster of that name already exists. Kueue's first install waits up to four minutes for
its webhook.

Results land in `results/<run-id>/`; `docs/data-format.md` describes every file.

## Caveats

- Nodes are simulated (kwok): placement and timing are real scheduler decisions, workloads do not run.
- Every number comes from one run. The suite judges behaviour; repeated runs and bands come before any
  number is quoted.
- The design and its decisions: `docs/superpowers/specs/2026-10-10-poc-kind-kwok-ginkgo-design.md`.
````

`docs/data-format.md`:

````markdown
# Run data format, version 1

Every run writes one directory, `results/<run-id>/`. `events.jsonl` is the source of truth; every other
file is derived from it and the files beside it, and `cmd/scs-report` rebuilds the derived files byte for
byte (`make report RUN=results/<run-id> OUT=<dir>`).

```
results/<run-id>/
  manifest.json          run ID, git commit, host, tool versions, fleet, and per profile: cluster,
                         feature gates, runtime config, image digests, manifest sha256, scheduler
                         settings, host-to-API-server clock offset at start and end, watch errors
  summary.md             capability x profile: documented | observed | agree | evidence
  <profile>/             vanilla or kueue
    events.jsonl         one line per observed change (below)
    probes.json          each probe: capability, parameters, end of its evidence window
    claims.yaml          the profile's documented claims, copied from claims/ at run time
    verdicts.json        per probe: documented claim, observed verdict, agree flag
    ginkgo-report.json   Ginkgo's JSON report; each probe attaches its verdict and its measurements
    ginkgo.log           the suite's output
    cluster-logs/        kind export logs: scheduler, controller-manager, kwok, Kueue, kubelet
```

## events.jsonl

One JSON object per line. `seq` counts from 1 without gaps; a reader must refuse a gap, an unknown
field, an unknown transition or a schema other than 1 (`pkg/events`).

| field | meaning |
|---|---|
| `schema` | 1 |
| `seq` | position in the file, from 1 |
| `run_id` | the run ID |
| `observed_at` | when the recorder wrote the change, RFC 3339 with nanoseconds, host clock; `seq` and `observed_at` rise together |
| `api_time` | the object's own timestamp for the change, when it has one |
| `api_time_precision` | `s` (pod and Job conditions, container states, creation and deletion times) or `us` (`events.k8s.io` Event time) |
| `kind` | `Pod`, `Job`, `Event` or `Workload` (Kueue) |
| `namespace`, `name`, `uid` | the object; for an Event, the object it is about |
| `probe`, `job`, `tenant` | from the suite's labels (`scs.arangogutierrez.github.io/...`) or the namespace's |
| `priority` | the pod's priority value, or the Kueue Workload's |
| `gpus` | `nvidia.com/gpu` limit of a pod; parallelism times that for a Job |
| `transition` | created, scheduled, running, succeeded, failed, deleted, preempted, evicted, admitted, quota_reserved, requeued, unschedulable |
| `node` | for `scheduled`, the node |
| `reason`, `message` | for an Event, its reason and note; for a condition, its reason and message |

What each kind records:

- **Pod**: created; scheduled (with the node); unschedulable, once per change of reason; running
  (phase Running); succeeded or failed; preempted (`DisruptionTarget` with reason
  `PreemptionByScheduler`) or evicted (any other `DisruptionTarget`); deleted.
- **Job**: created (a job's submission: under Kueue a queued Job has no pods), succeeded or failed (the
  `Complete` or `Failed` condition), deleted.
- **Event**: only reasons `Scheduled`, `FailedScheduling`, `Preempted`, `QuotaReserved`, `Admitted`,
  `Evicted`, about a Pod or Workload in a namespace the suite created. The cluster logs keep everything else.
- **Workload** (Kueue, `workloads.kueue.x-k8s.io` v1beta2): created; quota_reserved, admitted, preempted,
  evicted, requeued when the condition of that name turns True; deleted.

## How verdicts read the events

`pkg/verdict` uses `observed_at` throughout: one clock, nanosecond resolution. A probe's evidence is its
`probe` label's events up to the window end in `probes.json`, so cleanup after the window cannot change a
verdict. The rules are in `pkg/verdict` and the spec's section 5.

## Limits

- Timestamps compare within one run only; the clock offset in `manifest.json` bounds host-versus-cluster skew to a second.
- A recorder watch error, or a control-plane container restart, during a probe makes that probe a harness failure, not a verdict.
- Every number in a run comes from a single run. It is evidence for a verdict, not a result to quote.
````

- [ ] **Step 2: Tidy the module and run everything that needs no cluster**

Run: `cd "$W" && go mod tidy && make test && make lint && git -C "$W" status --short`
Expected: every unit suite `SUCCESS!` (apply 3, claims 6, cluster 4, events 8, fleet 2, kueue 8, kwok 5, recorder 12, report 9, scs-report 3, vanilla 5, verdict 44, workload 6); lint exits 0; status lists only `README.md`, `docs/data-format.md` and, if tidy changed them, `go.mod` and `go.sum`.

- [ ] **Step 3: Commit**

```bash
git -C "$W" add README.md docs/data-format.md go.mod go.sum
git -C "$W" commit -s -S -m "docs: explain how to run the suite and what each result file holds"
```

---

### Task 19: The full run on a cloud VM (spec D7), and acceptance

Carlos provisions the VM; nothing here creates cloud resources, pushes, or posts. Every full-suite run happens on the VM only.

**Files:**
- Create: `hack/vm-bootstrap.sh`

**VM to ask for:** Linux (Ubuntu 24.04 or later), amd64 or arm64; 8 vCPU or more, 16 GB RAM or more, 50 GB of SSD, local NVMe preferred (R1 saw a 2.3 s etcd fsync stall on the laptop make the scheduler and Kueue lose leader election; the suite now fails a probe when that happens, so fast disk keeps runs green); Docker 27 or later with the login user in the `docker` group; git, make, curl; outbound HTTPS to `registry-1.docker.io`, `registry.k8s.io`, `github.com`, `raw.githubusercontent.com`, `proxy.golang.org` and `sum.golang.org`; one cluster at a time, nothing else running on it.

- [ ] **Step 1: Write the bootstrap script**

`hack/vm-bootstrap.sh`:

```bash
#!/bin/bash
# Prepare a Linux VM (amd64 or arm64) for a full suite run: install Go
# 1.27.2, verified by sha256, under $PREFIX (default $HOME/.local), and check
# Docker access, the tools the Makefile uses, and outbound access to every
# host the suite pulls from.
#   bash hack/vm-bootstrap.sh
set -euo pipefail
GO_VERSION=1.27.2
PREFIX=${PREFIX:-$HOME/.local}
case "$PREFIX" in
  "" | "/") echo "vm-bootstrap: refusing PREFIX='$PREFIX'" >&2; exit 2 ;;
esac
[ "$(uname -s)" = Linux ] || { echo "vm-bootstrap: Linux only" >&2; exit 2; }
case "$(uname -m)" in
  x86_64) arch=amd64 sum=ecbadb99091a3f46e31f5f934b068b1864eafa7995211b39eaddf76996045fe5 ;;
  aarch64 | arm64) arch=arm64 sum=94f3e30b8e374bc285e7dadc11e0865726b9bc6e85b841ccceaabc0214c6b7c8 ;;
  *) echo "vm-bootstrap: unsupported architecture $(uname -m)" >&2; exit 2 ;;
esac
for tool in docker git make curl tar sha256sum; do
  command -v "$tool" >/dev/null || { echo "vm-bootstrap: $tool is missing" >&2; exit 2; }
done
docker info >/dev/null 2>&1 || {
  echo "vm-bootstrap: this user cannot reach the Docker daemon (add it to the docker group, then log in again)" >&2
  exit 2
}
tarball=go${GO_VERSION}.linux-${arch}.tar.gz
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
curl -sSfL -o "$tmp/$tarball" "https://go.dev/dl/$tarball"
echo "$sum  $tmp/$tarball" | sha256sum -c - >/dev/null || { echo "vm-bootstrap: $tarball checksum mismatch" >&2; exit 3; }
mkdir -p "$PREFIX"
rm -rf "$PREFIX/go"
tar -C "$PREFIX" -xzf "$tmp/$tarball"
"$PREFIX/go/bin/go" version
for host in registry-1.docker.io registry.k8s.io github.com raw.githubusercontent.com proxy.golang.org; do
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "https://$host/" || true)
  if [ -z "$code" ] || [ "$code" = 000 ]; then
    echo "vm-bootstrap: no route to $host" >&2
    exit 4
  fi
  echo "reachable: $host (HTTP $code)"
done
echo "docker: $(docker info --format '{{.ServerVersion}}, {{.NCPU}} CPUs, {{.MemTotal}} bytes')"
echo "next: export PATH=$PREFIX/go/bin:\$PATH"
```

Commit it:

```bash
git -C "$W" add hack/vm-bootstrap.sh
git -C "$W" commit -s -S -m "feat(hack): bootstrap a Linux VM for a full suite run" -m "Full runs moved off the laptop (spec D7); the script pins Go by checksum and checks Docker and egress before any cluster is created."
```

- [ ] **Step 2: Get the code to the VM without pushing**

On the Mac (`VM` is the address Carlos gives):

```bash
git -C "$W" bundle create /tmp/claude/scs-poc.bundle feat/poc && scp /tmp/claude/scs-poc.bundle "$VM":~/
ssh "$VM" 'git clone -b feat/poc ~/scs-poc.bundle scheduler-capabilities-suite && cd scheduler-capabilities-suite && bash hack/vm-bootstrap.sh'
```

Expected: the bootstrap prints `go version go1.27.2 linux/...`, one `reachable:` line per host, and the Docker summary. (Pushing `feat/poc` to GitHub instead is a separate outward action that needs Carlos's approval.)

- [ ] **Step 3: Unit specs on the VM**

Run on the VM: `cd ~/scheduler-capabilities-suite && export PATH=$HOME/.local/go/bin:$PATH && make test && make lint`
Expected: the same thirteen suites `SUCCESS!` as Task 18 Step 2.

- [ ] **Step 4: Full run, twice (acceptance items 2 and 4)**

Run on the VM, one after the other (each about 25 minutes):

```bash
cd ~/scheduler-capabilities-suite && export PATH=$HOME/.local/go/bin:$PATH && \
make e2e RUN_ID=full-1 > full-1.log 2>&1; echo "full-1 rc=$?" && \
make e2e RUN_ID=full-2 > full-2.log 2>&1; echo "full-2 rc=$?"
```

Expected: `full-1 rc=0` and `full-2 rc=0`. Each `results/full-N/summary.md` has twelve rows (six capabilities on two profiles) and the gang control under Controls with `partial`.

- [ ] **Step 5: The offline rebuild matches each run (acceptance item 3)**

```bash
for r in full-1 full-2; do
  make report RUN=results/$r OUT=/tmp/rebuild-$r >/dev/null && \
  cmp results/$r/summary.md /tmp/rebuild-$r/summary.md && \
  cmp results/$r/vanilla/verdicts.json /tmp/rebuild-$r/vanilla/verdicts.json && \
  cmp results/$r/kueue/verdicts.json /tmp/rebuild-$r/kueue/verdicts.json && echo "$r REBUILD-IDENTICAL"
done
```

Expected: `full-1 REBUILD-IDENTICAL` and `full-2 REBUILD-IDENTICAL`.

- [ ] **Step 6: The two runs give the same verdict table (acceptance item 4)**

```bash
table() { awk -F'|' '/^\| [a-z-]+ \| (vanilla|kueue) \|/ {print $2 "|" $3 "|" $4 "|" $5 "|" $6}' "results/$1/summary.md"; }
diff <(table full-1) <(table full-2) && echo SAME-VERDICTS
```

Expected: `SAME-VERDICTS`, with one known exception: vanilla fair-share may read `not_supported` in one run and `partial` in the other (no tenant policy, so the slot order is not defined; Task 15). Any differing row is kept and reported with both runs' events, never retried away: an unstable verdict is a finding for the meeting.

- [ ] **Step 7: Bring the results back and record them**

On the Mac: `mkdir -p /tmp/claude/scs-full && scp -r "$VM":~/scheduler-capabilities-suite/results/full-1 "$VM":~/scheduler-capabilities-suite/results/full-2 /tmp/claude/scs-full/`. The results stay out of git (`/results/` is ignored); record in the workspace ledger the run IDs, the VM's `manifest.json` host block, the two verdict tables and the Step 5 and 6 output.

---

## Appendix: the mutation check (for the reviewer of Tasks 1 to 8)

Spec section 7 requires every verdict rule's key condition to be mutation-checked. This script ran 32 mutants against the scratch module this plan was built from; all 32 were killed. Run it against the worktree once Task 8 is committed; every row must read `KILLED`. A `SURVIVED` row is a test gap to report; an `ANCHOR x0` row means the code no longer matches the plan.

```bash
cd "$W" && python3 /tmp/claude/scs-mutate.py "$W"
```

`/tmp/claude/scs-mutate.py` (outside the repository; it is a review aid, not part of the suite):

```text
#!/usr/bin/env python3
"""Mutation check for the suite's core packages: each mutant changes one
condition in a fresh copy of the module; its package's specs must fail.
Prints one row per mutant with the changed-line count and the result.
  python3 mutate.py MODULE_DIR   (MUT_ONLY=<substring> runs a subset)"""
import os, shutil, subprocess, sys

SRC = sys.argv[1] if len(sys.argv) > 1 else os.getcwd()
WORK = "/tmp/claude/scs-mutants"
ENV = dict(os.environ, GOMODCACHE=os.path.expanduser("~/go/pkg/mod"),
           GOCACHE=os.path.expanduser("~/Library/Caches/go-build"))

MUTANTS = [
    ("events: seq gap check", "pkg/events/events.go", "if want := int64(len(out) + 1); e.Seq != want {", "if want := int64(len(out) + 1); false && e.Seq != want {"),
    ("events: unknown fields allowed", "pkg/events/events.go", "\t\tdec.DisallowUnknownFields()\n", "\n"),
    ("Pods: window filter", "pkg/verdict/verdict.go", "e.Probe != probe || e.ObservedAt.After(until)", "e.Probe != probe"),
    ("Pods: probe filter", "pkg/verdict/verdict.go", "e.Kind != \"Pod\" || e.Probe != probe ||", "e.Kind != \"Pod\" ||"),
    ("gang: grace inclusive", "pkg/verdict/gang.go", "\tcase maxPartial > p.GraceSec:\n", "\tcase maxPartial >= p.GraceSec:\n"),
    ("gang: last pod is Size-1", "pkg/verdict/gang.go", "last, full := nthScheduled(ps, p.Size)", "last, full := nthScheduled(ps, p.Size-1)"),
    ("gang: unfull gang not extended", "pkg/verdict/gang.go", "\t\t\tif !full {\n\t\t\t\tlast = t.End", "\t\t\tif false && !full {\n\t\t\t\tlast = t.End"),
    ("gang: completion counts any pod", "pkg/verdict/gang.go", "if q.EndBy == events.Succeeded {", "if q.EndBy == events.Succeeded || q.EndBy == events.Deleted {"),
    ("priority: tie counts as ahead", "pkg/verdict/priority.go", "(!ok || q.Scheduled.Before(hs))", "(!ok || !q.Scheduled.After(hs))"),
    ("priority: all-before not detected", "pkg/verdict/priority.go", "\tcase len(p.QueuedJobs):\n", "\tcase -1:\n"),
    ("preemption: recreated victims counted", "pkg/verdict/preemption.go", "!q.Scheduled.IsZero() && q.Scheduled.Before(highCreated)", "!q.Scheduled.IsZero()"),
    ("preemption: end at cut remains", "pkg/verdict/preemption.go", "!q.End.IsZero() && !q.End.After(cut)", "!q.End.IsZero() && q.End.Before(cut)"),
    ("preemption: last high pod is first", "pkg/verdict/preemption.go", "th, ok := nthScheduled(high, p.HighPods)", "th, ok := nthScheduled(high, 1)"),
    ("fair-share: start at check excluded", "pkg/verdict/fairshare.go", "if q.Running.IsZero() || q.Running.After(check) {", "if q.Running.IsZero() || !q.Running.Before(check) {"),
    ("fair-share: read at the check instant", "pkg/verdict/fairshare.go", "\treturn best\n}", "\treturn cur\n}"),
    ("timeline: Job creation ignored", "pkg/verdict/timeline.go", "\t\tcase events.Created:\n\t\t\tsetOnce(&j.Created, e.ObservedAt)", "\t\tcase events.Created:\n\t\t\t_ = e"),
    ("timeline: Job-only job unseen", "pkg/verdict/timeline.go", "return ps, seen || len(ps) > 0", "return ps, len(ps) > 0 && seen || len(ps) > 0"),
    ("priority: time from pod creation", "pkg/verdict/priority.go", "hs.Sub(submitted).Seconds()", "hs.Sub(earliestCreated(high)).Seconds()"),
    ("fair-share: no demand cap", "pkg/verdict/fairshare.go", "give := math.Min(demand[t], remaining/float64(len(names)-i))", "give := remaining / float64(len(names)-i)"),
    ("fair-share: one window to the horizon", "pkg/verdict/fairshare.go", "w1 := math.Min(w0+windowSec, horizon)", "w1 := horizon"),
    ("fair-share: single-tenant windows used", "pkg/verdict/fairshare.go", "\t\tif len(demand) < 2 {\n", "\t\tif len(demand) < 1 {\n"),
    ("backfill: tolerance inclusive", "pkg/verdict/backfill.go", "\tcase delay > p.ToleranceSec:\n", "\tcase delay >= p.ToleranceSec:\n"),
    ("backfill: any small counts", "pkg/verdict/backfill.go", "(!started || s.Scheduled.Before(tbig))", "(true || s.Scheduled.Before(tbig))"),
    ("backfill: earliest is first blocker end", "pkg/verdict/backfill.go", "if b.End.After(earliest) {", "if earliest.IsZero() {"),
    ("topology: any rack count passes", "pkg/verdict/topology.go", "\t\tif len(racks) == 1 {\n", "\t\tif len(racks) >= 1 {\n"),
    ("evaluate: unknown params allowed", "pkg/verdict/evaluate.go", "\tdec.DisallowUnknownFields()\n\tif err := dec.Decode(into)", "\tif err := dec.Decode(into)"),
    ("recorder: unschedulable every retry", "pkg/recorder/changes.go", "if oc == nil || oc.Status != nc.Status || oc.Reason != nc.Reason {", "if oc == nil || oc.Status != nc.Status || oc.Reason != nc.Reason || true {"),
    ("recorder: preemption reported as eviction", "pkg/recorder/changes.go", "if nd.Reason == corev1.PodReasonPreemptionByScheduler {", "if false && nd.Reason == corev1.PodReasonPreemptionByScheduler {"),
    ("recorder: events from any namespace", "pkg/recorder/recorder.go", "\tinfo, ok := r.nsInfo(ev.Regarding.Namespace)\n\tif !ok {", "\tinfo, ok := r.nsInfo(ev.Regarding.Namespace)\n\tif false && !ok {"),
    ("recorder: workload condition every update", "pkg/recorder/workload.go", "if now && !was {", "if now || was {"),
    ("recorder: bind on every update", "pkg/recorder/changes.go", "if old.Spec.NodeName == \"\" && cur.Spec.NodeName != \"\" {", "if cur.Spec.NodeName != \"\" {"),
    ("recorder: running on every update", "pkg/recorder/changes.go", "if old.Status.Phase != corev1.PodRunning && cur.Status.Phase == corev1.PodRunning {", "if cur.Status.Phase == corev1.PodRunning {"),
]

def run():
    rows, survivors = [], 0
    only = os.environ.get('MUT_ONLY', '')
    for i, (name, path, old, new) in enumerate(MUTANTS, 1):
        if only and only not in name:
            continue
        d = os.path.join(WORK, f"m{i:02d}")
        if os.path.exists(d):
            shutil.rmtree(d)
        shutil.copytree(SRC, d, ignore=shutil.ignore_patterns('.git', 'results', '*.kubeconfig'))
        f = os.path.join(d, path)
        s = open(f).read()
        if s.count(old) != 1:
            rows.append((i, name, "-", f"ANCHOR x{s.count(old)}"))
            survivors += 1
            continue
        open(f, "w").write(s.replace(old, new))
        diff = subprocess.run(["diff", os.path.join(SRC, path), f], capture_output=True, text=True).stdout
        changed = sum(1 for l in diff.splitlines() if l.startswith(("<", ">")))
        pkg = "./" + os.path.dirname(path)
        r = subprocess.run(["go", "test", "-count=1", pkg], cwd=d, env=ENV, capture_output=True, text=True)
        verdict = "KILLED" if r.returncode != 0 and "FAIL" in r.stdout + r.stderr and "build failed" not in r.stdout + r.stderr else ("BUILD-FAIL" if "build failed" in r.stdout + r.stderr or "cannot" in r.stderr else "SURVIVED")
        if verdict != "KILLED":
            survivors += 1
        rows.append((i, name, changed, verdict))
    for row in rows:
        print("%2d  %-42s  changed=%s  %s" % row)
    print(f"mutants={len(rows)} not-killed={survivors}")
    return 1 if survivors else 0

if __name__ == "__main__":
    sys.exit(run())
```
