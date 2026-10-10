# Scheduling Capabilities Suite POC on kind, kwok and Ginkgo: design

Date: 2026-10-10. Owner: Carlos Eduardo Arango Gutierrez. Status: approved section by section in
conversation on 2026-10-10; this file is the written spec for review.

## 1. Intent

What Carlos said: the team meeting moved to next week and he wants to arrive with a proof of concept, in
`github.com/ArangoGutierrez/scheduler-capabilities-suite`, built on kind and kwok, with Ginkgo and Gomega,
producing logs and data that can later be processed for the Suite's main goal: measuring what Kubernetes
schedulers do with AI workloads, as opposed to what their feature tables claim.

What this spec assumes:

- New Go code in a Go module. Nothing is copied from `Zhanyl-tech/k8s-gpu-scheduler-lab` (MIT); if a later
  change reuses its trace generator or traces, its copyright notice comes with them.
- The deliverable is data. Probes write raw events; verdicts and summaries are derived from those events
  by functions that can run again offline.
- Every commit is signed; nothing is pushed, opened or posted without Carlos's approval of that action.

Success at the meeting: a teammate clones the repository, runs one command on a laptop with Docker, and
gets a table of six capabilities by two schedulers, each cell holding the documented claim, the observed
verdict and the evidence behind it, produced from files a later analysis can reprocess.

## 2. Decisions taken on 2026-10-10

| # | decision | how decided |
|---|---|---|
| D1 | The POC compares vanilla Kubernetes with Kueue. | Carlos, scope question |
| D2 | All five capabilities the paper commits to (gang scheduling, workload-aware preemption, fair-share, backfill, topology-aware placement) plus priority; all green on both schedulers by the meeting, with shallower probes where needed. | Carlos ("ALL of them", then "all green") |
| D3 | A green spec means the probe ran cleanly and recorded a verdict. Each probe carries the scheduler's documented claim; a mismatch between documented and observed behaviour is a finding in the report, not a test failure. Red means the harness broke. | Carlos ("Documented vs observed") |
| D4 | Runs on any laptop with Docker (macOS arm64 and Linux amd64) through one `make` target. No CI workflow yet. | Carlos |
| D5 | Structure: a Go harness library with Ginkgo specs, one profile per scheduler behind one interface. Not YAML scenarios, not shell orchestration. | Carlos (option A) |
| D6 | Kueue v0.20.1, the latest stable release on 2026-10-10, pinned by digest. | design session, as a reversible pin; the plan verifies every feature used exists in it |

## 3. Architecture

Go module `github.com/ArangoGutierrez/scheduler-capabilities-suite`, MIT. One purpose per unit:

```
pkg/workload/   scheduler-neutral job description: tenant, priority, pods, GPUs per pod, run time,
                gang yes or no, topology requirement
pkg/scheduler/  Profile interface: Install(cluster), Submit(job), Claims(); vanilla/ and kueue/ implement it
pkg/cluster/    kind cluster up and down through kind's Go library; kwok controller and kwok's "general"
                stages from pinned v0.8.0 manifests, verified by sha256
pkg/fleet/      kwok GPU nodes (nvidia.com/gpu) with block and rack topology labels
pkg/recorder/   client-go watches on Pods, Events and Kueue Workloads, written to events.jsonl
pkg/verdict/    pure functions from recorded events to supported / partial / not supported, with evidence
pkg/claims/     loads claims/<scheduler>.yaml: the documented claim and its citation per capability
cmd/scs-report/ rebuilds verdicts.json and summary.md from a run's events.jsonl
test/e2e/       Ginkgo suite, one spec file per capability, written once for both profiles
claims/         vanilla.yaml, kueue.yaml
docs/data-format.md   the versioned events schema
Makefile        make test (unit) | make e2e SCHEDULER=vanilla|kueue|all | make lint
```

Rules that keep the comparison fair:

- Both profiles use the same kind node image (`kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5`),
  the same feature gates and runtime configuration, and the same fleet. The only difference between them
  is Kueue installed or not. Both clusters run the API server with
  `--runtime-config=scheduling.k8s.io/v1beta1=true` and the API server, controller-manager and scheduler
  with `--feature-gates=GenericWorkload=true`, as the 2026-10-02 fidelity runs did, plus any Workload API
  topology gate the plan confirms for 1.37.
- Specs never branch on the scheduler. Anything scheduler-specific (Workload API groups, PriorityClasses,
  Kueue queues, quotas, cohorts, topology-aware scheduling) lives behind `Profile.Submit` and
  `Profile.Install`.
- Ginkgo and Gomega for unit tests as well as end-to-end tests, so there is one test style.
- Every object a probe creates carries `app.kubernetes.io/part-of: scheduler-capabilities-suite` and labels
  under the prefix `scs.arangogutierrez.github.io/` (a domain Carlos controls; renamed if the repository
  moves to a group-owned organization) for probe, tenant, job and run ID.

## 4. Run flow

`make e2e SCHEDULER=all` runs the vanilla profile, then the Kueue profile, one after the other and never at
the same time, because kwok's simulated time is the wall clock. Each profile is one `ginkgo` invocation
writing to `results/<run-id>/<profile>/`; the run ID is the UTC start time plus the git short SHA, with
`-dirty` when the tree has uncommitted changes.

Suite setup, per profile:

1. Create kind cluster `scs-<profile>`: one control-plane node from the pinned image, with the gates above.
2. Install the kwok controller and kwok's general stages from the pinned v0.8.0 manifests.
3. Create the fleet: four kwok nodes of 8 GPUs (32 GPUs) in two racks of two nodes (rack A: n1, n2;
   rack B: n3, n4) under one block. CPU and memory are large enough that GPUs are the only constraint.
4. Install the profile. For Kueue: its manifests, a wait until its webhook answers, then the queues and
   quotas each probe needs.
5. Start the recorder and wait until its watches have synced.
6. Health check: four nodes Ready, 32 GPUs allocatable, the scheduler (and Kueue) ready. A failure here is a
   red run.

Specs run in order, one at a time (Ginkgo `Ordered`, `Serial`). Each probe uses its own namespaces, starts
from an empty fleet, and its cleanup waits until no probe pod remains on the fleet. Every workload is a
`batch/v1` Job on both profiles. kwok's `pod-complete` stage finishes a Job's pod after the time in its
`pod-complete.stage.kwok.x-k8s.io/delay` annotation; the probe sets the jitter annotation so that each pod's
run time is exact. Each probe has an explicit timeout.

Suite teardown: flush the recorder, write the verdicts, run `kind export logs` into the results directory,
and delete the cluster unless `KEEP_CLUSTER=1`.

Budget: simulated pod run times of 10 to 60 seconds; about 12 minutes per profile and under 25 minutes for
both on a laptop. A teammate needs Docker, Go and make. The kind CLI is not needed (kind's Go library is),
Ginkgo runs through `go run` at the version pinned in `go.mod`, and kubectl is optional.

## 5. Probes

All probes run on the 32-GPU fleet; t0 is the probe's first submission. Each verdict rule reads only the
recorded events. The numbers below fix each scenario's arithmetic; the plan may adjust a duration if a
measured setup step needs more room, and records why.

| probe | scenario | supported / partial / not supported |
|---|---|---|
| gang | Two gangs of 3 pods x 8 GPUs, submitted together, 20 s per pod; only one fits at a time. | No gang ever has some pods running while others of the same gang are pending for longer than a 5 s grace, and both gangs complete / a gang was partly placed but both completed / a partial placement persisted until the timeout. |
| priority | At t0 four low-priority 8-GPU pods fill the fleet, with run times 30, 40, 50 and 60 s, so slots free one at a time. Once all four are running (t1), four more low-priority pods are submitted; a high-priority 8-GPU pod follows at t1 + 2 s with preemption disabled. | The high-priority pod takes the first freed slot / it is placed ahead of some of the earlier low-priority pods / it is placed after all four (arrival order). |
| preemption | One low-priority gang of 4 pods x 8 GPUs fills the fleet for 300 s. A high-priority job of 2 pods x 8 GPUs arrives with preemption allowed. | The high-priority job runs and no pod of the victim gang is left running after a 5 s grace / it runs but the victim gang is left partly running / it does not run before the timeout. Also recorded: time to schedule and GPU-seconds lost to eviction. |
| fair-share | Tenants A and B are each entitled to 16 GPUs. A submits 8 pods x 8 GPUs (60 s each) at t0, so four run and four queue. B submits 4 pods x 8 GPUs (60 s each) at t0 + 10 s. | Read at tc = t0 + 75 s (B's submission plus one job length plus a 5 s tolerance; A's first four pods all finish at t0 + 60 s): B has 16 GPUs running / B has more than 0 and fewer than 16 / B has none. Also recorded: the fairness index and the largest tenant shortfall from the Methods section's fairness-over-time metric, with 10 s windows instead of one hour because the probe lasts about two minutes. |
| backfill | Three 8-GPU blocker pods run for 60 s from tb, leaving one node free. A 4-pod x 8-GPU gang arrives at tb + 1 s; its earliest possible start is tb + 60 s. Four short jobs (8 GPUs, 25 s each) arrive at tb + 2 to tb + 5 s; on the free node the first two finish by tb + 52 s and a third would end at tb + 77 s. | At least one short job runs before the gang starts and the gang starts within 5 s of tb + 60 s / short jobs ran but the gang started more than 5 s late (no reservation) / no short job ran before the gang started. |
| topology | Node n1 (rack A) is filled by an 8-GPU pod for 300 s. A job of 2 pods x 8 GPUs that must sit in one rack is submitted, its placement recorded, the job deleted and the fleet drained; three attempts. Only rack B (n3, n4) satisfies the requirement. | All pods in one rack in 3 of 3 attempts / in 1 or 2 of 3 / in none. If ties among n2, n3 and n4 are broken uniformly, a topology-blind scheduler lands in one rack with probability 1/3 per attempt, so 3 of 3 by chance has probability 1/27. |

How each profile expresses a probe (Workload API gangs, PriorityClasses and namespaces as tenants on
vanilla; Kueue queues, quotas, a cohort, fair sharing, preemption settings and topology-aware scheduling on
Kueue) is the plan's research task: it pulls each project's documented mechanism and claim for the pinned
versions into `claims/*.yaml`, each with a URL citation. Where a scheduler documents no mechanism for a
capability, its claim says so and the probe submits the plain workload; the observed verdict is then
expected to be "not supported", which is a finding, not a failure.

A timeout that is part of a verdict rule (deadlock, starvation, no preemption) produces a verdict. Any
other timeout is a harness failure.

Simulated nodes show where pods were placed, not whether a placement made them faster; the report says so
for the topology probe.

## 6. Data outputs

`events.jsonl` is the source of truth; everything else in a run is derived from it, and the derivation is
checked.

```
results/<run-id>/
  manifest.json     schema version, run ID, git commit and dirty flag, start and end time (UTC); host OS,
                    architecture, CPU count, memory, Docker version; Go, Ginkgo and Gomega versions; every
                    image by digest (kind node, kwok, Kueue); sha256 of every applied manifest; fleet spec;
                    each profile's feature gates, scheduler configuration and Kueue configuration; the offset
                    between host clock and API server clock, measured at start and end
  summary.md        capability x scheduler: documented | observed | agree | evidence
  <profile>/
    events.jsonl    one line per observed change
    verdicts.json   per capability: documented claim and citation, observed verdict, agree flag,
                    measurements, and the event sequence numbers the verdict rests on
    ginkgo-report.json   Ginkgo's JSON report; each probe attaches its verdict with AddReportEntry and its
                         numbers through a gmeasure Experiment
    cluster-logs/   output of `kind export logs`: kube-scheduler, controller-manager, kwok, Kueue, kubelet
```

An `events.jsonl` line, schema version 1:

| field | meaning |
|---|---|
| `schema` | 1 |
| `seq` | position in the file, from 1 |
| `run_id` | the run ID |
| `observed_at` | when the recorder received the change, RFC 3339 with nanoseconds |
| `api_time` | the object's own timestamp for the change (condition transition time, Event time, deletion time) |
| `api_time_precision` | `s` or `us`: a pod condition's transition time has one-second resolution, an `events.k8s.io` Event's time has microseconds |
| `kind` | `Pod`, `Event` or `Workload` (Kueue) |
| `namespace`, `name`, `uid` | the object |
| `probe`, `job`, `tenant`, `priority`, `gpus` | from the suite's labels and the pod spec |
| `transition` | one of created, scheduled, running, succeeded, failed, deleted, preempted, evicted, admitted, quota_reserved, requeued, unschedulable |
| `node`, `reason`, `message` | where applicable |

`cmd/scs-report` rebuilds `verdicts.json` and `summary.md` from `events.jsonl` and the run's claims, through
the same verdict functions the specs call. Its output must equal the files the run wrote, byte for byte.
Any later analysis (a Go analyzer, or a converter into the lab's Python paper metrics) starts from a format
shown to be complete. `docs/data-format.md` documents the schema with its version.

Numbers a probe records (times to schedule, GPU-seconds lost, fairness index) come from single runs. They
are evidence for the verdicts, not results for the paper: the Fair Evaluation Standards require repeated
runs and bands before any number is quoted.

## 7. Testing and error handling

Unit tests (`make test`, no cluster, under 30 seconds), written test first:

- `pkg/verdict`: per probe, hand-built event fixtures with literal expected verdicts, at least one fixture
  per outcome (18 or more), plus fixtures exactly at the 5 s grace and tolerance limits. Each rule's key
  condition is mutation-checked: deleting or flipping it must turn a test red.
- `pkg/recorder`: client-go's fake clientset (one layer of mocking) feeds pod and Event updates; the JSONL
  lines written are compared with literal expected lines.
- `pkg/claims`: a missing claim or citation for any capability and profile is an error.
- `cmd/scs-report`: run on a fixture events file, its output equals a committed expected output.

End-to-end (`make e2e`): the six probes on both profiles, plus one control. The gang control, vanilla
only, submits the same two gangs as plain Jobs with no gang grouping and must record a partial placement,
which shows the gang probe detects the failure on a live cluster and not only on fixtures.

Two kinds of failure are kept apart:

- The scheduler's behaviour is data: any verdict, "not supported" included, is a green spec.
- A broken harness is red: the cluster or fleet not coming up, an object creation error, a timeout that is
  not part of a verdict rule, cleanup that does not drain the fleet, or a recorder relist during a probe's
  window (it may have missed events, so that probe's evidence is invalid).

Every wait is a Gomega `Eventually` with an explicit timeout and polling interval; the only sleeps are a
scenario's own submission offsets. On any failure `kind export logs` runs, and `KEEP_CLUSTER_ON_FAILURE=1`
keeps the cluster for inspection.

## 8. Acceptance

The implementation plan turns each item into a runnable command.

1. `make test` exits 0; `go vet ./...` exits 0; `gofmt -l .` prints nothing.
2. `make e2e SCHEDULER=all` exits 0 on Carlos's Mac: six probes green on each profile, each with a
   recorded verdict, and the gang control recording a partial placement.
3. `scs-report` output equals the run's own `verdicts.json` and `summary.md` (`cmp` exits 0).
4. A second full run gives the same verdict table, or the difference is reported, with the topology probe
   (1/3 chance per attempt under uniform tie-breaking) the expected suspect.
5. One full run on a Linux amd64 cloud VM.
6. Every entry in `claims/*.yaml` has a citation URL for the pinned version.

## 9. Facts checked on 2026-10-10, and facts the plan must check before code

Checked:

- The new repository is public, MIT, copyright Carlos, one commit (`86f6e85`, `LICENSE` only).
- Mac toolchain: Docker Desktop 29.6.2 with 14 CPUs and 17 GB, kind v0.33.0, Go 1.27.2, Ginkgo CLI 2.28.1;
  Ginkgo v2.33.0 and Gomega v1.44.0 in the module cache; kwok not installed (not needed).
- Gomega v1.44.0's `gmeasure` has `NewExperiment`, `RecordValue`, `RecordDuration`, `MeasureDuration`,
  `GetStats`; Ginkgo v2.33.0 has `AddReportEntry` (`reporting_dsl.go:90`).
- kwok v0.8.0 is the latest release. Its general `pod-complete` stage
  (`kustomize/stage/pod/general/pod-complete.yaml` at `v0.8.0`) reads the per-pod delay from the annotation
  `pod-complete.stage.kwok.x-k8s.io/delay` and the jitter from `pod-complete.stage.kwok.x-k8s.io/jitter-delay`,
  with defaults of 1000 ms and 5000 ms.
- Kubernetes 1.37 gangs on kind: the 2026-10-02 fidelity runs used `--runtime-config=scheduling.k8s.io/v1beta1=true`
  and `--feature-gates=GenericWorkload=true` on the API server, controller-manager and scheduler; the
  scheduler's metrics listed `TopologyAwareWorkloadScheduling` (off) as a known gate.
- Kueue v0.20.1 and v0.19.8 were released on 2026-10-08.
- The kind node digest above was pulled and run on the cloud VM (amd64) on 2026-10-05.

To check before code (each is a plan task with the command and its output):

1. The node image digest is a multi-architecture index that pulls on macOS arm64.
2. The general `pod-complete` stage's selector matches pods owned by a Job, and how its jitter value
   combines with the delay, so that a pod's run time can be made exact.
3. How a Job's pods join a Workload API gang in 1.37, and the name and maturity of any Workload API
   topology feature.
4. In Kueue v0.20.1: fair sharing and cohort configuration, workload priority and preemption, queueing
   strategies, all-or-nothing admission for multi-pod Jobs, and topology-aware scheduling on kwok nodes
   with `nvidia.com/gpu`.
5. Whether enabling `GenericWorkload` interferes with Kueue's handling of Jobs on the Kueue profile.
6. kind v0.33.0's Go library API for creating a cluster with a given config and exporting logs.

## 10. Out of scope

- CI. Schedulers other than vanilla Kubernetes and Kueue.
- Trace replay at load and repeated-run bands; those are the lab's role in the Methods section.
- Quoting any number from a POC run.
- Measuring the runtime effect of topology-aware placement.
- Cluster autoscaling, storage and I/O, and public results publication (deferred by the whitepaper group).
- Any push, pull request, issue or comment without Carlos's approval of that action.

## 11. Risks

- Six probes on two profiles, all green within a week, is tight. Build order: harness core, gang,
  priority, preemption, fair-share, backfill, topology. Probes stay shallow (one scenario each) to fit.
- Kueue and the Workload API may overlap in how they treat gangs (check 5). If they conflict, the Kueue
  profile keeps `GenericWorkload` on for parity and submits gangs through Kueue's own mechanism; the
  manifest records it.
- Laptop timing noise: verdict rules use tolerances of seconds against durations of 10 to 60 seconds, and
  the two profiles never run at the same time.
- Tie-breaking: the topology probe's chance argument assumes uniform tie-breaking among equally scored
  nodes; the evidence records the scores' source (scheduler logs) so a skewed tie-break can be seen.
