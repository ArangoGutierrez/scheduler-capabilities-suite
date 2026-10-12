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

Two controls run on vanilla only: `gang-control` and `topology-control` repeat the gang and topology scenarios
without the mechanism under test (no PodGroup gang; no one-rack constraint). A verdict that its control also reaches
has not shown that the mechanism made the difference. `summary.md` lists controls under Controls.

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
start if a kind cluster of that name already exists. Kueue's first install spends one to four minutes
waiting for its webhook (the suite allows eight).

Results land in `results/<run-id>/`; `docs/data-format.md` describes every file.

## Interrupting a run

- Press Ctrl-C once and wait: the probe's cleanup, the cluster log export and the cluster delete still run
  (one interrupted run spent 16 s in probe cleanup). Under `SCHEDULER=all` the next profile does not start
  after an interrupt.
- A second Ctrl-C skips cleanup (Ginkgo prints "Interrupt again to skip cleanup") and leaves
  `scs-<profile>` behind.
- Closing the terminal (SIGHUP) or SIGKILL gets no cleanup; run long runs under `tmux` or `nohup`.
- A cluster left behind makes the next run refuse and print the command to delete it:
  `kind delete cluster --name scs-<profile>`.

## What the verdicts can claim

- **Fair share on Kueue.** Each tenant's entitlement is its ClusterQueue's nominal quota, and the two quotas add up to
  the fleet, so tenant B's share comes from reclaiming the quota tenant A borrowed (`reclaimWithinCohort`). Every
  preemption the runs recorded has the reason `InCohortReclamation`, and a run without the `fairSharing`
  configuration gave the same verdict. The scenario does not exercise fair sharing of borrowable capacity; that needs
  a cohort with capacity no tenant is entitled to.
- **The fair-share check window.** The rule reads tenant B's GPUs one job length (plus 5 s) after B arrives, when
  tenant A's first jobs have already ended, so a scheduler that only lets B in as A's jobs finish can also reach
  `supported`. `worst_shortfall_gpu_s` tells the two apart: about 95 GPU-seconds in the Kueue runs, where B's first
  16 GPUs were placed within 3.5 s, against roughly 16 GPUs x 51 s, over 800, for waiting until A's jobs end.
- **Backfill.** Neither profile documents a reservation for a waiting gang, so both claims expect `partial`: short
  jobs run on free nodes and can delay the gang.
- **Topology.** With one rack-a node busy, a scheduler that ignores racks places a 2-pod gang in one rack by chance
  one time in three (two of the three free nodes are in rack B), so three of three by chance is about one run in 27.
  `topology-control` shows what vanilla does without the constraint.

## Caveats

- Nodes are simulated (kwok): placement and timing are real scheduler decisions, workloads do not run.
- Every number comes from one run. The suite judges behaviour; repeated runs and bands come before any
  number is quoted.
- The design and its decisions: `docs/superpowers/specs/2026-10-10-poc-kind-kwok-ginkgo-design.md`.
