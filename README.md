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
  `kind delete cluster --name scs-<profile>`. Without the kind CLI, run
  `go run sigs.k8s.io/kind delete cluster --name scs-<profile>` from this checkout.

## What the verdicts can claim

- **Fair share on Kueue.** Each tenant's entitlement is its ClusterQueue's nominal quota, and the two quotas add up to
  the fleet, so tenant B's share comes from reclaiming the quota tenant A borrowed (`reclaimWithinCohort`). Every
  preemption the runs recorded has the reason `InCohortReclamation`, and a run without the `fairSharing`
  configuration gave the same verdict. The scenario does not exercise fair sharing of borrowable capacity; that needs
  a cohort with capacity no tenant is entitled to.
- **The fair-share check window.** The rule takes the most GPUs tenant B held at any moment within one job length
  (plus 5 s) of arriving. By the end of that window tenant A's first jobs have ended, so a scheduler that only lets B
  in as A's jobs finish can also reach `supported`, and vanilla does. `worst_shortfall_gpu_s`, the larger of the two
  tenants' totals, tells the two apart. In the two full runs on a Linux VM it was 92.4 and 92.5 GPU-seconds on
  Kueue, where B held its 16 GPUs 3.2 s after arriving, against 801.0 and 801.1 on vanilla, where B waited 50 s for
  A's jobs to end and then got all 16 at once. `jain_median` reads 1 on Kueue and 0.5 on vanilla in both runs.
  `b_after_first_submit_s` records when B arrived: 10 s after A by design, later if A took longer to fill the fleet.
- **Backfill.** Neither profile documents a reservation for a waiting gang, so both claims expect `partial`: short
  jobs run on free nodes and can delay the gang. The rule counts only the time short jobs held nodes past the
  gang's earliest possible start (`short_hold_s`, about 25 to 28 s in the POC runs); the rest of the delay is the
  scheduler's own start latency (`big_start_after_free_s`: about 15 s for a vanilla gang's retry, 3 s on Kueue).
- **Topology.** With one rack-a node busy, a scheduler that picks at random among the free nodes places a 2-pod gang
  in one rack one time in three (two of the three free nodes are in rack B), so three of three by chance is about one
  run in 27. `topology-control` shows what vanilla does without the constraint. By the same odds the control itself
  varies: in the two VM full runs it placed 0 of 3 and then 1 of 3 attempts in one rack, so it read `not_supported`
  once and `partial` once.

## Caveats

- Nodes are simulated (kwok): placement and timing are real scheduler decisions, workloads do not run.
- Every number comes from one run. The suite judges behaviour; repeated runs and bands come before any
  number is quoted.
- The design and its decisions: `docs/superpowers/specs/2026-10-10-poc-kind-kwok-ginkgo-design.md`.
