# Reading the results

Every run writes one directory, `results/<run-id>/`. This page explains what to read first, what each
verdict means, and what it can and cannot claim. [data-format.md](data-format.md) is the field-by-field
reference for every file.

## The run directory

```
results/<run-id>/
  manifest.json          what ran: commit, dirty flag, host, tool versions, fleet; per profile: cluster,
                         feature gates, image digests, manifest sha256s, scheduler settings,
                         clock offsets, watch errors
  summary.md             documented vs observed for every probe of every profile
  vanilla/               one directory per profile that ran (vanilla, kueue, kai, slurm-bridge)
    events.jsonl         every recorded object change: the source of truth
    probes.json          each finished probe's parameters and window end
    claims.yaml          the claims file the run used
    verdicts.json        each probe's claim, verdict, measurements and evidence
    ginkgo.log           the suite's output
    ginkgo-report.json   Ginkgo's JSON report, with each probe's verdict and measurements attached
    cluster-logs/        kind's log export: node, kubelet, and every pod's logs
    scs.kubeconfig       the cluster's kubeconfig, emptied when the cluster is deleted
```

## summary.md

An example, from the committed test fixture (`cmd/scs-report/testdata/summary.md`), not a real run:

```markdown
# Scheduling capabilities: documented vs observed

Run fixture-run. Each verdict comes from a single run: evidence for the POC, not a result to quote.

| capability | profile | documented | observed | agree | evidence |
|---|---|---|---|---|---|
| gang | vanilla | supported | supported | yes | no gang was partly placed for more than 5 s, and all 2 gangs completed |
| priority | vanilla | supported | not_supported | no | all 1 queued lower-priority jobs were scheduled before the high-priority job |

## Findings

- vanilla, priority: documented supported, observed not_supported (all 1 queued lower-priority jobs were scheduled before the high-priority job).

## Controls

- vanilla, gang-control: partial (gang gang-b was partly placed for 21.5 s; all 2 gangs completed).
```

| column | meaning |
|---|---|
| capability | one of gang, priority, preemption, fair-share, backfill, topology; rows follow that order |
| profile | vanilla, then kueue, then kai, then slurm-bridge |
| documented | the `expected` outcome of the profile's claim in `claims.yaml` |
| observed | the outcome the verdict rule computed from the recorded events |
| agree | `yes` when documented and observed are the same outcome |
| evidence | the rule's one-line reason, with the numbers it rests on |

**Findings** lists every row whose `agree` is `no`. A finding is a result of the run, not a failure.
When there is none, the section says so.

**Controls** lists the control probes. They have no documented claim, so they never appear in the
table and never produce a finding. Read each next to the probe it controls.

**Profiles without verdicts** appears only when a profile ran but finished no probe.

A probe that broke the harness before its verdict records none: it has no row, no entry in `probes.json`, and its
failure is in `ginkgo.log`. A missing row is a harness failure, not a negative result.

## Outcomes, and green vs red

| outcome | meaning |
|---|---|
| `supported` | the scheduler did what the capability asks, by the rule's test |
| `partial` | it did some of it (some attempts, some pods, or later than the tolerance) |
| `not_supported` | it did none of it, or the job the scenario hinges on never ran |

A **green** run (`make e2e` exits 0) means every selected probe ran cleanly and recorded a verdict,
whatever that verdict is. A **red** run means the harness broke and its evidence cannot be trusted:
the scenario never took shape, a watch failed or relisted during a probe, a control-plane container
restarted during a probe, a rule found the scenario was not exercised (for example, a preemption victim
that completed instead of being evicted), a probe's namespaces did not drain, the end-of-run clock
check failed, or the offline rebuild disagreed with the live verdicts.

## Per capability

Each rule reads only its probe's events up to the probe's `window_end`. Times are the recorder's
`observed_at`. [options.md](options.md#probe-parameters) has every scenario's jobs and parameters.

**On slurm-bridge**, three caveats hold for every verdict (`claims/slurm-bridge.yaml`). Slurm sees nodes,
not GPUs: every job drops its GPU request, and each probe pod takes a whole 8-GPU node, so node
placement equals GPU placement here and nothing more; the profile refuses a job whose pods are not whole
nodes. Preemption reaches Kubernetes as a plain pod deletion by slurm-bridge. Topology is Slurm's
default switch-aware placement, since slurm-bridge cannot ask Slurm for one rack.

### gang

**Measures** whether each gang's pods were placed together. A gang is partly placed from its first
pod's scheduling until its third (`size`), or until the window ends if it never gets three.

| measurement | meaning |
|---|---|
| `gangs` | number of gangs (2) |
| `gangs_completed` | gangs with at least `size` pods succeeded |
| `max_partial_s` | the longest time any gang was partly placed |

| outcome | when |
|---|---|
| `supported` | no gang partly placed for more than `grace_sec` (5 s), and both completed |
| `partial` | a gang was partly placed for more than 5 s, but both completed |
| `not_supported` | a gang did not complete before the window closed (the probe waits up to 150 s) |

**Can claim**: the scheduler held a gang that could not fit instead of placing part of it.
**Cannot claim**: much on its own. Read it next to `gang-control`, which submits the same two jobs as
plain Jobs. If the control also reaches `supported`, the scenario did not need the gang mechanism to get
there. A gang that a queue holds as a suspended Job, with no pods, counts as neither placed nor
completed until it is admitted.

### priority

**Measures** queue order: four low-priority jobs are queued, then a high-priority job arrives 2 s
later, and nodes free up one at a time as the fillers end. No profile allows preemption here
(vanilla's high-priority class has `preemptionPolicy: Never`; the kueue ClusterQueue has no preemption
policy; the kai Queue protects running work for 24 h with `preemptMinRuntime` and `reclaimMinRuntime`,
since KAI does not read `preemptionPolicy`; on slurm-bridge every job of this probe is in one Slurm
partition, and Slurm preempts only across partition tiers), so this probe tests queue order only.

| measurement | meaning |
|---|---|
| `queued_jobs` | number of queued low-priority jobs (4) |
| `queued_before_high` | queued jobs whose first pod was scheduled strictly before the high-priority job's first pod |
| `high_time_to_schedule_s` | from the high-priority Job's creation to its first pod's scheduling (absent if never scheduled) |

| outcome | when |
|---|---|
| `supported` | no queued job went first |
| `partial` | some, not all, went first |
| `not_supported` | all went first, or the high-priority job was never scheduled |

**Can claim**: whether priority reordered the queue in this run. **Cannot claim**: anything about
preemption, or about a queue longer than four.

### preemption

**Measures** whether making room for a high-priority job evicted the low-priority gang as a whole. The
victim pods are those scheduled before the high-priority Job was created. A victim pod counts as
evicted when it ended, in any way other than success, no later than 5 s (`grace_sec`) after the
high-priority job's second pod was scheduled.

| measurement | meaning |
|---|---|
| `victim_pods_before` | victim pods running when the high-priority job arrived (4) |
| `victim_pods_evicted` | of those, how many had ended by the cut |
| `victim_pods_remaining` | how many were still running at the cut |
| `high_time_to_schedule_s` | from the high-priority Job's creation to its last pod's scheduling |
| `gpu_seconds_lost` | GPU-seconds the evicted victim pods had run before they ended |

| outcome | when |
|---|---|
| `supported` | no victim pod was left running at the cut |
| `partial` | some victim pods were left running |
| `not_supported` | the high-priority job did not get both pods scheduled |

**Can claim**: whether the whole gang went, rather than just the two nodes the high-priority job needed.
**Cannot claim**: why a pod ended: the rule counts any end other than success, so it does not tell a
scheduler preemption from a Kueue suspension or a deletion. Every Job has a `backoffLimit` of 10, so the
Job controller does not delete the survivors itself after the first eviction. A victim pod that
succeeded before the cut makes the probe red, since preemption was not exercised.

**On slurm-bridge** the high-priority job goes to a Slurm partition of a higher `PriorityTier` over the
same nodes, and Slurm cancels the victim gang, one Slurm job, as a whole (`PreemptMode=CANCEL`).
slurm-bridge then deletes the victim pods, with no `DisruptionTarget` condition, so the recorder sees
deletions, not evictions; a kwok Stage moves each deleted pod to `Failed`. slurm-bridge checks its pods'
Slurm jobs every 30 s, so the 5 s grace measures that resync as much as Slurm.

### fair-share

**Measures** whether tenant B, entitled to 16 of the 32 GPUs, got its share after tenant A had filled
the fleet. The check is the most GPUs B had running at any moment between its first submission and one
job length plus tolerance later (60 + 5 = 65 s).

| measurement | meaning |
|---|---|
| `b_max_gpus_by_check` | the most GPUs B held at once within the check window |
| `check_offset_s` | the check window's length (65) |
| `b_after_first_submit_s` | how long after A's first submission B arrived: 10 s by design, later if A took longer to fill the fleet |
| `worst_shortfall_gpu_s` | over 10 s windows, each tenant's total of max(0, fair share minus GPU-seconds delivered); the larger of the two tenants |
| `jain_median`, `jain_p5` | the median and 5th percentile, over windows where both tenants wanted GPUs, of Jain's index of delivered over fair share: 1 when both got their share, 0.5 when one of two got none (absent if no window qualified) |
| `windows_used`, `windows_excluded` | windows counted in the Jain index, and windows left out (no demand, one tenant, nothing delivered) |

The fair share per window is a max-min split of the fleet capped by each tenant's demand; while both
tenants want more than half, it is 16 GPUs each.

| outcome | when |
|---|---|
| `supported` | B held 16 GPUs at some moment in the check window |
| `partial` | B held some, but fewer than 16 |
| `not_supported` | B held none |

**The check window is generous.** A's jobs run 60 s and B arrives 10 s after A, so A's first jobs end
about 50 s into B's 65 s window. A scheduler with no fair-share mechanism, which lets B in only as A's
jobs finish, can also reach `supported`. `worst_shortfall_gpu_s` tells the two apart: B kept out for
about 50 s accrues on the order of 16 GPUs x 50 s = 800 GPU-seconds of shortfall, while a scheduler that
reclaims B's share within seconds accrues a small fraction of that (the rule's arithmetic, not a
measurement). Vanilla is the scheduler this describes: it has no fair-share mechanism and its claim
expects `not_supported`, yet it can still read `supported` as A's jobs churn, so read
`worst_shortfall_gpu_s` before taking a vanilla finding here as fair sharing. Compare the shortfall and
`jain_median` across profiles, not just the outcome.

**On Kueue** each tenant is a ClusterQueue with a 16-GPU nominal quota in one cohort, with
`reclaimWithinCohort: Any`. B's share comes from reclaiming the quota A borrowed. The two quotas add up
to the whole fleet, so Kueue's fair sharing has nothing left to divide: this probe does not exercise
fair sharing of borrowable capacity, which would need a cohort with capacity no tenant is entitled to
(`claims/kueue.yaml`, `mechanism`).

**On KAI** each tenant is a leaf Queue with a 16-GPU quota under a parent Queue for the probe, with
`reclaimMinRuntime: 0s`. B's share comes from quota reclaim of what A holds over its quota. As on Kueue,
the quotas add up to the fleet, so KAI's over-quota fair share has nothing to divide here
(`claims/kai.yaml`, `mechanism`).

**On slurm-bridge** there is no fair share to measure: Slurm computes its fair-share factor only from
its accounting database, which the profile does not install. Tenants are namespaces, every job has the
same priority, and B's jobs wait behind A's queued jobs in submission order; the claim expects
`not_supported`.

### backfill

**Measures** whether short jobs ran in the gap before a waiting fleet-wide gang without delaying it.
Three blockers hold three nodes; the gang's earliest possible start is the moment the last blocker pod
ends. The gang starts when its fourth pod is scheduled.

| measurement | meaning |
|---|---|
| `small_before_big` | short jobs whose first pod was scheduled before the gang started |
| `big_start_delay_s` | gang start minus earliest possible start |
| `short_hold_s` | the longest time any short pod held a node past the earliest possible start (0 if none did) |
| `big_start_after_free_s` | `big_start_delay_s` minus `short_hold_s`: the scheduler's own start latency once the nodes were free |

The three delay measurements are absent when the gang never started.

| outcome | when |
|---|---|
| `supported` | short jobs ran first, and held nodes no more than `tolerance_sec` (5 s) past the earliest start |
| `partial` | short jobs ran first and held nodes longer, or ran but the gang never started |
| `not_supported` | no short job ran before the gang |

**Attribution.** Only the time short jobs held nodes past the earliest start counts against them. The
rest of the gang's delay is the scheduler's own start latency (on vanilla, a waiting gang's retry
backoff can add up to about 17 s), recorded separately as `big_start_after_free_s` so it is not blamed
on backfill. Vanilla, Kueue and KAI document no reservation for a waiting gang, so their claims
expect `partial`. Slurm documents backfill with reservations, but plans by whole-minute time limits:
the profile gives the 60 s blockers and the 25 s short jobs the same 2-minute limit, so a short job
would be expected to end after the gang's planned start and Slurm holds it, and its claim expects
`not_supported` (`claims/slurm-bridge.yaml`, `mechanism`).

### topology

**Measures** whether a 2-pod gang that asks for one rack lands in one rack. Node `n1` in `rack-a` is
busy, so only `rack-b` can take the gang whole. Three attempts run one after another; an attempt counts
when its first two scheduled pods share a rack.

| measurement | meaning |
|---|---|
| `attempts` | number of attempts (3) |
| `attempts_one_rack` | attempts placed in one rack |

| outcome | when |
|---|---|
| `supported` | all three in one rack |
| `partial` | one or two |
| `not_supported` | none (an attempt that never got both pods scheduled does not count) |

**The odds.** With `n1` busy, the free nodes are `n2` (`rack-a`), `n3` and `n4` (`rack-b`). A scheduler
that picks two of them at random lands in one rack one time in three, so three of three by chance
happens about one run in 27. `topology-control` runs the same attempts without the constraint, on
vanilla, and shows what the scheduler does anyway; by the same odds the control itself varies from run
to run. A single `supported` is suggestive; only repeated runs separate the mechanism from luck.

**On slurm-bridge** the gang cannot ask for one rack, so the verdict is Slurm's default best-fit over
its switch tree (leaf switch `s1` for `rack-a`, `s2` for `rack-b`), not a requested constraint. Its
scheduler also ignores the filler's pin to `n1` and lets Slurm choose the node, which still leaves one
rack with two free nodes.

## Before you trust a run

Check these in `manifest.json` and the run's own files:

| check | where | what you want |
|---|---|---|
| the code that ran | `git_commit`, `git_dirty` | a commit you can name, and `git_dirty: false`; a dirty run ran code nobody can check out |
| the run is green | the last lines of each `<profile>/ginkgo.log`, and `make e2e`'s exit code | `SUCCESS!` and exit 0 |
| every probe you asked for has a row | `summary.md` | one row per capability and profile, and the controls under Controls |
| no watch trouble | `profiles.<profile>.watch_errors` | 0. A watch error or relist while a probe is open fails that probe; a nonzero count on a green run happened between probes, before the first or after the last |
| the clocks agree | `profiles.<profile>.clock_offset_start_s`, `clock_offset_end_s` | both between -1 and 1. The API server stamps whole seconds, so that is as close as the suite can tell. No check rejects a run on this value, and `clock_offset_end_s` is absent when the end check could not measure |
| the host | `host` | the Docker CPUs and memory you expected; a full run belongs on a VM ([quick-start.md](quick-start.md#laptop-or-vm)) |

## Comparing runs

One run is one sample. On a simulated fleet, timing varies from run to run, and a verdict near a
threshold (a 5 s grace, a one-in-three topology draw) can flip. Before saying a profile supports a
capability:

- run the same commit several times, with `git_dirty: false`, on the same class of host;
- report the outcome counts (for example, how many runs were `supported`) and the range of each
  measurement, not a single value;
- compare profiles by their spreads, and treat a difference as real only when it survives repetition.

## Rebuilding offline

```sh
make report RUN=results/<run-id> OUT=/tmp/scs-rebuild
```

`cmd/scs-report` reads `manifest.json` (for the run ID), and each profile's `events.jsonl`,
`probes.json` and `claims.yaml`, and writes `summary.md` and `<profile>/verdicts.json`. No cluster is
needed, and the output matches the run's own files byte for byte. At the end of every profile the
suite re-evaluates that profile's verdicts from the files on disk and fails if any differs from the
verdict the probe computed live. Without `OUT` it rewrites the files in the run directory. It exits 2 on a usage error and
1 on a run it cannot read, and writes nothing in that case.

Rebuilding is how a change to a verdict rule is checked against old evidence: rebuild an old run with
the new code into a fresh `OUT` and compare the two summaries. The claims used are the run's copy, not
the current `claims/`.
