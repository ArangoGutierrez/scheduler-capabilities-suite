# Run data format, version 1

Every run writes one directory, `results/<run-id>/`. `events.jsonl` is the source of truth. `verdicts.json`
and `summary.md` are derived from it, `probes.json` and `claims.yaml`, and `cmd/scs-report` rebuilds
those two byte for byte (`make report RUN=results/<run-id> OUT=<dir>`). [results.md](results.md)
explains how to read them.

```
results/<run-id>/
  manifest.json          run ID, git commit, host, tool versions, fleet, and per profile: cluster,
                         feature gates, runtime config, image digests, kwok manifest sha256s, scheduler
                         settings (Kueue: manifest sha256, webhook wait), host-to-API-server clock
                         offset at start and end, watch errors
  summary.md             capability x profile: documented | observed | agree | evidence; then
                         Findings, Controls, and profiles without verdicts
  <profile>/             vanilla or kueue
    events.jsonl         one line per observed change (below)
    probes.json          each probe: capability, parameters, end of its evidence window
    claims.yaml          the profile's documented claims, copied from claims/ at run time
    verdicts.json        per probe: documented claim, observed verdict, agree flag (a control has
                         no claim and no agree flag)
    ginkgo-report.json   Ginkgo's JSON report; each probe attaches its verdict and its measurements
    ginkgo.log           the suite's output
    cluster-logs/        kind export logs: scheduler, controller-manager, kwok, Kueue, kubelet
    scs.kubeconfig       the kind cluster's kubeconfig; kind empties it when it deletes the cluster
```

## events.jsonl

One JSON object per line. `seq` counts from 1 without gaps; a reader must refuse a gap, an unknown
field, an unknown transition or a schema other than 1 (`pkg/events`).

| field | meaning |
|---|---|
| `schema` | 1 |
| `seq` | position in the file, from 1 |
| `run_id` | the run ID |
| `observed_at` | when the recorder wrote the change, RFC 3339 with nanoseconds, host clock; `seq` and `observed_at` rise together; the reader rejects a file where `observed_at` falls |
| `api_time` | the object's own timestamp for the change, when it has one |
| `api_time_precision` | `s` (pod and Job conditions, container states, creation and deletion times, an Event with only its deprecated timestamp) or `us` (`events.k8s.io` Event time); absent when there is no `api_time` |
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
  `Evicted`, about a Pod or Workload in a namespace the suite created. Events with other reasons are not kept, and the suite does not export Event objects; the cluster logs hold only what each component logs.
- **Workload** (Kueue, `workloads.kueue.x-k8s.io` v1beta2): created; quota_reserved, admitted, preempted,
  evicted, requeued when the condition of that name turns True; deleted.

## How verdicts read the events

`pkg/verdict` uses `observed_at` throughout: one clock, nanosecond resolution. A probe's evidence is its
`probe` label's events up to the window end in `probes.json`, so cleanup after the window cannot change a
verdict. The rules are in `pkg/verdict`; [results.md](results.md#per-capability) describes each one.

## Limits

- Timestamps compare within one run only. `manifest.json` records the host-versus-cluster clock offset at the start and end of each profile (`clock_offset_start_s`, `clock_offset_end_s`), to about a second; no check rejects a run on it. `clock_offset_end_s` is absent when the end-of-run check failed to measure.
- A recorder watch error or relist, or a control-plane container restart, during a probe makes that probe a harness failure, not a verdict. `watch_errors` in `manifest.json` counts both kinds over the whole profile.
- Every number in a run comes from a single run. It is evidence for a verdict, not a result to quote.
