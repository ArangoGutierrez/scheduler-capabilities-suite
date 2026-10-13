# Scheduling Capabilities Suite

Measures what a Kubernetes scheduler does with AI-style workloads, as opposed to what its documentation
claims. Feature tables say "supports gang scheduling"; this suite submits two gangs that cannot both fit
and records what actually happens to every pod.

Each **probe** runs one capability scenario on a simulated GPU fleet (a kind cluster with four kwok nodes
of 8 GPUs each), records every object change to `events.jsonl`, turns those events into an observed
verdict (`supported`, `partial` or `not_supported`), and compares it with the scheduler's documented
**claim**.

| capability | scenario (4 nodes x 8 GPUs) |
|---|---|
| gang | two 3-pod gangs that cannot both fit |
| priority | a high-priority pod arrives behind four queued low-priority pods |
| preemption | a high-priority job needs room held by a low-priority gang |
| fair-share | tenant A floods the fleet; tenant B, entitled to half, arrives 10 s later |
| backfill | short jobs arrive while a fleet-wide gang waits for capacity |
| topology | a 2-pod gang must sit in one rack while the other rack has a busy node |

Two profiles are measured: **vanilla** (kube-scheduler 1.37 with the Workload API) and **kueue**
(Kueue v0.20.1 in front of kube-scheduler).

## Status

Proof of concept. Every verdict comes from a single run: it is evidence that the harness works, not a
result to quote. Repeated runs and a spread come before any number is published.

## Requirements

- Docker
- Go 1.27
- make, bash and git

kind (as a Go library), Ginkgo, the kwok manifests and the Kueue manifests are pinned in this repository;
the node and controller images are pulled on first use. [docs/quick-start.md](docs/quick-start.md) has a
check command for each requirement.

## Quick start

```sh
make test                                                 # unit specs, no cluster, under a minute
make e2e SCHEDULER=vanilla FOCUS=gang                     # one probe on one profile, about 4 minutes
make report RUN=results/<run-id> OUT=/tmp/scs-rebuild     # rebuild verdicts and summary offline
```

Each run writes `results/<run-id>/summary.md`. This one is rendered from the committed test fixture
(`cmd/scs-report/testdata/summary.md`), not from a real run:

```markdown
| capability | profile | documented | observed | agree | evidence |
|---|---|---|---|---|---|
| gang | vanilla | supported | supported | yes | no gang was partly placed for more than 5 s, and all 2 gangs completed |
| priority | vanilla | supported | not_supported | no | all 1 queued lower-priority jobs were scheduled before the high-priority job |

## Findings

- vanilla, priority: documented supported, observed not_supported (all 1 queued lower-priority jobs were scheduled before the high-priority job).

## Controls

- vanilla, gang-control: partial (gang gang-b was partly placed for 21.5 s; all 2 gangs completed).
```

A disagreement between documented and observed is a **finding**: a result, not a failure.

## Documentation

| document | read it to |
|---|---|
| [docs/quick-start.md](docs/quick-start.md) | run the suite for the first time, clean up, fix common problems |
| [docs/architecture.md](docs/architecture.md) | see how a probe flows from cluster to verdict, and the design rules |
| [docs/options.md](docs/options.md) | look up make targets, variables, labels, probe parameters, claims files and pins |
| [docs/results.md](docs/results.md) | read a run's results and know what each verdict can and cannot claim |
| [docs/data-format.md](docs/data-format.md) | look up every file and field a run writes |
| [CONTRIBUTING.md](CONTRIBUTING.md) | set up, test, commit, and add a probe or a scheduler profile |

## Repository layout

```
claims/            documented claims per profile, with citations
cmd/scs-report/    offline rebuild of verdicts and summary from a recorded run
hack/              e2e.sh (runs the suite per profile), vm-bootstrap.sh (prepares a Linux VM)
pkg/               the suite's packages; docs/architecture.md has one line on each
test/e2e/          the Ginkgo suite: one file per probe, plus the suite and harness
docs/              the documentation above
```

## License

MIT; see [LICENSE](LICENSE).
