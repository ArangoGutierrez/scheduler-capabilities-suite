# Options reference

## Make targets

| target | what it runs |
|---|---|
| `make test` | every unit suite, no cluster: `go tool ginkgo --no-color -r --skip-package=e2e --label-filter='!toolchain && !live'` |
| `make lint` | `go vet ./...`, `gofmt -l .` (fails if it lists any file), and the `toolchain` specs of `pkg/verdict`, which compile the package for arm64 and for amd64 with `GOAMD64=v3` and fail on any fused multiply-add instruction |
| `make e2e` | `hack/e2e.sh "$(SCHEDULER)" "$(RUN_ID)" "$(RESULTS)" "$(FOCUS)"`: the e2e suite once per profile, one cluster at a time within one invocation |
| `make report` | `go run ./cmd/scs-report [-out $(OUT)] $(RUN)`: rebuild `verdicts.json` and `summary.md` from a recorded run |

## Make variables

| variable | default | used by | meaning |
|---|---|---|---|
| `SCHEDULER` | `all` | e2e | `vanilla`, `kueue`, `kai`, `slurm-bridge`, or `all` (vanilla, then kueue, then kai, then slurm-bridge) |
| `FOCUS` | empty (every probe) | e2e | a Ginkgo label filter expression; see [the labels](#probe-labels-for-focus) |
| `RESULTS` | `$(CURDIR)/results` | e2e | parent directory of run directories; made absolute by `hack/e2e.sh` |
| `RUN_ID` | `<UTC time %Y%m%dT%H%M%SZ>-<short commit>`, plus `-dirty` when `git status --porcelain --untracked-files=normal` is not empty | e2e | name of the run directory under `RESULTS` |
| `RUN` | none, required | report | the run directory to rebuild, for example `results/<run-id>` |
| `OUT` | empty: rewrite inside `RUN` | report | directory to write the rebuilt files into |
| `GO` | `go` | test, lint, report | the go command; `hack/e2e.sh` calls `go` from `PATH` directly |

`hack/e2e.sh` adds these Ginkgo flags for each profile: `--no-color -v --timeout=60m
--json-report=ginkgo-report.json --output-dir=<run dir>/<profile>`, plus `--label-filter=$(FOCUS)` when
`FOCUS` is set. It stops before creating anything if git cannot read the checkout (exit 2), and skips a
profile whose `<run dir>/<profile>` already exists (exit 2 at the end). After a Ctrl-C it does not start
the next profile. Its output is also saved to `<run dir>/<profile>/ginkgo.log`.

## Environment variables

| variable | read by | meaning |
|---|---|---|
| `KEEP_CLUSTER=1` | `test/e2e/suite_test.go` | keep the kind cluster after the run |
| `KEEP_CLUSTER_ON_FAILURE=1` | `test/e2e/suite_test.go` | keep it only when the run failed, including a failure during setup or in the end-of-run report |
| `SCS_PROFILE`, `SCS_RUN_ID`, `SCS_RESULTS`, `SCS_GIT_COMMIT`, `SCS_GIT_DIRTY` | `test/e2e/suite_test.go` | set by `hack/e2e.sh`; do not set them by hand. The suite refuses to start without a profile it knows, a run ID, a commit, and an absolute results path |
| `SCS_TEST_KUBECONFIG` | `pkg/apply/apply_test.go` | kubeconfig for the one `live` spec, whose current context must be a `kind-scs-*` cluster. Run it with `SCS_TEST_KUBECONFIG=<path> go tool ginkgo --label-filter=live ./pkg/apply`; without the variable it skips |
| `PREFIX` | `hack/vm-bootstrap.sh` | where Go is installed (`$PREFIX/go`); default `$HOME/.local`; `/` and empty are refused |

A kept cluster is not deleted by the next run: the next run refuses to start until you delete it
([quick-start.md](quick-start.md#6-interrupting-and-cleaning-up)).

## Probe labels for FOCUS

Each probe is one Ginkgo spec with one label. The probes run in this order:

| label | file | profiles |
|---|---|---|
| `gang` | `test/e2e/gang_test.go` | all |
| `gang-control` | `test/e2e/gang_test.go` | vanilla only (skipped on kueue, kai and slurm-bridge) |
| `priority` | `test/e2e/priority_test.go` | all |
| `preemption` | `test/e2e/preemption_test.go` | all |
| `fair-share` | `test/e2e/fairshare_test.go` | all |
| `backfill` | `test/e2e/backfill_test.go` | all |
| `topology` | `test/e2e/topology_test.go` | all |
| `topology-control` | `test/e2e/topology_test.go` | vanilla only (skipped on kueue, kai and slurm-bridge) |

`FOCUS` takes any [Ginkgo label filter](https://onsi.github.io/ginkgo/#spec-labels) expression, for
example:

```sh
make e2e SCHEDULER=vanilla FOCUS=gang                     # gang only, not gang-control
make e2e SCHEDULER=vanilla FOCUS='gang || gang-control'   # the probe and its control
make e2e SCHEDULER=kueue FOCUS='!topology'                # every probe except topology
```

A `FOCUS` that matches no label (a typo such as `fairshare`) runs nothing and still exits 0: Ginkgo
prints `Will run 0 of 8 specs`, skips the suite setup (no cluster is created), and the run directory
gets only `ginkgo.log` and `ginkgo-report.json`, no `summary.md`. Check the `Will run N of 8 specs`
line.

## Probe parameters

The fleet is 4 nodes x 8 GPUs (32 GPUs), in two racks of two. Every job is low priority unless the
table says otherwise, and every pod asks for 8 GPUs, a whole node. The parameters column is what the
probe writes to `probes.json` and what the verdict rule reads.

| probe | scenario | parameters (`probes.json`) |
|---|---|---|
| `gang`, `gang-control` | two 3-pod jobs, `gang-a` and `gang-b`, 20 s each; 48 GPUs asked of 32. `gang` makes them gangs, the control does not. The probe waits up to 150 s for both to complete | `jobs: [gang-a, gang-b]`, `size: 3`, `grace_sec: 5` |
| `priority` | four 1-pod fillers (30, 40, 50, 60 s) take the fleet; four 1-pod jobs `low-1`..`low-4` (30 s) queue behind them; 2 s later a 1-pod high-priority job `high` (30 s). Waits up to 150 s for `high` to be scheduled | `high_job: high`, `queued_jobs: [low-1, low-2, low-3, low-4]` |
| `preemption` | a 4-pod gang `victim` (300 s, evicted as a whole if preempted) takes the fleet; 2 s later a 2-pod high-priority job `high` that may preempt. Waits up to 90 s for both `high` pods to be scheduled, then, if they were, 6 s more | `high_job: high`, `high_pods: 2`, `victim_job: victim`, `grace_sec: 5` |
| `fair-share` | tenants `a` and `b`, each entitled to 16 GPUs. Tenant a submits eight 1-pod jobs (60 s): four fill the fleet and four queue; 10 s after a's first submission, tenant b submits four. The probe ends 70 s after b's first submission | `tenant_b: b`, `entitlement_gpus: 16`, `job_length_sec: 60`, `tolerance_sec: 5`, `capacity_gpus: 32`, `window_sec: 10` |
| `backfill` | three 1-pod blockers (60 s) hold three nodes; 1 s later a 4-pod gang `big` (20 s) that needs the whole fleet; then four 1-pod jobs `short-1`..`short-4` (25 s), one a second. Waits up to 3 minutes for `big` to be scheduled | `big_job: big`, `big_pods: 4`, `blocker_jobs: [blocker-1, blocker-2, blocker-3]`, `small_jobs: [short-1, short-2, short-3, short-4]`, `tolerance_sec: 5` |
| `topology`, `topology-control` | a 1-pod filler (300 s) pinned to `n1` in `rack-a`; then three 2-pod gangs `topo-1`..`topo-3` (20 s), one at a time, each removed after it is placed or after 45 s. `topology` asks for one rack; the control does not | `attempt_jobs: [topo-1, topo-2, topo-3]`, `pods_per_job: 2`, `rack_of: {n1: rack-a, n2: rack-a, n3: rack-b, n4: rack-b}` |

Each probe has a spec timeout of 8 minutes (backfill: 10 minutes), and the whole e2e run has a 60-minute
Ginkgo timeout. [results.md](results.md#per-capability) explains what each rule does with these
parameters.

## Claims files

`claims/vanilla.yaml`, `claims/kueue.yaml`, `claims/kai.yaml` and `claims/slurm-bridge.yaml` hold each
profile's documented claims.
The format:

```yaml
scheduler: vanilla                    # required
version: Kubernetes v1.37.0 (...)     # required, free text
claims:
  gang:                               # one entry per capability, all six required
    claim: '...'                      # quoted from the cited page, not empty
    citation: https://...             # must start with https://
    expected: supported               # supported | partial | not_supported
    mechanism: ...                    # how the profile exercises it, not empty
```

The six capabilities are `gang`, `priority`, `preemption`, `fair-share`, `backfill` and `topology`.
Controls have no claim. The loader (`pkg/claims`) rejects an unknown field, an unknown or missing
capability, a non-https citation, an empty claim or mechanism, and any `expected` outside the three
outcomes. `expected` is the outcome the cited documentation implies under the suite's rule; the run's
`agree` column compares it with the observed outcome.

At the end of each profile the suite copies the claims file into `<run dir>/<profile>/claims.yaml`.
`make report` uses that copy, so an edit to `claims/` changes the next run, not old ones.

## Pinned versions

| what | pin | where |
|---|---|---|
| kind node image (Kubernetes v1.37.0, amd64 and arm64) | `kindest/node:v1.37.0@sha256:a1ed56cf...` | `NodeImage` in `pkg/cluster/cluster.go` |
| kwok v0.8.0 | embedded manifests, each checked against `SHA256SUMS` at install; image `registry.k8s.io/kwok/kwok:v0.8.0` | `pkg/kwok/kwok.go`, `pkg/kwok/manifests/` |
| Kueue v0.20.1 | embedded release manifest checked against `SHA256SUMS`; image `registry.k8s.io/kueue/kueue:v0.20.1` | `Version` in `pkg/scheduler/kueue/kueue.go`, `pkg/scheduler/kueue/manifests/` |
| KAI Scheduler v0.18.5 | embedded render of chart `oci://ghcr.io/kai-scheduler/kai-scheduler/kai-scheduler:v0.18.5` (default values) checked against `SHA256SUMS`; images `ghcr.io/kai-scheduler/kai-scheduler/*:v0.18.5` | `Version`, `ChartOCIDigest` and `ChartSHA256` in `pkg/scheduler/kai/kai.go`, `pkg/scheduler/kai/manifests/` |
| slurm-bridge v1.2.3, slurm-operator v1.2.3, Slurm 26.05.4 | embedded renders of the charts `oci://ghcr.io/slinkyproject/charts/{slurm-operator-crds,slurm-operator,slurm,slurm-bridge}:1.2.3` (helm v4.3.0, values in `manifests/values/`, images pinned there by digest), the scheduler-plugins v0.33.5 PodGroup CRD and slurm-bridge's `hack/token.yaml`, each checked against `SHA256SUMS`; no key or certificate is embedded: Install generates `slurm.key` and `jwt.key` | `Version`, `SlurmVersion`, `Files` and `Sources` in `pkg/scheduler/slurmbridge/slurmbridge.go`, `pkg/scheduler/slurmbridge/manifests/` |
| kind library, client-go, Ginkgo, Gomega | `sigs.k8s.io/kind v0.33.0`, `k8s.io/client-go v0.37.1`, `ginkgo/v2 v2.33.0`, `gomega v1.44.0` | `go.mod` (Ginkgo is a `tool` dependency) |
| Go | `go 1.27` | `go.mod`; `hack/vm-bootstrap.sh` installs 1.27.2 |

Every run records the versions it actually used in `manifest.json`: tool versions, the image ID of
every container in `kube-system`, `kueue-system`, `kai-scheduler`, `slinky` and `slurm`, each kwok
manifest's sha256, and the Kueue manifest's, the KAI render's or each slurm-bridge file's sha256.
