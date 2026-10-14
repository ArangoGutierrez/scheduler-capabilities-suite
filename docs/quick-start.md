# Quick start

This page takes you from a fresh clone to one recorded probe and an offline rebuild of its results.
A **probe** is one capability scenario (gang, priority, preemption, fair-share, backfill, topology) run
against one scheduler **profile** (`vanilla`, `kueue` or `kai`) on a fresh kind cluster.

## 1. Prerequisites

| need | check it with | what you want to see |
|---|---|---|
| Docker, running, reachable by your user | `docker info --format '{{.ServerVersion}}'` | a version, not an error |
| Go 1.27 or newer | `go version` | `go1.27` or later (`go.mod` says `go 1.27`) |
| make, bash, git | `make --version`, `bash --version`, `git --version` | any version |
| CPUs and memory given to Docker | `docker info --format '{{.NCPU}} CPUs, {{.MemTotal}} bytes'` | see below |

You do not need the kind CLI, kwok, Kueue, KAI, helm or Ginkgo installed: kind is used as a Go library,
Ginkgo runs as `go tool ginkgo`, and the kwok and Kueue manifests and the KAI chart render are embedded
in the repository. The first run pulls the pinned kind node image and the kwok, Kueue and KAI controller
images, so it needs network access to Docker Hub, `registry.k8s.io` and, for the kai profile, `ghcr.io`,
plus `proxy.golang.org` for Go modules.

The suite does not enforce a CPU or memory minimum. What has been measured: one probe at a time on a
laptop works; a full run of the vanilla and kueue profiles took about 15 minutes on a 12-vCPU, 22 GiB
Linux VM. The kai profile has not been timed yet. See [Laptop or VM](#laptop-or-vm) before a full run.

## 2. Clone and run the unit specs

```sh
git clone https://github.com/ArangoGutierrez/scheduler-capabilities-suite.git
cd scheduler-capabilities-suite
make test
```

`make test` runs every unit suite with no cluster: about 3 minutes on the first build, while Go compiles
the dependencies, and under a minute after that. It ends like this:

```
[...] verdict - 74/76 specs ••••••••••••••••••••••••••••••••SS•••••••••••••••••••••••••••••••••••••••••• SUCCESS! [...] PASS
[...] workload - 6/6 specs •••••• SUCCESS! [...] PASS

Ginkgo ran 13 suites in [...]
Test Suite Passed
```

The skipped specs (`S`) are the ones labeled `toolchain` and `live`; `make lint` runs the first, and
[options.md](options.md#environment-variables) explains the second.

## 3. Run one probe

```sh
make e2e SCHEDULER=vanilla FOCUS=gang
```

This takes about 4 minutes. It creates a kind cluster named `scs-vanilla`, installs kwok and a fleet of
four fake 8-GPU nodes, runs the gang probe, writes the results, and deletes the cluster. The output,
with timestamps and durations trimmed:

```
bash hack/e2e.sh "vanilla" "<run-id>" "<repo>/results" "gang"
Running Suite: e2e - <repo>/test/e2e
...
Will run 1 of 8 specs
------------------------------
[BeforeSuite]
  STEP: creating kind cluster scs-vanilla
  STEP: installing kwok and moving kind's DaemonSets off the fleet
  STEP: creating the fleet: 4 nodes, 32 GPUs
  STEP: installing the vanilla profile
  STEP: starting the recorder
[BeforeSuite] PASSED
------------------------------
probes gang [Serial, gang]
  verdict - ...
    {Capability:gang Outcome:<outcome> Reason:<reason> Measurements:map[gangs:2 gangs_completed:... max_partial_s:...] EvidenceSeq:[...]}
  ...
  gang on vanilla: <outcome> (<reason>)
•
------------------------------
SSSSSSS
------------------------------
[AfterSuite] PASSED
------------------------------
[DeferCleanup (Suite)]
  STEP: exporting cluster logs
[DeferCleanup (Suite)] PASSED
...
Ran 1 of 8 Specs in [...] seconds
SUCCESS! -- 1 Passed | 0 Failed | 0 Pending | 7 Skipped
PASS

Ginkgo ran 1 suite in [...]
Test Suite Passed
results: <repo>/results/<run-id>
```

`SUCCESS` means the probe ran cleanly and recorded a verdict, whatever that verdict is. The last line
names the run directory. The run ID is the UTC start time, the short commit, and `-dirty` when the
checkout has uncommitted or untracked changes, for example `20260101T120000Z-abc1234-dirty`.

`FOCUS` takes a Ginkgo label filter; [options.md](options.md#probe-labels-for-focus) lists the labels.

## 4. Look at the results, then rebuild them

```
results/<run-id>/
  manifest.json      what ran: commit, host, tool versions, fleet, cluster settings
  summary.md         documented vs observed, per capability and profile
  vanilla/
    events.jsonl     every recorded object change: the source of truth
    probes.json      each probe's parameters and the end of its evidence window
    claims.yaml      the documented claims, copied from claims/vanilla.yaml
    verdicts.json    each probe's verdict and measurements
    ginkgo.log, ginkgo-report.json, cluster-logs/, scs.kubeconfig
```

Read `summary.md` first; [results.md](results.md) explains each column. Then rebuild the verdicts and
the summary from the recorded events alone, with no cluster:

```sh
make report RUN=results/<run-id> OUT=/tmp/scs-rebuild
```

It prints the command it runs (`go run ./cmd/scs-report -out /tmp/scs-rebuild results/<run-id>`) and
writes `/tmp/scs-rebuild/summary.md` and `/tmp/scs-rebuild/vanilla/verdicts.json`. They match the run's
own files byte for byte:

```sh
cmp results/<run-id>/summary.md /tmp/scs-rebuild/summary.md && echo identical
```

Without `OUT`, `make report` rewrites the files inside the run directory.

## 5. A full run

```sh
make e2e
```

With no `FOCUS` and the default `SCHEDULER=all`, every probe runs on `vanilla`, then on `kueue`, then on
`kai`, one cluster at a time (`scs-vanilla`, `scs-kueue`, `scs-kai`), into one run directory. Kueue's
first install waits one to four minutes for its webhook to answer.

### Laptop or VM

- **One probe at a time on a laptop is fine.**
- **One `make e2e` at a time per host.** The cluster lock refuses a second run of the same profile, but
  not a run of another profile, and two clusters side by side compete for CPU and disk.
- **A full run belongs on a Linux VM.** On a laptop an etcd fsync stall (2.3 s) once restarted the
  scheduler, the controller-manager and Kueue mid-run. The suite treats a control-plane restart during a
  probe as a harness failure, so such a run is red, not wrong, but it is wasted.

To prepare a Linux VM (amd64 or arm64):

```sh
bash hack/vm-bootstrap.sh
export PATH=$HOME/.local/go/bin:$PATH
```

`vm-bootstrap.sh` checks that `docker`, `git`, `make`, `curl`, `tar` and `sha256sum` exist and that your
user can reach the Docker daemon, installs Go 1.27.2 (verified by sha256) under `$PREFIX/go`
(`PREFIX` defaults to `$HOME/.local`), checks that `registry-1.docker.io`, `registry.k8s.io`,
`github.com`, `raw.githubusercontent.com` and `proxy.golang.org` answer over HTTPS, and prints the
`export PATH=...` line to run next. It does not check the hosts those redirect to (for example
`auth.docker.io` or `cdn.registry.k8s.io`), so an image pull can still fail behind a strict egress
allowlist.

A full run of the vanilla and kueue profiles takes about 15 minutes on a 12-vCPU, 22 GiB VM; the kai
profile has not been timed yet. Run it under `tmux` or `nohup`: closing the terminal kills it without
cleanup.

## 6. Interrupting and cleaning up

- **Ctrl-C once, then wait.** The probe's cleanup, the cluster log export and the cluster delete still
  run. With `SCHEDULER=all`, the next profile does not start after an interrupt.
- **A second Ctrl-C** skips cleanup (Ginkgo prints "Interrupt again to skip cleanup") and leaves the
  `scs-<profile>` cluster behind.
- **Closing the terminal (SIGHUP) or `kill -9`** gets no cleanup at all: Ginkgo handles only SIGINT
  and SIGTERM.

To keep a cluster on purpose, for debugging: `KEEP_CLUSTER=1 make e2e ...` keeps it always, and
`KEEP_CLUSTER_ON_FAILURE=1 make e2e ...` keeps it only when the run fails. The suite prints the cluster
name and its kubeconfig path (`results/<run-id>/<profile>/scs.kubeconfig`).

To remove a cluster the suite left behind:

```sh
kind delete cluster --name scs-vanilla                    # with the kind CLI
go run sigs.k8s.io/kind delete cluster --name scs-vanilla # without it, from this checkout
```

Only delete `scs-vanilla`, `scs-kueue` or `scs-kai`; the suite never touches any other kind cluster.

## 7. Troubleshooting

**`kind cluster "scs-vanilla" already exists; the suite does not reuse or delete it.`**
A cluster of that name is left over from an interrupted run, or was kept with `KEEP_CLUSTER`. The
message ends with the delete command; run it (see above) and start again.

**`another suite run holds /tmp/scs-vanilla.lock; the suite runs one cluster per profile at a time`**
Another `make e2e` on this host is using that profile's cluster. Wait for it to finish. The lock is
released when that run deletes its cluster, or when its process exits, however it exits.

**`e2e: <dir> exists; choose another RUN_ID`** or **`... already holds events.jsonl from an earlier run;
choose another run ID`**
A run directory is never overwritten. Leave `RUN_ID` unset (it defaults to a fresh timestamp) or pick a
new one.

**The kueue profile sits at "installing the kueue profile" for minutes.**
That is Kueue's webhook coming up: one to four minutes on a first install, and the suite allows eight
before it fails with `kueue webhook not ready after 8m0s: <last error>`. The wait it took is recorded in
`manifest.json` as `profiles.kueue.scheduler.kueue_webhook_wait`.

**The kai profile sits at "installing the kai profile" for minutes.**
KAI's operator creates the scheduler, binder, admission and queue controllers, and their webhooks, after
the chart's objects are applied, so the suite waits until those Deployments are available and the
webhooks answer a dry-run Queue, PodGroup and Pod. It allows eight minutes before it fails with
`kai not ready after 8m0s: <last error>`. The wait it took is recorded in `manifest.json` as
`profiles.kai.scheduler.kai_ready_wait`.

**`kwok: controller not available: ...`, or the fleet never becomes ready.**
The suite allows 6 minutes for the kwok controller to become available and 2 minutes for the four fake
nodes to report Ready with their GPUs. A timeout here is usually a slow or failed image pull
(`registry.k8s.io/kwok/kwok:v0.8.0`) or a Docker VM short on CPU or memory. Look in
`results/<run-id>/<profile>/cluster-logs/` (kind's log export, including each pod's logs), give Docker
more resources, or move to a VM.

**`Will run 0 of 8 specs`, `SUCCESS!`, and no `summary.md`.**
`FOCUS` matched no probe label, so nothing ran; the exit code is still 0. Check the spelling against
[the labels](options.md#probe-labels-for-focus) (for example `fair-share`, not `fairshare`).

**`e2e: git cannot read this checkout (copy it as a git clone)`**
The run records the commit it ran, so it needs a git checkout, not a plain copy of the files.

**`SCS_GIT_COMMIT is empty: run the suite through make e2e, which records the commit`**
You ran `go test ./test/e2e` or `ginkgo` directly. Use `make e2e`, which sets the suite's `SCS_*`
variables.
