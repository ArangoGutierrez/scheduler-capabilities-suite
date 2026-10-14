# Contributing

Read [docs/architecture.md](docs/architecture.md) first: it names the packages and the design rules a
change must keep.

## Development setup

You need Go 1.27, make, bash and git; Docker as well for `make e2e`.
[docs/quick-start.md](docs/quick-start.md) has a check command for each. Ginkgo is a `tool` dependency
in `go.mod`, so `go tool ginkgo` needs no separate install.

## Checks before a commit

```sh
make test   # every unit suite, no cluster; about 3 minutes on the first build, under a minute after
make lint   # go vet, gofmt, and the verdict package's no-fused-multiply-add check
```

Both must pass. Run one live probe (`make e2e SCHEDULER=<profile> FOCUS=<label>`) when you change a
probe, a profile, the recorder or the cluster setup; unit specs cannot see those seams. Run a full
`make e2e` on a Linux VM, not on a laptop ([quick-start.md](docs/quick-start.md#laptop-or-vm)).

Specs use Ginkgo v2 and Gomega, like the rest of the repository.

## Commits

- Conventional format: `type(scope): description`, with the scope naming the package or area, for
  example `fix(verdict): ...` or `docs(results): ...`.
- The body says **why**. The diff already shows what changed.
- Every commit is signed off and signed: `git commit -s -S`.
- One concern per commit.

## The test bar

A spec exists to catch a specific bug. Before you call a spec done:

- **It must fail when the rule is broken.** Break the code it covers (delete the guard, flip the
  comparison, change the threshold), run the spec, see it go red, then restore the code. A spec that
  stays green is not testing anything; rewrite it.
- **Assert the exact value**: the exact outcome, the exact reason string, the exact error message, a
  literal number. Not "an error occurred", not a prefix.
- **Cover the edges and the error paths**: each outcome of a verdict rule, each `validate` error, the
  boundary values (exactly at the grace, one past it).
- **Do not repeat the implementation in the test.** Expected values come from the scenario worked out
  by hand, not from calling the code's own helpers.

Verdict rules have two more rules of their own:

- **Pure.** No clock, no randomness, no I/O: the same events and parameters must give the same verdict,
  or offline rebuilds break. Read times only from the events.
- **No fused multiply-add.** Wrap every product that reaches an addition or subtraction in an explicit
  `float64(...)`, as `pkg/verdict/fairshare.go` does. Go may otherwise fuse it on arm64, and a run on
  arm64 and its rebuild on amd64 would differ in the last bit. `make lint` checks the compiled package
  for both architectures.

## Adding a probe

A probe is a scenario in `test/e2e/` and a verdict rule in `pkg/verdict/`. For a new capability
`<cap>`:

1. **The rule**, in `pkg/verdict/<cap>.go`: a `<Cap>Params` struct with JSON tags (this is what
   `probes.json` stores), a `validate` method, and a function `(Timeline, <Cap>Params) (Verdict, error)`
   that sets `Outcome`, `Reason`, `Measurements` and `EvidenceSeq`. Return an error, not a verdict, when
   the events show the scenario was not exercised; that makes the run red.
2. **Its specs**, in `pkg/verdict/<cap>_test.go`, built with the fixture helpers in
   `pkg/verdict/fixture_test.go`: every outcome and every error, each mutation-checked as above.
3. **Registration**, in `pkg/verdict/evaluate.go`: a `Cap<Cap>` constant, the capability in
   `Capabilities` (this sets its row order in `summary.md`), and a `case` in `evaluate` that decodes the
   parameters and calls the rule.
4. **The claims**: an entry for `<cap>` in `claims/vanilla.yaml` and `claims/kueue.yaml`, quoted from the
   scheduler's documentation with an https citation. The loader requires every capability, so also add
   it to the claims YAML in the specs and fixtures: `pkg/claims/claims_test.go`,
   `pkg/report/report_test.go` and `cmd/scs-report/testdata/run/vanilla/claims.yaml`. A run recorded
   before the change has no claim for `<cap>`, so rebuild it with the commit that recorded it.
5. **The scenario**, in `test/e2e/<cap>_test.go`: `var _ = register(<order>, <fn>)` with an order not yet
   used (the existing probes use 10 to 60; a duplicate panics), and one
   `It("<cap>", Label("<cap>"), ..., SpecTimeout(...))` that calls `startProbe` or `startScenario`,
   `submit`, `mustAwait` for preconditions, `await` for the outcome, and `finish(ctx, verdict.Cap<Cap>,
   params)`. The harness deletes the probe's namespaces afterwards, and the next probe refuses to start
   on a fleet that still has suite pods.
6. **Job features**: if the scenario needs something `workload.Job` cannot express, add the field and
   its check to `pkg/workload/workload.go`, then teach each profile's `Submit` (one package per
   profile under `pkg/scheduler/`: `vanilla`, `kueue` and `kai`).
7. **Docs**: the label and parameters in [docs/options.md](docs/options.md); the measurements and
   what the verdict can claim in [docs/results.md](docs/results.md); the probe order in step 5 of
   [docs/architecture.md](docs/architecture.md#data-flow); and, for a capability, its row in the
   capability table of the [README](README.md).

A **control** is added the same way, except that it gets a constant but no place in `Capabilities` and
no claim, and its spec skips the profiles it does not apply to (see `gang-control` in
`test/e2e/gang_test.go`).

## Adding a scheduler profile

For a new profile `<name>`:

1. **The profile**, in `pkg/scheduler/<name>/<name>.go`: a type that implements `scheduler.Profile`
   (`pkg/scheduler/scheduler.go`): `Name`, `ClaimsFile`, `Install`, `Prepare`, `Submit`, `Remove`,
   `Cleanup` and `WorkloadGVR`. Embed any release manifest and pin it by sha256, as
   `pkg/scheduler/kueue` does with `manifests/SHA256SUMS`. Return from `Install` what the run manifest
   should record (versions, sha256s, settings).
2. **Its specs**, in `pkg/scheduler/<name>/<name>_test.go`, including one that loads its claims file.
3. **The claims**: `claims/<name>.yaml`, every capability, each quoted and cited
   ([docs/options.md](docs/options.md#claims-files)).
4. **Wiring**:
   - `test/e2e/suite_test.go`: add the constructor to `profiles`;
   - `pkg/report/report.go`: add the name to `Profiles`, or `summary.md` will never show it;
   - `hack/e2e.sh`: add it to the list `all` expands to;
   - the `SCHEDULER` comment in the `Makefile`.
5. **Its system namespace**: if the scheduler runs its own controllers, add their namespace next to
   `kueue-system` in `controlPlaneRestarts` (`test/e2e/harness_test.go`), so a restart invalidates the
   probe, and in `images` (`test/e2e/suite_test.go`), so the manifest records their image digests.
6. **Its workload object**: `WorkloadGVR` makes the recorder watch one more resource, and
   `pkg/recorder/workload.go` reads it with Kueue's Workload conditions. A scheduler with a different
   object needs its own recording in `pkg/recorder`, and maybe a new `kind` in
   [docs/data-format.md](docs/data-format.md).
7. **Docs**: the profile in the README and in [docs/options.md](docs/options.md).

Run every probe on the new profile once, on a VM, before opening the pull request.
