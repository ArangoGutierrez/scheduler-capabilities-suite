#!/bin/bash
# Run the e2e suite once per profile, one cluster at a time.
#   hack/e2e.sh PROFILES RUN_ID RESULTS [LABEL_FILTER]
# PROFILES is "vanilla", "kueue", "kai", "slurm-bridge" or "all". LABEL_FILTER
# (a Ginkgo label expression such as "gang") runs only matching probes.
set -u
profiles=$1 run_id=$2 results=$3 filter=${4:-}
[ "$profiles" = all ] && profiles="vanilla kueue kai slurm-bridge"
# A run that cannot name its commit cannot be tied to its code: stop before
# anything is created.
commit=$(git rev-parse --short HEAD) || { echo "e2e: git cannot read this checkout (copy it as a git clone)" >&2; exit 2; }
status=$(git status --porcelain --untracked-files=normal) || { echo "e2e: git cannot read this checkout (copy it as a git clone)" >&2; exit 2; }
dirty=false
[ -z "$status" ] || dirty=true
mkdir -p "$results" && results=$(cd "$results" && pwd)
rc=0
# One Ctrl-C reaches the running suite, which cleans up and exits; the loop
# must then stop rather than create the next profile's cluster.
interrupted=
trap 'interrupted=1' INT
for p in $profiles; do
  out="$results/$run_id/$p"
  # tee and Ginkgo would overwrite an earlier run's log and report before
  # the suite's own guard runs.
  if [ -e "$out" ]; then
    echo "e2e: $out exists; choose another RUN_ID" >&2
    rc=2
    continue
  fi
  mkdir -p "$out"
  args=(--no-color -v --timeout=60m --json-report=ginkgo-report.json --output-dir="$out")
  [ -n "$filter" ] && args+=(--label-filter="$filter")
  SCS_PROFILE=$p SCS_RUN_ID=$run_id SCS_RESULTS=$results SCS_GIT_COMMIT=$commit SCS_GIT_DIRTY=$dirty \
    go tool ginkgo "${args[@]}" ./test/e2e 2>&1 | tee -i "$out/ginkgo.log"
  r=${PIPESTATUS[0]}
  if [ "$r" -ne 0 ]; then
    echo "e2e: profile $p failed (rc $r)" >&2
    rc=$r
  fi
  [ -n "$interrupted" ] && { echo "e2e: interrupted, not starting the next profile" >&2; break; }
done
echo "results: $results/$run_id"
exit "$rc"
