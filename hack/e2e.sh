#!/bin/bash
# Run the e2e suite once per profile, one cluster at a time.
#   hack/e2e.sh PROFILES RUN_ID RESULTS [LABEL_FILTER]
# PROFILES is "vanilla", "kueue" or "all". LABEL_FILTER (a Ginkgo label
# expression such as "gang") runs only matching probes.
set -u
profiles=$1 run_id=$2 results=$3 filter=${4:-}
[ "$profiles" = all ] && profiles="vanilla kueue"
mkdir -p "$results" && results=$(cd "$results" && pwd)
commit=$(git rev-parse --short HEAD)
dirty=false
git diff --quiet HEAD -- || dirty=true
rc=0
for p in $profiles; do
  out="$results/$run_id/$p"
  mkdir -p "$out"
  args=(--no-color -v --timeout=60m --json-report=ginkgo-report.json --output-dir="$out")
  [ -n "$filter" ] && args+=(--label-filter="$filter")
  SCS_PROFILE=$p SCS_RUN_ID=$run_id SCS_RESULTS=$results SCS_GIT_COMMIT=$commit SCS_GIT_DIRTY=$dirty \
    go tool ginkgo "${args[@]}" ./test/e2e 2>&1 | tee "$out/ginkgo.log"
  r=${PIPESTATUS[0]}
  if [ "$r" -ne 0 ]; then
    echo "e2e: profile $p failed (rc $r)" >&2
    rc=$r
  fi
done
echo "results: $results/$run_id"
exit "$rc"
