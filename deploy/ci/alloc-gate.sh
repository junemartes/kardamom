#!/usr/bin/env bash
# This CI gate checks for allocation regressions. It runs the three DHAT
# harnesses. It fails if allocs/op or bytes/op exceed the ceilings in
# perf/alloc-baselines.env. The script prints wall time for reference. It
# does not gate on wall time, because wall time depends on the machine.
set -euo pipefail
cd "$(dirname "$0")/../.."
source perf/alloc-baselines.env

run() { # <name> <dir> <test> <env...>
  local name=$1 dir=$2 test=$3; shift 3
  local raw="/tmp/alloc-gate-$name.log"
  # Keep the full cargo output, including stderr. A compile or runtime
  # failure must show in the log. An earlier version sent stderr to
  # /dev/null, so a broken build failed the job with no output.
  if ! (cd "$dir" && env "$@" cargo test --test "$test" --release -- --ignored --nocapture) >"$raw" 2>&1; then
    echo "== $name: HARNESS FAILED (cargo test exit != 0); last 40 lines:"
    tail -40 "$raw"
    return 1
  fi
  local out
  out=$(grep -E "allocs/(tx|op)|bytes/(tx|op)|wall/(tx|op)" "$raw" || true)
  if [[ -z "$out" ]]; then
    echo "== $name: NO MEASUREMENT LINES in harness output; last 40 lines:"
    tail -40 "$raw"
    return 1
  fi
  echo "== $name"; echo "$out"
  local allocs bytes
  allocs=$(echo "$out" | grep -E "allocs" | grep -oE "[0-9]+\.?[0-9]*" | head -1)
  bytes=$(echo "$out" | grep -E "bytes" | grep -oE "[0-9]+" | head -1)
  local max_a_var="${name^^}_MAX_ALLOCS" max_b_var="${name^^}_MAX_BYTES"
  local max_a=${!max_a_var} max_b=${!max_b_var}
  awk -v a="$allocs" -v ma="$max_a" -v b="$bytes" -v mb="$max_b" -v n="$name" 'BEGIN {
    bad = 0
    if (a+0 > ma+0) { printf "ALLOC REGRESSION: %s %.2f allocs/op > ceiling %.2f\n", n, a, ma; bad = 1 }
    if (b+0 > mb+0) { printf "ALLOC REGRESSION: %s %d bytes/op > ceiling %d\n", n, b, mb; bad = 1 }
    exit bad
  }'
}

run engine    crates/bench     alloc_profile         KARDAMOM_PROFILE_OPS=mix
run sequencer crates/sequencer alloc_profile
run ingress   crates/bench     alloc_profile_ingress
echo "alloc gate: PASS (ceilings: perf/alloc-baselines.env)"
