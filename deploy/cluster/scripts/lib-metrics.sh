# shellcheck shell=bash
# =============================================================================
# lib-metrics.sh — shared Prometheus scrape + parse helpers.
# =============================================================================
# This file is sourced, never run directly, by chaos.sh (through
# chaos-probes.sh), ci-cluster.sh, and smoke-load.sh. It replaces 10+
# per-script scrape and parse sites. These sites had drifted into three
# different awk metric-match patterns:
#     chaos.sh       '"[{ ]"'        (val_metric/seqa_metric — matches most
#                                     samples, but not a label-less bare-name
#                                     sample line)
#     ci-cluster.sh  '"([{ ])"'      (same limit)
#     smoke-load.sh  '"([{ ]|$)"'    (also matches label-less samples)
# This file uses '"([{ ]|$)"' everywhere. This pattern also matches
# label-less samples, so every consumer parses the exposition format the
# same way.
#
# This file does not depend on lib.sh, and defines no log() or fail().
# smoke-load.sh sources this file while keeping its own "RESULT: FAIL" fail
# contract.

# Fetch one /metrics body. Try the bridge IP directly first, then fall back
# to docker exec.
#   $1 = bridge ip   ("" skips the direct probe: exporter is loopback-only)
#   $2 = node container ("" skips the docker-exec fallback: direct-only)
#   $3 = metrics port
# Use the direct probe when it is available. A hard `docker kill` of a
# privileged sibling container can stall the runner's dockerd for tens of
# seconds. This would take every `docker exec` probe down with it, and
# look like a pipeline stall when nothing is wrong. The function prints
# the body and returns non-zero if no probe answers. Under
# `set -euo pipefail`, callers must guard the capture:
# `$(fetch_metrics ... || true)`. An unguarded failing assignment kills
# the whole script with no fail message.
fetch_metrics() {
  local ip="$1" node="$2" port="$3"
  if [ -n "${ip}" ]; then
    curl -fsS --max-time 5 "http://${ip}:${port}/metrics" 2>/dev/null && return 0
  fi
  [ -n "${node}" ] || return 1
  timeout 8 docker exec "${node}" curl -fsS --max-time 5 \
    "http://127.0.0.1:${port}/metrics" 2>/dev/null
}

# Extract one metric from a captured /metrics body.
#   $1 = body   $2 = metric name   $3 = mode: first (default) | sum
#   first : int value of the first matching sample (gauges). Empty if none
#           match.
#   sum   : int sum of $NF across all matching samples (per-label counters).
#           Empty if no sample matched. A scrape failure, or a metric not
#           yet registered, is not the same as zero.
# awk truncates to an int (%d). Gauges may render as floats or in
# scientific notation.
# awk always reads the entire body, with no early exit. The producer is a
# bash expansion, so there is no pipe-buffer or SIGPIPE risk either way.
# Reading the full body here matches the suite's rule: consumers never
# exit early. See the SIGPIPE note in the chaos-probes.sh file header.
prom_value() {
  local body="$1" metric="$2" mode="${3:-first}"
  if [ "${mode}" = "sum" ]; then
    printf '%s\n' "${body}" | awk -v m="${metric}" \
      '$0 ~ "^"m"([{ ]|$)" && $0 !~ /^#/ { s += $NF; n++ } END { if (n) printf "%d", s }'
  else
    printf '%s\n' "${body}" | awk -v m="${metric}" \
      '$0 ~ "^"m"([{ ]|$)" && $0 !~ /^#/ { if (!n) { printf "%d", $NF; n=1 } }'
  fi
}
