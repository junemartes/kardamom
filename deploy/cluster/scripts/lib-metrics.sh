# shellcheck shell=bash
# =============================================================================
# lib-metrics.sh — shared Prometheus scrape + parse helpers.
# =============================================================================
# SOURCED (never executed) by chaos.sh (via chaos-probes.sh), ci-cluster.sh and
# smoke-load.sh. Consolidates the 10+ per-script scrape/parse sites that had
# drifted into three different awk metric-match regexes:
#     chaos.sh       '"[{ ]"'        (val_metric/seqa_metric — misses nothing in
#                                     practice, but cannot match a label-less
#                                     bare-name sample line)
#     ci-cluster.sh  '"([{ ])"'      (same limitation)
#     smoke-load.sh  '"([{ ]|$)"'    (also matches label-less samples)
# Unified here on '"([{ ]|$)"' — the one that also matches label-less samples —
# so every consumer parses the exposition format identically.
#
# No dependency on lib.sh (no log/fail): smoke-load.sh sources this while
# keeping its own "RESULT: FAIL" fail contract.

# Fetch one /metrics body: bridge-DIRECT first, docker-exec fallback.
#   $1 = bridge ip   ("" => skip the direct probe: loopback-only exporter)
#   $2 = node container ("" => skip the docker-exec fallback: direct-only)
#   $3 = metrics port
# The direct probe matters where available: a hard `docker kill` of a
# privileged sibling can stall the runner's dockerd for tens of seconds,
# taking every `docker exec` probe down with it and reading as a pipeline
# stall when nothing is wrong (issue #76). Prints the body; returns non-zero
# if no probe answered. Callers under `set -euo pipefail` MUST guard the
# capture (`$(fetch_metrics ... || true)`) — an unguarded failing assignment
# kills the whole script with NO fail message (the val_metric lesson).
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
#   first : int value of the FIRST matching sample (gauges); empty if absent.
#   sum   : int sum of $NF across ALL matching samples (per-label counters);
#           empty if NO sample matched (scrape failure / metric not yet
#           registered is NOT zero — the sequencer-lapse "scrape failure is
#           not zero" contract).
# awk int-truncates (%d) — gauges may render as floats / scientific notation.
# awk always reads the ENTIRE body (no early exit): the producer is a bash
# expansion so there is no pipe-buffer/SIGPIPE hazard either way, but keeping
# the consumer full-read matches the suite's no-early-exit-consumer doctrine
# (see the SIGPIPE header in chaos-probes.sh / PR #158).
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
