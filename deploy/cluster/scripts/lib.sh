# shellcheck shell=bash
# =============================================================================
# lib.sh — shared control-node helpers for the cluster scripts.
# =============================================================================
# Sourced by chaos.sh and ci-cluster.sh (they previously carried private copies
# of these). Everything here talks to Nomad THROUGH the control node container
# (docker exec), the proven access pattern for the DinD cluster: the runner
# only needs the host docker socket, not a routable NOMAD_ADDR.
#
# Overridable before sourcing:
#   NOMAD_ADDR_INT   Nomad HTTP endpoint as seen FROM the control node
#   CONTROL          control node container name

NOMAD_ADDR_INT="${NOMAD_ADDR_INT:-http://192.168.56.10:4646}"
CONTROL="${CONTROL:-kardamom-control-0}"

# Shared log/fail (previously redefined per script). smoke.sh, smoke-load.sh
# and local-cluster.sh keep their own definitions (their fail semantics differ:
# "RESULT: FAIL — ..." is part of their output contract) — they do not source
# this file's log/fail.
#
# FAIL_PREFIX: set BEFORE sourcing to brand the failure line (chaos.sh sets
# "CHAOS FAIL" so its CI-log contract is unchanged).
FAIL_PREFIX="${FAIL_PREFIX:-FAIL}"
log() { echo "==> $*"; }
fail() {
  # BOTH streams (chaos.sh's dual-stream fail, kept verbatim): stderr for the
  # exit path, stdout so the message lands in-order in the CI log next to the
  # case's own lines (a fail seen only on a reordered/dropped stderr reads as
  # a silent death — observed on run 30281 series: a case aborted with no
  # visible CHAOS FAIL line).
  echo "${FAIL_PREFIX}: $*"
  echo "${FAIL_PREFIX}: $*" >&2
  exit 1
}

# Run a command on the control node with NOMAD_ADDR set. $1 is a bash snippet
# (may reference "$1".."$N"); remaining args are passed positionally.
on_control() {
  local script="$1"; shift
  docker exec "${CONTROL}" bash -lc "export NOMAD_ADDR=${NOMAD_ADDR_INT}; ${script}" _ "$@"
}

# First RUNNING alloc id for a job, via Nomad's -t Go template (the proven form
# from ci-cluster.sh's churn — robust to tabular-format changes).
running_alloc() {
  on_control 'nomad job allocs -t "{{range .}}{{if eq .ClientStatus \"running\"}}{{.ID}} {{end}}{{end}}" "$1"' "$1" 2>/dev/null \
    | tr ' ' '\n' | grep -m1 .
}

# All RUNNING alloc ids for a job, one per line (may be empty).
running_allocs() {
  on_control 'nomad job allocs -t "{{range .}}{{if eq .ClientStatus \"running\"}}{{.ID}} {{end}}{{end}}" "$1"' "$1" 2>/dev/null \
    | tr ' ' '\n' | grep . || true
}

# All alloc ids for a job (any client status), one per line.
all_allocs() {
  on_control 'nomad job allocs -t "{{range .}}{{.ID}} {{end}}" "$1"' "$1" 2>/dev/null \
    | tr ' ' '\n' | grep . || true
}

# Count of RUNNING allocs for a job.
count_running() {
  on_control 'nomad job allocs -t "{{range .}}{{if eq .ClientStatus \"running\"}}x{{end}}{{end}}" "$1"' "$1" 2>/dev/null \
    | tr -cd 'x' | wc -c | tr -d ' '
}
