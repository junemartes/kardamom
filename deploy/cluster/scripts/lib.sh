# shellcheck shell=bash
# =============================================================================
# lib.sh — shared control-node helpers for the cluster scripts.
# =============================================================================
# Both chaos.sh and ci-cluster.sh source this file. All commands here reach
# Nomad through the control node container, using docker exec. This is the
# proven access pattern for the DinD cluster. The runner needs only the host
# docker socket. It does not need a routable NOMAD_ADDR.
#
# Overridable before sourcing:
#   NOMAD_ADDR_INT   Nomad HTTP endpoint, as seen from the control node
#   CONTROL          control node container name

NOMAD_ADDR_INT="${NOMAD_ADDR_INT:-http://192.168.56.10:4646}"
CONTROL="${CONTROL:-kardamom-control-0}"

# This file defines shared log() and fail() functions. smoke.sh,
# smoke-load.sh, and local-cluster.sh keep their own definitions. Their
# fail() output has a different format: "RESULT: FAIL — ..." is part of
# their output contract. They do not source this file's log() or fail().
#
# FAIL_PREFIX: set this before sourcing the file, to change the failure
# line prefix. chaos.sh sets it to "CHAOS FAIL" to keep its CI-log contract.
FAIL_PREFIX="${FAIL_PREFIX:-FAIL}"
log() { echo "==> $*"; }
fail() {
  # Write to both streams. stderr covers the exit path. stdout keeps the
  # message in order in the CI log, next to the case's own lines. If only
  # stderr carries the message, log reordering can drop it, and the
  # failure looks like a silent abort.
  echo "${FAIL_PREFIX}: $*"
  echo "${FAIL_PREFIX}: $*" >&2
  exit 1
}

# Run a command on the control node, with NOMAD_ADDR set. $1 is a bash
# snippet; it may reference "$1" through "$N". Remaining args pass through
# by position.
on_control() {
  local script="$1"; shift
  docker exec "${CONTROL}" bash -lc "export NOMAD_ADDR=${NOMAD_ADDR_INT}; ${script}" _ "$@"
}

# Get the first running alloc id for a job. This uses a Nomad -t Go
# template, which stays correct even if the table format changes.
running_alloc() {
  on_control 'nomad job allocs -t "{{range .}}{{if eq .ClientStatus \"running\"}}{{.ID}} {{end}}{{end}}" "$1"' "$1" 2>/dev/null \
    | tr ' ' '\n' | grep -m1 .
}

# Get all running alloc ids for a job, one per line. The result can be empty.
running_allocs() {
  on_control 'nomad job allocs -t "{{range .}}{{if eq .ClientStatus \"running\"}}{{.ID}} {{end}}{{end}}" "$1"' "$1" 2>/dev/null \
    | tr ' ' '\n' | grep . || true
}

# Get all alloc ids for a job, in any client status, one per line.
all_allocs() {
  on_control 'nomad job allocs -t "{{range .}}{{.ID}} {{end}}" "$1"' "$1" 2>/dev/null \
    | tr ' ' '\n' | grep . || true
}

# Count the running allocs for a job.
count_running() {
  on_control 'nomad job allocs -t "{{range .}}{{if eq .ClientStatus \"running\"}}x{{end}}{{end}}" "$1"' "$1" 2>/dev/null \
    | tr -cd 'x' | wc -c | tr -d ' '
}
