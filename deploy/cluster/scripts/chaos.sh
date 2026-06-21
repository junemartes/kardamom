#!/usr/bin/env bash
# =============================================================================
# chaos.sh — resilience/chaos suite for the kardamom cluster.
# =============================================================================
#
# For each failure case: start a steady background load (the kardamom-load
# harness in --chaos-mode, which soaks at a fixed rate and asserts every
# ACCEPTED tx eventually receipts), inject a failure, assert the cluster
# auto-recovers within an SLO AND the pipeline resumes producing blocks, then
# wait for the load to finish and assert its verdict (no missing receipts, no
# frozen executor).
#
# Runs inside the orchestrator/runner (shares the host docker socket + reaches
# the cluster bridge), exactly like ci-cluster.sh. The cluster is DinD: each
# node is a privileged container `kardamom-<class>-<i>` running its own dockerd,
# and the pipeline services are inner Nomad docker-driver tasks. So:
#   * graceful kill  = `nomad alloc stop` (via control-0)         → restart
#   * hard crash     = `docker exec <node> docker kill <inner>`   → restart
#   * node failure   = `docker kill <node>` (whole node)          → reschedule
#
# Recovery semantics depend on topology (singletons on a single role-node can't
# reschedule to a peer; a node-failure of an executor with no spare role-node
# degrades to count-1 until the node returns) — the assertions below encode the
# *achievable* outcome per case rather than blindly expecting a fresh alloc on a
# new node.
#
# ENV knobs (all optional):
#   RPC_URL                  ingress JSON-RPC      (default http://192.168.56.31:8545)
#   LOAD_BIN                 kardamom-load path    (default <root>/target/release/kardamom-load)
#   CHAOS_TPS                steady load rate      (default 50)
#   CHAOS_CASE_S             per-case load window  (default 45)
#   LOAD_MAX_GAP             keep-pace gap bound   (default 5)
#   CHAOS_RESTART_SLO_S      same-node restart SLO (default 30)
#   CHAOS_RESCHEDULE_SLO_S   node-loss recovery SLO(default 120)
#   CHAOS_CASES              space-separated cases (default a representative subset)
#   INJECT_DELAY             secs of load before injecting (default 10)
#   CHAOS_ACCT_BASE          first funded account index per case (default 7)
#
# Cases: graceful-executor hard-executor graceful-ingress hard-ingress
#        graceful-sequencer hard-sequencer sealer-graceful sealer-hard
#        node-failure-executor
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

NOMAD_ADDR_INT="http://192.168.56.10:4646"
CONTROL="kardamom-control-0"
RPC_URL="${RPC_URL:-http://192.168.56.31:8545}"
# Explicit chain-id (ingress eth_chainId returns a default ≠ the cluster chain).
CHAIN_ID="${CHAIN_ID:-412346}"
LOAD_BIN="${LOAD_BIN:-${ROOT}/target/release/kardamom-load}"
CHAOS_TPS="${CHAOS_TPS:-50}"
CHAOS_CASE_S="${CHAOS_CASE_S:-45}"
LOAD_MAX_GAP="${LOAD_MAX_GAP:-5}"
# Service jobs use force_pull=true, so a restart re-pulls the image from the
# in-cluster registry before the task comes back — allow for that.
CHAOS_RESTART_SLO_S="${CHAOS_RESTART_SLO_S:-60}"
CHAOS_RESCHEDULE_SLO_S="${CHAOS_RESCHEDULE_SLO_S:-120}"
CHAOS_CASES="${CHAOS_CASES:-graceful-executor hard-executor sealer-hard node-failure-executor}"
INJECT_DELAY="${INJECT_DELAY:-10}"
# Each case's steady load uses ONE dedicated funded account (a fresh nonce chain
# from 0), so cases never collide and never leave nonce gaps. Genesis funds
# Anvil accounts #0..#15; ci-cluster.sh reserves #0 (gate) and #1..#6 (load
# harness), leaving #7..#15 = up to 9 cases. CHAOS_ACCT advances per case.
CHAOS_ACCT_BASE="${CHAOS_ACCT_BASE:-7}"
CHAOS_ACCT="${CHAOS_ACCT_BASE}"

# Sealer/executor metrics ports + container names (mirror smoke-load defaults).
SEALER_NODE="kardamom-sealer-0"
SEALER_PORT=9003

LOAD_PID=""
log()  { echo "==> $*"; }
fail() { echo "CHAOS FAIL: $*" >&2; exit 1; }

cleanup() {
  [ -n "${LOAD_PID}" ] && kill "${LOAD_PID}" 2>/dev/null || true
}
trap cleanup EXIT

[ -x "${LOAD_BIN}" ] || fail "kardamom-load not found/executable at ${LOAD_BIN}"

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

# Count of RUNNING allocs for a job.
count_running() {
  on_control 'nomad job allocs -t "{{range .}}{{if eq .ClientStatus \"running\"}}x{{end}}{{end}}" "$1"' "$1" 2>/dev/null \
    | tr -cd 'x' | wc -c | tr -d ' '
}

sealer_boundaries() {
  docker exec "${SEALER_NODE}" curl -fsS --max-time 5 "http://127.0.0.1:${SEALER_PORT}/metrics" 2>/dev/null \
    | awk '/^kardamom_sealer_boundaries_emitted_total/{print $NF; exit}'
}

# --- injectors --------------------------------------------------------------

inject_graceful() { # <job>
  local alloc; alloc="$(running_alloc "$1")"
  [ -n "${alloc}" ] || fail "no running alloc to stop for job $1"
  log "graceful: nomad alloc stop ${alloc} (job $1)"
  on_control 'nomad alloc stop "$1"' "${alloc}" >/dev/null
}

inject_hard() { # <node-container> <task-name>
  log "hard: docker kill inner ${2} container on ${1}"
  docker exec "$1" sh -c 'docker kill $(docker ps --filter name='"$2"' -q | head -1)' >/dev/null \
    || fail "could not hard-kill ${2} on ${1}"
}

# --- assertions -------------------------------------------------------------

assert_count() { # <job> <min-running> <slo-secs>
  local t=0
  until [ "$(count_running "$1")" -ge "$2" ]; do
    sleep 3; t=$((t+3))
    [ "${t}" -ge "$3" ] && fail "$1 did not reach >= $2 running within $3s (have $(count_running "$1"))"
  done
  log "$1 has >= $2 running alloc(s) after ${t}s"
}

assert_progress() { # asserts the sealer keeps emitting block boundaries
  local b0 b1
  b0="$(sealer_boundaries || true)"; b0="${b0:-0}"
  sleep 10
  b1="$(sealer_boundaries || true)"; b1="${b1:-0}"
  awk "BEGIN{exit !(${b1}>${b0})}" \
    && log "pipeline progressing (sealer boundaries ${b0} -> ${b1})" \
    || fail "pipeline NOT progressing after recovery (sealer boundaries ${b0} -> ${b1})"
}

assert_load_pass() { # <json> <case>
  if grep -q '"pass": true' "$1" 2>/dev/null; then
    log "load verdict PASS for case $2"
  else
    echo "----- load report ($2) [$1] -----" >&2
    if [ -s "$1" ]; then
      sed -n '1,80p' "$1" >&2
    else
      echo "(report JSON missing/empty — kardamom-load did not finish writing it)" >&2
    fi
    echo "----- kardamom-load stdout/stderr (/tmp/chaos-$2.load.log) -----" >&2
    tail -n 80 "/tmp/chaos-$2.load.log" 2>/dev/null >&2 || echo "(no load log)" >&2
    fail "load verdict not PASS for case $2"
  fi
}

# --- the per-case driver ----------------------------------------------------

run_case() { # <case-name>
  local name="$1"
  local out="/tmp/chaos-${name}.json"
  local logf="/tmp/chaos-${name}.load.log"
  log "================= CHAOS CASE: ${name} ================="

  # One dedicated fresh funded account per case (single sender, nonces from 0)
  # so cases never collide / leave nonce gaps on the never-reset chain.
  local acct="${CHAOS_ACCT}"
  CHAOS_ACCT=$(( CHAOS_ACCT + 1 ))
  [ "${acct}" -le 15 ] || fail "ran out of funded chaos accounts (#${acct} > 15); reduce CHAOS_CASES"

  # Steady background load for the whole inject+recover window. Drain deadline
  # outlives the recovery SLO so txs accepted before/around the kill still
  # receipt after recovery.
  local drain=$(( CHAOS_RESCHEDULE_SLO_S + 60 ))
  "${LOAD_BIN}" --rpc "${RPC_URL}" --chain-id "${CHAIN_ID}" --chaos-mode --duration "${CHAOS_CASE_S}s" \
    --target-tps "${CHAOS_TPS}" --senders 1 --sender-offset "${acct}" \
    --nonce-start 0 --assert-all-delivered --completeness accepted \
    --max-gap "${LOAD_MAX_GAP}" --scrape executor,sealer,ingress,sequencer \
    --drain-timeout "${drain}s" --output "${out}" >"${logf}" 2>&1 &
  LOAD_PID=$!

  sleep "${INJECT_DELAY}"

  case "${name}" in
    graceful-executor)  inject_graceful executor;                       assert_count executor 3 "${CHAOS_RESTART_SLO_S}" ;;
    hard-executor)      inject_hard kardamom-executor-0 executor;       assert_count executor 3 "${CHAOS_RESTART_SLO_S}" ;;
    graceful-ingress)   inject_graceful ingress;                        assert_count ingress 1 "${CHAOS_RESTART_SLO_S}" ;;
    hard-ingress)       inject_hard kardamom-ingress-0 ingress;         assert_count ingress 1 "${CHAOS_RESTART_SLO_S}" ;;
    graceful-sequencer) inject_graceful sequencer;                      assert_count sequencer 2 "${CHAOS_RESTART_SLO_S}" ;;
    hard-sequencer)     inject_hard kardamom-sequencer-0 sequencer;     assert_count sequencer 2 "${CHAOS_RESTART_SLO_S}" ;;
    sealer-graceful)    inject_graceful sealer;                         assert_count sealer 1 "${CHAOS_RESTART_SLO_S}" ;;
    # KNOWN GAP: after a HARD sealer crash the executors freeze and don't
    # re-attach to the restarted sealer's canonical tx_ordering (sealer is a
    # singleton SPOF; HA is future work). Excluded from the always-on CI suite
    # (see cluster-e2e.yml); tracked in issue #58. Kept here to reproduce.
    sealer-hard)        inject_hard kardamom-sealer-0 sealer;           assert_count sealer 1 "${CHAOS_RESTART_SLO_S}" ;;
    node-failure-executor)
      # Kill the whole node container. With 3 executor-role nodes + distinct_hosts
      # the lost replica can't reschedule onto a peer (none free), so the cluster
      # degrades to 2 and must keep progressing; bringing the node back recovers 3.
      log "node-failure: docker kill kardamom-executor-2 (whole node)"
      docker kill kardamom-executor-2 >/dev/null || fail "could not kill node kardamom-executor-2"
      assert_count executor 2 "${CHAOS_RESTART_SLO_S}"
      assert_progress
      log "node-failure: docker start kardamom-executor-2 (node returns)"
      docker start kardamom-executor-2 >/dev/null || fail "could not restart node kardamom-executor-2"
      assert_count executor 3 "${CHAOS_RESCHEDULE_SLO_S}"
      ;;
    *) fail "unknown chaos case: ${name}" ;;
  esac

  # Pipeline must be producing blocks again after recovery.
  assert_progress

  # Let the background load finish its window + drain, then check its verdict.
  wait "${LOAD_PID}" || true
  LOAD_PID=""
  assert_load_pass "${out}" "${name}"
  log "CHAOS CASE ${name}: PASS"
}

log "chaos suite: cases=[${CHAOS_CASES}] tps=${CHAOS_TPS} case_s=${CHAOS_CASE_S} restart_slo=${CHAOS_RESTART_SLO_S} reschedule_slo=${CHAOS_RESCHEDULE_SLO_S}"
for c in ${CHAOS_CASES}; do
  run_case "${c}"
done
log "chaos suite PASSED (${CHAOS_CASES})"
