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
# degrades to count-1 until the node returns) — the assertions encode the
# *achievable* outcome per case rather than blindly expecting a fresh alloc on a
# new node.
#
# CLUSTER MODE (Phase 3): the deploy now uses the CLUSTERED sealer — a 3-member
# Aeron Cluster (Raft) running as the Nomad job `cluster` (one member per sealer
# node .51/.52/.53; memberId 0/1/2 == kardamom-sealer-0/1/2 via the node-IP
# derivation). There is NO single kardamom-sealer and NO Prometheus endpoint on
# the (Java) cluster node, so cluster-mode progress is measured from the
# EXECUTOR's `kardamom_executor_block_number` gauge (the executor applies blocks
# committed out of the cluster's egress) — see executor_progress() in
# chaos-probes.sh. The three cluster-* cases exercise Raft leader-kill /
# follower-kill / quorum-loss. The component-chaos cases (executor/ingress/
# sequencer/sealer kills) are still present and can run against either topology;
# against a legacy single-sealer deploy assert_progress falls back to the
# sealer-boundary probe.
#
# SPLIT LAYOUT: this file is a thin dispatcher — knobs, the single EXIT trap,
# the per-case scaffolding (account/shard pinning, load launch + injection
# gate, post-case common asserts + verdict) and the case_<name>() dispatch.
# Everything else lives in files SOURCED into THIS shell (they are libraries,
# never executed as children — the injectors set KILLED_* globals consumed by
# assert_count, CHAOS_ACCT advances across cases, and the cleanup trap must
# see LOAD_PID). Only chaos.sh installs an EXIT trap; sourced files never do.
#   lib.sh                        control-node helpers + log/fail
#   lib-topology.sh               node-class model (nodes/IPs/ports)
#   lib-metrics.sh                fetch_metrics + prom_value (scrape/parse)
#   chaos-probes.sh               has_line/has_match + read-only probes
#   chaos-asserts.sh              injectors, alloc-log evidence, assert_*
#   chaos-cases-component.sh      graceful/hard-*, node-failure, restore drills
#   chaos-cases-archive.sh        archive loss/wipe/corruption + archive_tool
#   chaos-cases-cluster.sh        Raft leader/follower/rejoin/quorum cases
#   chaos-cases-validator.sh      lapse/join/cpu-squeeze + warm-up/freeze helpers
#   chaos-cases-seq-retention.sh  sequencer-lapse + retention-overrun
# NEVER add a `producer | grep -q` assert anywhere in the suite — see the
# SIGPIPE/pipefail note atop chaos-probes.sh (PR #158); has_line/has_match are
# the assert primitives.
#
# ENV knobs (all optional):
#   RPC_URL                  ingress JSON-RPC      (default http://192.168.56.31:8545)
#   LOAD_BIN                 kardamom-load path    (default <root>/target/release/kardamom-load)
#   CHAOS_TPS                steady load rate      (default 50)
#   CHAOS_CASE_S             per-case load window  (default 45)
#   LOAD_MAX_GAP             keep-pace gap bound   (default 5)
#   CHAOS_RESTART_SLO_S      same-node restart SLO (default 30)
#   CHAOS_RESCHEDULE_SLO_S   node-loss recovery SLO(default 120)
#   CHAOS_LEADER_SLO_S       new-leader election SLO (default 30)
#   CHAOS_CASES              space-separated cases (default a representative subset)
#   INJECT_DELAY             MIN secs of load before injecting (default 10)
#   LOAD_FLOW_TIMEOUT_S      max extra secs to wait for load to actually flow
#                            (ingress received counter advancing) before
#                            refusing to inject (default 60)
#   CHAOS_ACCT_BASE          first funded account index per case (default 7)
#
# Cases: graceful-executor hard-executor graceful-ingress hard-ingress
#        graceful-sequencer hard-sequencer sequencer-replica-kill
#        sequencer-lapse
#        sealer-graceful sealer-hard
#        node-failure-executor archive-driver-loss
#        cluster-leader-kill cluster-follower-kill cluster-quorum-loss-recover
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

# Shared control-node helpers (on_control, running_alloc, count_running, ...)
# + log/fail. FAIL_PREFIX keeps this suite's dual-stream "CHAOS FAIL:" lines.
FAIL_PREFIX="CHAOS FAIL"
# shellcheck source=deploy/cluster/scripts/lib.sh
source "${SCRIPT_DIR}/lib.sh"
# Node-class model (EXECUTOR_NODES/IPS, validator/ingress nodes+ports, ...).
# shellcheck source=deploy/cluster/scripts/lib-topology.sh
source "${SCRIPT_DIR}/lib-topology.sh"
# Scrape + parse (fetch_metrics bridge-first probe, prom_value).
# shellcheck source=deploy/cluster/scripts/lib-metrics.sh
source "${SCRIPT_DIR}/lib-metrics.sh"

RPC_URL="${RPC_URL:-http://192.168.56.31:8545}"
# Explicit chain-id (ingress eth_chainId returns a default ≠ the cluster chain).
CHAIN_ID="${CHAIN_ID:-412346}"
LOAD_BIN="${LOAD_BIN:-${ROOT}/target/release/kardamom-load}"
# Archive repair tool (archive-corruption case); built with the service bins.
REREP_BIN="${REREP_BIN:-${ROOT}/target/release/kardamom-archive-rereplicate}"
CHAOS_TPS="${CHAOS_TPS:-50}"
CHAOS_CASE_S="${CHAOS_CASE_S:-45}"
LOAD_MAX_GAP="${LOAD_MAX_GAP:-5}"
# Service jobs use force_pull=true, so a restart re-pulls the image from the
# in-cluster registry before the task comes back — allow for that.
CHAOS_RESTART_SLO_S="${CHAOS_RESTART_SLO_S:-60}"
CHAOS_RESCHEDULE_SLO_S="${CHAOS_RESCHEDULE_SLO_S:-120}"
# Raft re-election after a leader loss is fast (a few election timeouts), but the
# leader log line has to surface in the alloc's stdout AND nomad has to ship it,
# so give it a generous window before we call it a failure.
CHAOS_LEADER_SLO_S="${CHAOS_LEADER_SLO_S:-30}"
CHAOS_CASES="${CHAOS_CASES:-graceful-executor hard-executor cluster-leader-kill node-failure-executor}"
INJECT_DELAY="${INJECT_DELAY:-10}"
LOAD_FLOW_TIMEOUT_S="${LOAD_FLOW_TIMEOUT_S:-60}"
# Each case's steady load uses ONE dedicated funded account (a fresh nonce chain
# from 0), so cases never collide and never leave nonce gaps. Genesis funds
# Anvil accounts #0..#15; ci-cluster.sh reserves #0 (gate) and #1..#6 (load
# harness), leaving #7..#15 = up to 9 cases. CHAOS_ACCT advances per case.
CHAOS_ACCT_BASE="${CHAOS_ACCT_BASE:-7}"
CHAOS_ACCT="${CHAOS_ACCT_BASE}"
# Sender→shard map for the 16 funded Anvil accounts (index = account number):
# shard = first 8 bytes of keccak256(address) as a BE u64, mod partition_count=2
# (crates/ingress/src/routing.rs::partition_for). Fixed addresses + fixed hash
# ⇒ stable forever. Derivation: cast keccak <address> | cut -c3-18, % 2.
# Used to PIN a case's load onto a specific shard (sequencer-replica-kill).
ACCT_SHARD=(0 1 1 0 0 0 0 0 0 0 1 0 1 1 0 1)

LOAD_PID=""
cleanup() {
  [ -n "${LOAD_PID}" ] && kill "${LOAD_PID}" 2>/dev/null || true
}
# The ONE EXIT trap of the whole suite. Sourced files must never install
# their own (a sourced trap would silently REPLACE this one and orphan the
# background load).
trap cleanup EXIT

[ -x "${LOAD_BIN}" ] || fail "kardamom-load not found/executable at ${LOAD_BIN}"

# Probes, injectors/asserts, and the case_<name>() bodies — all sourced into
# THIS shell (see the SPLIT LAYOUT note above).
# shellcheck source=deploy/cluster/scripts/chaos-probes.sh
source "${SCRIPT_DIR}/chaos-probes.sh"
# shellcheck source=deploy/cluster/scripts/chaos-asserts.sh
source "${SCRIPT_DIR}/chaos-asserts.sh"
# shellcheck source=deploy/cluster/scripts/chaos-cases-component.sh
source "${SCRIPT_DIR}/chaos-cases-component.sh"
# shellcheck source=deploy/cluster/scripts/chaos-cases-archive.sh
source "${SCRIPT_DIR}/chaos-cases-archive.sh"
# shellcheck source=deploy/cluster/scripts/chaos-cases-cluster.sh
source "${SCRIPT_DIR}/chaos-cases-cluster.sh"
# shellcheck source=deploy/cluster/scripts/chaos-cases-validator.sh
source "${SCRIPT_DIR}/chaos-cases-validator.sh"
# shellcheck source=deploy/cluster/scripts/chaos-cases-seq-retention.sh
source "${SCRIPT_DIR}/chaos-cases-seq-retention.sh"

# --- the per-case driver ----------------------------------------------------

run_case() { # <case-name>
  local name="$1"
  local out="/tmp/chaos-${name}.json"
  local logf="/tmp/chaos-${name}.load.log"

  # Case bodies are case_<name>() functions (dashes → underscores) in the
  # sourced chaos-cases-*.sh files. Unknown cases fail before any load or
  # account is spent.
  local fn="case_${name//-/_}"
  declare -F "${fn}" >/dev/null || fail "unknown chaos case: ${name}"

  log "================= CHAOS CASE: ${name} ================="

  # One dedicated fresh funded account per case (single sender, nonces from 0)
  # so cases never collide / leave nonce gaps on the never-reset chain.
  if [ "${name}" = "sequencer-replica-kill" ] || [ "${name}" = "sequencer-lapse" ]; then
    # PIN this case's load to SHARD 0 — the shard whose replica A is killed
    # (or paused). An arbitrary account lands on shard 0 or 1 by address
    # hash, so ~half of runs would otherwise drive an UNTOUCHED shard and the
    # failover assertions would prove nothing about the kill. Accounts skipped
    # here are burned, never reused (their nonce chains stay untouched at 0).
    while [ "${CHAOS_ACCT}" -le 15 ] && [ "${ACCT_SHARD[${CHAOS_ACCT}]}" -ne 0 ]; do
      log "${name}: skipping funded account #${CHAOS_ACCT} (shard ${ACCT_SHARD[${CHAOS_ACCT}]}; case needs shard 0)"
      CHAOS_ACCT=$(( CHAOS_ACCT + 1 ))
    done
  fi
  local acct="${CHAOS_ACCT}"
  CHAOS_ACCT=$(( CHAOS_ACCT + 1 ))
  [ "${acct}" -le 15 ] || fail "ran out of funded chaos accounts (#${acct} > 15); reduce CHAOS_CASES"

  # Per-case load window. sequencer-replica-kill needs load still FLOWING
  # after the killed replica's restart SLO — its post-restart coverage
  # assertion observes the restarted replica publishing refs for the pinned
  # shard, which requires live traffic — so its window is widened to cover
  # inject + restart + margin regardless of the global CHAOS_CASE_S.
  local case_s="${CHAOS_CASE_S}"
  if [ "${name}" = "sequencer-replica-kill" ]; then
    local min_s=$(( INJECT_DELAY + CHAOS_RESTART_SLO_S + 60 ))
    [ "${case_s}" -lt "${min_s}" ] && case_s="${min_s}"
  fi
  # sequencer-lapse: load must still be flowing after the pause window so the
  # resumed replica's resync + the twin-coverage verdicts observe live traffic.
  if [ "${name}" = "sequencer-lapse" ]; then
    local min_s=$(( INJECT_DELAY + SEQ_LAPSE_S + 60 ))
    [ "${case_s}" -lt "${min_s}" ] && case_s="${min_s}"
  fi
  # retention-overrun: the freeze only overruns the retention window if frames
  # keep FLOWING for its whole duration, and the post-thaw repair (fetch +
  # restart + restore) needs live traffic to prove the rejoin — so the load
  # window must cover freeze + recovery, not the global CHAOS_CASE_S.
  if [ "${name}" = "retention-overrun" ] || [ "${name}" = "retention-overrun-validator" ]; then
    local min_s=$(( INJECT_DELAY + RETENTION_FREEZE_CAP_S + 120 ))
    [ "${case_s}" -lt "${min_s}" ] && case_s="${min_s}"
  fi
  # cpu-squeeze: load must keep flowing through the whole starvation window
  # AND the recovery assert — a squeeze with an idle pipeline exercises
  # nothing (the divergence it hunts needs live traffic + catch-up).
  if [ "${name}" = "cpu-squeeze" ]; then
    local min_s=$(( INJECT_DELAY + SQUEEZE_CYCLES * (SQUEEZE_S + SQUEEZE_RELEASE_S) + 90 ))
    [ "${case_s}" -lt "${min_s}" ] && case_s="${min_s}"
  fi

  # Ingress baseline BEFORE the load starts, for the injection gate below.
  local rx0
  rx0="$(ingress_received || echo 0)"

  # Steady background load for the whole inject+recover window. Drain deadline
  # outlives the recovery SLO so txs accepted before/around the kill still
  # receipt after recovery.
  local drain=$(( CHAOS_RESCHEDULE_SLO_S + 60 ))
  "${LOAD_BIN}" --rpc "${RPC_URL}" --chain-id "${CHAIN_ID}" --chaos-mode --duration "${case_s}s" \
    --target-tps "${CHAOS_TPS}" --senders 1 --sender-offset "${acct}" \
    --nonce-start 0 --assert-all-delivered --completeness accepted \
    --max-gap "${LOAD_MAX_GAP}" --scrape executor,ingress,sequencer \
    --drain-timeout "${drain}s" --output "${out}" >"${logf}" 2>&1 &
  LOAD_PID=$!

  # Injection gate: INJECT_DELAY is a MINIMUM, not proof that load is flowing —
  # on a thrashed runner the harness can still be pre-generating/connecting
  # when a fixed sleep expires, and a kill into an idle pipeline asserts
  # nothing. Require the ingress received counter to move past its pre-load
  # baseline (bounded by LOAD_FLOW_TIMEOUT_S) before injecting.
  sleep "${INJECT_DELAY}"
  local rx1 waited=0
  while :; do
    rx1="$(ingress_received || echo "${rx0}")"
    [ "${rx1}" -gt "${rx0}" ] && break
    kill -0 "${LOAD_PID}" 2>/dev/null \
      || fail "${name}: kardamom-load exited before any tx reached ingress (see ${logf})"
    [ "${waited}" -ge "${LOAD_FLOW_TIMEOUT_S}" ] \
      && fail "${name}: load not flowing after ${LOAD_FLOW_TIMEOUT_S}s (ingress received ${rx0} -> ${rx1}); refusing to inject into an idle pipeline"
    sleep 3; waited=$(( waited + 3 ))
  done
  log "load flowing (ingress received ${rx0} -> ${rx1}); injecting"

  # Inject + case-specific asserts.
  "${fn}"

  # Pipeline must be producing blocks again after recovery. In cluster mode the
  # sealer Prometheus endpoint doesn't exist, so use the executor progress probe;
  # otherwise assert_progress prefers the legacy sealer-boundary probe.
  # node-failure gets the WIDE window: right after `docker start` of the killed
  # node, the runner is saturated by nomad rescheduling + the returning node
  # force-pulling every image through the in-cluster registry — on 4-core CI
  # runners even the metric scrapes time out through that thrash ("block 0 ->
  # 0" with the max-across-replicas probe = nobody answered), while the same
  # case passes cleanly on a 12-core host. Same evidence-based widening as the
  # quorum-loss case's 180s.
  case "${name}" in
    cluster-*)              assert_executor_progress ;;
    node-failure-*)         assert_executor_progress 180 ;;
    *)                      assert_progress ;;
  esac

  # EVERY case must leave a fully-healthy executor fleet, not just "some
  # replica progressing" — see assert_executors_converged. This is what makes
  # a green run meaningful: a case whose kill target (or an innocent
  # bystander, e.g. an executor wedged by a sequencer restart) never truly
  # recovers now fails HERE instead of hiding behind the fleet-max probes.
  assert_executors_converged "${name}"

  # Let the background load finish its window + drain, then check its verdict.
  wait "${LOAD_PID}" || true
  LOAD_PID=""
  case "${name}" in
    # Killing any Raft cluster member (leader, follower, or losing quorum) under
    # sustained high load causes a brief ordering hiccup in which the sequencer
    # rejects some past-nonce txs (seq_dropped). Those were never ACCEPTED, so they
    # are not a delivery gap — the cluster still delivered every accepted tx
    # (missing==0). Assert gapless delivery of accepted txs for the cluster cases
    # (tolerating seq_dropped); the component-chaos cases keep the strict verdict
    # (their redundancy — 3 executors / 2 sequencers / ingress restart — should NOT
    # drop any tx).
    cluster-*) assert_accepted_delivered "${out}" "${name}" ;;
    *)         assert_load_pass "${out}" "${name}" ;;
  esac
  log "CHAOS CASE ${name}: PASS"
}

log "chaos suite: cases=[${CHAOS_CASES}] tps=${CHAOS_TPS} case_s=${CHAOS_CASE_S} restart_slo=${CHAOS_RESTART_SLO_S} reschedule_slo=${CHAOS_RESCHEDULE_SLO_S} leader_slo=${CHAOS_LEADER_SLO_S}"
for c in ${CHAOS_CASES}; do
  run_case "${c}"
done
log "chaos suite PASSED (${CHAOS_CASES})"
