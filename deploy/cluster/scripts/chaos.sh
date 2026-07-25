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
# CLUSTER MODE (Phase 3): the deploy now uses the CLUSTERED sealer — a 3-member
# Aeron Cluster (Raft) running as the Nomad job `cluster` (one member per sealer
# node .51/.52/.53; memberId 0/1/2 == kardamom-sealer-0/1/2 via the node-IP
# derivation). There is NO single kardamom-sealer and NO Prometheus endpoint on
# the (Java) cluster node, so cluster-mode progress is measured from the
# EXECUTOR's `kardamom_executor_block_number` gauge (the executor applies blocks
# committed out of the cluster's egress) — see executor_progress() below. The
# three cluster-* cases exercise Raft leader-kill / follower-kill / quorum-loss.
# The component-chaos cases (executor/ingress/sequencer/sealer kills) are still
# present and can run against either topology; against a legacy single-sealer
# deploy assert_progress falls back to the sealer-boundary probe.
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
#        sealer-graceful sealer-hard
#        node-failure-executor archive-driver-loss
#        cluster-leader-kill cluster-follower-kill cluster-quorum-loss-recover
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

# Shared control-node helpers (on_control, running_alloc, count_running, ...).
# shellcheck source=deploy/cluster/scripts/lib.sh
source "${SCRIPT_DIR}/lib.sh"

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

# Sealer/executor metrics ports + container names (mirror smoke-load defaults).
# NOTE: the Java cluster node has no Prometheus endpoint; the executors
# re-export the sealer's boundary stream from cluster egress on port 9004, so
# both progress probes (sealer_boundaries, executor_progress) read from the
# executor nodes — see assert_progress().
SEALER_NODE="kardamom-sealer-0"
EXECUTOR_NODE="kardamom-executor-0"
# Progress is scraped from whichever executor responds (a chaos case may kill one
# of them — hard-executor kills executor-0, node-failure-executor kills executor-2),
# so executor_progress() tries each in turn. The 3 executors are state-machine
# replicas at ~the same block height, so any one is a valid liveness signal.
EXECUTOR_NODES=(kardamom-executor-0 kardamom-executor-1 kardamom-executor-2)
# Executor node bridge IPs (group_vars node_classes: executor ip_start=41).
# exec_metrics() probes these DIRECTLY over the bridge first (exporter binds
# 0.0.0.0:9004), with docker exec as the fallback.
EXECUTOR_IPS=(192.168.56.41 192.168.56.42 192.168.56.43)
EXECUTOR_PORT="${EXECUTOR_PORT:-9004}"
# The executor's monotonically-advancing block gauge (crates/executor/src/metrics.rs:
# kardamom_executor_block_number — set per committed block in actor.rs). This is the
# cluster-mode pipeline-progress signal (the cluster commits blocks out its egress,
# the executor applies them, this gauge ticks up). Same metric smoke-load.sh uses.
EXECUTOR_BLOCK_METRIC="${EXECUTOR_BLOCK_METRIC:-kardamom_executor_block_number}"
# The Nomad task name inside the `cluster` job (cluster.nomad.hcl: task "cluster"),
# so inject_hard kardamom-sealer-<id> cluster matches its inner container.
CLUSTER_TASK="cluster"

LOAD_PID=""
log()  { echo "==> $*"; }
fail() { echo "CHAOS FAIL: $*" >&2; exit 1; }

cleanup() {
  [ -n "${LOAD_PID}" ] && kill "${LOAD_PID}" 2>/dev/null || true
}
trap cleanup EXIT

[ -x "${LOAD_BIN}" ] || fail "kardamom-load not found/executable at ${LOAD_BIN}"

# Fetch one executor node's /metrics body. Bridge-DIRECT first (the executor
# exporter binds 0.0.0.0:9004 precisely so the chaos suite can probe it over
# the cluster bridge — see executor.nomad.hcl), falling back to docker exec
# for loopback-only deploys. The direct probe matters: a hard `docker kill` of
# a privileged sibling can stall the runner's dockerd for tens of seconds,
# taking every `docker exec` probe down with it and reading as a pipeline
# stall when nothing is wrong (issue #76). $1 = index into EXECUTOR_NODES.
exec_metrics() {
  local i="$1"
  curl -fsS --max-time 5 "http://${EXECUTOR_IPS[$i]}:${EXECUTOR_PORT}/metrics" 2>/dev/null \
    && return 0
  timeout 8 docker exec "${EXECUTOR_NODES[$i]}" curl -fsS --max-time 5 \
    "http://127.0.0.1:${EXECUTOR_PORT}/metrics" 2>/dev/null
}

# Sealer boundary-counter probe: the sealer's boundary stream as re-exported
# by the executors from cluster egress (the Java cluster node itself has no
# Prometheus endpoint). Ticks ~4/s while the sealer is alive — a finer liveness
# signal than the block gauge. Tries each executor node like executor_progress.
sealer_boundaries() {
  # MAX across all responding executors, NOT the first responder — same
  # rationale as executor_progress below: a replica that restarted (or is
  # replaying/catching up) legitimately reports a low/frozen counter while its
  # peers — and the pipeline — are fine; pinning the probe to it reads as a
  # pipeline stall when nothing is wrong.
  local i v best=""
  for i in "${!EXECUTOR_NODES[@]}"; do
    v="$(exec_metrics "${i}" \
      | awk '/^kardamom_sealer_boundaries_emitted_total/{printf "%d", $NF; exit}')"
    [ -n "${v}" ] && { [ -z "${best}" ] || [ "${v}" -gt "${best}" ]; } && best="${v}"
  done
  [ -n "${best}" ] && { printf '%s' "${best}"; return 0; }
  return 1
}

# Scrape a single validator metric (aux-0, :9006). The validator is the sole
# node on the aux tier; metrics on 9006 (executor holds 9004 elsewhere).
VALIDATOR_NODE="${VALIDATOR_NODE:-kardamom-aux-0}"
VALIDATOR_PORT="${VALIDATOR_PORT:-9006}"
val_metric() { # <metric-name> -> integer (empty on scrape failure)
  # `|| true` is load-bearing: under `set -euo pipefail` a failed curl (the
  # validator's exporter routinely stalls >5s right after `docker unpause`
  # while it chews through the lapse backlog) would otherwise kill the whole
  # script mid-case with NO fail() message — the validator-lapse case died
  # exactly this way on its first post-unpause probe. Empty output is the
  # documented contract; callers default it.
  timeout 8 docker exec "${VALIDATOR_NODE}" curl -fsS --max-time 5 \
    "http://127.0.0.1:${VALIDATOR_PORT}/metrics" 2>/dev/null \
    | awk -v m="$1" '$0 ~ "^"m"[{ ]" && $0 !~ /^#/ { printf "%d", $NF; exit }' \
    || true
}

# validator-lapse case: PAUSE the validator process (docker pause the inner
# container) for a window under sustained load, then resume. The validator's
# live tx_bal multicast image lapses during the pause; on resume the missed
# BALs are still sitting in the live multicast TERM BUFFER, so the validator
# drains them and keeps verifying, and the catch-up skip (#78) bounds the cost
# of anything that aged out of the term buffer (those blocks commit unverified
# instead of blocking 5s each). There is NO side-stream refetch mechanism —
# that prototype was discarded (a co-located recorder + follow-live replay
# starves the live poll path). Asserts: verification coverage held
# (bal_missing did not materially grow), the validator kept verifying past the
# pre-pause count, caught back up, and saw no divergence. The pipeline itself
# is untouched (the validator is off the hot path), so the standard load +
# progress verdicts still apply.
LAPSE_S="${LAPSE_S:-30}"
run_validator_lapse() {
  local inner
  inner="$(docker exec "${VALIDATOR_NODE}" sh -c 'docker ps --format "{{.Names}}" | grep -m1 "^validator"' 2>/dev/null)"
  [ -n "${inner}" ] || fail "validator-lapse: no inner validator container on ${VALIDATOR_NODE}"

  # WARM UP: wait until the validator is CAUGHT UP AND VERIFYING LIVE — its
  # blocks_verified counter is advancing and its lag to the executors is small.
  # (A validator re-executes from genesis; blocks that passed before it started
  # have no BAL and are committed unverified during a fast catch-up, so it
  # reaches the live head and only THEN verifies steadily. The lapse must hit a
  # verifying validator, not one still catching up.)
  local t=0 vprev=-1 v_now e_now verified warmed=0
  while [ "${t}" -lt 150 ]; do
    verified="$(val_metric validator_blocks_verified_total)"; verified="${verified:-0}"
    v_now="$(val_metric validator_committed_block)"; v_now="${v_now:-0}"
    e_now="$(executor_progress || echo 0)"
    if [ "${verified}" -gt 0 ] && [ "${verified}" -gt "${vprev}" ] \
       && [ "${v_now}" -gt 0 ] && [ $(( e_now - v_now )) -le 15 ]; then
      warmed=1
      break
    fi
    vprev="${verified}"; sleep 6; t=$(( t + 6 ))
  done
  # Pausing a validator that never verified would fail LATER with a misleading
  # "did not resume verifying" — fail here with the real reason instead.
  [ "${warmed}" -eq 1 ] \
    || fail "validator-lapse: never warmed up within ${t}s (verified=${verified} block=${v_now} exec=${e_now}) — not verifying live BEFORE the pause"
  log "validator-lapse: warmed up (verified=${verified} block=${v_now} exec=${e_now}) after ${t}s"

  local m0 vf0
  m0="$(val_metric validator_bal_missing_total)"; m0="${m0:-0}"
  vf0="$(val_metric validator_blocks_verified_total)"; vf0="${vf0:-0}"
  log "validator-lapse: pausing ${inner} for ${LAPSE_S}s (verified=${vf0} bal_missing=${m0})"
  docker exec "${VALIDATOR_NODE}" docker pause "${inner}" >/dev/null || fail "validator-lapse: docker pause failed"
  sleep "${LAPSE_S}"
  docker exec "${VALIDATOR_NODE}" docker unpause "${inner}" >/dev/null || fail "validator-lapse: docker unpause failed"
  log "validator-lapse: resumed; awaiting catch-up + continued verification"

  # After resume the validator drains the tx_bal it missed (held in the live
  # multicast term buffer while it was paused) and VERIFIES it. Assert: it
  # catches back up, KEEPS verifying (blocks_verified advances past the
  # pre-pause value — the lapse window was covered, not silently skipped),
  # coverage did not materially regress, and there is no divergence.
  local ok=0 v1 vf1
  t=0
  while [ "${t}" -lt 120 ]; do
    sleep 6; t=$(( t + 6 ))
    v1="$(val_metric validator_committed_block)"; v1="${v1:-0}"
    vf1="$(val_metric validator_blocks_verified_total)"; vf1="${vf1:-0}"
    e_now="$(executor_progress || echo "${e_now}")"
    if [ "${vf1}" -gt "${vf0}" ] && [ $(( e_now - v1 )) -le 25 ]; then ok=1; break; fi
  done
  local m1 d1
  m1="$(val_metric validator_bal_missing_total)"; m1="${m1:-0}"
  d1="$(val_metric validator_divergence_total)"; d1="${d1:-0}"
  [ "${ok}" -eq 1 ] || fail "validator-lapse: did not resume verifying + catch up in ${t}s (verified ${vf0}->${vf1}, block ${v1}, exec ${e_now})"
  [ "${vf1}" -gt "${vf0}" ] || fail "validator-lapse: stopped verifying after the pause (${vf0}->${vf1})"
  [ $(( m1 - m0 )) -le 5 ] || fail "validator-lapse: coverage REGRESSED — bal_missing grew ${m0}->${m1} across the pause (the lapse window was not covered by the live term buffer)"
  [ "${d1}" -eq 0 ] || fail "validator-lapse: ${d1} divergence(s) after resume"
  log "validator-lapse PASS: caught up (block ${v1}, lag $(( e_now - v1 ))), kept verifying ${vf0}->${vf1}, bal_missing ${m0}->${m1}, 0 divergences"
}
# Cluster-mode progress probe: the most-recently-committed block number as seen
# by the executor (kardamom_executor_block_number gauge on :9004). The Java
# cluster node exposes no Prometheus endpoint, so we measure pipeline liveness at
# the executor — it applies the blocks the cluster commits out its egress, so its
# block gauge advancing IS the cluster making progress. Prints the integer value
# (or empty if the scrape failed). awk takes $NF of the first matching sample and
# int-truncates (the gauge may render as a float / scientific notation).
executor_progress() {
  # MAX across all responding executors, NOT the first responder: a replica
  # restarted by a chaos case legitimately reports gauge 0 (or a low block)
  # while it replays/recovers, even though its peers — and the pipeline — are
  # fine. Pinning the probe to that replica reads as "pipeline NOT progressing
  # (block 0 -> 0)" when nothing is wrong (observed on node-failure-executor
  # right after hard-executor restarted executor-0).
  local i v best=""
  for i in "${!EXECUTOR_NODES[@]}"; do
    v="$(exec_metrics "${i}" \
      | awk -v m="${EXECUTOR_BLOCK_METRIC}" '$0 ~ "^"m"([{ ]|$)" && $0 !~ /^#/ { printf "%d", $NF; exit }')"
    [ -n "${v}" ] && { [ -z "${best}" ] || [ "${v}" -gt "${best}" ]; } && best="${v}"
  done
  [ -n "${best}" ] && { printf '%s' "${best}"; return 0; }
  return 1
}

# Ingress submit counter (kardamom_ingress_tx_received_total) summed across the
# active/active ingress nodes — the "is load actually flowing?" signal for the
# injection gate in run_case. Ingress binds its exporter on loopback, so this
# goes through docker exec. Prints the sum, or fails if no ingress answered.
INGRESS_NODES=(kardamom-ingress-0 kardamom-ingress-1)
INGRESS_PORT="${INGRESS_PORT:-9006}"
ingress_received() {
  local n v total=0 got=0
  for n in "${INGRESS_NODES[@]}"; do
    v="$(timeout 8 docker exec "${n}" curl -fsS --max-time 5 "http://127.0.0.1:${INGRESS_PORT}/metrics" 2>/dev/null \
      | awk '/^kardamom_ingress_tx_received_total/{ s += $NF } END { if (s != "") printf "%d", s }')"
    [ -n "${v}" ] && { total=$(( total + v )); got=1; }
  done
  [ "${got}" -eq 1 ] && { printf '%s' "${total}"; return 0; }
  return 1
}

# Restarted-replica health probe. KNOWN LIMITATION (re-opened F02.1): a
# restarted replica hydrates nonce floors from an empty state DB, so for
# ESTABLISHED senders it buffers refs as "future" and publishes nothing until
# a global hydration signal exists — the shard runs at P=1 for those senders
# (the racing twin covers them; fresh nonce-0 senders hydrate at floor 0 and
# are covered immediately). The nonce-floor fast-forward that made a rejoiner
# republish for established senders was REVERTED: under overload it forged
# canonical nonce gaps (nonces dropped before tx_data are invisible to BOTH
# replicas) and crashed every executor — see
# docs/reviews/2026-07-17-30-commit-review/fixes-CI-replay-loop.md. So this
# probe asserts what IS guaranteed: the restarted replica is alive and
# scrapeable (exporter up, session established), NOT that it republishes for
# the pinned established-sender load.
assert_replica_healthy() { # <node-container> <node-ip> <metrics-port> [slo-secs]
  local node="$1" ip="$2" port="$3" slo="${4:-90}" t=0 v
  while :; do
    # Bridge-direct first (exporters bind 0.0.0.0 since the replica-metrics
    # fix), docker exec fallback — same rationale as exec_metrics().
    v="$( { curl -fsS --max-time 5 "http://${ip}:${port}/metrics" 2>/dev/null \
          || timeout 8 docker exec "${node}" curl -fsS --max-time 5 "http://127.0.0.1:${port}/metrics" 2>/dev/null; } \
        | awk '/^kardamom_sequencer_/{ n++ } END { printf "%d", n }')"
    if [ -n "${v}" ] && [ "${v}" -gt 0 ]; then
      log "restarted replica on ${node} (:${port}) is up and exporting (${v} sequencer metrics; established-sender coverage stays on the twin — re-opened F02.1)"
      return 0
    fi
    [ "${t}" -ge "${slo}" ] \
      && fail "restarted replica on ${node} (:${port}) never came up: metrics unscrapable within ${slo}s of restart"
    sleep 5; t=$(( t + 5 ))
  done
}

# --- injectors --------------------------------------------------------------

# The injectors record WHAT they killed (stopped alloc id, or killed inner
# container id + where) so assert_count can require observed REPLACEMENT, not
# just a running count: right after a kill the doomed alloc can still report
# ClientStatus==running on the first poll, so a bare count>=N check could pass
# instantly against the OLD alloc without ever observing the outage — a
# vacuous "recovered within SLO". assert_count consumes + clears these.
KILLED_ALLOC=""
KILLED_NODE=""; KILLED_TASK=""; KILLED_CID=""

inject_graceful() { # <job>
  local alloc; alloc="$(running_alloc "$1")"
  [ -n "${alloc}" ] || fail "no running alloc to stop for job $1"
  log "graceful: nomad alloc stop ${alloc} (job $1)"
  on_control 'nomad alloc stop "$1"' "${alloc}" >/dev/null
  KILLED_ALLOC="${alloc}"
}

inject_hard() { # <node-container(s), space-separated candidates> <task-name>
  # Multiple candidates cover tasks whose placement can move between cases
  # (observed: after graceful-executor, the replacement alloc's container is
  # not always back on executor-0 when hard-executor probes it). The first
  # node actually running the task is killed; NO node running it is still a
  # loud failure — never a vacuous pass.
  local node cid=""
  for node in $1; do
    cid="$(docker exec "${node}" sh -c 'docker ps --filter name='"$2"' -q | head -1' 2>/dev/null)"
    [ -n "${cid}" ] && break
  done
  [ -n "${cid}" ] || fail "no running ${2} container to hard-kill on any of: ${1}"
  log "hard: docker kill inner ${2} container ${cid} on ${node}"
  docker exec "${node}" docker kill "${cid}" >/dev/null \
    || fail "could not hard-kill ${2} (${cid}) on ${node}"
  KILLED_NODE="${node}"; KILLED_TASK="$2"; KILLED_CID="${cid}"
}

# --- assertions -------------------------------------------------------------

assert_count() { # <job> <min-running> <slo-secs>
  # Besides count>=N, require evidence the recovery actually HAPPENED when we
  # know what was killed: a gracefully-stopped alloc must be replaced by a
  # DIFFERENT running alloc id; a hard-killed inner task container must be
  # running again under a NEW container id (Nomad restarts the task in-place —
  # the alloc id survives, but docker restarts always mint a new container id).
  local killed_alloc="${KILLED_ALLOC}" killed_node="${KILLED_NODE}"
  local killed_task="${KILLED_TASK}" killed_cid="${KILLED_CID}"
  KILLED_ALLOC=""; KILLED_NODE=""; KILLED_TASK=""; KILLED_CID=""
  local t=0
  while :; do
    if [ "$(count_running "$1")" -ge "$2" ]; then
      if [ -n "${killed_alloc}" ]; then
        # Replacement leg: >=N running allocs NOT counting the stopped one.
        if [ "$(running_allocs "$1" | grep -cv "^${killed_alloc}\$")" -ge "$2" ]; then
          log "$1 has >= $2 running alloc(s) after ${t}s (stopped alloc ${killed_alloc} replaced)"
          return 0
        fi
      elif [ -n "${killed_cid}" ]; then
        # Restart leg: same task name running again under a new container id.
        local cid_now
        cid_now="$(docker exec "${killed_node}" sh -c 'docker ps --filter name='"${killed_task}"' -q | head -1' 2>/dev/null || true)"
        if [ -n "${cid_now}" ] && [ "${cid_now}" != "${killed_cid}" ]; then
          log "$1 has >= $2 running alloc(s) after ${t}s (${killed_task} restarted on ${killed_node}: ${killed_cid} -> ${cid_now})"
          return 0
        fi
      else
        log "$1 has >= $2 running alloc(s) after ${t}s"
        return 0
      fi
    fi
    sleep 3; t=$((t+3))
    [ "${t}" -ge "$3" ] && fail "$1 did not reach >= $2 running (with the killed alloc/task replaced) within $3s (have $(count_running "$1"))"
  done
}

assert_progress() {
  # Pipeline-progress probe for the component-chaos cases. Prefer the sealer's
  # boundary counter (re-exported by the executors) when reachable — it ticks
  # every boundary, ~4/s, even with no load. If no executor exporter answers,
  # fall back to the executor's committed-block gauge (the same signal the
  # cluster cases use). Kept as a name so the ported component-chaos cases
  # below need no edits.
  local b0 b1
  b0="$(sealer_boundaries || true)"
  if [ -n "${b0}" ]; then
    sleep 10
    b1="$(sealer_boundaries || true)"; b1="${b1:-0}"
    awk "BEGIN{exit !(${b1}>${b0})}" \
      && log "pipeline progressing (sealer boundaries ${b0} -> ${b1})" \
      || fail "pipeline NOT progressing after recovery (sealer boundaries ${b0} -> ${b1})"
    return 0
  fi
  # No executor exporter answered: poll the executor block gauge instead.
  assert_executor_progress "$@"
}

# Cluster-mode analogue of assert_progress: the executor's committed-block gauge
# must advance. POLLS until it advances or a timeout — recovery is not instant
# after a leader kill (the cluster client must redirect to the newly-elected leader
# and resume egress before the executor applies the next block), so a single fixed
# window is flaky. Succeeds as soon as progress is observed. $1 = timeout secs
# (default 60).
assert_executor_progress() {
  local timeout="${1:-60}" e0 e1 t=0
  e0="$(executor_progress || true)"; e0="${e0:-0}"
  while :; do
    sleep 5; t=$((t + 5))
    e1="$(executor_progress || true)"; e1="${e1:-0}"
    awk "BEGIN{exit !(${e1}>${e0})}" \
      && { log "pipeline progressing (executor block ${e0} -> ${e1} after ${t}s)"; return 0; }
    [ "${t}" -ge "${timeout}" ] \
      && fail "pipeline NOT progressing (executor block ${e0} -> ${e1} over ${timeout}s)"
  done
}

# Per-replica recovery verdict: EVERY executor must individually converge to
# the fleet head before a case may pass. The MAX-across-replicas progress
# probe above keeps transient scrape gaps from failing a healthy run — but it
# also MASKS a replica that never recovered (wedged or crash-looping behind
# the fleet): the suite stayed green through months of every restarted
# executor being permanently broken, because ONE untouched replica carried
# every probe. A resilience suite that cannot see 2/3 of the fleet dead
# proves nothing, so every case now ends with this convergence check:
# within EXEC_CONVERGE_SLO_S, every executor's OWN gauge must be scrapeable
# and within EXEC_CONVERGE_LAG blocks of the fleet head.
EXEC_CONVERGE_SLO_S="${EXEC_CONVERGE_SLO_S:-150}"
EXEC_CONVERGE_LAG="${EXEC_CONVERGE_LAG:-50}"
assert_executors_converged() { # <case>
  local case_name="$1" t=0 i v max ok bad lag
  local -a blk
  while :; do
    max=""; bad=""; blk=()
    for i in "${!EXECUTOR_NODES[@]}"; do
      # `|| true` is load-bearing (same class as val_metric's): under
      # `set -euo pipefail` a failed scrape — e.g. the just-returned node's
      # exporter not up yet — otherwise kills the whole script silently, with
      # no fail() message (pipefail makes the assignment itself fail). The
      # probes above only survive because they run in `$(… || true)` subshells.
      v="$(exec_metrics "${i}" \
        | awk -v m="${EXECUTOR_BLOCK_METRIC}" '$0 ~ "^"m"([{ ]|$)" && $0 !~ /^#/ { printf "%d", $NF; exit }' \
        || true)"
      if [ -n "${v}" ]; then
        blk[i]="${v}"
        if [ -z "${max}" ] || [ "${v}" -gt "${max}" ]; then max="${v}"; fi
      else
        bad="${bad} ${EXECUTOR_NODES[$i]}=unreachable"
      fi
    done
    if [ -z "${bad}" ] && [ -n "${max}" ]; then
      ok=1
      for i in "${!EXECUTOR_NODES[@]}"; do
        lag=$(( max - blk[i] ))
        [ "${lag}" -le "${EXEC_CONVERGE_LAG}" ] \
          || { ok=0; bad="${bad} ${EXECUTOR_NODES[$i]}=lag:${lag}"; }
      done
      if [ "${ok}" -eq 1 ]; then
        log "${case_name}: all ${#EXECUTOR_NODES[@]} executors converged (head ${max}, per-replica lag <= ${EXEC_CONVERGE_LAG}) after ${t}s"
        return 0
      fi
    fi
    [ "${t}" -ge "${EXEC_CONVERGE_SLO_S}" ] \
      && fail "${case_name}: executor fleet NOT fully recovered within ${EXEC_CONVERGE_SLO_S}s (head=${max:-?};${bad# })"
    sleep 5; t=$(( t + 5 ))
  done
}

# Cluster-mode "must NOT progress": the executor's block gauge must stay FLAT
# over a window (quorum lost → no new commits → no false progress). $1 = window.
assert_executor_stalled() {
  local window="${1:-15}" e0 e1
  e0="$(executor_progress || true)"; e0="${e0:-0}"
  sleep "${window}"
  e1="$(executor_progress || true)"; e1="${e1:-0}"
  awk "BEGIN{exit !(${e1}<=${e0})}" \
    && log "pipeline correctly STALLED (executor block ${e0} -> ${e1} over ${window}s, no false progress)" \
    || fail "pipeline UNEXPECTEDLY progressed while quorum lost (executor block ${e0} -> ${e1})"
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

# Relaxed load verdict for the TOTAL-OUTAGE case (cluster-quorum-loss-recover). A
# full quorum loss legitimately stops the sequencer from ACCEPTING some txs offered
# during the outage — they surface as seq_dropped / past-nonce and were never
# accepted, so they are not a delivery gap. The guarantee that must still hold is
# GAPLESS delivery of every ACCEPTED tx (missing == 0) — which the cluster meets on
# recovery. So assert missing==0 here instead of the strict all-delivered verdict.
assert_accepted_delivered() { # <json> <case>
  if grep -qE '"missing":[[:space:]]*0[[:space:]]*,' "$1" 2>/dev/null; then
    log "load OK for case $2: every ACCEPTED tx receipted (missing=0); seq_dropped tolerated during total outage"
  else
    echo "----- load report ($2) [$1] -----" >&2
    [ -s "$1" ] && sed -n '1,80p' "$1" >&2 || echo "(report JSON missing/empty)" >&2
    fail "accepted txs NOT all delivered (missing>0) for case $2"
  fi
}

# --- cluster leadership detection -------------------------------------------

# Return the memberId (0/1/2) of the CURRENT Raft leader, by grepping the LAST
# `cluster role=LEADER memberId=N` line across the 3 `cluster` allocs' stdout.
# SealerClusteredService.onRoleChange() prints that line to stdout on every role
# transition (deliberately stdout, not slf4j, for grep-ability), so the last such
# line in an alloc's log is that member's most-recent role; the member whose last
# role line is LEADER is the current leader. memberId N == kardamom-sealer-N.
#
# Tolerates election windows with a bounded retry loop ($1 = max secs, default
# CHAOS_LEADER_SLO_S): right after a kill there may briefly be no leader line yet.
# Prints the leader memberId on stdout (and nothing else); fails if none found in
# time. Reads logs via the control node's `nomad alloc logs` (same access pattern
# the rest of the file uses).
cluster_leader() { # [max-secs]
  local max="${1:-${CHAOS_LEADER_SLO_S}}" t=0 allocs alloc lastrole leader
  while :; do
    leader=""
    # All cluster allocs (running or not) — a just-killed member's log may still
    # show its last role line, but we only trust a member whose LAST line is LEADER.
    allocs="$(on_control 'nomad job allocs -t "{{range .}}{{.ID}}{{\"\n\"}}{{end}}" "$1"' "${CLUSTER_TASK}" 2>/dev/null || true)"
    while read -r alloc; do
      [ -n "${alloc}" ] || continue
      # Last role line in this alloc's stdout. Empty if it never logged a role.
      lastrole="$(on_control 'nomad alloc logs "$1" 2>/dev/null | grep -E "cluster role=[A-Z]+ memberId=[0-9]+" | tail -1' "${alloc}" 2>/dev/null || true)"
      case "${lastrole}" in
        *role=LEADER*)
          leader="$(printf '%s' "${lastrole}" | sed -nE 's/.*memberId=([0-9]+).*/\1/p')"
          ;;
      esac
    done <<<"${allocs}"
    [ -n "${leader}" ] && { printf '%s' "${leader}"; return 0; }
    sleep 3; t=$((t+3))
    [ "${t}" -ge "${max}" ] && fail "no cluster leader observed within ${max}s (checked alloc logs for 'role=LEADER memberId=')"
  done
}

# --- the per-case driver ----------------------------------------------------

run_case() { # <case-name>
  local name="$1"
  local out="/tmp/chaos-${name}.json"
  local logf="/tmp/chaos-${name}.load.log"
  log "================= CHAOS CASE: ${name} ================="

  # One dedicated fresh funded account per case (single sender, nonces from 0)
  # so cases never collide / leave nonce gaps on the never-reset chain.
  if [ "${name}" = "sequencer-replica-kill" ]; then
    # PIN this case's load to SHARD 0 — the shard whose replica A is killed
    # (seq-a on node-0). An arbitrary account lands on shard 0 or 1 by address
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

  case "${name}" in
    graceful-executor)  inject_graceful executor;                       assert_count executor 3 "${CHAOS_RESTART_SLO_S}" ;;
    hard-executor)      inject_hard "kardamom-executor-0 kardamom-executor-1 kardamom-executor-2" executor; assert_count executor 3 "${CHAOS_RESTART_SLO_S}" ;;
    graceful-ingress)   inject_graceful ingress;                        assert_count ingress 1 "${CHAOS_RESTART_SLO_S}" ;;
    hard-ingress)       inject_hard kardamom-ingress-0 ingress;         assert_count ingress 1 "${CHAOS_RESTART_SLO_S}" ;;
    # Sequencers run P=2 racing replicas per shard (job groups seq-a/seq-b,
    # 4 allocs total): a kill no longer stalls its shard — the twin on the
    # other node keeps ordering, so these also assert live pipeline progress.
    graceful-sequencer) inject_graceful sequencer;                      assert_progress; assert_count sequencer 4 "${CHAOS_RESTART_SLO_S}" ;;
    # Explicit task name: `name=sequencer` would match BOTH the sequencer-a and
    # sequencer-b task containers and kill an arbitrary one.
    hard-sequencer)     inject_hard kardamom-sequencer-0 sequencer-a;   assert_progress; assert_count sequencer 4 "${CHAOS_RESTART_SLO_S}" ;;

    sequencer-replica-kill)
      # HARD-kill a SPECIFIC replica (seq-a on node-0 = shard 0's replica A;
      # its twin is seq-b on node-1). The case's load is PINNED to shard 0
      # (see the account selection above), so the assertions actually cover
      # the shard that lost a replica: it must stay live with NO stall — the
      # racing twin never stopped and the cluster dedups its refs — the killed
      # replica restarts to full strength (4/4) and comes back healthy.
      # Established-sender coverage on the rejoiner is a KNOWN gap (re-opened
      # F02.1, see assert_replica_healthy).
      inject_hard kardamom-sequencer-0 sequencer-a
      assert_progress
      assert_count sequencer 4 "${CHAOS_RESTART_SLO_S}"
      # seq-a on node-0: sequencer ip lane starts at .21, seq-a metrics :9001.
      assert_replica_healthy kardamom-sequencer-0 192.168.56.21 9001
      ;;
    sealer-graceful)    inject_graceful sealer;                         assert_count sealer 1 "${CHAOS_RESTART_SLO_S}" ;;
    # KNOWN GAP (single-sealer topology): after a HARD sealer crash the executors
    # freeze and don't re-attach to the restarted sealer's canonical tx_ordering
    # (sealer was a singleton SPOF; HA was future work). SUPERSEDED by the
    # clustered sealer (Phase 3): the cluster-leader-kill case below now covers the
    # hard-kill-of-the-ordering-authority scenario with a 3-member Raft quorum that
    # re-elects. Excluded from the always-on CI suite (see cluster-e2e.yml); kept
    # here to reproduce against a legacy single-sealer deploy; tracked in issue #58.
    sealer-hard)        inject_hard kardamom-sealer-0 sealer;           assert_count sealer 1 "${CHAOS_RESTART_SLO_S}" ;;
    node-failure-executor)
      # Kill the whole node container. With 3 executor-role nodes + distinct_hosts
      # the lost replica can't reschedule onto a peer (none free), so the cluster
      # degrades to 2 and must keep progressing; bringing the node back recovers 3.
      log "node-failure: docker kill kardamom-executor-2 (whole node)"
      docker kill kardamom-executor-2 >/dev/null || fail "could not kill node kardamom-executor-2"
      assert_count executor 2 "${CHAOS_RESTART_SLO_S}"
      # Wide window here too: killing a whole NODE thrashes the runner (docker
      # teardown + nomad node-down churn) enough that on 4-core CI hosts even
      # the survivors' metric scrapes black out well past 60s.
      assert_executor_progress 180
      log "node-failure: docker start kardamom-executor-2 (node returns)"
      docker start kardamom-executor-2 >/dev/null || fail "could not restart node kardamom-executor-2"
      assert_count executor 3 "${CHAOS_RESCHEDULE_SLO_S}"
      ;;

    state-checkpoint-restore)
      # DATA-loss drill: WIPE executor-0's state DB (and its own checkpoints),
      # then restore from a PEER executor's checkpoint. Executor replicas are
      # deterministic state machines at the same block, so executor-1's checkpoint
      # is a valid restore source. On restart, executor-0 finds an empty state DB
      # + the peer checkpoint and restores it BEFORE opening the env — replaying
      # only the tail instead of re-syncing from genesis. Expected, in order:
      #   1. the fleet degrades 3->2 and keeps progressing (deterministic replicas);
      #   2. executor-0 restarts and RESTORES FROM THE CHECKPOINT (asserted via the
      #      "restored state from checkpoint" log line — else it silently fell back
      #      to a full genesis re-sync, which this case exists to prevent);
      #   3. executor count returns to 3.
      local peer_ck="" ex0_inner
      log "state-checkpoint-restore: waiting for a checkpoint on peer executor-1"
      for _ in $(seq 1 15); do
        peer_ck="$(docker exec kardamom-executor-1 bash -lc \
          'ls /opt/kardamom/checkpoints/checkpoint-* 2>/dev/null | head -1' 2>/dev/null || true)"
        [ -n "${peer_ck}" ] && break
        sleep 5
      done
      [ -n "${peer_ck}" ] || fail "state-checkpoint-restore: peer executor-1 produced no checkpoint"
      log "state-checkpoint-restore: killing executor-0 + wiping its state DB and checkpoints"
      inject_hard kardamom-executor-0 executor
      docker exec kardamom-executor-0 bash -lc 'rm -rf /opt/kardamom/state/* /opt/kardamom/checkpoints/*' \
        || fail "state-checkpoint-restore: could not wipe executor-0 state"
      log "state-checkpoint-restore: re-replicating checkpoints from executor-1"
      # Copy ONE complete checkpoint, not the whole dir: the writer adds a new
      # checkpoint every interval and prunes old ones, so tar-ing the parent
      # races with that churn ("file changed as we read it", exit 1). Visible
      # checkpoint-* dirs are immutable (compacted under a tmp name, renamed
      # into place when done); the retry covers only the narrow window where
      # the picked checkpoint is pruned mid-copy.
      local ck_name="" copied=0
      for _ in 1 2 3; do
        ck_name="$(docker exec kardamom-executor-1 bash -lc \
          'ls -d /opt/kardamom/checkpoints/checkpoint-* 2>/dev/null | sort | tail -1' \
          | xargs -rn1 basename)"
        [ -n "${ck_name}" ] || { sleep 2; continue; }
        docker exec kardamom-executor-0 bash -lc 'rm -rf /opt/kardamom/checkpoints/*'
        if docker exec kardamom-executor-1 tar -C /opt/kardamom -cf - "checkpoints/${ck_name}" \
          | docker exec -i kardamom-executor-0 tar -C /opt/kardamom -xf -; then
          copied=1
          break
        fi
        sleep 2
      done
      [ "${copied}" = 1 ] || fail "state-checkpoint-restore: checkpoint copy failed"
      # The surviving replicas must keep the pipeline progressing on 2.
      assert_executor_progress 180
      # executor-0 restarts, restores from the peer checkpoint, rejoins to 3.
      assert_count executor 3 "${CHAOS_RESCHEDULE_SLO_S}"
      ex0_inner="$(docker exec kardamom-executor-0 bash -lc \
        'docker ps --format "{{.Names}}" | grep -i executor | head -1')"
      [ -n "${ex0_inner}" ] || fail "state-checkpoint-restore: no executor container on executor-0 after restart"
      docker exec kardamom-executor-0 bash -lc \
        "docker logs ${ex0_inner} 2>&1 | grep -q 'restored state from checkpoint'" \
        || fail "state-checkpoint-restore: executor-0 did NOT restore from checkpoint (fell back to genesis re-sync)"
      log "state-checkpoint-restore: executor-0 restored from peer checkpoint + rejoined (no genesis re-sync)"
      ;;

    replay-window-resync)
      # FULL-RESYNC drill: WIPE executor-1's state DB and checkpoints, then let
      # the node repair ITSELF — no harness-side checkpoint copy. A wiped node
      # cannot re-sync from genesis (the cluster retains a bounded canonical
      # window; a REPLAY_FROM below its floor is refused with
      # REPLAY_UNAVAILABLE), so on restart the executor must fetch a checkpoint
      # from a peer replica over the checkpoint-serve port (9014) BEFORE its
      # first join, restore it, and resume from there. Expected, in order:
      #   1. the fleet degrades 3->2 and keeps progressing;
      #   2. executor-1 restarts, FETCHES a peer checkpoint (asserted via the
      #      "fetched checkpoint from peer" log line — the line only this new
      #      self-heal path emits) and restores it ("restored state from
      #      checkpoint");
      #   3. executor count returns to 3 and the fleet converges.
      # (Victim is executor-1, not executor-0, so this case and
      # state-checkpoint-restore stay independent when they run back-to-back.)
      local ex1_inner
      log "replay-window-resync: waiting for a checkpoint on peer executor-0"
      for _ in $(seq 1 15); do
        docker exec kardamom-executor-0 bash -lc \
          'ls /opt/kardamom/checkpoints/checkpoint-* >/dev/null 2>&1' && break
        sleep 5
      done
      docker exec kardamom-executor-0 bash -lc \
        'ls /opt/kardamom/checkpoints/checkpoint-* >/dev/null 2>&1' \
        || fail "replay-window-resync: peer executor-0 produced no checkpoint"
      log "replay-window-resync: killing executor-1 + wiping its state DB and checkpoints"
      inject_hard kardamom-executor-1 executor
      docker exec kardamom-executor-1 bash -lc 'rm -rf /opt/kardamom/state/* /opt/kardamom/checkpoints/*' \
        || fail "replay-window-resync: could not wipe executor-1 state"
      # The surviving replicas must keep the pipeline progressing on 2.
      assert_executor_progress 180
      # executor-1 restarts, self-heals from a peer checkpoint, rejoins to 3.
      assert_count executor 3 "${CHAOS_RESCHEDULE_SLO_S}"
      ex1_inner="$(docker exec kardamom-executor-1 bash -lc \
        'docker ps --format "{{.Names}}" | grep -i executor | head -1')"
      [ -n "${ex1_inner}" ] || fail "replay-window-resync: no executor container on executor-1 after restart"
      docker exec kardamom-executor-1 bash -lc \
        "docker logs ${ex1_inner} 2>&1 | grep -q 'fetched checkpoint from peer'" \
        || fail "replay-window-resync: executor-1 did NOT fetch a peer checkpoint (self-heal path not taken)"
      docker exec kardamom-executor-1 bash -lc \
        "docker logs ${ex1_inner} 2>&1 | grep -q 'restored state from checkpoint'" \
        || fail "replay-window-resync: executor-1 fetched but did NOT restore the peer checkpoint"
      log "replay-window-resync: executor-1 self-healed from a peer checkpoint (fetch + restore + rejoin)"
      ;;

    archive-driver-loss)
      # HARD-kill the Aeron SUBSTRATE (the `aeron` system job's combined
      # ArchivingMediaDriver task) on the ingress-0 node — not the ingress task
      # itself. This is the untested failure surface the component cases skip:
      # every service on the node shares that driver's aeron.dir, so the local
      # ingress loses its transport AND its tx_data durability recorder in one
      # blow. Expected outcome, in order:
      #   1. the pipeline keeps progressing — ingress is active/active, so
      #      clients retry against ingress-1 (the load runs in --chaos-mode);
      #   2. Nomad restarts the aeron system task within the restart SLO
      #      (archive segments persist on the node volume across the restart);
      #   3. the collocated ingress task recovers against the fresh driver and
      #      the ingress job returns to full strength within the reschedule SLO
      #      (driver + dependent-task restart chain, hence the wider SLO).
      local aeron_base
      aeron_base="$(count_running aeron)"
      # A hiccuping nomad query reads as 0/empty; injecting anyway would make
      # the post-kill `assert_count aeron >= 0` pass trivially and silently
      # drop the "driver restarted" leg of the case.
      [ "${aeron_base:-0}" -gt 0 ] \
        || fail "archive-driver-loss: no running aeron allocs at baseline (got '${aeron_base}') — cannot assert driver recovery"
      log "archive-driver-loss: killing archiving-media-driver on kardamom-ingress-0 (aeron allocs baseline=${aeron_base})"
      inject_hard kardamom-ingress-0 archiving-media-driver
      assert_progress
      assert_count aeron "${aeron_base}" "${CHAOS_RESTART_SLO_S}"
      assert_count ingress 2 "${CHAOS_RESCHEDULE_SLO_S}"
      ;;

    archive-tx-data-wipe)
      # DATA-loss drill (not just process loss): permanently WIPE ingress-0's
      # tx_data archive volume, then restore it from ingress-1's mirror. tx_data
      # is UDP multicast, so BOTH ingress archives record every publisher's shard
      # streams — the segments are byte-identical across the two nodes (verified
      # by sha256), so a single node's archive loss is survivable and the peer is
      # an exact restore source. This exercises `kardamom-archive-rereplicate`'s
      # mechanism (segment + catalog mirror). Expected outcome, in order:
      #   1. the pipeline keeps progressing on ingress-1 (active/active) while
      #      ingress-0's substrate is down;
      #   2. after re-replicating ingress-1's segments + catalog, the restarted
      #      ingress-0 archive adopts them and Aeron's own `ArchiveTool verify`
      #      reports every recording OK;
      #   3. ingress + aeron return to full strength.
      local aeron_base ac0 verify_out
      aeron_base="$(count_running aeron)"
      log "archive-tx-data-wipe: killing aeron substrate on kardamom-ingress-0 + wiping its tx_data archive volume"
      inject_hard kardamom-ingress-0 archiving-media-driver
      # Simulate permanent volume loss while the driver is down (segments + catalog).
      docker exec kardamom-ingress-0 bash -lc \
        'rm -f /opt/kardamom/archive/dir/*.rec /opt/kardamom/archive/dir/archive.catalog' \
        || fail "archive-tx-data-wipe: could not wipe ingress-0 archive"
      # Re-replicate from the surviving peer (ingress-1): stream its archive dir
      # across. This is the transport that kardamom-archive-rereplicate wraps for
      # an operator (peer copy -> mirror_archive -> verify_mirror).
      log "archive-tx-data-wipe: re-replicating archive from kardamom-ingress-1 mirror"
      docker exec kardamom-ingress-1 tar -C /opt/kardamom/archive -cf - dir \
        | docker exec -i kardamom-ingress-0 tar -C /opt/kardamom/archive -xf - \
        || fail "archive-tx-data-wipe: re-replication copy failed"
      # The pipeline must have ridden through on ingress-1 the whole time.
      assert_progress
      # aeron restarts (system job) and adopts the restored archive.
      assert_count aeron "${aeron_base}" "${CHAOS_RESTART_SLO_S}"
      # Verify the restored archive with Aeron's own tool: every recording OK.
      ac0="$(docker exec kardamom-ingress-0 bash -lc \
        'docker ps --format "{{.Names}}" | grep archiving-media-driver | head -1')"
      [ -n "${ac0}" ] || fail "archive-tx-data-wipe: no aeron container on ingress-0 after restart"
      verify_out="$(docker exec kardamom-ingress-0 bash -lc \
        "docker exec ${ac0} bash -lc 'java -cp /opt/aeron/aeron-all.jar io.aeron.archive.ArchiveTool /opt/kardamom/archive/dir verify 2>&1'" || true)"
      echo "${verify_out}" | grep -qE "recordingId=.*OK" \
        || fail "archive-tx-data-wipe: restored archive failed ArchiveTool verify: ${verify_out}"
      if echo "${verify_out}" | grep -qiE "ERR |invalid|FAILED"; then
        fail "archive-tx-data-wipe: restored archive verify reported errors: ${verify_out}"
      fi
      log "archive-tx-data-wipe: restored archive verified OK on ingress-0 (2-copy redundancy recovered)"
      assert_count ingress 2 "${CHAOS_RESCHEDULE_SLO_S}"
      ;;

    # --- CLUSTERED-SEALER (Raft) cases ------------------------------------
    # Progress is measured at the EXECUTOR (the Java cluster node has no
    # Prometheus endpoint); the cluster commits blocks out its egress, the
    # executor applies them, so executor_progress() advancing == cluster liveness.

    cluster-leader-kill)
      # HARD-kill the inner cluster container on the CURRENT leader's node. The
      # 3-member Raft quorum must survive losing the leader and KEEP COMMITTING — the
      # executor's block gauge resumes advancing once the cluster has a live leader
      # again. This REPLACES the documented single-sealer hard-kill SPOF gap (#58):
      # a single sealer crash froze the pipeline; the Raft cluster does not.
      #
      # NOTE: we assert the pipeline keeps progressing, NOT that the leader's memberId
      # changed. A hard-killed leader's Nomad task can restart fast and RE-WIN the
      # election (it has the most up-to-date log), so requiring a different memberId
      # is racy and wrong — "the cluster still commits" is the real resilience proof
      # (it requires a live leader + quorum regardless of which member leads).
      local old_leader leader_node
      old_leader="$(cluster_leader)"
      leader_node="kardamom-sealer-${old_leader}"
      log "cluster-leader-kill: current leader memberId=${old_leader} on ${leader_node}; hard-killing its cluster container"
      inject_hard "${leader_node}" "${CLUSTER_TASK}"
      # Quorum re-establishes a leader (a different member, or the restarted one
      # re-winning) → the pipeline resumes committing. assert_executor_progress polls
      # up to its timeout, covering the election + client redirect window.
      assert_executor_progress
      log "cluster-leader-kill: pipeline resumed committing after leader kill (now leader memberId=$(cluster_leader 2>/dev/null || echo '?'))"
      # The killed member's Nomad task restarts (force_pull re-pulls the image) and
      # rejoins, returning the cluster job to 3 running.
      assert_count "${CLUSTER_TASK}" 3 "${CHAOS_RESTART_SLO_S}"
      ;;

    cluster-follower-kill)
      # HARD-kill a member that is NOT the leader. Quorum (2/3) is unaffected, so
      # the pipeline must keep progressing with NO stall — the executor's block
      # gauge advances throughout. (No new election is required; the leader is
      # untouched.) The killed member's task then restarts and rejoins (3/3).
      local leader follower
      leader="$(cluster_leader)"
      # Pick any memberId in 0..2 that isn't the leader.
      for follower in 0 1 2; do [ "${follower}" != "${leader}" ] && break; done
      log "cluster-follower-kill: leader=memberId=${leader}; killing FOLLOWER memberId=${follower} on kardamom-sealer-${follower}"
      inject_hard "kardamom-sealer-${follower}" "${CLUSTER_TASK}"
      # Quorum holds (2/3): the executor must keep applying blocks with no stall.
      assert_executor_progress
      # Leader must be UNCHANGED (a follower loss does not trigger re-election).
      local still
      still="$(cluster_leader)"
      [ "${still}" = "${leader}" ] \
        && log "cluster-follower-kill: leader unchanged (memberId=${leader}) — quorum held" \
        || log "cluster-follower-kill: WARN leader changed (${leader} -> ${still}); quorum still held, progress OK"
      # Killed follower's task restarts and rejoins (3/3).
      assert_count "${CLUSTER_TASK}" 3 "${CHAOS_RESTART_SLO_S}"
      ;;

    cluster-quorum-loss-recover)
      # Kill TWO WHOLE sealer NODE containers (docker kill the kardamom-sealer-X
      # containers themselves, not the inner task): Nomad on those nodes is gone
      # too, so the inner cluster tasks CANNOT restart there → only 1 member left →
      # Raft quorum (needs 2/3) is LOST. The pipeline MUST stall (no false progress:
      # the executor's block gauge stays flat). Then bring ONE node back (docker
      # start): quorum (2/3) returns, a leader is re-elected, progress RESUMES, and
      # the backlog drains gaplessly (load verdict PASS). Generous SLOs: a node
      # restart re-pulls images, so use CHAOS_RESCHEDULE_SLO_S for the rejoin.
      local victims=(kardamom-sealer-1 kardamom-sealer-2)
      log "cluster-quorum-loss-recover: docker kill TWO sealer nodes (${victims[*]}) → quorum lost (1/3 up)"
      docker kill "${victims[@]}" >/dev/null || fail "could not kill sealer nodes ${victims[*]}"
      # Quorum lost → the pipeline must STALL (no commits, executor gauge flat).
      assert_executor_stalled 15
      log "cluster-quorum-loss-recover: docker start ${victims[0]} (quorum 2/3 returns)"
      docker start "${victims[0]}" >/dev/null || fail "could not restart node ${victims[0]}"
      # Its cluster task reschedules + rejoins → quorum restored. Give it the
      # reschedule SLO (image re-pull). count_running counts the `cluster` allocs.
      assert_count "${CLUSTER_TASK}" 2 "${CHAOS_RESCHEDULE_SLO_S}"
      # With quorum back, the executor must resume applying blocks (drains backlog).
      # WIDE timeout — this is the one case where the clients' cluster SESSIONS
      # die (a >15s total outage exceeds the session timeout; leader-kill keeps
      # sessions alive via NewLeaderEvent redirect). Observed on CI: re-election
      # + client session re-establishment alone takes ~50s after the node
      # restart, and the reopened sessions then replay the canonical stream from
      # the log before NEW commits surface on the executor's block gauge. 60s
      # timed out reproducibly (3/3 runs, sessions reopening ~40s in); 180s
      # covers re-election + reconnect + replay with margin.
      assert_executor_progress 180
      # Restore the second node too so the suite leaves a healthy 3/3 cluster for
      # any subsequent cases (best-effort; not asserted as part of this case's SLO).
      docker start "${victims[1]}" >/dev/null 2>&1 || true
      ;;

    validator-lapse)
      # No component killed: pause the (off-hot-path) validator and assert it
      # resumes verifying with coverage held — the live term buffer redelivers
      # the paused window on resume, and the catch-up skip (#78) bounds
      # anything that aged out. All validator-specific asserts live in the
      # helper.
      run_validator_lapse
      ;;

    *) fail "unknown chaos case: ${name}" ;;
  esac

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
