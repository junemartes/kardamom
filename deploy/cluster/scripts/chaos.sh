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
#        sequencer-lapse
#        sealer-graceful sealer-hard
#        node-failure-executor archive-driver-loss
#        cluster-leader-kill cluster-follower-kill cluster-quorum-loss-recover
# =============================================================================
set -euo pipefail

# SIGPIPE-safe substring / ERE tests over captured command output. NEVER use
# `echo "${big}" | grep -q` (or `producer | grep -q`) for asserts in this
# script: `grep -q` exits at the first match, and once the producer's output
# exceeds the pipe buffer (64KB) the producer takes SIGPIPE (141) — under
# `set -o pipefail` that DISCARDS the successful match. Observed live: the
# retention-overrun asserts reported "consumer never hit REPLAY_UNAVAILABLE"
# while the refusal sat in the very alloc logs they grepped (the per-iteration
# `echo: write error: Broken pipe` spam in the CI log was each match being
# thrown away). Negated sites are worse — a SIGPIPE'd match reads as absence.
# Pure-bash matching has no pipe to break.
has_line()  { [[ "$1" == *"$2"* ]]; }   # fixed substring (no regex chars)
has_match() { [[ "$1" =~ $2 ]]; }       # POSIX ERE

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

# Shared control-node helpers (on_control, running_alloc, count_running, ...).
# shellcheck source=deploy/cluster/scripts/lib.sh
source "${SCRIPT_DIR}/lib.sh"

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
fail() {
  # BOTH streams: stderr for the exit path, stdout so the message lands
  # in-order in the CI log next to the case's own lines (a fail seen only
  # on a reordered/dropped stderr reads as a silent death — observed on
  # run 30281 series: a case aborted with no visible CHAOS FAIL line).
  echo "CHAOS FAIL: $*"
  echo "CHAOS FAIL: $*" >&2
  exit 1
}

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
# Forensics for validator-lapse failures: the validator lives alone on the
# aux tier and the generic failure dump does not cover it — capture the
# nomad view, the node's container states, and the validator's own log tail
# so a dark-endpoint verdict is attributable (process dead vs exec wedge vs
# supervisor not restarting).
val_debug() {
  log "validator-lapse DEBUG: nomad validator job status:"
  on_control 'nomad job status validator 2>/dev/null | tail -12' 2>/dev/null || true
  log "validator-lapse DEBUG: containers on ${VALIDATOR_NODE}:"
  timeout 15 docker exec "${VALIDATOR_NODE}" sh -c 'docker ps -a --format "{{.Names}} {{.Status}}" | head -6' 2>/dev/null || true
  log "validator-lapse DEBUG: validator container log tail:"
  timeout 20 docker exec "${VALIDATOR_NODE}" sh -c \
    'docker logs --tail 25 "$(docker ps -a --format "{{.Names}}" | grep -m1 "^validator")" 2>&1 | tail -20' 2>/dev/null || true
}

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

  local m0 vf0 started0
  m0="$(val_metric validator_bal_missing_total)"; m0="${m0:-0}"
  vf0="$(val_metric validator_blocks_verified_total)"; vf0="${vf0:-0}"
  started0="$(timeout 15 docker exec "${VALIDATOR_NODE}" docker inspect -f '{{.State.StartedAt}}' "${inner}" 2>/dev/null || true)"

  # SIGSTOP + VERIFIED freeze (#108): `docker pause` silently no-ops in the
  # nested-DinD freezer, so every prior run of this case asserted against a
  # validator that never lapsed — the asserts pass identically with no
  # actual pause. Signals hit the task's PID 1 regardless of freezer
  # delegation, and the mid-freeze probe makes a silent no-op IMPOSSIBLE.
  log "validator-lapse: freezing ${inner} (SIGSTOP) for ${LAPSE_S}s (verified=${vf0} bal_missing=${m0} started=${started0:-?})"
  docker exec "${VALIDATOR_NODE}" docker kill -s STOP "${inner}" >/dev/null \
    || fail "validator-lapse: SIGSTOP failed"
  sleep 3
  if timeout 8 docker exec "${VALIDATOR_NODE}" curl -fsS --max-time 3 \
      "http://127.0.0.1:${VALIDATOR_PORT}/metrics" >/dev/null 2>&1; then
    docker exec "${VALIDATOR_NODE}" docker kill -s CONT "${inner}" >/dev/null 2>&1 || true
    fail "validator-lapse: freeze did NOT take effect (metrics endpoint still answering mid-freeze)"
  fi
  log "validator-lapse: freeze verified (metrics endpoint dark)"
  sleep $(( LAPSE_S - 3 ))
  if ! timeout 20 docker exec "${VALIDATOR_NODE}" docker kill -s CONT "${inner}" >/dev/null 2>&1; then
    # A failed CONT is NOT a case error by itself: the frozen task can be
    # REPLACED under us mid-freeze (supervisor action) — which IS the
    # newborn path — or the node exec can be transiently wedged. The
    # sampling loop below owns the verdict (end state: verifying live,
    # caught up, zero divergences); the thaw mechanics must never abort
    # the case.
    local cur0
    cur0="$(timeout 15 docker exec "${VALIDATOR_NODE}" sh -c 'docker ps --format "{{.Names}}" | grep -m1 "^validator"' 2>/dev/null || true)"
    if [ -n "${cur0}" ] && [ "${cur0}" != "${inner}" ]; then
      log "validator-lapse: SIGCONT target gone — container replaced during freeze (${inner} -> ${cur0}); newborn path"
    else
      sleep 5
      timeout 20 docker exec "${VALIDATOR_NODE}" docker kill -s CONT "${inner}" >/dev/null 2>&1 \
        || log "validator-lapse: SIGCONT failed twice (state unknown); relying on supervisor + sampling asserts"
    fi
  fi

  # VERIFIED THAW (mirror of the verified freeze): a CONT that silently
  # misses leaves a FROZEN ORPHAN squatting the metrics port — every
  # supervisor replacement then dies instantly on EADDRINUSE (fatal in
  # kardamom_obs::init), burns the restart budget, and mode=fail strands
  # the validator permanently (reproduced locally; the 240s dark-endpoint
  # run). Within a grace window the endpoint must answer OR the container
  # must have been replaced; otherwise KILL the frozen container so the
  # port frees and the supervisor restarts into clean air.
  local thaw_ok=0 tw=0 curX
  while [ "${tw}" -lt 30 ]; do
    sleep 5; tw=$(( tw + 5 ))
    if timeout 8 docker exec "${VALIDATOR_NODE}" curl -fsS --max-time 3 \
        "http://127.0.0.1:${VALIDATOR_PORT}/metrics" >/dev/null 2>&1; then
      thaw_ok=1; break
    fi
    curX="$(timeout 15 docker exec "${VALIDATOR_NODE}" sh -c 'docker ps --format "{{.Names}}" | grep -m1 "^validator"' 2>/dev/null || true)"
    if [ -n "${curX}" ] && [ "${curX}" != "${inner}" ]; then
      thaw_ok=1; log "validator-lapse: container replaced during/after freeze (${inner} -> ${curX})"; break
    fi
  done
  if [ "${thaw_ok}" -ne 1 ]; then
    log "validator-lapse: thaw NOT confirmed after ${tw}s — killing the frozen orphan (releases the metrics port for the supervisor's replacement)"
    timeout 20 docker exec "${VALIDATOR_NODE}" docker kill "${inner}" >/dev/null 2>&1 || true
  fi

  # POST-THAW, identity decides the contract. A ${LAPSE_S}s freeze exceeds
  # the media driver's client-liveness timeout: the validator's aeron client
  # is EVICTED and the process fail-stops on thaw → Nomad restarts it → the
  # DESIGNED recovery loop (persisted-cursor resume + archive replay-merge +
  # catch-up mode) — the crash-only path production actually takes, never
  # end-to-end asserted before this case. Either path must END the same way:
  # verifying LIVE again, caught up, zero divergences.
  #   - SURVIVOR (container StartedAt unchanged; sub-eviction freeze): the
  #     original term-buffer contract — verified advances past the
  #     pre-freeze count and bal_missing growth stays within tolerance.
  #   - NEWBORN (StartedAt changed): counters RESET; catch-up commits the
  #     freeze-window backlog unverified BY DESIGN (#78), so bal_missing is
  #     not comparable across the restart — assert it verifies live from
  #     the fresh counter, catches up, and reports zero divergences.
  local t=0 ok=0 path="" cur started1 vf1 v1 e_now d1
  while [ "${t}" -lt 240 ]; do
    sleep 10; t=$(( t + 10 ))
    cur="$(timeout 15 docker exec "${VALIDATOR_NODE}" sh -c 'docker ps --format "{{.Names}}" | grep -m1 "^validator"' 2>/dev/null || true)"
    started1=""
    if [ -n "${cur}" ]; then
      started1="$(timeout 15 docker exec "${VALIDATOR_NODE}" docker inspect -f '{{.State.StartedAt}}' "${cur}" 2>/dev/null || true)"
    fi
    vf1="$(val_metric validator_blocks_verified_total)"
    v1="$(val_metric validator_committed_block)"
    e_now="$(executor_progress || echo 0)"
    if [ -z "${vf1}" ] || [ -z "${v1}" ]; then
      log "validator-lapse: sample t=${t}s SCRAPE FAILED (not counted)"
      continue
    fi
    if [ -n "${started1}" ] && [ -n "${started0}" ] && [ "${started1}" != "${started0}" ]; then
      path="newborn"
      log "validator-lapse: sample t=${t}s path=newborn verified=${vf1} block=${v1} exec=${e_now}"
      if [ "${vf1}" -gt 0 ] && [ "${v1}" -gt 0 ] && [ $(( e_now - v1 )) -le 25 ]; then ok=1; break; fi
    else
      path="survivor"
      log "validator-lapse: sample t=${t}s path=survivor verified=${vf1} block=${v1} exec=${e_now}"
      if [ "${vf1}" -gt "${vf0}" ] && [ $(( e_now - v1 )) -le 25 ]; then ok=1; break; fi
    fi
  done
  if [ "${ok}" -ne 1 ]; then
    val_debug
    fail "validator-lapse: validator not verifying live + caught up within ${t}s of thaw (path=${path:-unknown}, verified=${vf1:-?}, block=${v1:-?}, exec=${e_now:-?})"
  fi
  d1="$(val_metric validator_divergence_total)"; d1="${d1:-0}"
  [ "${d1}" -eq 0 ] || fail "validator-lapse: ${d1} divergence(s) after recovery"
  if [ "${path}" = "survivor" ]; then
    local m1
    m1="$(val_metric validator_bal_missing_total)"; m1="${m1:-0}"
    [ $(( m1 - m0 )) -le 5 ] \
      || fail "validator-lapse: coverage REGRESSED on the survivor path — bal_missing grew ${m0}->${m1} (lapse window not covered by the live term buffer)"
    log "validator-lapse PASS (survivor): kept verifying ${vf0}->${vf1}, bal_missing ${m0}->${m1}, 0 divergences"
  else
    log "validator-lapse PASS (newborn): crash-only recovery verified — fresh process verifying live (verified=${vf1}, lag $(( e_now - v1 ))), 0 divergences (bal_missing not comparable across restart; catch-up commits the freeze backlog unverified by design, #78)"
  fi
}

# validator-join (#143): a FRESH validator joining a chain already in
# progress. Stop the running validator, wipe its state + checkpoint staging,
# and let Nomad restart it with nothing: the newborn must ADOPT an executor
# peer checkpoint (the cold-start half of the replay-unavailable fallback),
# bootstrap the hashed mirror + trie from that trie-off image, catch up to
# the live head, and RESUME VERIFIED execution with zero divergences —
# proving both sync and state correctness (the divergence latch re-executes
# and cross-checks every post-join block against the executors' BAL +
# receipts, and the MPT root advancing proves the bootstrapped trie is
# coherent). Executors checkpoint every 20s from bring-up, so a peer
# checkpoint always exists; the adoption log grep keeps the case
# non-vacuous — a genesis-replay join would NOT print it.
# --- cpu-squeeze: whole-stack CPU-starvation drill ------------------------
# Recreates the degraded-CI-runner storm ON PURPOSE: every kardamom node
# container is cgroup-throttled AT ONCE (docker update --cpus), so executors,
# sealers, ingress, sequencers and the validator all starve together — Aeron
# sessions lapse, back-pressure engages everywhere, and the validator falls
# into catch-up exactly like the 4-core GH runners at loadavg 17-32. That
# window produced the 2026-08-03 load-shard divergence (halt -> restart ->
# CLEAN re-validation of the same blocks: non-deterministic on replay, the
# replay-overlap class). Ambient starvation found it by luck; this drill
# hunts it deliberately. Invariant under squeeze: NO divergence, ever —
# starvation may slow the validator, never fork its verdict.
SQUEEZE_S="${SQUEEZE_S:-120}"
SQUEEZE_CPUS_PER_NODE="${SQUEEZE_CPUS_PER_NODE:-0.75}"
SQUEEZE_RECOVER_S="${SQUEEZE_RECOVER_S:-180}"
# Oscillation: N squeeze->release cycles instead of one long squeeze. The
# replay-overlap class needs the TRANSITION (sessions lapse under squeeze,
# then reconnect + replay-merge on release) — repeated cycles exercise that
# machinery far harder than one sustained squeeze of the same total length.
SQUEEZE_CYCLES="${SQUEEZE_CYCLES:-1}"
SQUEEZE_RELEASE_S="${SQUEEZE_RELEASE_S:-30}"

run_cpu_squeeze() {
  # Warm-up gate (same as validator-lapse): the squeeze must hit a validator
  # VERIFYING LIVE — squeezing one still in catch-up asserts nothing.
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
  [ "${warmed}" -eq 1 ] \
    || fail "cpu-squeeze: validator never verifying live within ${t}s (verified=${verified} block=${v_now} exec=${e_now})"
  log "cpu-squeeze: warmed up (verified=${verified} block=${v_now} exec=${e_now}) after ${t}s"

  # Node containers on the HOST engine (the DinD outer layer): the cgroup
  # limit cascades to every inner task. Control/registry stay untouched —
  # the drill starves the STACK, not the harness's own probes.
  local nodes
  nodes="$(docker ps --format '{{.Names}}' | grep -E '^kardamom-(executor|sequencer|ingress|sealer|aux)-[0-9]+$' || true)"
  [ -n "${nodes}" ] || fail "cpu-squeeze: no kardamom node containers found on the host engine"
  local n n_count cyc
  n_count="$(wc -l <<<"${nodes}")"
  log "cpu-squeeze: ${SQUEEZE_CYCLES} cycle(s) of ${SQUEEZE_S}s at ${SQUEEZE_CPUS_PER_NODE} CPUs across ${n_count} node containers (release ${SQUEEZE_RELEASE_S}s between)"
  for cyc in $(seq 1 "${SQUEEZE_CYCLES}"); do
    for n in ${nodes}; do
      docker update --cpus "${SQUEEZE_CPUS_PER_NODE}" "${n}" >/dev/null \
        || fail "cpu-squeeze: docker update --cpus failed for ${n}"
    done
    # Verify the squeeze TOOK (a silently-ignored limit would assert nothing
    # — the validator-lapse docker-pause lesson): NanoCpus must be non-zero.
    local nano
    nano="$(docker inspect -f '{{.HostConfig.NanoCpus}}' "$(head -1 <<<"${nodes}")")"
    [ "${nano:-0}" -gt 0 ] || fail "cpu-squeeze: throttle did not take (NanoCpus=${nano})"
    log "cpu-squeeze: cycle ${cyc}/${SQUEEZE_CYCLES} squeezing ${SQUEEZE_S}s"
    sleep "${SQUEEZE_S}"

    # Restore — two passes, best-effort second: leaving a node throttled
    # would poison every later case/assert on this cluster.
    for n in ${nodes}; do
      docker update --cpus 0 "${n}" >/dev/null 2>&1 \
        || { sleep 2; docker update --cpus 0 "${n}" >/dev/null 2>&1; } \
        || log "cpu-squeeze: WARNING restore failed for ${n} (still throttled)"
    done
    log "cpu-squeeze: cycle ${cyc}/${SQUEEZE_CYCLES} released"
    [ "${cyc}" -lt "${SQUEEZE_CYCLES}" ] && sleep "${SQUEEZE_RELEASE_S}"
  done
  log "cpu-squeeze: restored full CPU; asserting recovery + invariants"

  # Recovery: pipeline advances, validator returns to verifying live.
  assert_progress
  t=0; vprev=-1; local recovered=0
  while [ "${t}" -lt "${SQUEEZE_RECOVER_S}" ]; do
    verified="$(val_metric validator_blocks_verified_total)"; verified="${verified:-0}"
    v_now="$(val_metric validator_committed_block)"; v_now="${v_now:-0}"
    e_now="$(executor_progress || echo 0)"
    if [ "${verified}" -gt 0 ] && [ "${verified}" -gt "${vprev}" ] \
       && [ "${v_now}" -gt 0 ] && [ $(( e_now - v_now )) -le 15 ]; then
      recovered=1
      break
    fi
    vprev="${verified}"; sleep 6; t=$(( t + 6 ))
  done
  [ "${recovered}" -eq 1 ] \
    || fail "cpu-squeeze: validator not verifying live within ${SQUEEZE_RECOVER_S}s of restore (verified=${verified} block=${v_now} exec=${e_now})"

  # THE invariant: zero divergences — metric AND logs (the metric resets if
  # the validator restarted mid-squeeze; a pre-restart divergence still shows
  # in the old alloc's log, exactly the 2026-08-03 signature).
  local div
  div="$(val_metric validator_divergence_total)"; div="$(printf '%.0f' "${div:-0}")"
  [ "${div}" -eq 0 ] || fail "cpu-squeeze: validator counted ${div} divergence(s) under starvation"
  local valloc vlogs
  while read -r valloc; do
    [ -z "${valloc}" ] && continue
    vlogs="$(on_control 'nomad alloc logs "$1" 2>/dev/null' "${valloc}" 2>/dev/null || true)"
    if has_line "${vlogs}" "halted on divergence"; then
      echo "----- divergence context (alloc ${valloc}) -----" >&2
      printf '%s\n' "${vlogs}" \
        | grep -B3 -A10 "halted on divergence" >&2 || true
      docker exec "${VALIDATOR_NODE}" sh -c \
        'for f in /opt/kardamom/state/divergence-*.json; do [ -f "$f" ] && { echo "== $f"; head -c 4096 "$f"; echo; }; done' \
        >&2 2>/dev/null || true
      fail "cpu-squeeze: validator diverged under starvation (alloc ${valloc}; context above)"
    fi
  done < <(all_allocs validator)
  log "cpu-squeeze PASS: ${n_count} nodes starved ${SQUEEZE_S}s at ${SQUEEZE_CPUS_PER_NODE} CPUs, validator recovered (verified=${verified}, lag $(( e_now - v_now ))), 0 divergences"
}

run_validator_join() {
  local inner
  inner="$(docker exec "${VALIDATOR_NODE}" sh -c 'docker ps --format "{{.Names}}" | grep -m1 "^validator"' 2>/dev/null)"
  [ -n "${inner}" ] || fail "validator-join: no inner validator container on ${VALIDATOR_NODE}"

  # Warm up: the chain must be far enough along that adoption skips real work,
  # and the pre-join validator must be verifying live so the case-end state
  # ("verifying again") is meaningful.
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
  [ "${warmed}" -eq 1 ] \
    || fail "validator-join: cluster never warmed up within ${t}s (verified=${verified} block=${v_now} exec=${e_now})"
  log "validator-join: warmed up (verified=${verified} block=${v_now} exec=${e_now}); wiping the validator for a fresh join"

  # Container NAMES survive a task restart (task-<alloc-id>), so newborn
  # identity is the docker StartedAt timestamp — the validator-lapse case's
  # lesson, relearned on this case's first CI run (sync succeeded, the
  # name-based newborn detection never fired).
  local started0
  started0="$(timeout 15 docker exec "${VALIDATOR_NODE}" docker inspect -f '{{.State.StartedAt}}' "${inner}" 2>/dev/null || true)"

  # Kill, then wipe inside the restart delay (the job's restart stanza waits
  # 15s before the replacement container starts — the wipe wins the race, and
  # a wipe-first order would race the LIVE mdbx instead).
  docker exec "${VALIDATOR_NODE}" docker kill "${inner}" >/dev/null \
    || fail "validator-join: kill failed"
  timeout 15 docker exec "${VALIDATOR_NODE}" sh -c \
      'rm -rf /opt/kardamom/state/validator /opt/kardamom/checkpoints/*' \
    || fail "validator-join: state wipe failed"
  log "validator-join: validator killed, state + checkpoint staging wiped"

  # The newborn must appear, ADOPT a peer checkpoint, catch up, and verify.
  local deadline=$(( $(date +%s) + 240 )) newborn="" joined=0 vf1=0 v1=0
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    if [ -z "${newborn}" ]; then
      local cur started1
      cur="$(timeout 15 docker exec "${VALIDATOR_NODE}" sh -c 'docker ps --format "{{.Names}}" | grep -m1 "^validator"' 2>/dev/null || true)"
      if [ -n "${cur}" ]; then
        started1="$(timeout 15 docker exec "${VALIDATOR_NODE}" docker inspect -f '{{.State.StartedAt}}' "${cur}" 2>/dev/null || true)"
        if [ -n "${started1}" ] && [ "${started1}" != "${started0}" ]; then
          newborn="${cur}"
          log "validator-join: newborn container ${newborn} up (started ${started1})"
        fi
      fi
    fi
    vf1="$(val_metric validator_blocks_verified_total)"; vf1="${vf1:-0}"
    v1="$(val_metric validator_committed_block)"; v1="${v1:-0}"
    e_now="$(executor_progress || echo 0)"
    if [ -n "${newborn}" ] && [ "${vf1}" -gt 0 ] && [ "${v1}" -gt 0 ] \
       && [ $(( e_now - v1 )) -le 25 ]; then
      joined=1
      break
    fi
    sleep 10
  done
  if [ "${joined}" -ne 1 ]; then
    val_debug
    fail "validator-join: fresh validator not verifying + caught up within 240s (newborn=${newborn:-none}, verified=${vf1}, block=${v1}, exec=${e_now})"
  fi

  # Non-vacuity: the join must have gone through ADOPTION (the #143 path),
  # incl. the trie bootstrap of the trie-off executor image — a genesis
  # replay would satisfy the sync asserts without exercising either.
  local nlogs
  nlogs="$(timeout 20 docker exec "${VALIDATOR_NODE}" docker logs "${newborn}" 2>&1 | tail -400 || true)"
  has_line "${nlogs}" "adopted state from checkpoint" \
    || { val_debug; fail "validator-join: newborn did not adopt a peer checkpoint (genesis replay? peers unreachable?)"; }
  has_line "${nlogs}" "trie bootstrap complete" \
    || { val_debug; fail "validator-join: adopted state but no trie bootstrap ran (trie-off image not detected?)"; }

  # State correctness: zero divergences across the whole join (the latch
  # covers every post-join block the validator actually verified), and the
  # MPT root observation must be advancing (the bootstrapped trie is live).
  local div root_blk
  div="$(val_metric validator_divergence_total)"; div="${div:-0}"
  [ "${div}" -eq 0 ] || { val_debug; fail "validator-join: ${div} divergence(s) after join"; }
  root_blk="$(val_metric validator_state_root_block)"; root_blk="${root_blk:-0}"
  [ "${root_blk}" -gt 0 ] \
    || { val_debug; fail "validator-join: no MPT state-root observation after join (trie dead?)"; }
  log "validator-join PASS: fresh validator adopted a peer checkpoint, bootstrapped the trie, caught up (lag $(( e_now - v1 ))), verifying live (verified=${vf1}), root observed at block ${root_blk}, 0 divergences"
}

# sequencer-lapse case: PAUSE one racing replica of shard 0 (seq-a on
# kardamom-sequencer-0) for a window under pinned shard-0 load, then resume.
# The twin (seq-b, other node) keeps ordering — the pipeline must never
# stall. On resume the paused replica must DETECT the lapse (boundary-silence
# / watermark-jump on the cluster egress it now consumes) and enter
# receipt-floor resync (docs/agents/sequencer-lag-resync-spec.md) instead of
# blindly re-offering its stale backlog: proven-executed refs are dropped on
# receipt evidence, everything else publishes (the cluster dedup absorbs
# within-window re-offers exactly as before). Asserts: pipeline progress
# held, kardamom_sequencer_resync_entered_total INCREMENTED across the pause
# (the startup enter predates the case), the replica is still exporting
# after resume, and the standard load + convergence verdicts apply.
SEQ_LAPSE_S="${SEQ_LAPSE_S:-30}"
seqa_metric() { # <metric-name> -> integer sum across label lines (empty on scrape failure)
  # seq-a on node-0: sequencer ip lane starts at .21, seq-a metrics :9001
  # (mirrors assert_replica_healthy's bridge-first + exec-fallback probe).
  { curl -fsS --max-time 5 "http://192.168.56.21:9001/metrics" 2>/dev/null \
    || timeout 8 docker exec kardamom-sequencer-0 curl -fsS --max-time 5 \
      "http://127.0.0.1:9001/metrics" 2>/dev/null; } \
  | awk -v m="$1" '$0 ~ "^"m"[{ ]" && $0 !~ /^#/ { s += $NF } END { printf "%d", s }' \
  || true
}
seqb_twin_metric() { # <metric-name> — shard 0's replica B: seq-b on node-1 (.22:9011)
  { curl -fsS --max-time 5 "http://192.168.56.22:9011/metrics" 2>/dev/null \
    || timeout 8 docker exec kardamom-sequencer-1 curl -fsS --max-time 5 \
      "http://127.0.0.1:9011/metrics" 2>/dev/null; } \
  | awk -v m="$1" '$0 ~ "^"m"[{ ]" && $0 !~ /^#/ { s += $NF } END { printf "%d", s }' \
  || true
}
# Forensics for sequencer-lapse failures: container identity (was the task
# REPLACED under us? same container still running?), the full resync metric
# block, and the current sequencer-a container's recent log lines. The first
# CI iterations of this case failed with signatures explainable only by
# process identity confusion — make the next one self-diagnosing.
seqa_debug() {
  log "sequencer-lapse DEBUG: inner containers on kardamom-sequencer-0:"
  docker exec kardamom-sequencer-0 sh -c 'docker ps -a --format "{{.Names}} {{.Status}}" | head -6' 2>/dev/null || true
  log "sequencer-lapse DEBUG: resync metrics at .21:9001:"
  { curl -fsS --max-time 5 "http://192.168.56.21:9001/metrics" 2>/dev/null \
    || timeout 8 docker exec kardamom-sequencer-0 curl -fsS --max-time 5 "http://127.0.0.1:9001/metrics" 2>/dev/null; } \
    | grep -E "resync|watermark|floor" | head -12 || true
  log "sequencer-lapse DEBUG: current sequencer-a log tail:"
  docker exec kardamom-sequencer-0 sh -c \
    'docker logs --tail 20 "$(docker ps --format "{{.Names}}" | grep -m1 "^sequencer-a")" 2>&1 | grep -E "RESYNC|LAG|resync|panic" | tail -10' 2>/dev/null || true
}

run_sequencer_lapse() {
  local inner
  inner="$(docker exec kardamom-sequencer-0 sh -c 'docker ps --format "{{.Names}}" | grep -m1 "^sequencer-a"' 2>/dev/null)"
  [ -n "${inner}" ] || fail "sequencer-lapse: no inner sequencer-a container on kardamom-sequencer-0"

  # DETECTION is asserted on the TWIN (shard 0's replica B, node-1 seq-b):
  # freezing replica A wedges its egress session, which stalls the sealer's
  # single service thread on the offer deadline — a genuine cluster-wide
  # boundary-arrival gap every RUNNING replica's feed must flag. The frozen
  # replica itself takes the crash-only path instead: a freeze past the
  # media driver's client-liveness timeout gets the aeron client EVICTED, so
  # on thaw the process fail-stops and Nomad restarts it — a fresh process
  # with zeroed counters (observed: run 30180670099 — post-thaw log shows
  # `RESYNC enter reason=Startup` 10s after SIGCONT; asserting lag counters
  # on it reads a newborn, not a survivor). With #99 fixed the restart
  # rejoins cleanly — assert_replica_healthy covers that half.
  local l0 r0
  l0="$(seqa_metric kardamom_sequencer_resync_lag_suspected_total)"; l0="${l0:-0}"
  r0="$(seqa_metric kardamom_sequencer_resync_entered_total)"; r0="${r0:-0}"
  # SIGSTOP/SIGCONT, NOT `docker pause`: the nested cgroup freezer inside a
  # privileged DinD node can silently no-op (observed: a "paused" replica
  # kept serving metrics and never lapsed — the detection asserts failed
  # because there was no lapse to detect; CI run 30178211248). Signals hit
  # the task's PID 1 (the sequencer binary) regardless of freezer
  # delegation. The mid-freeze probe below makes a silent no-op IMPOSSIBLE:
  # a frozen process cannot answer HTTP, so if metrics still respond the
  # case fails loudly instead of asserting against a replica that never
  # lapsed. (validator-lapse shares the docker-pause pattern and never
  # verifies its freeze — flagged for follow-up, see PR notes.)
  # Container identity BEFORE the freeze: the newborn-vs-survivor decision
  # below cannot ride counter values alone — the typical pre-freeze baseline
  # is entered=1 (one startup enter, exited), and a restarted process ALSO
  # reads entered=1: equal values satisfy neither "incremented" nor "below
  # baseline", which timed this case out on main (run 30227283947:
  # "lag 0 -> 0, entered 1 -> 1, mode 0"). A Nomad in-place restart creates
  # a NEW container generation, so StartedAt is the unambiguous signal.
  local started0
  started0="$(docker exec kardamom-sequencer-0 docker inspect -f '{{.State.StartedAt}}' "${inner}" 2>/dev/null)"
  log "sequencer-lapse: freezing ${inner} (SIGSTOP) for ${SEQ_LAPSE_S}s (lag_suspected=${l0} resync_entered=${r0} started=${started0:-?})"
  docker exec kardamom-sequencer-0 docker kill -s STOP "${inner}" >/dev/null \
    || fail "sequencer-lapse: SIGSTOP failed"
  sleep 3
  if curl -fsS --max-time 3 "http://192.168.56.21:9001/metrics" >/dev/null 2>&1; then
    docker exec kardamom-sequencer-0 docker kill -s CONT "${inner}" >/dev/null 2>&1 || true
    fail "sequencer-lapse: freeze did NOT take effect (metrics endpoint still answering mid-freeze)"
  fi
  log "sequencer-lapse: freeze verified (metrics endpoint dark)"
  sleep $(( SEQ_LAPSE_S - 3 ))
  docker exec kardamom-sequencer-0 docker kill -s CONT "${inner}" >/dev/null \
    || fail "sequencer-lapse: SIGCONT failed"
  log "sequencer-lapse: resumed; twin must have covered (no stall)"

  # The pipeline never depended on the paused replica — the twin raced on.
  assert_progress

  # DETECTION + RESPONSE (on the LAPSED replica): before the consumer-
  # filtered egress fan-out, this case asserted the TWIN's lag counter —
  # the frozen session's backpressure blocked the leader's per-session
  # offer loop, stalling boundaries CLUSTER-WIDE, and the healthy twin
  # flagged that collateral stall. Publisher-only sessions are no longer in
  # the fan-out, so a frozen replica cannot starve anyone's egress and the
  # twin correctly sees nothing. What must still hold is the lapse contract
  # on the replica that actually lapsed, on EITHER recovery path:
  #   - survivor: the process outlived the freeze; its boundary-gap
  #     detector flags (lag_suspected increments past the pre-freeze
  #     baseline) and/or resync engages (entered increments / mode >= 1);
  #   - newborn: the freeze got the aeron client evicted, the process
  #     fail-stopped and Nomad restarted it — counters RESET, so a value
  #     BELOW the pre-freeze baseline that is nonetheless >= 1 proves the
  #     fresh process entered resync (RESYNC enter reason=Startup).
  #
  # SCRAPE FAILURE IS NOT ZERO: only successful scrapes count toward the
  # verdict (the post-thaw exec fallback can wedge for minutes — the
  # issue-#76 pattern), every sample is logged, and the window is generous.
  local t=0 l1 r1 mode raw good=0
  while :; do
    l1="$(seqa_metric kardamom_sequencer_resync_lag_suspected_total)"
    r1="$(seqa_metric kardamom_sequencer_resync_entered_total)"
    mode="$(seqa_metric kardamom_sequencer_resync_mode)"
    if [ -n "${l1}" ] || [ -n "${r1}" ]; then
      good=$(( good + 1 ))
      log "sequencer-lapse: lapsed-replica sample t=${t}s lag=${l1:-?} entered=${r1:-?} mode=${mode:-?} (scrape ok #${good})"
      # Survivor paths: counters moved past their pre-freeze baselines, or
      # resync mode is currently active.
      if [ -n "${l1}" ] && [ "${l1}" -gt "${l0}" ]; then break; fi
      if [ -n "${r1}" ] && [ "${r1}" -gt "${r0}" ]; then break; fi
      if [ -n "${mode}" ] && [ "${mode}" -ge 1 ]; then break; fi
      # Newborn path, IDENTITY-based: a counter below its baseline implies a
      # restart, but the converse fails when the baseline equals the fresh
      # process's value (entered 1 -> restart -> entered 1 satisfies nothing
      # value-shaped — the exact miss that timed this case out on main). The
      # container generation is unambiguous: StartedAt changed ⇒ Nomad
      # restarted the task across the freeze (crash-only path: the aeron
      # client was evicted at ~10s of freeze, the process fail-stopped on
      # thaw) ⇒ entered >= 1 on the FRESH process is its startup resync
      # engaging — the lapse contract holds.
      local cur started1
      cur="$(docker exec kardamom-sequencer-0 sh -c 'docker ps --format "{{.Names}}" | grep -m1 "^sequencer-a"' 2>/dev/null)"
      started1=""
      [ -n "${cur}" ] && started1="$(docker exec kardamom-sequencer-0 docker inspect -f '{{.State.StartedAt}}' "${cur}" 2>/dev/null)"
      if [ -n "${started0}" ] && [ -n "${started1}" ] && [ "${started1}" != "${started0}" ] \
         && [ -n "${r1}" ] && [ "${r1}" -ge 1 ]; then
        log "sequencer-lapse: replica RESTARTED across the freeze (container ${started0} -> ${started1}); fresh process entered startup resync (entered=${r1})"
        break
      fi
      # Value-shaped newborn fallback (kept for the started0-unknown case —
      # docker exec can wedge post-thaw, #76 pattern).
      if [ -n "${r1}" ] && [ "${r1}" -lt "${r0}" ] && [ "${r1}" -ge 1 ]; then
        log "sequencer-lapse: replica restarted across the freeze (entered ${r0} -> ${r1}); startup resync engaged"
        break
      fi
    else
      log "sequencer-lapse: sample t=${t}s SCRAPE FAILED (not counted as zero)"
    fi
    if [ "${t}" -ge 240 ]; then
      seqa_debug
      if [ "${good}" -eq 0 ]; then
        fail "sequencer-lapse: lapsed-replica metrics unreachable for 240s after resume (0 successful scrapes) — cannot judge detection"
      fi
      fail "sequencer-lapse: lapsed replica never engaged resync within 240s of resume (lag ${l0} -> ${l1:-?}, entered ${r0} -> ${r1:-?}, mode ${mode:-?}, ${good} good scrapes)"
    fi
    sleep 10; t=$(( t + 10 ))
  done
  log "sequencer-lapse: lapsed replica engaged resync (lag ${l0} -> ${l1:-?}, entered ${r0} -> ${r1:-?}, mode ${mode:-?})"

  assert_replica_healthy kardamom-sequencer-0 192.168.56.21 9001
  log "sequencer-lapse PASS: progress held, lag detected, resync engaged, replica healthy"
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

# retention-overrun / retention-overrun-validator: the LIVE replay-window
# overrun tier (recovery-D), which NO other case reaches. state-checkpoint-
# restore and replay-window-resync both start from a WIPED node; here a
# RUNNING consumer is frozen (SIGSTOP) until the cluster's bounded egress
# retention rolls past its cursor, so on thaw its REPLAY_FROM is refused
# (REPLAY_UNAVAILABLE) and the node must repair itself end-to-end: fetch a
# peer checkpoint at/above the floor, park the stale state DB, exit, restart,
# restore (executor) / adopt (validator, #143), rejoin. The freeze also
# crosses the 90s cluster session timeout, so the resume goes through a fresh
# session — the long-halt path.
#
# ONLY meaningful on a cluster DEPLOYED with a small egress retention:
# KARDAMOM_CLUSTER_RETENTION must hold the same value deploy.sh injected as
# -Dkardamom.cluster.retention (the chaos-retention CI shard sets one env var
# and both read it). At the default 65536 frames the freeze would need ~11
# minutes of sustained 200tps to overrun — the reason this tier had never
# executed before this case existed.
KARDAMOM_CLUSTER_RETENTION="${KARDAMOM_CLUSTER_RETENTION:-}"
# executor-2 is the victim: 0/1 stay untouched as checkpoint donors.
RETENTION_VICTIM_EXEC_IDX=2

# Hard cap on the adaptive freeze (below). Overrun is declared from OBSERVED
# traffic, so the cap only trips when the load is too slow to ever roll the
# window — a loud, named failure instead of a vacuous pass.
RETENTION_FREEZE_CAP_S="${RETENTION_FREEZE_CAP_S:-600}"

run_retention_overrun() { # <executor|validator>
  local kind="$1" node port inner cid0
  [ -n "${KARDAMOM_CLUSTER_RETENTION}" ] \
    || fail "retention-overrun(${kind}): KARDAMOM_CLUSTER_RETENTION is not set — this case only means something on a cluster deployed with a small -Dkardamom.cluster.retention (deploy.sh injects it from the same env var)"

  if [ "${kind}" = "executor" ]; then
    node="${EXECUTOR_NODES[${RETENTION_VICTIM_EXEC_IDX}]}"; port="${EXECUTOR_PORT}"
  else
    node="${VALIDATOR_NODE}"; port="${VALIDATOR_PORT}"
  fi
  inner="$(timeout 15 docker exec "${node}" sh -c \
    'docker ps --format "{{.Names}}" | grep -im1 '"${kind}" 2>/dev/null)"
  [ -n "${inner}" ] || fail "retention-overrun(${kind}): no inner ${kind} container on ${node}"
  cid0="$(timeout 15 docker exec "${node}" sh -c \
    'docker ps --filter name='"${inner}"' -q | head -1' 2>/dev/null || true)"

  # The repair needs a checkpoint DONOR: recovery-D fetches from a peer at or
  # above the post-freeze floor, and donors checkpoint every 20s while live —
  # but only once the chain is moving. Waiting here (not failing later with a
  # misleading "resync did not complete") names the real precondition.
  log "retention-overrun(${kind}): waiting for a checkpoint on donor executor-0"
  for _ in $(seq 1 15); do
    docker exec kardamom-executor-0 bash -lc \
      'ls /opt/kardamom/checkpoints/checkpoint-* >/dev/null 2>&1' && break
    sleep 5
  done
  docker exec kardamom-executor-0 bash -lc \
    'ls /opt/kardamom/checkpoints/checkpoint-* >/dev/null 2>&1' \
    || fail "retention-overrun(${kind}): donor executor-0 produced no checkpoint"

  # Freeze must hit a LIVE consumer (non-vacuity): its own gauge advancing.
  local p0 p1 t=0 live=0
  while [ "${t}" -lt 120 ]; do
    if [ "${kind}" = "executor" ]; then
      p1="$(exec_metrics "${RETENTION_VICTIM_EXEC_IDX}" \
        | awk -v m="${EXECUTOR_BLOCK_METRIC}" '$0 ~ "^"m"([{ ]|$)" && $0 !~ /^#/ { printf "%d", $NF; exit }')"
    else
      p1="$(val_metric validator_committed_block)"
    fi
    p1="${p1:-0}"
    if [ -n "${p0:-}" ] && [ "${p1}" -gt "${p0}" ]; then live=1; break; fi
    p0="${p1}"; sleep 6; t=$(( t + 6 ))
  done
  [ "${live}" -eq 1 ] \
    || fail "retention-overrun(${kind}): victim not demonstrably live before the freeze (gauge ${p0:-?} -> ${p1:-?}); freezing a dead consumer asserts nothing"

  # SIGSTOP + VERIFIED freeze (#108 lesson: `docker pause` no-ops silently in
  # the nested-DinD freezer; a probe that still answers means no freeze).
  # The freeze is ADAPTIVE, sized by OBSERVED traffic, not the target rate:
  # the first run of this case froze a fixed 2*retention/CHAOS_TPS seconds
  # while the runner delivered ~36tps of the 200tps target — the retained
  # window never even filled, the floor never left genesis, and the thawed
  # replay was served in full. Overrun needs frames-SINCE-FREEZE > retention;
  # accepted txs (ingress counter) are the observable lower-bound proxy for
  # egress frames (boundary ticks only add to it).
  local rx_freeze rx_now delta=0 elapsed=0 need=$(( 2 * KARDAMOM_CLUSTER_RETENTION ))
  rx_freeze="$(ingress_received || echo 0)"
  log "retention-overrun(${kind}): freezing ${inner} on ${node} until ${need} frames flow past it (retention=${KARDAMOM_CLUSTER_RETENTION}, cap ${RETENTION_FREEZE_CAP_S}s)"
  docker exec "${node}" docker kill -s STOP "${inner}" >/dev/null \
    || fail "retention-overrun(${kind}): SIGSTOP failed"
  sleep 3
  if timeout 8 docker exec "${node}" curl -fsS --max-time 3 \
      "http://127.0.0.1:${port}/metrics" >/dev/null 2>&1; then
    docker exec "${node}" docker kill -s CONT "${inner}" >/dev/null 2>&1 || true
    fail "retention-overrun(${kind}): freeze did NOT take effect (metrics endpoint still answering mid-freeze)"
  fi
  log "retention-overrun(${kind}): freeze verified (metrics endpoint dark)"
  elapsed=3
  while :; do
    sleep 15; elapsed=$(( elapsed + 15 ))
    rx_now="$(ingress_received || echo "${rx_freeze}")"
    delta=$(( rx_now - rx_freeze ))
    # Both legs matter: the window must ROLL PAST the frozen cursor (delta)
    # AND the 90s cluster session must lapse (elapsed), so the resume goes
    # through a fresh session whose REPLAY_FROM is genuinely below the floor.
    [ "${delta}" -ge "${need}" ] && [ "${elapsed}" -ge 120 ] && break
    if [ "${elapsed}" -ge "${RETENTION_FREEZE_CAP_S}" ]; then
      timeout 20 docker exec "${node}" docker kill -s CONT "${inner}" >/dev/null 2>&1 || true
      fail "retention-overrun(${kind}): load too slow to overrun the retention window — only ${delta} of ${need} frames flowed in ${elapsed}s (≈$(( delta / elapsed ))tps); raise the load rate or lower KARDAMOM_CLUSTER_RETENTION"
    fi
  done
  log "retention-overrun(${kind}): window overrun (${delta} frames in ${elapsed}s ≈ $(( delta / elapsed ))tps); thawing"
  timeout 20 docker exec "${node}" docker kill -s CONT "${inner}" >/dev/null 2>&1 \
    || log "retention-overrun(${kind}): SIGCONT failed (container may have been replaced mid-freeze); the log asserts below own the verdict"

  # The recovery-D evidence is split across container GENERATIONS: the thawed
  # process logs the refusal + fetch + park, then EXITS; its restarted
  # successor logs the restore/adopt. Nomad GCs the dead generation's
  # container immediately, so the only stream holding BOTH halves is the
  # alloc's own Nomad log (job name == consumer kind for both victims).
  local needle_restored
  if [ "${kind}" = "executor" ]; then
    needle_restored='restored state from checkpoint'
  else
    needle_restored='adopted state from checkpoint'
  fi
  local logs unavailable=0 fetched=0 prepared=0 restored=0
  t=0
  while :; do
    logs="$(job_alloc_logs "${kind}")"
    has_line "${logs}" 'cluster replay unavailable' && unavailable=1
    has_line "${logs}" 'fetched checkpoint from peer' && fetched=1
    has_line "${logs}" 'resync prepared: peer checkpoint staged' && prepared=1
    has_line "${logs}" "${needle_restored}" && restored=1
    [ "${unavailable}" = 1 ] && [ "${prepared}" = 1 ] && [ "${restored}" = 1 ] && break
    sleep 6; t=$(( t + 6 ))
    if [ "${t}" -ge 300 ]; then
      log "retention-overrun(${kind}) DEBUG: recovery-relevant alloc-log lines:"
      job_alloc_logs "${kind}" | grep -aE "replay unavailable|resync|fetched checkpoint|already present locally|restored state|adopted state|parked" | tail -30 || true
      [ "${unavailable}" = 1 ] \
        || fail "retention-overrun(${kind}): consumer never hit REPLAY_UNAVAILABLE after a ${elapsed}s freeze with ${delta} frames flowed — the retention tier was NOT exercised (is the deployed -Dkardamom.cluster.retention actually ${KARDAMOM_CLUSTER_RETENTION}?)"
      # 'resync prepared' is the repair-ran proof: it prints after BOTH fetch
      # outcomes (a transfer, or the peer's block already present locally —
      # the short-circuit that once made a fetch-line assert read a healthy
      # recovery as "the repair path did not run").
      [ "${prepared}" = 1 ] \
        || fail "retention-overrun(${kind}): REPLAY_UNAVAILABLE hit but the peer-checkpoint resync never completed (no 'resync prepared'; fetched_line=${fetched}) — donors dark, or --checkpoint-peers misconfigured"
      fail "retention-overrun(${kind}): resync prepared but the restarted ${kind} never logged '${needle_restored}'"
    fi
  done
  log "retention-overrun(${kind}): REPLAY_UNAVAILABLE -> fetch -> park -> restart -> restore observed (${t}s after thaw)"

  # The victim must be a RESTARTED process, not the thawed original limping on
  # (docker restarts always mint a new container id).
  local cid_now
  cid_now="$(timeout 15 docker exec "${node}" sh -c \
    'docker ps --filter name='"${inner}"' -q | head -1' 2>/dev/null || true)"
  [ -n "${cid_now}" ] && [ "${cid_now}" != "${cid0}" ] \
    || fail "retention-overrun(${kind}): victim container was not restarted (cid ${cid0:-?} -> ${cid_now:-gone}) — the park/exit/restore loop did not complete"

  if [ "${kind}" = "executor" ]; then
    # Rejoined replica must catch the fleet (assert_executors_converged runs
    # at case end for every case); the pipeline itself must be moving.
    assert_executor_progress 180
  else
    # The adopted validator must RESUME VERIFYING, not just commit: adoption
    # marks everything through the checkpoint unverified, so verified-total
    # advancing is the proof the tail re-execution actually restarted.
    local v0 v1
    v0="$(val_metric validator_blocks_verified_total)"; v0="${v0:-0}"
    t=0
    while :; do
      sleep 10; t=$(( t + 10 ))
      v1="$(val_metric validator_blocks_verified_total)"; v1="${v1:-0}"
      [ "${v1}" -gt "${v0}" ] && break
      [ "${t}" -ge 240 ] \
        && fail "retention-overrun(validator): adopted validator never resumed verifying (blocks_verified ${v0} -> ${v1} over ${t}s)"
    done
    log "retention-overrun(validator): verifying resumed after adoption (blocks_verified ${v0} -> ${v1}, ${t}s)"
  fi
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
# Concatenated stdout+stderr of every alloc of a Nomad job. THE evidence
# source for lines that straddle a task restart: Nomad's docker driver GCs
# the dead container on an in-place restart, so `docker logs` on the node
# silently loses everything the dying generation said (first retention-
# overrun run: the sealer provably refused the replay and the executor
# provably self-repaired, but the refusal/fetch/park lines lived in the
# reaped container and the case read "never happened"). Nomad's own alloc
# log files persist across restarts of the same alloc.
job_alloc_logs() { # <nomad-job>
  local allocs alloc
  allocs="$(on_control 'nomad job allocs -t "{{range .}}{{.ID}}{{\"\n\"}}{{end}}" "$1"' "$1" 2>/dev/null || true)"
  while read -r alloc; do
    [ -n "${alloc}" ] || continue
    on_control 'nomad alloc logs "$1" 2>/dev/null; nomad alloc logs -stderr "$1" 2>/dev/null' \
      "${alloc}" 2>/dev/null || true
  done <<<"${allocs}"
}

# Concatenated stdout of every cluster alloc (running or not) — the uniform
# evidence source for snapshot/restore lines. Alloc logs survive in-place task
# restarts, so pre-kill and post-rejoin lines land in the same stream; callers
# assert on counts increasing, not mere presence.
cluster_alloc_logs() {
  local allocs alloc
  allocs="$(on_control 'nomad job allocs -t "{{range .}}{{.ID}}{{\"\n\"}}{{end}}" "$1"' "${CLUSTER_TASK}" 2>/dev/null || true)"
  while read -r alloc; do
    [ -n "${alloc}" ] || continue
    on_control 'nomad alloc logs "$1" 2>/dev/null' "${alloc}" 2>/dev/null || true
  done <<<"${allocs}"
}

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
      local peer_ck="" scr_r0 scr_now
      log "state-checkpoint-restore: waiting for a checkpoint on peer executor-1"
      for _ in $(seq 1 15); do
        peer_ck="$(docker exec kardamom-executor-1 bash -lc \
          'ls /opt/kardamom/checkpoints/checkpoint-* 2>/dev/null | head -1' 2>/dev/null || true)"
        [ -n "${peer_ck}" ] && break
        sleep 5
      done
      [ -n "${peer_ck}" ] || fail "state-checkpoint-restore: peer executor-1 produced no checkpoint"
      # Count-baseline over the alloc log: earlier cases' restarts also log
      # restores, and the evidence must survive container GC + multiple
      # restart generations (docker logs on the current container missed a
      # generation in round 5's crash-loop).
      scr_r0="$(job_alloc_logs executor | grep -c 'restored state from checkpoint' || true)"
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
        # Self-heal short-circuit: since recovery-D the restarted executor
        # fetches a peer checkpoint on cold start and immediately writes AND
        # PRUNES its own checkpoints — racing this loop for the same
        # directory (round 7: three consecutive copies were pruned away
        # before the completeness probe ran). A self-healed victim satisfies
        # this case's product assertion — a checkpoint restore, not a
        # genesis re-sync — with the very same evidence line.
        scr_now="$(job_alloc_logs executor | grep -c 'restored state from checkpoint' || true)"
        if [ "${scr_now}" -gt "${scr_r0}" ]; then
          log "state-checkpoint-restore: executor-0 self-healed from a peer before the harness copy landed"
          copied=1
          break
        fi
        ck_name="$(docker exec kardamom-executor-1 bash -lc \
          'ls -d /opt/kardamom/checkpoints/checkpoint-* 2>/dev/null | sort | tail -1' \
          | xargs -rn1 basename)"
        [ -n "${ck_name}" ] || { sleep 2; continue; }
        docker exec kardamom-executor-0 bash -lc 'rm -rf /opt/kardamom/checkpoints/*'
        ck_rc=0
        docker exec kardamom-executor-1 tar -C /opt/kardamom --warning=no-file-changed -cf - "checkpoints/${ck_name}" \
          | docker exec -i kardamom-executor-0 tar -C /opt/kardamom -xf - || ck_rc=$?
        # tar rc=1 = live-writer drift; restore-side validation + replay
        # fallback (recovery C/D) is the integrity gate. But a tar that raced
        # the source's PRUNE can deliver a TORN copy (image without MANIFEST)
        # with rc<=1 — the executor now refuses + quarantines such a copy and
        # self-heals from a peer, but verify completeness here so this case
        # exercises the LOCAL-restore path it exists for, not the network
        # fallback.
        if [ "${ck_rc}" -le 1 ] && docker exec kardamom-executor-0 bash -lc \
            "test -s '/opt/kardamom/checkpoints/${ck_name}/MANIFEST' && test -s '/opt/kardamom/checkpoints/${ck_name}/mdbx.dat'"; then
          copied=1
          break
        fi
        log "state-checkpoint-restore: copy of ${ck_name} incomplete or failed (raced the writer's prune?); retrying"
        sleep 2
      done
      [ "${copied}" = 1 ] || fail "state-checkpoint-restore: checkpoint copy failed"
      # The surviving replicas must keep the pipeline progressing on 2.
      assert_executor_progress 180
      # executor-0 restarts, restores from the peer checkpoint, rejoins to 3.
      assert_count executor 3 "${CHAOS_RESCHEDULE_SLO_S}"
      local scr_t=0
      while :; do
        scr_now="$(job_alloc_logs executor | grep -c 'restored state from checkpoint' || true)"
        [ "${scr_now}" -gt "${scr_r0}" ] && break
        sleep 6; scr_t=$(( scr_t + 6 ))
        [ "${scr_t}" -ge 120 ] \
          && fail "state-checkpoint-restore: executor-0 did NOT restore from checkpoint (count ${scr_r0} -> ${scr_now}) — fell back to genesis re-sync"
      done
      log "state-checkpoint-restore: executor-0 restored from checkpoint + rejoined (restore count ${scr_r0} -> ${scr_now}, no genesis re-sync)"
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
      # Fetch + restore take real time (a checkpoint image is hundreds of MB
      # and the node tries every peer that advertises something newer), so
      # poll for the two log lines instead of grepping once — the first CI run
      # failed with the restore landing 1.3s after a one-shot grep.
      local t=0 fetched=0 restored=0
      while :; do
        ex1_inner="$(docker exec kardamom-executor-1 bash -lc \
          'docker ps --format "{{.Names}}" | grep -i executor | head -1' 2>/dev/null || true)"
        if [ -n "${ex1_inner}" ]; then
          docker exec kardamom-executor-1 bash -lc \
            "docker logs ${ex1_inner} 2>&1 | grep -q 'fetched checkpoint from peer'" && fetched=1
          docker exec kardamom-executor-1 bash -lc \
            "docker logs ${ex1_inner} 2>&1 | grep -q 'restored state from checkpoint'" && restored=1
          [ "${fetched}" = 1 ] && [ "${restored}" = 1 ] && break
        fi
        sleep 5; t=$((t+5))
        if [ "${t}" -ge 90 ]; then
          [ "${fetched}" = 1 ] \
            || fail "replay-window-resync: executor-1 did NOT fetch a peer checkpoint (self-heal path not taken)"
          fail "replay-window-resync: executor-1 fetched but did NOT restore the peer checkpoint within 90s"
        fi
      done
      log "replay-window-resync: executor-1 self-healed from a peer checkpoint (fetch + restore + rejoin, ${t}s)"
      ;;

    retention-overrun)
      run_retention_overrun executor
      ;;

    retention-overrun-validator)
      run_retention_overrun validator
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
      #
      # NEVER transplant archive-mark.dat from a LIVE source: the peer's daemon
      # heartbeats it, so the copy looks "active" to the victim's restarting
      # Archive, which then crash-loops on 'active Mark file detected' until the
      # copied heartbeat ages out — observed blowing the 60s restart SLO (the
      # recurring 'aeron did not reach >= 8 running (have 7)' flake, on main and
      # PRs). The victim's own mark file was deliberately preserved by the wipe
      # above and its heartbeat died with the killed driver, so it is already
      # stale by restart time and the daemon starts cleanly.
      # And copy the CATALOG first, via a STABLE read (issue #98): the mirror's
      # daemon rewrites catalog entries on recording lifecycle events, and this
      # very injection triggers some — ingress-0's publishers died with its
      # driver, so the mirror STOPS those recordings and rewrites exactly those
      # entries seconds later. A copy racing such a write captures a torn entry
      # (stored checksum != descriptor) that fails a CRC-armed verify. Two
      # consecutive identical snapshots guarantee a consistent image; copying
      # the catalog before the segments guarantees segment data covers every
      # position it references. (kardamom-archive-rereplicate does the same.)
      log "archive-tx-data-wipe: re-replicating archive from kardamom-ingress-1 mirror"
      local cat_h1 cat_h2 stable=0
      for _ in $(seq 1 10); do
        cat_h1="$(docker exec kardamom-ingress-1 sha256sum /opt/kardamom/archive/dir/archive.catalog | cut -d' ' -f1)"
        cat_h2="$(docker exec kardamom-ingress-1 sha256sum /opt/kardamom/archive/dir/archive.catalog | cut -d' ' -f1)"
        if [ -n "${cat_h1}" ] && [ "${cat_h1}" = "${cat_h2}" ]; then
          docker exec kardamom-ingress-1 cat /opt/kardamom/archive/dir/archive.catalog \
            | docker exec -i kardamom-ingress-0 bash -lc 'cat > /opt/kardamom/archive/dir/archive.catalog' \
            || fail "archive-tx-data-wipe: catalog copy failed"
          cat_h2="$(docker exec kardamom-ingress-1 sha256sum /opt/kardamom/archive/dir/archive.catalog | cut -d' ' -f1)"
          [ "${cat_h1}" = "${cat_h2}" ] && { stable=1; break; }
        fi
        sleep 1
      done
      [ "${stable}" = 1 ] || fail "archive-tx-data-wipe: mirror catalog never stabilized across 10 attempts"
      local seg_rc=0
      docker exec kardamom-ingress-1 tar -C /opt/kardamom/archive --warning=no-file-changed \
        --exclude='dir/archive-mark.dat' --exclude='dir/archive.catalog' -cf - dir \
        | docker exec -i kardamom-ingress-0 tar -C /opt/kardamom/archive -xf - \
        || seg_rc=$?
      # tar rc=1 = segment appended under the live recorder mid-copy; the
      # torn tail is what the restart-side verify/heal (#94/#95) handles.
      [ "${seg_rc}" -le 1 ] \
        || fail "archive-tx-data-wipe: re-replication copy failed (rc=${seg_rc})"
      # The pipeline must have ridden through on ingress-1 the whole time.
      assert_progress
      # aeron restarts (system job) and adopts the restored archive.
      assert_count aeron "${aeron_base}" "${CHAOS_RESTART_SLO_S}"
      # Verify the restored archive with Aeron's own tool: every recording OK.
      ac0="$(docker exec kardamom-ingress-0 bash -lc \
        'docker ps --format "{{.Names}}" | grep archiving-media-driver | head -1')"
      [ -n "${ac0}" ] || fail "archive-tx-data-wipe: no aeron container on ingress-0 after restart"
      # CRC-ARMED verify is the regression gate for issue #98: every data
      # frame's recorded CRC32 plus file availability/structure. One class of
      # ERR is TOLERATED (counted + logged): 'invalid Catalog checksum'.
      # Aeron 1.45 patches catalog entries when active recordings are adopted
      # /stopped out-of-band (ArchiveTool.verify writes recovered stop
      # positions without recomputing the entry checksum; the daemon's
      # adoption path behaves the same in CI evidence), so entry checksums on
      # a restored-and-adopted archive go stale by construction — an upstream
      # gap, not a torn transplant. Frame CRCs are unaffected and remain the
      # authoritative integrity signal. A crashed tool never passes.
      local v_ok=0 stale_entries=0
      for _ in 1 2 3; do
        verify_out="$(docker exec kardamom-ingress-0 bash -lc \
          "docker exec ${ac0} bash -lc 'java --add-opens java.base/java.util.zip=ALL-UNNAMED -cp /opt/aeron/aeron-all.jar io.aeron.archive.ArchiveTool /opt/kardamom/archive/dir verify -a -checksum io.aeron.archive.checksum.Crc32 2>&1'" || true)"
        stale_entries="$(echo "${verify_out}" | grep -c 'invalid Catalog checksum' || true)"
        # Non-checksum errors counted with `grep -c` (reads ALL input — no
        # early exit, so no SIGPIPE) instead of a filtered `grep -q` chain.
        local other_err
        other_err="$(echo "${verify_out}" | grep -v 'invalid Catalog checksum' | grep -ciE "ERR |FAILED" || true)"
        if ! has_line "${verify_out}" 'Exception' \
          && has_match "${verify_out}" "recordingId=.*OK" \
          && [ "${other_err:-0}" -eq 0 ]; then
          v_ok=1
          break
        fi
        sleep 5
      done
      if [ "${v_ok}" != 1 ]; then
        echo "${verify_out}" | tail -20
        fail "archive-tx-data-wipe: restored archive failed CRC-armed verify after retries"
      fi
      if [ "${stale_entries:-0}" -gt 0 ]; then
        log "archive-tx-data-wipe: note — ${stale_entries} adoption-staled catalog entry checksum(s) tolerated (Aeron 1.45 gap)"
      fi
      log "archive-tx-data-wipe: restored archive verified OK on ingress-0 (2-copy redundancy recovered)"
      assert_count ingress 2 "${CHAOS_RESCHEDULE_SLO_S}"
      ;;

    archive-corruption)
      # DATA-corruption drill (present-but-wrong, not missing): flip bytes
      # mid-segment in ingress-0's tx_data archive — length preserved, so a
      # size check can't see it — then DETECT it with a CRC-armed
      # `ArchiveTool verify` (record-time CRC32 is enabled in the driver) and
      # HEAL only the corrupt segment from ingress-1's mirror via
      # `kardamom-archive-rereplicate --diff/--heal`. File-level surgery
      # requires the victim's archive daemon STOPPED throughout, so the node is
      # drained for the window (pipeline rides on ingress-1, as the other
      # archive cases prove). Expected, in order:
      #   1. pre-heal verify FAILS on the corrupted archive (detection);
      #   2. the Rust tool's --diff names the corrupted segment and --heal
      #      repairs exactly it from the mirror;
      #   3. post-heal CRC verify is clean, the node undrains, and aeron +
      #      ingress return to strength with the pipeline having progressed.
      local aeron_base node_id seg seg_name aeron_img tmp_dir verify_pre verify_post diverged
      [ -x "${REREP_BIN}" ] || fail "archive-corruption: kardamom-archive-rereplicate not at ${REREP_BIN}"
      aeron_base="$(count_running aeron)"
      [ -n "${aeron_base}" ] && [ "${aeron_base}" -gt 0 ] \
        || fail "archive-corruption: aeron baseline unavailable — refusing a vacuous pass"
      # Hold ingress-0 down for the whole surgery window: drain evicts the
      # aeron system task (which holds the catalog open) and keeps it down
      # until we undrain — a hard kill would race nomad's restart.
      node_id="$(on_control 'nomad node status -verbose 2>/dev/null | awk "/ingress-0/ {print \$1; exit}"')"
      [ -n "${node_id}" ] || fail "archive-corruption: could not resolve ingress-0 node id"
      aeron_img="$(docker exec kardamom-ingress-0 bash -lc \
        "docker ps -a --format '{{.Image}} {{.Names}}' | awk '/archiving/ {print \$1; exit}'")"
      [ -n "${aeron_img}" ] || fail "archive-corruption: could not resolve the aeron image on ingress-0"
      log "archive-corruption: draining ingress-0 node (${node_id})"
      on_control 'nomad node drain -enable -yes -deadline 2m "$1"' "${node_id}" >/dev/null \
        || fail "archive-corruption: drain enable failed"
      sleep 5
      # Pick a victim recording that verifies CLEAN at baseline. The archive's
      # catalog was restored from the live peer by archive-tx-data-wipe, and
      # which entries got torn in that copy is a per-run lottery (issue #98) —
      # this case tests SEGMENT corruption detect/heal, not catalog repair, so
      # it must start from a provably-clean recording: baseline OK -> corrupt
      # -> ERR -> heal -> OK is then a closed loop. Every verify/mark-valid is
      # scoped to that recording (segment name = <recordingId>-<base>.rec).
      local seg="" seg_name="" rid="" flip_at=-1 cand cand_name cand_rid cand_out cand_flip
      for cand in $(docker exec kardamom-ingress-0 bash -lc \
          'ls -S /opt/kardamom/archive/dir/*.rec 2>/dev/null | head -6'); do
        cand_name="$(basename "${cand}")"
        cand_rid="${cand_name%%-*}"
        # #126: recording ids are per-archive counters, so the victim's
        # post-restart/post-restore sessions (archive-driver-loss and
        # archive-tx-data-wipe both run earlier in this shard) own ids the
        # mirror never opened. A victim-only segment is unhealable from the
        # mirror BY CONSTRUCTION — no source bytes exist — so it cannot
        # drill the detect→heal loop; only candidates present on BOTH
        # archives qualify. (--diff now surfaces such segments as
        # "dest-only" instead of silently skipping them.)
        if ! docker exec kardamom-ingress-1 test -f "/opt/kardamom/archive/dir/${cand_name}" 2>/dev/null; then
          continue
        fi
        cand_out="$(docker exec kardamom-ingress-0 bash -lc \
          "docker run --rm -v /opt/kardamom/archive:/opt/kardamom/archive --entrypoint java ${aeron_img} \
           --add-opens java.base/java.util.zip=ALL-UNNAMED \
           -cp /opt/aeron/aeron-all.jar io.aeron.archive.ArchiveTool /opt/kardamom/archive/dir \
           verify ${cand_rid} -a -checksum io.aeron.archive.checksum.Crc32 2>&1" || true)"
        if has_line "${cand_out}" "recordingId=${cand_rid}) OK" \
          && ! has_line "${cand_out}" ') ERR'; then
          # Segment files are PRE-ALLOCATED (equal apparent size, ls -S order
          # arbitrary), and a flip landing in a frame HEADER can send verify's
          # frame-walk out of bounds (observed: JVM SIGSEGV in the CRC32
          # intrinsic) instead of a clean ERR. Walk the Aeron data-frame
          # headers and pick a flip offset INSIDE the payload of the largest
          # real data frame (type 0x01, skipping padding frames); -1 means the
          # segment has no usable frame — try the next candidate.
          cand_flip="$(docker exec kardamom-ingress-0 python3 -c "
b = open('${cand}', 'rb').read()
pos = 0; best = -1; bestlen = 0
while pos + 32 <= len(b):
    ln = int.from_bytes(b[pos:pos+4], 'little', signed=True)
    if ln <= 0:
        break  # first zero-length header = start of the unrecorded tail
    typ = int.from_bytes(b[pos+6:pos+8], 'little')
    if typ == 1 and ln >= 96 and pos + ln <= len(b) and ln > bestlen:
        best = pos; bestlen = ln
    pos += (ln + 31) // 32 * 32
print(best + 40 if best >= 0 else -1)
" 2>/dev/null || echo -1)"
          if [ "${cand_flip:--1}" -ge 0 ]; then
            seg="${cand}"; seg_name="${cand_name}"; rid="${cand_rid}"; flip_at="${cand_flip}"
            break
          fi
          continue
        fi
        # The probe marks a failing entry INVALID — put its state back as found.
        docker exec kardamom-ingress-0 bash -lc \
          "docker run --rm -v /opt/kardamom/archive:/opt/kardamom/archive --entrypoint java ${aeron_img} \
           --add-opens java.base/java.util.zip=ALL-UNNAMED \
           -cp /opt/aeron/aeron-all.jar io.aeron.archive.ArchiveTool /opt/kardamom/archive/dir \
           mark-valid ${cand_rid}" >/dev/null 2>&1 || true
      done
      [ -n "${seg}" ] \
        || fail "archive-corruption: no recording verifies clean at baseline (inherited catalog damage too broad — see issue #98)"
      # Corrupt 16 bytes inside a data frame's PAYLOAD — length unchanged,
      # frame structure intact, so verify reports a checksum ERR instead of
      # chasing a bogus frame length.
      log "archive-corruption: flipping payload bytes at ${flip_at} in ${seg_name} (recording ${rid})"
      docker exec kardamom-ingress-0 bash -lc \
        "printf 'KARDAMOM-CHAOS!!' | dd of=${seg} bs=1 seek=${flip_at} count=16 conv=notrunc status=none" \
        || fail "archive-corruption: byte flip failed"
      # DETECTION: CRC-armed verify on the frozen victim must NOT be clean.
      # (Run via a one-off container on the node — the daemon is down.)
      verify_pre="$(docker exec kardamom-ingress-0 bash -lc \
        "docker run --rm -v /opt/kardamom/archive:/opt/kardamom/archive --entrypoint java ${aeron_img} \
         --add-opens java.base/java.util.zip=ALL-UNNAMED \
         -cp /opt/aeron/aeron-all.jar io.aeron.archive.ArchiveTool /opt/kardamom/archive/dir \
         verify ${rid} -a -checksum io.aeron.archive.checksum.Crc32 2>&1" || true)"
      if has_line "${verify_pre}" 'Exception'; then
        echo "${verify_pre}" | tail -20
        fail "archive-corruption: verify tool crashed (not a detection)"
      fi
      has_match "${verify_pre}" "recordingId=${rid}[,)].* ERR" \
        || { echo "${verify_pre}" | tail -20; \
             fail "archive-corruption: CRC-armed verify did NOT flag recording ${rid} (detection hole)"; }
      log "archive-corruption: corruption detected by CRC-armed verify"
      # HEAL through the Rust tool on the runner: stage both copies, --diff
      # must name the corrupted segment, --heal repairs exactly it.
      tmp_dir="$(mktemp -d)"
      mkdir -p "${tmp_dir}/victim" "${tmp_dir}/mirror"
      docker exec kardamom-ingress-0 tar -C /opt/kardamom/archive -cf - dir \
        | tar -C "${tmp_dir}/victim" -xf - || fail "archive-corruption: staging victim copy failed"
      docker exec kardamom-ingress-1 tar -C /opt/kardamom/archive -cf - dir \
        | tar -C "${tmp_dir}/mirror" -xf - || fail "archive-corruption: staging mirror copy failed"
      diverged="$("${REREP_BIN}" --diff --source-dir "${tmp_dir}/mirror/dir" --dest-dir "${tmp_dir}/victim/dir" || true)"
      has_line "${diverged}" "${seg_name}" \
        || fail "archive-corruption: --diff did not name the corrupted segment ${seg_name}"
      local heal_out
      heal_out="$("${REREP_BIN}" --heal --segments "${seg_name}" --no-verify \
        --source-dir "${tmp_dir}/mirror/dir" --dest-dir "${tmp_dir}/victim/dir" 2>&1 || true)"
      has_line "${heal_out}" 'healed segments=1' \
        || { echo "${heal_out}" | tail -20; \
             fail "archive-corruption: --heal did not repair the segment"; }
      # Put ONLY the healed segment back, then re-validate + clear any INVALID
      # marks the detection verify persisted.
      tar -C "${tmp_dir}/victim" -cf - "dir/${seg_name}" \
        | docker exec -i kardamom-ingress-0 tar -C /opt/kardamom/archive -xf - \
        || fail "archive-corruption: writing healed segment back failed"
      # The detection verify marked the failing recording INVALID in the
      # catalog; clear the marks now that the bytes are healed. Recording ids
      # are harvested on the RUNNER from the pre-heal verify output (one
      # mark-valid container per id — a shell loop inside the node would run
      # `java` on the node itself, where it doesn't exist).
      docker exec kardamom-ingress-0 bash -lc \
        "docker run --rm -v /opt/kardamom/archive:/opt/kardamom/archive --entrypoint java ${aeron_img} \
         --add-opens java.base/java.util.zip=ALL-UNNAMED \
         -cp /opt/aeron/aeron-all.jar io.aeron.archive.ArchiveTool /opt/kardamom/archive/dir \
         mark-valid ${rid}" >/dev/null 2>&1 || true
      verify_post="$(docker exec kardamom-ingress-0 bash -lc \
        "docker run --rm -v /opt/kardamom/archive:/opt/kardamom/archive --entrypoint java ${aeron_img} \
         --add-opens java.base/java.util.zip=ALL-UNNAMED \
         -cp /opt/aeron/aeron-all.jar io.aeron.archive.ArchiveTool /opt/kardamom/archive/dir \
         verify ${rid} -a -checksum io.aeron.archive.checksum.Crc32 2>&1" || true)"
      if has_line "${verify_post}" 'Exception'; then
        echo "${verify_post}" | tail -20
        fail "archive-corruption: post-heal verify tool crashed"
      fi
      if ! has_line "${verify_post}" "recordingId=${rid}) OK"; then
        echo "${verify_post}" | tail -20
        fail "archive-corruption: post-heal verify does not show recording ${rid} OK"
      fi
      if has_match "${verify_post}" "recordingId=${rid}[,)].* ERR"; then
        echo "${verify_post}" | tail -20
        fail "archive-corruption: post-heal verify still reports errors on recording ${rid}"
      fi
      rm -rf "${tmp_dir}"
      log "archive-corruption: healed + CRC verify clean; undraining ingress-0"
      on_control 'nomad node drain -disable -yes "$1"' "${node_id}" >/dev/null \
        || fail "archive-corruption: drain disable failed"
      assert_count aeron "${aeron_base}" "${CHAOS_RESTART_SLO_S}"
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
      local leader follower fk_r0 fk_r1 fk_t
      leader="$(cluster_leader)"
      # Pick any memberId in 0..2 that isn't the leader.
      for follower in 0 1 2; do [ "${follower}" != "${leader}" ] && break; done
      # A snapshot must exist BEFORE the kill: an intact-dir restart is where
      # the sealer's snapshot RESTORE path actually runs (Aeron 1.44 static
      # membership replays a BLANK member from log position 0 instead — see
      # cluster-member-rejoin), and before the in-process scheduler existed
      # this path had never executed outside unit tests.
      log "cluster-follower-kill: leader=memberId=${leader}; waiting for a cluster snapshot"
      fk_t=0
      local fk_logs
      while :; do
        fk_logs="$(cluster_alloc_logs)"
        has_line "${fk_logs}" 'cluster SNAPSHOT triggered' && break
        sleep 10; fk_t=$(( fk_t + 10 ))
        [ "${fk_t}" -ge 300 ] \
          && fail "cluster-follower-kill: no snapshot within ${fk_t}s — is the snapshot scheduler running?"
      done
      fk_r0="$(cluster_alloc_logs | grep -c "sealer snapshot RESTORED memberId=${follower}" || true)"
      log "cluster-follower-kill: snapshot present (member ${follower} restore count ${fk_r0}); killing FOLLOWER memberId=${follower} on kardamom-sealer-${follower}"
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
      # The restarted member's dirs are INTACT, so it must recover by loading
      # its local latest snapshot — bounded restart time — not by replaying
      # the whole lifetime log. Count-increase, because earlier restarts in
      # this shard may have restored already.
      fk_t=0
      while :; do
        fk_r1="$(cluster_alloc_logs | grep -c "sealer snapshot RESTORED memberId=${follower}" || true)"
        [ "${fk_r1}" -gt "${fk_r0}" ] && break
        sleep 10; fk_t=$(( fk_t + 10 ))
        [ "${fk_t}" -ge 180 ] \
          && fail "cluster-follower-kill: restarted member never logged 'sealer snapshot RESTORED' (count ${fk_r0} -> ${fk_r1}) — the snapshot restore path did not run on an intact-dir restart"
      done
      log "cluster-follower-kill: member ${follower} restored from snapshot on restart (count ${fk_r0} -> ${fk_r1}, ${fk_t}s)"
      ;;

    cluster-member-rejoin)
      # BLANK-member catch-up drill — the "join mid-way with an empty state"
      # edge for the RAFT MEMBERS themselves. A follower's cluster dir AND
      # archive are wiped after its kill, so the restarted member owns
      # NOTHING. Under Aeron 1.44 STATIC membership a blank member is caught
      # up by replicating and replaying the leader's LOG FROM POSITION 0 —
      # snapshots are NOT transferred to blank members (they bound the
      # restart time of members whose dirs survive; that path is asserted by
      # cluster-follower-kill). Full log replay is deterministic, so the
      # correct outcome here is: a FRESH-at-genesis service start, the whole
      # log replayed (proven via a post-rejoin snapshot TAKEN), 3/3 running,
      # pipeline unaffected throughout (quorum 2/3 held). First run of this case
      # proved exactly that (the wiped member replayed to the live head and
      # resumed serving replay sessions). NOTE the cost this documents: a
      # blank member's rejoin time grows with the lifetime log — bounding it
      # needs log purge after snapshot, tracked as audit follow-up.
      local leader follower f0 f1 taken0 taken1 t
      leader="$(cluster_leader)"
      for follower in 0 1 2; do [ "${follower}" != "${leader}" ] && break; done

      # Baselines BEFORE the wipe (counts, not presence: bring-up also logs a
      # fresh start, and every earlier scheduler tick adds TAKEN lines).
      f0="$(cluster_alloc_logs | grep -c "sealer state FRESH at genesis memberId=${follower}" || true)"
      taken0="$(cluster_alloc_logs | grep -c "sealer snapshot TAKEN memberId=${follower}" || true)"

      log "cluster-member-rejoin: leader=memberId=${leader}; killing FOLLOWER memberId=${follower} and WIPING its cluster + archive dirs"
      inject_hard "kardamom-sealer-${follower}" "${CLUSTER_TASK}"
      docker exec "kardamom-sealer-${follower}" bash -lc \
        'rm -rf /opt/kardamom/cluster/* /opt/kardamom/archive/*' \
        || fail "cluster-member-rejoin: could not wipe memberId=${follower} state"

      # Quorum (2/3) holds: the pipeline keeps committing throughout.
      assert_executor_progress
      # The wiped member's task restarts and the job returns to 3/3.
      assert_count "${CLUSTER_TASK}" 3 "${CHAOS_RESTART_SLO_S}"

      # The restarted member must (a) start BLANK — fresh-at-genesis count
      # grew, proving the wipe took and this is genuinely the empty-state
      # path — and (b) finish the full log replay. Catch-up proof: the member
      # logs a NEW 'sealer snapshot TAKEN' — snapshots run on every member at
      # the same replicated log position, so taking one at a post-rejoin
      # position requires having replayed the log all the way there. (A
      # FOLLOWER role line cannot serve: a member that STARTS as follower
      # never gets an onRoleChange — round 2 measured role count 0 -> 0 on a
      # healthy rejoin.) Budget: full replay + one scheduler interval.
      t=0
      while :; do
        f1="$(cluster_alloc_logs | grep -c "sealer state FRESH at genesis memberId=${follower}" || true)"
        taken1="$(cluster_alloc_logs | grep -c "sealer snapshot TAKEN memberId=${follower}" || true)"
        [ "${f1}" -gt "${f0}" ] && [ "${taken1}" -gt "${taken0}" ] && break
        sleep 10; t=$(( t + 10 ))
        if [ "${t}" -ge 360 ]; then
          [ "${f1}" -gt "${f0}" ] \
            || fail "cluster-member-rejoin: restarted member did not start blank (fresh-at-genesis count ${f0} -> ${f1}) — the wipe did not take, this run proved nothing about empty-state rejoin"
          fail "cluster-member-rejoin: blank member never took a post-rejoin snapshot within ${t}s (TAKEN count ${taken0} -> ${taken1}) — log-replay catch-up wedged or the scheduler is not running"
        fi
      done
      log "cluster-member-rejoin: memberId=${follower} rejoined blank via full log replay (fresh ${f0}->${f1}, snapshot TAKEN ${taken0}->${taken1}, ${t}s); leader now memberId=$(cluster_leader 2>/dev/null || echo '?')"
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

    cpu-squeeze)
      # Whole-stack CPU starvation (no kills): throttle every node container
      # at once and assert the invariant that starvation may slow the
      # pipeline but never fork the validator's verdict. All squeeze
      # mechanics + asserts live in the helper.
      run_cpu_squeeze
      ;;

    validator-join)
      # Fresh validator joins the running chain mid-run: wipe + restart, must
      # adopt an executor peer checkpoint (#143 cold-start half, incl. the
      # trie bootstrap), catch up, and resume VERIFIED execution with zero
      # divergences. All asserts in the helper.
      run_validator_join
      ;;

    sequencer-lapse)
      # No component killed: pause ONE racing replica of shard 0 and assert
      # the twin covers (no stall) while the resumed replica detects the
      # lapse and enters receipt-floor resync. All asserts in the helper.
      run_sequencer_lapse
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
