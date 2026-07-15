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
#   INJECT_DELAY             secs of load before injecting (default 10)
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
# Raft re-election after a leader loss is fast (a few election timeouts), but the
# leader log line has to surface in the alloc's stdout AND nomad has to ship it,
# so give it a generous window before we call it a failure.
CHAOS_LEADER_SLO_S="${CHAOS_LEADER_SLO_S:-30}"
CHAOS_CASES="${CHAOS_CASES:-graceful-executor hard-executor cluster-leader-kill node-failure-executor}"
INJECT_DELAY="${INJECT_DELAY:-10}"
# Each case's steady load uses ONE dedicated funded account (a fresh nonce chain
# from 0), so cases never collide and never leave nonce gaps. Genesis funds
# Anvil accounts #0..#15; ci-cluster.sh reserves #0 (gate) and #1..#6 (load
# harness), leaving #7..#15 = up to 9 cases. CHAOS_ACCT advances per case.
CHAOS_ACCT_BASE="${CHAOS_ACCT_BASE:-7}"
CHAOS_ACCT="${CHAOS_ACCT_BASE}"

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
  local n v best=""
  for n in "${EXECUTOR_NODES[@]}"; do
    v="$(timeout 8 docker exec "${n}" curl -fsS --max-time 5 "http://127.0.0.1:${EXECUTOR_PORT}/metrics" 2>/dev/null \
      | awk '/^kardamom_sealer_boundaries_emitted_total/{printf "%d", $NF; exit}')"
    [ -n "${v}" ] && { [ -z "${best}" ] || [ "${v}" -gt "${best}" ]; } && best="${v}"
  done
  [ -n "${best}" ] && { printf '%s' "${best}"; return 0; }
  return 1
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
  local n v best=""
  for n in "${EXECUTOR_NODES[@]}"; do
    v="$(timeout 8 docker exec "${n}" curl -fsS --max-time 5 "http://127.0.0.1:${EXECUTOR_PORT}/metrics" 2>/dev/null \
      | awk -v m="${EXECUTOR_BLOCK_METRIC}" '$0 ~ "^"m"([{ ]|$)" && $0 !~ /^#/ { printf "%d", $NF; exit }')"
    [ -n "${v}" ] && { [ -z "${best}" ] || [ "${v}" -gt "${best}" ]; } && best="${v}"
  done
  [ -n "${best}" ] && { printf '%s' "${best}"; return 0; }
  return 1
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
    --max-gap "${LOAD_MAX_GAP}" --scrape executor,ingress,sequencer \
    --drain-timeout "${drain}s" --output "${out}" >"${logf}" 2>&1 &
  LOAD_PID=$!

  sleep "${INJECT_DELAY}"

  case "${name}" in
    graceful-executor)  inject_graceful executor;                       assert_count executor 3 "${CHAOS_RESTART_SLO_S}" ;;
    hard-executor)      inject_hard kardamom-executor-0 executor;       assert_count executor 3 "${CHAOS_RESTART_SLO_S}" ;;
    graceful-ingress)   inject_graceful ingress;                        assert_count ingress 1 "${CHAOS_RESTART_SLO_S}" ;;
    hard-ingress)       inject_hard kardamom-ingress-0 ingress;         assert_count ingress 1 "${CHAOS_RESTART_SLO_S}" ;;
    # Sequencers run P=2 racing replicas per shard (job groups seq-a/seq-b,
    # 4 allocs total): a kill no longer stalls its shard — the twin on the
    # other node keeps ordering, so these also assert live pipeline progress.
    graceful-sequencer) inject_graceful sequencer;                      assert_progress; assert_count sequencer 4 "${CHAOS_RESTART_SLO_S}" ;;
    hard-sequencer)     inject_hard kardamom-sequencer-0 sequencer;     assert_progress; assert_count sequencer 4 "${CHAOS_RESTART_SLO_S}" ;;

    sequencer-replica-kill)
      # HARD-kill a SPECIFIC replica (seq-a on node-0 = shard 0's replica A;
      # its twin is seq-b on node-1). The shard must stay live with NO stall —
      # the racing twin never stopped and the cluster dedups its refs — and
      # the killed replica restarts to full strength (4/4).
      inject_hard kardamom-sequencer-0 sequencer-a
      assert_progress
      assert_count sequencer 4 "${CHAOS_RESTART_SLO_S}"
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
      log "archive-driver-loss: killing archiving-media-driver on kardamom-ingress-0 (aeron allocs baseline=${aeron_base})"
      inject_hard kardamom-ingress-0 archiving-media-driver
      assert_progress
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
