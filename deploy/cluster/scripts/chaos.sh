#!/usr/bin/env bash
# =============================================================================
# chaos.sh — resilience/chaos suite for the kardamom cluster.
# =============================================================================
#
# For each failure case, this suite: starts a steady background load (the
# kardamom-load harness in --chaos-mode, which soaks at a fixed rate and
# checks that every accepted tx gets a receipt eventually), injects a
# failure, checks that the cluster recovers within an SLO and the
# pipeline resumes producing blocks, then waits for the load to finish
# and checks its verdict (no missing receipts, no frozen executor).
#
# This suite runs inside the orchestrator or runner, sharing the host
# docker socket and reaching the cluster bridge, exactly like
# ci-cluster.sh. The cluster runs Docker-in-Docker: each node is a
# privileged container `kardamom-<class>-<i>` running its own dockerd,
# and the pipeline services are inner Nomad docker-driver tasks. So:
#   * graceful kill  = `nomad alloc stop` (via control-0)         → restart
#   * hard crash     = `docker exec <node> docker kill <inner>`   → restart
#   * node failure   = `docker kill <node>` (whole node)          → reschedule
#
# Recovery depends on topology. A singleton on a single role-node cannot
# reschedule to a peer. A node failure on an executor with no spare
# role-node degrades to count-1 until the node returns. The assertions
# encode the achievable outcome for each case, instead of always
# expecting a fresh alloc on a new node.
#
# Cluster mode (Phase 3): the deploy now uses the clustered sealer, a
# 3-member Aeron Cluster running Raft, as the Nomad job `cluster`. One
# member runs per sealer node (.51/.52/.53); memberId 0/1/2 maps to
# kardamom-sealer-0/1/2 through the node-IP derivation. There is no
# single kardamom-sealer, and no Prometheus endpoint on the Java cluster
# node. So cluster-mode progress is measured from the executor's
# `kardamom_executor_block_number` gauge; the executor applies blocks the
# cluster commits out of its egress. See executor_progress() in
# chaos-probes.sh. The three cluster-* cases exercise Raft leader-kill,
# follower-kill, and quorum-loss. The component-chaos cases
# (executor/ingress/sequencer/sealer kills) still exist, and can run
# against either topology. Against a legacy single-sealer deploy,
# assert_progress falls back to the sealer-boundary probe.
#
# Split layout: this file is a thin dispatcher. It holds the settings,
# the single EXIT trap, the per-case scaffolding (account and shard
# pinning, load launch and injection gate, post-case common asserts and
# verdict), and the case_<name>() dispatch. Everything else lives in
# files sourced into this shell. They are libraries, never run as child
# processes: the injectors set KILLED_* globals that assert_count reads,
# CHAOS_ACCT advances across cases, and the cleanup trap must see
# LOAD_PID. Only chaos.sh installs an EXIT trap; sourced files never do.
#   lib.sh                        control-node helpers, log/fail
#   lib-topology.sh               node-class model (nodes, IPs, ports)
#   lib-metrics.sh                fetch_metrics, prom_value (scrape/parse)
#   validator-verdict.sh          divergence-log scan, shared with ci-cluster.sh
#   chaos-probes.sh               has_line/has_match, read-only probes
#   chaos-asserts.sh              injectors, alloc-log evidence, assert_*
#   chaos-cases-component.sh      graceful/hard-*, node-failure, restore drills
#   chaos-cases-archive.sh        archive loss/wipe/corruption, archive_tool
#   chaos-cases-cluster.sh        Raft leader/follower/rejoin/quorum cases
#   chaos-cases-validator.sh      lapse/join/cpu-squeeze, warm-up/freeze helpers
#   chaos-cases-seq-retention.sh  sequencer-lapse, retention-overrun
# Never add a `producer | grep -q` assert anywhere in the suite. See the
# SIGPIPE and pipefail note at the top of chaos-probes.sh. has_line and
# has_match are the assert primitives.
#
# Environment variables (all optional):
#   RPC_URL                  ingress JSON-RPC      (default http://192.168.56.31:8545)
#   LOAD_BIN                 kardamom-load path    (default <root>/target/release/kardamom-load)
#   CHAOS_TPS                steady load rate      (default 50)
#   CHAOS_CASE_S             per-case load window  (default 45)
#   LOAD_MAX_GAP             keep-pace gap bound   (default 5)
#   CHAOS_RESTART_SLO_S      same-node restart SLO (default 30)
#   CHAOS_RESCHEDULE_SLO_S   node-loss recovery SLO(default 120)
#   CHAOS_LEADER_SLO_S       new-leader election SLO (default 30)
#   CLUSTER_REJOIN_SLO_S     blank-member full-log-replay SLO (default 360)
#   CHAOS_CASES              space-separated cases (default a representative subset)
#   INJECT_DELAY             min seconds of load before injecting (default 10)
#   LOAD_FLOW_TIMEOUT_S      max extra seconds to wait for load to flow
#                            (ingress received counter advancing) before
#                            refusing to inject (default 60)
#   CHAOS_ACCT_BASE          first funded account index per case (default 7)
#
# Cases: graceful-executor hard-executor graceful-ingress hard-ingress
#        graceful-sequencer hard-sequencer sequencer-replica-kill
#        sequencer-lapse validator-lapse validator-join
#        node-failure-executor state-checkpoint-restore replay-window-resync
#        retention-overrun retention-overrun-validator
#        archive-driver-loss archive-tx-data-wipe archive-corruption
#        cluster-leader-kill cluster-follower-kill cluster-member-rejoin
#        cluster-quorum-loss-recover
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

# Shared control-node helpers (on_control, running_alloc, count_running,
# ...), plus log and fail. FAIL_PREFIX keeps this suite's dual-stream
# "CHAOS FAIL:" lines.
FAIL_PREFIX="CHAOS FAIL"
# shellcheck source=deploy/cluster/scripts/lib.sh
source "${SCRIPT_DIR}/lib.sh"
# Node-class model (EXECUTOR_NODES/IPS, validator and ingress nodes and
# ports, ...).
# shellcheck source=deploy/cluster/scripts/lib-topology.sh
source "${SCRIPT_DIR}/lib-topology.sh"
# Scrape and parse (fetch_metrics bridge-first probe, prom_value).
# shellcheck source=deploy/cluster/scripts/lib-metrics.sh
source "${SCRIPT_DIR}/lib-metrics.sh"
# Shared validator divergence-log scan (divergence_scan and
# _dump_context). This is the one implementation that ci-cluster.sh's
# §7c verdict also uses. The cpu-squeeze case uses it with this suite's
# fail() contract.
# shellcheck source=deploy/cluster/scripts/validator-verdict.sh
source "${SCRIPT_DIR}/validator-verdict.sh"

RPC_URL="${RPC_URL:-http://192.168.56.31:8545}"
# Set the chain id explicitly. ingress eth_chainId returns a default
# that differs from the cluster chain.
CHAIN_ID="${CHAIN_ID:-412346}"
LOAD_BIN="${LOAD_BIN:-${ROOT}/target/release/kardamom-load}"
# Archive repair tool (archive-corruption case). Built with the service
# binaries.
REREP_BIN="${REREP_BIN:-${ROOT}/target/release/kardamom-archive-rereplicate}"
CHAOS_TPS="${CHAOS_TPS:-50}"
# Rotate the ingress blast radius across runs (0 or 1). This is
# deterministic per CI run, based on GITHUB_RUN_ID parity, and
# overridable locally.
INGRESS_VICTIM="${INGRESS_VICTIM:-$(( ${GITHUB_RUN_ID:-0} % 2 ))}"
CHAOS_CASE_S="${CHAOS_CASE_S:-45}"
LOAD_MAX_GAP="${LOAD_MAX_GAP:-5}"
# Service jobs use force_pull=true, so a restart re-pulls the image from
# the in-cluster registry before the task comes back. Allow time for
# that.
CHAOS_RESTART_SLO_S="${CHAOS_RESTART_SLO_S:-60}"
CHAOS_RESCHEDULE_SLO_S="${CHAOS_RESCHEDULE_SLO_S:-120}"
# Raft re-election after a leader loss is fast, a few election
# timeouts. But the leader log line has to surface in the alloc's
# stdout, and Nomad has to ship it. So give it a generous window before
# calling it a failure.
CHAOS_LEADER_SLO_S="${CHAOS_LEADER_SLO_S:-30}"
# Budget for a wiped cluster member to replay the log back to the head
# (cluster-member-rejoin). This stays at the 360s the case used before
# its catch-up proof was corrected. The assertion got stricter; the time
# budget did not get more generous. Blank-member replay measured about
# 12.5 blocks per second on the dev host, so this budget covers the log
# this suite builds. A member that needs longer runs into the
# rejoin-cost-scales-with-log-length case that the audit tracks. It
# should fail here, rather than show up as a stall in a later case.
CLUSTER_REJOIN_SLO_S="${CLUSTER_REJOIN_SLO_S:-360}"
CHAOS_CASES="${CHAOS_CASES:-graceful-executor hard-executor cluster-leader-kill node-failure-executor}"
INJECT_DELAY="${INJECT_DELAY:-10}"
LOAD_FLOW_TIMEOUT_S="${LOAD_FLOW_TIMEOUT_S:-60}"
# Each case's steady load uses one dedicated funded account, with a
# fresh nonce chain from 0. So cases never collide, and never leave
# nonce gaps. Genesis funds Anvil accounts #0 through #15.
# ci-cluster.sh reserves #0 for the gate and #1 through #6 for the load
# harness, leaving #7 through #15, up to 9 cases. CHAOS_ACCT advances
# with each case.
CHAOS_ACCT_BASE="${CHAOS_ACCT_BASE:-7}"
CHAOS_ACCT="${CHAOS_ACCT_BASE}"
# Sender-to-shard map for the 16 funded Anvil accounts (index = account
# number). shard = first 8 bytes of keccak256(address), as a big-endian
# u64, mod partition_count=2 (crates/ingress/src/routing.rs::partition_for).
# Fixed addresses and a fixed hash function make this stable forever.
# Derivation: cast keccak <address> | cut -c3-18, then mod 2. This map
# pins a case's load onto a specific shard (sequencer-replica-kill).
ACCT_SHARD=(0 1 1 0 0 0 0 0 0 0 1 0 1 1 0 1)

LOAD_PID=""
cleanup() {
  [ -n "${LOAD_PID}" ] && kill "${LOAD_PID}" 2>/dev/null || true
}
# This is the one EXIT trap for the whole suite. Sourced files must
# never install their own. A sourced trap would silently replace this
# one, and orphan the background load.
trap cleanup EXIT

[ -x "${LOAD_BIN}" ] || fail "kardamom-load not found/executable at ${LOAD_BIN}"

# Probes, injectors and asserts, and the case_<name>() bodies. All
# sourced into this shell; see the split layout note above.
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

  # Case bodies are case_<name>() functions, with dashes turned to
  # underscores, in the sourced chaos-cases-*.sh files. An unknown case
  # fails before the suite spends any load or account.
  local fn="case_${name//-/_}"
  declare -F "${fn}" >/dev/null || fail "unknown chaos case: ${name}"

  log "================= CHAOS CASE: ${name} ================="

  # One dedicated fresh funded account per case, a single sender with
  # nonces from 0, so cases never collide or leave nonce gaps on the
  # never-reset chain.
  if [ "${name}" = "sequencer-replica-kill" ] || [ "${name}" = "sequencer-lapse" ] \
    || [ "${name}" = "graceful-sequencer" ] || [ "${name}" = "hard-sequencer" ]; then
    # Pin this case's load to shard 0, the shard whose replica A gets
    # killed or paused. An arbitrary account lands on shard 0 or 1 by
    # address hash, so about half of runs would otherwise drive an
    # untouched shard, and the failover assertions would prove nothing
    # about the kill. Accounts skipped here are burned, never reused;
    # their nonce chains stay at 0.
    while [ "${CHAOS_ACCT}" -le 15 ] && [ "${ACCT_SHARD[${CHAOS_ACCT}]}" -ne 0 ]; do
      log "${name}: skipping funded account #${CHAOS_ACCT} (shard ${ACCT_SHARD[${CHAOS_ACCT}]}; case needs shard 0)"
      CHAOS_ACCT=$(( CHAOS_ACCT + 1 ))
    done
  fi
  local acct="${CHAOS_ACCT}"
  CHAOS_ACCT=$(( CHAOS_ACCT + 1 ))
  [ "${acct}" -le 15 ] || fail "ran out of funded chaos accounts (#${acct} > 15); reduce CHAOS_CASES"

  # Set the per-case load window. sequencer-replica-kill needs load
  # still flowing after the killed replica's restart SLO. Its
  # post-restart coverage assertion checks that the restarted replica
  # publishes refs for the pinned shard, which needs live traffic. So
  # its window widens to cover inject, restart, and margin, regardless
  # of the global CHAOS_CASE_S.
  local case_s="${CHAOS_CASE_S}"
  if [ "${name}" = "sequencer-replica-kill" ]; then
    local min_s=$(( INJECT_DELAY + CHAOS_RESTART_SLO_S + 60 ))
    [ "${case_s}" -lt "${min_s}" ] && case_s="${min_s}"
  fi
  # sequencer-lapse: load must still flow after the pause window, so
  # the resumed replica's resync and the twin-coverage verdicts observe
  # live traffic.
  if [ "${name}" = "sequencer-lapse" ]; then
    local min_s=$(( INJECT_DELAY + SEQ_LAPSE_S + 60 ))
    [ "${case_s}" -lt "${min_s}" ] && case_s="${min_s}"
  fi
  # retention-overrun: the freeze overruns the retention window only if
  # frames keep flowing for its whole duration. The post-thaw repair
  # (fetch, restart, restore) needs live traffic to prove the rejoin.
  # So the load window must cover freeze plus recovery, not the global
  # CHAOS_CASE_S.
  if [ "${name}" = "retention-overrun" ] || [ "${name}" = "retention-overrun-validator" ]; then
    local min_s=$(( INJECT_DELAY + RETENTION_FREEZE_CAP_S + 120 ))
    [ "${case_s}" -lt "${min_s}" ] && case_s="${min_s}"
  fi
  # cpu-squeeze: load must keep flowing through the whole starvation
  # window and the recovery assert. A squeeze against an idle pipeline
  # exercises nothing; the divergence this case hunts needs live
  # traffic and catch-up.
  if [ "${name}" = "cpu-squeeze" ]; then
    local min_s=$(( INJECT_DELAY + SQUEEZE_CYCLES * (SQUEEZE_S + SQUEEZE_RELEASE_S) + 90 ))
    [ "${case_s}" -lt "${min_s}" ] && case_s="${min_s}"
  fi

  # Record the ingress baseline before the load starts, for the
  # injection gate below.
  local rx0
  rx0="$(ingress_received || echo 0)"

  # Run a steady background load for the whole inject-and-recover
  # window. The drain deadline outlives the recovery SLO, so txs
  # accepted before or around the kill still get a receipt after
  # recovery.
  local drain=$(( CHAOS_RESCHEDULE_SLO_S + 60 ))
  "${LOAD_BIN}" --rpc "${RPC_URL}" --chain-id "${CHAIN_ID}" --chaos-mode --duration "${case_s}s" \
    --target-tps "${CHAOS_TPS}" --senders 1 --sender-offset "${acct}" \
    --nonce-start 0 --assert-all-delivered --completeness accepted \
    --max-gap "${LOAD_MAX_GAP}" --scrape executor,ingress,sequencer \
    --drain-timeout "${drain}s" --output "${out}" >"${logf}" 2>&1 &
  LOAD_PID=$!

  # Injection gate: INJECT_DELAY is a minimum, not proof that load is
  # flowing. On a thrashed runner, the harness can still be
  # pre-generating or connecting when a fixed sleep expires, and a kill
  # into an idle pipeline checks nothing. Require the ingress received
  # counter to move past its pre-load baseline, bounded by
  # LOAD_FLOW_TIMEOUT_S, before injecting.
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

  # Inject the failure, then run case-specific asserts.
  "${fn}"

  # The pipeline must be producing blocks again after recovery. In
  # cluster mode, the sealer has no Prometheus endpoint, so this uses
  # the executor progress probe. Otherwise, assert_progress prefers the
  # legacy sealer-boundary probe. node-failure gets a wide window.
  # Right after `docker start` of the killed node, the runner is
  # saturated: Nomad is rescheduling, and the returning node is
  # force-pulling every image through the in-cluster registry. On
  # 4-core CI runners, even the metric scrapes time out through that
  # load ("block 0 -> 0" with the max-across-replicas probe means
  # nobody answered), while the same case passes cleanly on a 12-core
  # host. This widening uses the same reasoning as the quorum-loss
  # case's 180s.
  case "${name}" in
    cluster-*)              assert_executor_progress ;;
    node-failure-*)         assert_executor_progress 180 ;;
    *)                      assert_progress ;;
  esac

  # Every case must leave a fully healthy executor fleet, not just
  # "some replica progressing". See assert_executors_converged. This is
  # what makes a green run meaningful. A case whose kill target, or an
  # innocent bystander such as an executor wedged by a sequencer
  # restart, never truly recovers now fails here, instead of hiding
  # behind the fleet-max probes.
  assert_executors_converged "${name}"

  # Let the background load finish its window and drain, then check
  # its verdict.
  wait "${LOAD_PID}" || true
  LOAD_PID=""
  case "${name}" in
    # Killing any Raft cluster member (leader, follower, or a
    # quorum-loss case) under sustained high load causes a brief
    # ordering hiccup. The sequencer rejects some past-nonce txs
    # (seq_dropped). Those txs were never accepted, so they are not a
    # delivery gap; the cluster still delivered every accepted tx
    # (missing==0). Check gapless delivery of accepted txs for the
    # cluster cases, tolerating seq_dropped. The component-chaos cases
    # keep the strict verdict: their redundancy (3 executors, 2
    # sequencers, ingress restart) should never drop a tx.
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
