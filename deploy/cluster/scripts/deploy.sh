#!/usr/bin/env bash
# Deploy the kardamom Nomad job pipeline to the cluster.
#
# Order: Aeron substrate (system) + anvil L1, wait until their allocations are
# running, then the service jobs (cluster, sequencer, executor, ingress,
# da-watcher), then register the batcher periodic job.
#
# Talks to the Nomad server on r1. By default uses the HTTP API over the
# host-only network (NOMAD_ADDR). All values derive from
# ansible/group_vars/all.yml (control_ip 192.168.56.11, nomad_http 4646).
#
# Usage (robust to being run from anywhere; resolves paths relative to itself):
#   deploy/cluster/scripts/deploy.sh
#   NOMAD_ADDR=http://192.168.56.10:4646 deploy/cluster/scripts/deploy.sh
#   LOCKBOX_ADDRESS=0x... deploy/cluster/scripts/deploy.sh   # for da-watcher
set -euo pipefail

# --- locate the nomad/ job dir relative to this script ----------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NOMAD_DIR="${CLUSTER_DIR}/nomad"

# The job specs pull their template payloads with file("config/...") — those
# paths resolve against the CLI's working directory, so run from CLUSTER_DIR.
cd "${CLUSTER_DIR}"

export NOMAD_ADDR="${NOMAD_ADDR:-http://192.168.56.10:4646}"
LOCKBOX_ADDRESS="${LOCKBOX_ADDRESS:-}"
# The live batcher's DA batch inbox (KardamomL2Settlement). Deployed in Phase
# 2b via kardamom-deploy unless supplied; without either, the batcher job gets
# its PLACEHOLDER address and cannot post.
SETTLEMENT_ADDRESS="${SETTLEMENT_ADDRESS:-}"
L1_RPC="${L1_RPC:-http://192.168.56.10:8546}"
ROOT_DIR="$(cd "${CLUSTER_DIR}/../.." && pwd)"
DEPLOY_BIN="${DEPLOY_BIN:-${ROOT_DIR}/target/release/kardamom-deploy}"
# anvil dev account #0: factory owner + deploy gas payer (well-known dev key).
L1_OWNER="${L1_OWNER:-0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266}"
L1_OWNER_KEY="${L1_OWNER_KEY:-0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80}"
# anvil dev account #2: the batcher EOA (`l1Batcher`; must match the job's
# batcher_key — see nomad/batcher.nomad.hcl).
BATCHER_EOA="${BATCHER_EOA:-0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC}"
L2_CHAIN_ID="${L2_CHAIN_ID:-412346}"

echo "==> Nomad endpoint: ${NOMAD_ADDR}"
echo "==> Job specs:      ${NOMAD_DIR}"

if ! command -v nomad >/dev/null 2>&1; then
  echo "ERROR: 'nomad' CLI not found on PATH. Install Nomad or run this on r1." >&2
  exit 1
fi

# --- helpers ----------------------------------------------------------------

run_job() {
  local file="$1"
  shift
  echo "==> nomad run ${file} $*"
  # `nomad job run` exits 2 on "failed to place all allocations". That is
  # EXPECTED for a system job that constrains itself off some nodes — e.g. aeron
  # (tier != control) is correctly filtered from the control node, which Nomad
  # reports as one unplaceable alloc. The job IS registered; wait_running() below
  # verifies the real placement, so tolerate exit 2 and only fail on other errors.
  local rc=0
  nomad job run "$@" "${NOMAD_DIR}/${file}" || rc=$?
  if [[ "${rc}" -ne 0 && "${rc}" -ne 2 ]]; then
    return "${rc}"
  fi
}

# Poll until a job has at least one running allocation and none pending (or
# timeout). A `failed` alloc that has been replaced stays in the job's alloc
# history forever, so "every alloc is running" would never become true after
# a single restart — instead treat "nothing pending + something running" as
# converged and surface any failed allocs as a warning.
wait_running() {
  local job="$1"
  local timeout="${2:-120}"
  local waited=0
  echo "==> Waiting for job '${job}' allocations to be running (timeout ${timeout}s)..."
  while true; do
    local statuses running pending failed
    statuses="$(nomad job allocs -t '{{range .}}{{.ClientStatus}}{{"\n"}}{{end}}' "${job}" 2>/dev/null || true)"
    running="$(grep -cx 'running' <<<"${statuses}" || true)"
    pending="$(grep -cx 'pending' <<<"${statuses}" || true)"
    failed="$(grep -cx 'failed' <<<"${statuses}" || true)"
    if (( running >= 1 && pending == 0 )); then
      echo "    job '${job}': ${running} allocation(s) running."
      if (( failed > 0 )); then
        echo "    WARNING: job '${job}' also has ${failed} failed alloc(s) in its history." >&2
      fi
      return 0
    fi
    if (( waited >= timeout )); then
      echo "ERROR: timed out waiting for '${job}' (last ${timeout}s)." >&2
      nomad job status "${job}" >&2 || true
      return 1
    fi
    sleep 3
    waited=$((waited + 3))
  done
}

# --- 1. Aeron substrate (system job on all nodes) ---------------------------
echo
echo "### Phase 1: Aeron substrate"
run_job "aeron.system.nomad.hcl"
# Generous timeout: every worker node force-pulls the aeron image from the single
# in-cluster registry at once, slow under CI CPU contention (the dedicated cluster
# nodes added more concurrent pullers). Comes up well under this cap on a healthy
# run; the cap only guards against premature failure when bring-up is slow.
wait_running "aeron" 600

# --- (REMOVED) recorder quorum ----------------------------------------------
# The custom recorders + Q-of-N quorum aggregator were removed in favour of
# archive-at-the-sealer durability: the sealer (Phase 3, launched first)
# records its own tx_ordering MDC publication and publishes the durable
# watermark ingress --ack-policy on-quorum gates on. No separate recorder /
# quorum jobs to bring up.

# --- 2. In-cluster L1 (anvil on r1) -----------------------------------------
echo
echo "### Phase 2: anvil L1"
run_job "anvil.nomad.hcl"
wait_running "anvil" 240

# --- 2b. Settlement contract (the live batcher's DA batch inbox) ------------
# Deployed with kardamom-deploy (embedded bytecode; deterministic proxy
# address). anvil's dev accounts are pre-funded, so no funding step is needed.
if [[ -z "${SETTLEMENT_ADDRESS}" ]]; then
  if [[ -x "${DEPLOY_BIN}" ]]; then
    echo
    echo "### Phase 2b: KardamomL2Settlement deploy (${L1_RPC})"
    # Fresh anvil has no ERC-7955 CREATE2 factory; install its runtime via
    # anvil_setCode (dev-chain-only bootstrap; idempotent).
    "${DEPLOY_BIN}" --rpc-url "${L1_RPC}" --owner "${L1_OWNER}" bootstrap-7955-anvil
    "${DEPLOY_BIN}" --rpc-url "${L1_RPC}" --owner "${L1_OWNER}" \
      deploy --private-key "${L1_OWNER_KEY}" \
      KardamomL2Settlement --l2-chain-id "${L2_CHAIN_ID}" --l2-minter "${BATCHER_EOA}"
    # `addresses` prints registry ids as hashes, not names; the settlement is
    # the ONLY contract this deploy registers for the chain id, so the first
    # proxy line is it.
    SETTLEMENT_ADDRESS="$("${DEPLOY_BIN}" --rpc-url "${L1_RPC}" --owner "${L1_OWNER}" \
      addresses --l2-chain-id "${L2_CHAIN_ID}" \
      | awk '/proxy/ && !found {print $2; found=1}')"
    if [[ -z "${SETTLEMENT_ADDRESS}" ]]; then
      echo "ERROR: could not resolve the KardamomL2Settlement proxy address" >&2
      exit 1
    fi
    echo "    settlement proxy: ${SETTLEMENT_ADDRESS}"
  else
    echo "WARNING: ${DEPLOY_BIN} not found and SETTLEMENT_ADDRESS unset;" >&2
    echo "         batcher will use its PLACEHOLDER address and cannot post." >&2
  fi
fi

# --- 3. Service pipeline ----------------------------------------------------
# The 3-member Aeron Cluster (Raft) sealer is launched FIRST: it owns BOTH
# ordering and durability (durability folds into the Raft log + per-member
# archive), so the cluster must have elected a leader and be accepting ingress
# before the sequencer cluster-clients connect and the executor subscribes to
# its egress. (The standalone single-sealer job has been removed; the Aeron
# Cluster — cluster.nomad.hcl — is the sole orderer/durability.)
echo
echo "### Phase 3: service pipeline"

# da-watcher needs the chain-specific Lockbox address. If not supplied, fall
# back to the job's PLACEHOLDER default (won't work against a real chain).
DA_WATCHER_ARGS=()
if [[ -n "${LOCKBOX_ADDRESS}" ]]; then
  DA_WATCHER_ARGS=(-var "lockbox_address=${LOCKBOX_ADDRESS}")
else
  echo "WARNING: LOCKBOX_ADDRESS not set; da-watcher will use its PLACEHOLDER" >&2
  echo "         default address (deposit path will not function)." >&2
fi

# Bring the cluster up FIRST and wait until it has converged (leader elected +
# archive ready) before the sequencer cluster-clients connect and the executor
# subscribes to its egress. Election + archive recovery takes longer than the
# old single-sealer start, hence the 180s budget.
# Egress retention override (retention-overrun chaos case): the chaos-retention
# shard exports KARDAMOM_CLUSTER_RETENTION and chaos.sh reads the SAME env var
# to size its freeze, so the deployed window and the case's expectations can
# never drift apart. Unset = the jobspec default (the sealer's own default).
CLUSTER_ARGS=()
if [[ -n "${KARDAMOM_CLUSTER_RETENTION:-}" ]]; then
  echo "==> cluster: egress retention override: ${KARDAMOM_CLUSTER_RETENTION} frames"
  CLUSTER_ARGS=(-var "cluster_retention=${KARDAMOM_CLUSTER_RETENTION}")
fi
run_job "cluster.nomad.hcl" "${CLUSTER_ARGS[@]}"
wait_running "cluster" 300
run_job "sequencer.nomad.hcl"
# Bring the ingress up BEFORE the executor. The ingress is the tx_receipts
# SUBSCRIBER and the executor is the must-deliver PUBLISHER; starting the
# subscriber first means the executor's receipt publications connect immediately
# instead of stalling against a not-yet-present subscriber during bring-up (which
# would back-pressure the exec→commit channel and freeze state). The ingress also
# subscribes the quorum watermark (from the already-running aggregator) here.
run_job "ingress.nomad.hcl"
wait_running "ingress" 240
run_job "executor.nomad.hcl"
# The validator is a passive follower (subscribes to the same multicast
# channels + its own cluster egress session); it can come up alongside the
# executors — it re-executes from genesis regardless of join order.
run_job "validator.nomad.hcl"
# (The ${arr[@]+...} expansion keeps `set -u` happy on bash 3.2 — macOS'
# /bin/bash — where expanding an empty array is an "unbound variable" error.)
run_job "da-watcher.nomad.hcl" ${DA_WATCHER_ARGS[@]+"${DA_WATCHER_ARGS[@]}"}

# The cluster was already waited-on above (before the sequencer connected).
wait_running "sequencer" 240
wait_running "executor" 240
wait_running "validator" 240
wait_running "da-watcher" 240

# --- 4. Batcher (live L1 service, #39) --------------------------------------
echo
echo "### Phase 4: batcher (live L1 service)"
BATCHER_ARGS=()
if [[ -n "${SETTLEMENT_ADDRESS}" ]]; then
  BATCHER_ARGS=(-var "settlement_address=${SETTLEMENT_ADDRESS}")
fi
run_job "batcher.nomad.hcl" ${BATCHER_ARGS[@]+"${BATCHER_ARGS[@]}"}
if [[ -n "${SETTLEMENT_ADDRESS}" ]]; then
  wait_running "batcher" 240
else
  # Placeholder settlement address: the batcher cannot read/post and will
  # crash-loop until redeployed with a real address — register it but don't
  # gate the deploy on it converging.
  echo "    batcher registered (placeholder settlement address; posting disabled)."
fi

echo
echo "==> Deploy complete. Run scripts/smoke.sh to exercise the pipeline."
