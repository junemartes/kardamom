#!/usr/bin/env bash
# Deploy the kardamom Nomad job pipeline to the cluster.
#
# Order: Aeron substrate (system) + anvil L1, wait until their allocations are
# running, then the service jobs (sealer, sequencer, executor, ingress,
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
wait_running "aeron" 180

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
wait_running "anvil" 120

# --- 3. Service pipeline ----------------------------------------------------
# The sealer is launched FIRST: besides ordering, it owns the durability
# sidecar, so its durable watermark must be flowing before ingress (on-quorum)
# needs it.
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

run_job "sealer.nomad.hcl"
run_job "sequencer.nomad.hcl"
# Bring the ingress up BEFORE the executor. The ingress is the tx_receipts
# SUBSCRIBER and the executor is the must-deliver PUBLISHER; starting the
# subscriber first means the executor's receipt publications connect immediately
# instead of stalling against a not-yet-present subscriber during bring-up (which
# would back-pressure the exec→commit channel and freeze state). The ingress also
# subscribes the quorum watermark (from the already-running aggregator) here.
run_job "ingress.nomad.hcl"
wait_running "ingress" 120
run_job "executor.nomad.hcl"
# (The ${arr[@]+...} expansion keeps `set -u` happy on bash 3.2 — macOS'
# /bin/bash — where expanding an empty array is an "unbound variable" error.)
run_job "da-watcher.nomad.hcl" ${DA_WATCHER_ARGS[@]+"${DA_WATCHER_ARGS[@]}"}

wait_running "sealer" 120
wait_running "sequencer" 120
wait_running "executor" 120
wait_running "da-watcher" 120

# --- 4. Batcher periodic job (offline/dry-run) ------------------------------
echo
echo "### Phase 4: batcher (periodic, dry-run)"
run_job "batcher.nomad.hcl"
echo "    batcher registered as a periodic job (next run on its cron schedule)."

echo
echo "==> Deploy complete. Run scripts/smoke.sh to exercise the pipeline."
