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
#   NOMAD_ADDR=http://192.168.56.11:4646 deploy/cluster/scripts/deploy.sh
#   LOCKBOX_ADDRESS=0x... deploy/cluster/scripts/deploy.sh   # for da-watcher
set -euo pipefail

# --- locate the nomad/ job dir relative to this script ----------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NOMAD_DIR="${CLUSTER_DIR}/nomad"

export NOMAD_ADDR="${NOMAD_ADDR:-http://192.168.56.11:4646}"
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
  nomad job run "$@" "${NOMAD_DIR}/${file}"
}

# Poll until a job's allocations are all running (or timeout). For system/batch
# jobs that span multiple nodes this waits for every alloc.
wait_running() {
  local job="$1"
  local timeout="${2:-120}"
  local waited=0
  echo "==> Waiting for job '${job}' allocations to be running (timeout ${timeout}s)..."
  while true; do
    # Count allocs not in 'running' client status for the latest deployment.
    local statuses
    statuses="$(nomad job status -short "${job}" 2>/dev/null || true)"
    if nomad job allocs -t '{{range .}}{{.ClientStatus}}{{"\n"}}{{end}}' "${job}" 2>/dev/null \
        | grep -qx 'running'; then
      # at least one running; ensure none are pending/failed-and-retrying
      local bad
      bad="$(nomad job allocs -t '{{range .}}{{.ClientStatus}}{{"\n"}}{{end}}' "${job}" 2>/dev/null \
              | grep -vx 'running' | grep -vx 'complete' || true)"
      if [[ -z "${bad}" ]]; then
        echo "    job '${job}': all allocations running."
        return 0
      fi
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

# --- 2. In-cluster L1 (anvil on r1) -----------------------------------------
echo
echo "### Phase 2: anvil L1"
run_job "anvil.nomad.hcl"
wait_running "anvil" 120

# --- 3. Service pipeline ----------------------------------------------------
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
run_job "executor.nomad.hcl"
run_job "ingress.nomad.hcl"
run_job "da-watcher.nomad.hcl" "${DA_WATCHER_ARGS[@]}"

wait_running "sealer" 120
wait_running "sequencer" 120
wait_running "executor" 120
wait_running "ingress" 120
wait_running "da-watcher" 120

# --- 4. Batcher periodic job (offline/dry-run) ------------------------------
echo
echo "### Phase 4: batcher (periodic, dry-run)"
run_job "batcher.nomad.hcl"
echo "    batcher registered as a periodic job (next run on its cron schedule)."

echo
echo "==> Deploy complete. Run scripts/smoke.sh to exercise the pipeline."
