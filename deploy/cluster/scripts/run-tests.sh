#!/usr/bin/env bash
# Test gates only. The Makefile container-* targets own provisioning and teardown.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ROOT="$(cd "${CLUSTER_DIR}/../.." && pwd)"
cd "${CLUSTER_DIR}"
source "${SCRIPT_DIR}/lib.sh"
source "${SCRIPT_DIR}/lib-topology.sh"
source "${SCRIPT_DIR}/lib-metrics.sh"
source "${SCRIPT_DIR}/ci-stages.sh"
source "${SCRIPT_DIR}/validator-verdict.sh"
topology_load

log "smoke test (gate: single-tx must pass before load smoke runs)"
./scripts/smoke.sh

# --- 7. Sustained-load invariant gate (Rust harness: fixed-rate soak,
# must-deliver, drop accounting, keep-pace), then 8. chaos and
# resilience suite. Both are env-gated stages, with bodies in
# ci-stages.sh. Both stages drive the Rust kardamom-load harness, built
# alongside the service binaries by the workflow, staged at
# target/release. Durations and rates are tunable through env
# variables, so PR runs stay short, and a full soak can be dialed up
# through the cluster-e2e.yml workflow_dispatch or matrix shard
# settings.
#
# Funded-account budget: genesis prefunds Anvil accounts #0 through
# #17, each with its own contiguous nonce chain on the never-reset
# chain. A fresh account's first tx must be nonce 0, with no gaps.
# Allocation: #0 is the gate smoke above; #1 through #6 are the
# sustained-load harness; #7 through #15 are one fresh account per
# chaos case (see chaos.sh); #16 is the ingress-churn failover re-smoke
# (step 7b); #17 is the fallback executor-churn re-smoke. Every check
# owns its own account, so every smoke tx is nonce 0. There is no NONCE
# setting and no nonce continuation.
# RUN_LOAD and RUN_CHAOS (default 1) let a CI shard run just one stage,
# so the full suite can split across runners; each shard brings up its
# own cluster. When both are unset, they default to 1, and the local or
# single-runner path runs smoke plus the default cluster chaos cases.
# CHAOS_CASES selects which chaos cases to run. In cluster mode, the
# default set is the three Raft cases (see chaos.sh).
LOAD_BIN="${ROOT}/target/release/kardamom-load"
if [[ -x "${LOAD_BIN}" ]]; then
  if [[ "${RUN_LOAD:-1}" == "1" ]]; then
    stage_load
  else
    log "RUN_LOAD=0 — skipping sustained-load stage (chaos-only shard)"
  fi

  # The chain-semantics suite (Target C) is off by default
  # (RUN_SEMANTICS=0), so it runs only on its own shard. See
  # stage_semantics in ci-stages.sh.
  if [[ "${RUN_SEMANTICS:-0}" == "1" ]]; then
    stage_semantics
  fi

  if [[ "${RUN_CHAOS:-1}" == "1" ]]; then
    stage_chaos
  else
    log "RUN_CHAOS=0 — skipping chaos stage (load-only shard)"
  fi
else
  stage_fallback_load
fi

# --- 7b. Ingress active/active failover, and the multicast-receipts freeze guard
# See stage_ingress_churn in ci-stages.sh for the failover and 2a
# freeze-guard reasoning.
stage_ingress_churn

# --- 7c. Validator sync and keep-up verdict -------------------------------------
# The validator followed everything the shard just did: bring-up,
# smoke, and load or chaos. The full verdict (liveness, sync and
# keep-up, BAL verification with zero divergences, trie shadow-checks)
# lives in validator-verdict.sh, which also holds the divergence-log
# scan shared with the chaos suite.
log "validator verdict: sync + keep-up + BAL cross-check (no divergence)"
run_validator_verdict

log "cluster-e2e PASSED"
