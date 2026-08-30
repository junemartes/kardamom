# shellcheck shell=bash
# =============================================================================
# validator-verdict.sh — the validator divergence/sync verdict, in one place.
# =============================================================================
# SOURCED (never executed) by ci-cluster.sh AND chaos.sh: the divergence-log
# scan (divergence_scan + divergence_dump_context) is THE shared fail-stop
# evidence check — previously duplicated between ci-cluster.sh §7c and the
# chaos cpu-squeeze case, the drift class the 2026-08-07 audit flagged — and
# run_validator_verdict is ci-cluster.sh's §7c stage (chaos.sh sources it
# un-called). This file must NOT install traps (the sourcing entry script
# owns the single EXIT trap). Requires lib.sh (log, on_control, all_allocs),
# lib-topology.sh (VALIDATOR_NODE/VALIDATOR_NODES/VALIDATOR_PORT,
# EXECUTOR_NODES/EXECUTOR_PORT) and lib-metrics.sh (fetch_metrics,
# prom_value).
#
# SIGPIPE doctrine (PR #158, re-fixed here in #177): NEVER
# `producer | grep -q` under `set -o pipefail`. grep -q exits at the first
# match; once the producer's output exceeds the pipe buffer the producer
# takes SIGPIPE (141) and the successful match is DISCARDED — a real
# "halted on divergence" reads as absence. Capture first, then
# substring-match in pure bash (no pipe to break).

# --- shared divergence-log scan ----------------------------------------------

# The divergence metric resets if the validator alloc restarted (recovery
# loop); a PRE-restart divergence still shows in the alloc log — so scan
# EVERY alloc of the job (not just the first): a rescheduled validator gets a
# new alloc, and the "halted on divergence" line would live in the OLD
# alloc's log. On a hit, DIVERGENCE_ALLOC/DIVERGENCE_LOGS carry the evidence
# for divergence_dump_context and the caller's verdict line; returns 1 on a
# clean scan. Deliberately NO fail()/exit here — the two consumers own
# different fail contracts (ci-cluster's "FAIL: ..." + exit 1, chaos's
# dual-stream fail()).
DIVERGENCE_ALLOC=""
DIVERGENCE_LOGS=""
divergence_scan() { # -> 0 if any validator alloc logged "halted on divergence"
  DIVERGENCE_ALLOC=""; DIVERGENCE_LOGS=""
  local valloc vlogs
  while read -r valloc; do
    [ -z "${valloc}" ] && continue
    vlogs="$(on_control 'nomad alloc logs "$1" 2>/dev/null' "${valloc}" 2>/dev/null || true)"
    # Capture-then-match, pure bash — see the SIGPIPE doctrine above.
    if [[ "${vlogs}" == *"halted on divergence"* ]]; then
      DIVERGENCE_ALLOC="${valloc}"; DIVERGENCE_LOGS="${vlogs}"
      return 0
    fi
  done < <(all_allocs validator)
  return 1
}

# Print the divergence reason + context to stderr after a divergence_scan hit:
# the divergence line names claimed vs recomputed (or the receipt fields) —
# without this the CI log proves a divergence happened but not WHICH (the
# reason string only lived on the ephemeral runner). Also surfaces any
# flight-recorder dumps on the validator node. Diagnostics only — every step
# best-effort, never changes the verdict.
divergence_dump_context() {
  echo "----- divergence context (alloc ${DIVERGENCE_ALLOC}) -----" >&2
  printf '%s\n' "${DIVERGENCE_LOGS}" \
    | grep -B3 -A10 "halted on divergence" >&2 || true
  echo "----- flight-recorder dumps on validator node (if any) -----" >&2
  docker exec "${VALIDATOR_NODE}" sh -c \
    'for f in /opt/kardamom/state/divergence-*.json; do [ -f "$f" ] && { echo "== $f"; head -c 4096 "$f"; echo; }; done' \
    >&2 2>/dev/null || true
}

# --- scrape helpers -----------------------------------------------------------

# Scrape one metric value from a node:port (docker-exec path; the int value of
# the first matching sample — every metric probed here is single-series).
scrape_metric() { # <node> <port> <metric-name> -> value or ""
  local body
  body="$(fetch_metrics "" "$1" "$2" || true)"
  prom_value "${body}" "$3" first
}

# Executor progress reference: any responding executor (state-machine replicas).
executor_block() {
  local v
  for n in "${EXECUTOR_NODES[@]}"; do
    v="$(scrape_metric "$n" "${EXECUTOR_PORT}" kardamom_executor_block_number)"
    [[ -n "$v" ]] && { printf '%.0f\n' "$v"; return 0; }
  done
  return 1
}

# --- ci-cluster.sh §7c: the full validator verdict ---------------------------

# The validator (validator.nomad.hcl, one alloc on the aux node — kept out of
# the executor-chaos blast radius)
# followed everything the shard just did — bring-up, smoke, load and/or chaos —
# re-executing every block through the shared engine and cross-checking against
# the executors' receipts + per-block BAL, advancing an MPT state root. Assert:
#   (a) LIVENESS  — its /metrics endpoint (:9006) answers: the process did NOT
#       fail-stop (a proven divergence exits 2 and the job never restarts).
#   (b) SYNC + KEEP-UP — validator_committed_block advances and closes to within
#       VALIDATOR_LAG_MAX of the executors' kardamom_executor_block_number.
#   (c) VERIFICATION — validator_blocks_verified_total > 0 (the BAL cross-check
#       actually ran) and validator_divergence_total is absent/0.
# This is the cluster-mode successor of the old multiprocess
# `multiprocess_e2e_validator_syncs_and_keeps_up` smoke (removed with the
# single-sealer full-pipeline e2e in the cluster-only migration).
#
# EXECUTOR_NODES + VALIDATOR_NODES/VALIDATOR_PORT/EXECUTOR_PORT come from
# lib-topology.sh (the validator lives on the aux node — see
# validator.nomad.hcl, kept out of the executor-chaos blast radius).
run_validator_verdict() {
  local deadline ok e_blk v_blk v_start n valloc
  local verified diverged shadow_checks shadow_mismatch
  VALIDATOR_LAG_MAX="${VALIDATOR_LAG_MAX:-10}"
  VALIDATOR_SYNC_TIMEOUT_S="${VALIDATOR_SYNC_TIMEOUT_S:-180}"

  # Retry window: a single-shot probe here raced slow container starts (fresh
  # image pull on the aux node) and failed an otherwise-healthy deploy — the
  # exporter installs on the validator's FIRST log line, so patience is all
  # that's needed to tell "starting" from "fail-stopped".
  VALIDATOR_NODE=""
  for _ in $(seq 1 24); do
    for n in "${VALIDATOR_NODES[@]}"; do
      if timeout 8 docker exec "$n" curl -fsS --max-time 5 "http://127.0.0.1:${VALIDATOR_PORT}/metrics" >/dev/null 2>&1; then
        VALIDATOR_NODE="$n"; break 2
      fi
    done
    sleep 5
  done
  [[ -n "${VALIDATOR_NODE}" ]] || { echo "FAIL: no validator /metrics on :${VALIDATOR_PORT} after 120s (fail-stopped — divergence or session death?)" >&2; exit 1; }
  log "validator found on ${VALIDATOR_NODE}"

  # What the verdict asserts per shard tracks what the DESIGN guarantees:
  #
  # LOAD shard — full sync + bounded lag. The canonical stream is loss-proof
  # WITHIN the retention window (cluster sessions + retention + REPLAY_FROM on
  # every establishment; a cursor below the retention floor gets
  # REPLAY_UNAVAILABLE and takes the peer-checkpoint resync path instead), and
  # under plain load the validator demonstrably keeps up.
  #
  # CHAOS shards — fail-stop safety + forward progress, NOT bounded lag. The
  # canonical stream survives chaos (verified: the subscription cursor advances
  # through leader kills and quorum loss via replay + catch-up ordering), but
  # the MULTICAST side-streams (tx_bal, tx_receipts, tx_data envelopes) have no
  # retention/refetch yet: a deliberately-slower-than-hot-path verifier that
  # falls behind a 200tps chaos barrage lapses those images and then pays the
  # BAL wait per block — bounded-lag-after-chaos requires the archive-backed
  # side-stream refetch (tracked follow-up), not test tuning.
  v_start="$(scrape_metric "${VALIDATOR_NODE}" "${VALIDATOR_PORT}" validator_committed_block)"; v_start="${v_start:-0}"
  v_start="$(printf '%.0f' "${v_start}")"
  # D-7: the semantics shard runs REAL (light) traffic through the scenario
  # drivers, so the validator can and must be held to the bounded-lag verdict —
  # it only got the weak forward-progress one because RUN_LOAD=0. The weak
  # verdict stays for CHAOS shards, where side-stream image lapse under a kill
  # barrage makes bounded lag a tracked follow-up, not an assertion.
  if [[ "${RUN_LOAD:-1}" == "1" || "${RUN_SEMANTICS:-0}" == "1" ]]; then
    deadline=$(( $(date +%s) + VALIDATOR_SYNC_TIMEOUT_S ))
    ok=0
    while (( $(date +%s) < deadline )); do
      e_blk="$(executor_block || echo "")"
      v_blk="$(scrape_metric "${VALIDATOR_NODE}" "${VALIDATOR_PORT}" validator_committed_block)"
      v_blk="$(printf '%.0f' "${v_blk:-0}")"
      if [[ -n "${e_blk}" ]] && (( v_blk > 0 )) && (( e_blk - v_blk <= VALIDATOR_LAG_MAX )); then
        ok=1; break
      fi
      sleep 5
    done
    e_blk="${e_blk:-?}"
    if (( ok != 1 )); then
      echo "FAIL: validator did not sync within ${VALIDATOR_SYNC_TIMEOUT_S}s (validator=${v_blk:-?} executor=${e_blk}, started at ${v_start})" >&2
      # A frozen committed-block gauge is the WEDGE signature (F3 class): the
      # reason lives only in the validator's own log on the ephemeral runner —
      # dump its tail or the failure is undiagnosable (the 2026-08-04 #155
      # load-shard wedge at block 227 left nothing).
      echo "----- validator alloc log tails -----" >&2
      while read -r valloc; do
        [[ -z "${valloc}" ]] && continue
        echo "== alloc ${valloc} (stderr tail)" >&2
        on_control 'nomad alloc logs -stderr "$1" 2>/dev/null | tail -60' "${valloc}" >&2 2>/dev/null || true
      done < <(all_allocs validator)
      exit 1
    fi
    log "validator synced: block ${v_blk} vs executor ${e_blk} (lag $(( e_blk - v_blk )) <= ${VALIDATOR_LAG_MAX})"
  else
    # Forward progress: the validator must still be verifying and committing.
    deadline=$(( $(date +%s) + 60 ))
    ok=0
    while (( $(date +%s) < deadline )); do
      v_blk="$(scrape_metric "${VALIDATOR_NODE}" "${VALIDATOR_PORT}" validator_committed_block)"
      v_blk="$(printf '%.0f' "${v_blk:-0}")"
      if (( v_blk > v_start )); then ok=1; break; fi
      sleep 5
    done
    if (( ok != 1 )); then
      echo "FAIL: validator made no progress after chaos (stuck at ${v_blk:-?})" >&2
      exit 1
    fi
    log "chaos shard: validator progressing (${v_start} -> ${v_blk}); bounded lag asserted on the load shard"
  fi

  verified="$(scrape_metric "${VALIDATOR_NODE}" "${VALIDATOR_PORT}" validator_blocks_verified_total)"
  verified="$(printf '%.0f' "${verified:-0}")"
  (( verified > 0 )) || { echo "FAIL: validator verified 0 blocks against the BAL (tx_bal not flowing?)" >&2; exit 1; }
  # D-2: a failed scrape must not read as "0 divergences" — retry, then fail
  # loudly rather than pass vacuously on a dead exporter.
  diverged=""
  for _ in 1 2 3 4 5; do
    diverged="$(scrape_metric "${VALIDATOR_NODE}" "${VALIDATOR_PORT}" validator_divergence_total)"
    [[ -n "${diverged}" ]] && break
    # Absent metric vs dead exporter: divergence_total is not exported until
    # first incremented, so a healthy validator has no such line. The canary
    # (committed_block, always present once live) proves the exporter answers;
    # the absent counter then genuinely IS 0.
    if [[ -n "$(scrape_metric "${VALIDATOR_NODE}" "${VALIDATOR_PORT}" validator_committed_block)" ]]; then
      diverged=0
      break
    fi
    sleep 3
  done
  [[ -n "${diverged}" ]] || { echo "FAIL: validator exporter unscrapeable after 5 tries — cannot assert 0 divergences" >&2; exit 1; }
  diverged="$(printf '%.0f' "${diverged}")"
  (( diverged == 0 )) || { echo "FAIL: validator counted ${diverged} divergence(s)" >&2; exit 1; }
  # Metric AND logs: the metric resets if the alloc restarted (recovery loop);
  # a pre-restart divergence still shows in the alloc log — the shared
  # divergence_scan above catches it there (chaos.sh's cpu-squeeze case runs
  # the SAME scan; see the SIGPIPE doctrine in this file's header).
  if divergence_scan; then
    echo "FAIL: validator halted on divergence (found in alloc ${DIVERGENCE_ALLOC} log)" >&2
    divergence_dump_context
    exit 1
  fi
  # Incremental-trie shadow-check (--trie-shadow-check 8 in validator.nomad.hcl):
  # every 8th committed block recomputes the state root by FULL rebuild and
  # compares it to the node-incremental walker's (every-block checking would
  # saturate a CI core — see the cadence rationale in validator.nomad.hcl). A
  # mismatch fail-stops the validator (caught by the liveness probe above);
  # assert the counters directly too.
  # Runs on every shard: the validator commits blocks (v_blk > 0 asserted above
  # on both the load and chaos paths), so checks_total must be > 0 everywhere.
  shadow_checks="$(scrape_metric "${VALIDATOR_NODE}" "${VALIDATOR_PORT}" kardamom_state_trie_shadow_checks_total)"
  shadow_checks="$(printf '%.0f' "${shadow_checks:-0}")"
  (( shadow_checks > 0 )) || { echo "FAIL: trie shadow-check never ran (checks=${shadow_checks})" >&2; exit 1; }
  shadow_mismatch="$(scrape_metric "${VALIDATOR_NODE}" "${VALIDATOR_PORT}" kardamom_state_trie_shadow_mismatch_total)"
  shadow_mismatch="$(printf '%.0f' "${shadow_mismatch:-0}")"
  (( shadow_mismatch == 0 )) || { echo "FAIL: incremental state trie diverged from full rebuild (${shadow_mismatch} mismatches)" >&2; exit 1; }
  log "validator verdict PASSED: ${verified} blocks BAL-verified, 0 divergences, ${shadow_checks} shadow-checks / 0 mismatches"
}
