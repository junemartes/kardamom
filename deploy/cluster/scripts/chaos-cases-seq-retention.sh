# shellcheck shell=bash
# =============================================================================
# chaos-cases-seq-retention.sh — sequencer-lapse + retention-overrun cases.
# =============================================================================
# SOURCED into chaos.sh's shell (never executed as a child). This file must
# NOT install traps (chaos.sh owns the single EXIT trap). Uses the shared
# freeze_verified/thaw_container (chaos-cases-validator.sh), the seqa/seqb
# probes (chaos-probes.sh) and the alloc-log evidence helpers
# (chaos-asserts.sh).

# --- sequencer-lapse ---------------------------------------------------------

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

# Forensics for sequencer-lapse failures: container identity (was the task
# REPLACED under us? same container still running?), the full resync metric
# block, and the current sequencer-a container's recent log lines. The first
# CI iterations of this case failed with signatures explainable only by
# process identity confusion — make the next one self-diagnosing.
seqa_debug() {
  log "sequencer-lapse DEBUG: inner containers on kardamom-sequencer-0:"
  docker exec kardamom-sequencer-0 sh -c 'docker ps -a --format "{{.Names}} {{.Status}}" | head -6' 2>/dev/null || true
  log "sequencer-lapse DEBUG: resync metrics at .21:9001:"
  { fetch_metrics 192.168.56.21 kardamom-sequencer-0 9001 || true; } \
    | grep -E "resync|watermark|floor" | head -12 || true
  log "sequencer-lapse DEBUG: current sequencer-a log tail:"
  docker exec kardamom-sequencer-0 sh -c \
    'docker logs --tail 20 "$(docker ps --format "{{.Names}}" | grep -m1 "^sequencer-a")" 2>&1 | grep -E "RESYNC|LAG|resync|panic" | tail -10' 2>/dev/null || true
}

run_sequencer_lapse() {
  local inner
  inner="$(inner_container kardamom-sequencer-0 sequencer-a)"
  [ -n "${inner}" ] || fail "sequencer-lapse: no inner sequencer-a container on kardamom-sequencer-0"

  # DETECTION was once asserted on the TWIN (shard 0's replica B, node-1
  # seq-b): freezing replica A wedged its egress session, which stalled the
  # sealer's single service thread on the offer deadline — a cluster-wide
  # boundary-arrival gap every RUNNING replica's feed had to flag. With the
  # consumer-filtered egress fan-out, publisher-only sessions are no longer
  # in the fan-out, so a frozen replica cannot starve anyone's egress and
  # the twin correctly sees nothing — the lapse contract is asserted on the
  # replica that actually lapsed (below). The frozen replica itself often
  # takes the crash-only path: a freeze past the media driver's
  # client-liveness timeout gets the aeron client EVICTED, so on thaw the
  # process fail-stops and Nomad restarts it — a fresh process with zeroed
  # counters (observed: run 30180670099 — post-thaw log shows `RESYNC enter
  # reason=Startup` 10s after SIGCONT; asserting lag counters on it reads a
  # newborn, not a survivor). With #99 fixed the restart rejoins cleanly —
  # assert_replica_healthy covers that half.
  local l0 r0
  l0="$(seqa_metric kardamom_sequencer_resync_lag_suspected_total)"; l0="${l0:-0}"
  r0="$(seqa_metric kardamom_sequencer_resync_entered_total)"; r0="${r0:-0}"
  # Container identity BEFORE the freeze: the newborn-vs-survivor decision
  # below cannot ride counter values alone — the typical pre-freeze baseline
  # is entered=1 (one startup enter, exited), and a restarted process ALSO
  # reads entered=1: equal values satisfy neither "incremented" nor "below
  # baseline", which timed this case out on main (run 30227283947:
  # "lag 0 -> 0, entered 1 -> 1, mode 0"). A Nomad in-place restart creates
  # a NEW container generation, so StartedAt is the unambiguous signal.
  local started0
  started0="$(container_started_at kardamom-sequencer-0 "${inner}")"
  # SIGSTOP/SIGCONT + verified freeze, NOT `docker pause` — see
  # freeze_verified (#108: the nested cgroup freezer inside a privileged
  # DinD node can silently no-op; CI run 30178211248).
  log "sequencer-lapse: freezing ${inner} (SIGSTOP) for ${SEQ_LAPSE_S}s (lag_suspected=${l0} resync_entered=${r0} started=${started0:-?})"
  freeze_verified kardamom-sequencer-0 "${inner}" 9001 sequencer-lapse 192.168.56.21
  sleep $(( SEQ_LAPSE_S - 3 ))
  thaw_container kardamom-sequencer-0 "${inner}" \
    || fail "sequencer-lapse: SIGCONT failed"
  log "sequencer-lapse: resumed; twin must have covered (no stall)"

  # The pipeline never depended on the paused replica — the twin raced on.
  assert_progress

  # DETECTION + RESPONSE (on the LAPSED replica), on EITHER recovery path:
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
  local t=0 l1 r1 mode good=0
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
      cur="$(inner_container kardamom-sequencer-0 sequencer-a)"
      started1=""
      [ -n "${cur}" ] && started1="$(container_started_at kardamom-sequencer-0 "${cur}")"
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

# --- retention-overrun -------------------------------------------------------

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
  inner="$(inner_container "${node}" "${kind}")"
  [ -n "${inner}" ] || fail "retention-overrun(${kind}): no inner ${kind} container on ${node}"
  cid0="$(timeout 15 docker exec "${node}" sh -c \
    'docker ps --filter name='"${inner}"' -q | head -1' 2>/dev/null || true)"

  # The repair needs a checkpoint DONOR: executor-0 stays untouched.
  wait_peer_checkpoint kardamom-executor-0 "retention-overrun(${kind})"

  # Freeze must hit a LIVE consumer (non-vacuity): its own gauge advancing.
  local p0 p1 t=0 live=0
  while [ "${t}" -lt 120 ]; do
    if [ "${kind}" = "executor" ]; then
      p1="$(prom_value "$(exec_metrics "${RETENTION_VICTIM_EXEC_IDX}" || true)" \
        "${EXECUTOR_BLOCK_METRIC}" first)"
    else
      p1="$(val_metric validator_committed_block)"
    fi
    p1="${p1:-0}"
    if [ -n "${p0:-}" ] && [ "${p1}" -gt "${p0}" ]; then live=1; break; fi
    p0="${p1}"; sleep 6; t=$(( t + 6 ))
  done
  [ "${live}" -eq 1 ] \
    || fail "retention-overrun(${kind}): victim not demonstrably live before the freeze (gauge ${p0:-?} -> ${p1:-?}); freezing a dead consumer asserts nothing"

  # SIGSTOP + VERIFIED freeze (freeze_verified, #108). The freeze is
  # ADAPTIVE, sized by OBSERVED traffic, not the target rate: the first run
  # of this case froze a fixed 2*retention/CHAOS_TPS seconds while the
  # runner delivered ~36tps of the 200tps target — the retained window never
  # even filled, the floor never left genesis, and the thawed replay was
  # served in full. Overrun needs frames-SINCE-FREEZE > retention; accepted
  # txs (ingress counter) are the observable lower-bound proxy for egress
  # frames (boundary ticks only add to it).
  local rx_freeze rx_now delta=0 elapsed=0 need=$(( 2 * KARDAMOM_CLUSTER_RETENTION ))
  rx_freeze="$(ingress_received || echo 0)"
  log "retention-overrun(${kind}): freezing ${inner} on ${node} until ${need} frames flow past it (retention=${KARDAMOM_CLUSTER_RETENTION}, cap ${RETENTION_FREEZE_CAP_S}s)"
  freeze_verified "${node}" "${inner}" "${port}" "retention-overrun(${kind})"
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
      thaw_container "${node}" "${inner}" || true
      fail "retention-overrun(${kind}): load too slow to overrun the retention window — only ${delta} of ${need} frames flowed in ${elapsed}s (≈$(( delta / elapsed ))tps); raise the load rate or lower KARDAMOM_CLUSTER_RETENTION"
    fi
  done
  log "retention-overrun(${kind}): window overrun (${delta} frames in ${elapsed}s ≈ $(( delta / elapsed ))tps); thawing"
  thaw_container "${node}" "${inner}" \
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

# --- case entry points (dispatched from run_case) ----------------------------

case_sequencer_lapse() {
  # No component killed: pause ONE racing replica of shard 0 and assert
  # the twin covers (no stall) while the resumed replica detects the
  # lapse and enters receipt-floor resync. All asserts in the helper.
  run_sequencer_lapse
}

case_retention_overrun() {
  run_retention_overrun executor
}

case_retention_overrun_validator() {
  run_retention_overrun validator
}
