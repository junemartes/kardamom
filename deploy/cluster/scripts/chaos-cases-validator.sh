# shellcheck shell=bash
# =============================================================================
# chaos-cases-validator.sh — validator-centric cases (lapse, fresh join,
# whole-stack CPU squeeze) + the shared warm-up gate and SIGSTOP freeze/thaw.
# =============================================================================
# SOURCED into chaos.sh's shell (never executed as a child). This file must
# NOT install traps (chaos.sh owns the single EXIT trap).

# --- shared warm-up gate -----------------------------------------------------

# Wait until the validator is CAUGHT UP AND VERIFYING LIVE — its
# blocks_verified counter is advancing and its lag to the executors is small.
# (A validator re-executes from genesis; blocks that passed before it started
# have no BAL and are committed unverified during a fast catch-up, so it
# reaches the live head and only THEN verifies steadily. A lapse/squeeze must
# hit a verifying validator, not one still catching up.) Replaces the warm-up
# loop previously repeated 4×. Returns 0 once warmed, 1 on timeout; either way
# the last observations land in WARM_VERIFIED/WARM_BLOCK/WARM_EXEC/WARM_T so
# callers can log/fail with the real numbers.
WARM_VERIFIED=0; WARM_BLOCK=0; WARM_EXEC=0; WARM_T=0
wait_validator_verifying_live() { # <timeout-s> [max-lag (default 15)]
  local timeout_s="$1" max_lag="${2:-15}" t=0 vprev=-1 verified=0 v_now=0 e_now=0
  while [ "${t}" -lt "${timeout_s}" ]; do
    verified="$(val_metric validator_blocks_verified_total)"; verified="${verified:-0}"
    v_now="$(val_metric validator_committed_block)"; v_now="${v_now:-0}"
    e_now="$(executor_progress || echo 0)"
    if [ "${verified}" -gt 0 ] && [ "${verified}" -gt "${vprev}" ] \
       && [ "${v_now}" -gt 0 ] && [ $(( e_now - v_now )) -le "${max_lag}" ]; then
      WARM_VERIFIED="${verified}"; WARM_BLOCK="${v_now}"; WARM_EXEC="${e_now}"; WARM_T="${t}"
      return 0
    fi
    vprev="${verified}"; sleep 6; t=$(( t + 6 ))
  done
  WARM_VERIFIED="${verified}"; WARM_BLOCK="${v_now}"; WARM_EXEC="${e_now}"; WARM_T="${t}"
  return 1
}

# --- shared SIGSTOP freeze / thaw --------------------------------------------

# SIGSTOP + VERIFIED freeze (#108): `docker pause` silently no-ops in the
# nested-DinD freezer, so a "paused" task can keep running and every assert
# then passes against a victim that never lapsed (observed on both the
# validator-lapse and sequencer-lapse cases; CI run 30178211248). Signals hit
# the task's PID 1 regardless of freezer delegation, and the mid-freeze probe
# makes a silent no-op IMPOSSIBLE: a frozen process cannot answer HTTP, so if
# metrics still respond the case fails loudly instead of asserting against a
# replica that never lapsed. Sleeps 3s before probing — callers subtract 3
# from their freeze window. $5 (bridge ip) is optional: when set the probe is
# bridge-first like fetch_metrics; empty means docker-exec-only (loopback
# exporters).
freeze_verified() { # <node> <inner-container> <metrics-port> <case-context> [bridge-ip]
  local node="$1" inner="$2" port="$3" ctx="$4" ip="${5:-}"
  docker exec "${node}" docker kill -s STOP "${inner}" >/dev/null \
    || fail "${ctx}: SIGSTOP failed"
  sleep 3
  if fetch_metrics "${ip}" "${node}" "${port}" >/dev/null 2>&1; then
    docker exec "${node}" docker kill -s CONT "${inner}" >/dev/null 2>&1 || true
    fail "${ctx}: freeze did NOT take effect (metrics endpoint still answering mid-freeze)"
  fi
  log "${ctx}: freeze verified (metrics endpoint dark)"
}

# SIGCONT a frozen inner container (bounded; silent). Returns non-zero on
# failure — a failed CONT is NOT automatically a case error: the frozen task
# can be REPLACED under us mid-freeze (supervisor action), or the node exec
# can be transiently wedged; each case owns that verdict.
thaw_container() { # <node> <inner-container>
  timeout 20 docker exec "$1" docker kill -s CONT "$2" >/dev/null 2>&1
}

# --- validator-lapse ---------------------------------------------------------

# validator-lapse case: PAUSE the validator process for a window under
# sustained load, then resume. The validator's live tx_bal multicast image
# lapses during the pause; on resume the missed BALs are still sitting in the
# live multicast TERM BUFFER, so the validator drains them and keeps
# verifying, and the catch-up skip (#78) bounds the cost of anything that aged
# out of the term buffer (those blocks commit unverified instead of blocking
# 5s each). There is NO side-stream refetch mechanism — that prototype was
# discarded (a co-located recorder + follow-live replay starves the live poll
# path). Asserts: verification coverage held (bal_missing did not materially
# grow), the validator kept verifying past the pre-pause count, caught back
# up, and saw no divergence. The pipeline itself is untouched (the validator
# is off the hot path), so the standard load + progress verdicts still apply.
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
  inner="$(inner_container "${VALIDATOR_NODE}" validator)"
  [ -n "${inner}" ] || fail "validator-lapse: no inner validator container on ${VALIDATOR_NODE}"

  # WARM UP: the lapse must hit a verifying validator, not one still catching
  # up. Pausing a validator that never verified would fail LATER with a
  # misleading "did not resume verifying" — fail here with the real reason.
  wait_validator_verifying_live 150 \
    || fail "validator-lapse: never warmed up within ${WARM_T}s (verified=${WARM_VERIFIED} block=${WARM_BLOCK} exec=${WARM_EXEC}) — not verifying live BEFORE the pause"
  log "validator-lapse: warmed up (verified=${WARM_VERIFIED} block=${WARM_BLOCK} exec=${WARM_EXEC}) after ${WARM_T}s"

  local m0 vf0 started0
  m0="$(val_metric validator_bal_missing_total)"; m0="${m0:-0}"
  vf0="$(val_metric validator_blocks_verified_total)"; vf0="${vf0:-0}"
  started0="$(container_started_at "${VALIDATOR_NODE}" "${inner}")"

  log "validator-lapse: freezing ${inner} (SIGSTOP) for ${LAPSE_S}s (verified=${vf0} bal_missing=${m0} started=${started0:-?})"
  freeze_verified "${VALIDATOR_NODE}" "${inner}" "${VALIDATOR_PORT}" validator-lapse
  sleep $(( LAPSE_S - 3 ))
  if ! thaw_container "${VALIDATOR_NODE}" "${inner}"; then
    # A failed CONT is NOT a case error by itself: the frozen task can be
    # REPLACED under us mid-freeze (supervisor action) — which IS the
    # newborn path — or the node exec can be transiently wedged. The
    # sampling loop below owns the verdict (end state: verifying live,
    # caught up, zero divergences); the thaw mechanics must never abort
    # the case.
    local cur0
    cur0="$(inner_container "${VALIDATOR_NODE}" validator)"
    if [ -n "${cur0}" ] && [ "${cur0}" != "${inner}" ]; then
      log "validator-lapse: SIGCONT target gone — container replaced during freeze (${inner} -> ${cur0}); newborn path"
    else
      sleep 5
      thaw_container "${VALIDATOR_NODE}" "${inner}" \
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
    if fetch_metrics "" "${VALIDATOR_NODE}" "${VALIDATOR_PORT}" >/dev/null 2>&1; then
      thaw_ok=1; break
    fi
    curX="$(inner_container "${VALIDATOR_NODE}" validator)"
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
    cur="$(inner_container "${VALIDATOR_NODE}" validator)"
    started1=""
    if [ -n "${cur}" ]; then
      started1="$(container_started_at "${VALIDATOR_NODE}" "${cur}")"
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
  d1="$(val_metric_req validator_divergence_total 'validator-lapse divergence==0 assert')"
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

# --- validator-join ----------------------------------------------------------

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
run_validator_join() {
  local inner
  inner="$(inner_container "${VALIDATOR_NODE}" validator)"
  [ -n "${inner}" ] || fail "validator-join: no inner validator container on ${VALIDATOR_NODE}"

  # Warm up: the chain must be far enough along that adoption skips real work,
  # and the pre-join validator must be verifying live so the case-end state
  # ("verifying again") is meaningful.
  wait_validator_verifying_live 150 \
    || fail "validator-join: cluster never warmed up within ${WARM_T}s (verified=${WARM_VERIFIED} block=${WARM_BLOCK} exec=${WARM_EXEC})"
  log "validator-join: warmed up (verified=${WARM_VERIFIED} block=${WARM_BLOCK} exec=${WARM_EXEC}); wiping the validator for a fresh join"

  # Container NAMES survive a task restart (task-<alloc-id>), so newborn
  # identity is the docker StartedAt timestamp — the validator-lapse case's
  # lesson, relearned on this case's first CI run (sync succeeded, the
  # name-based newborn detection never fired).
  local started0
  started0="$(container_started_at "${VALIDATOR_NODE}" "${inner}")"

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
  local deadline=$(( $(date +%s) + 240 )) newborn="" joined=0 vf1=0 v1=0 e_now=0
  while [ "$(date +%s)" -lt "${deadline}" ]; do
    if [ -z "${newborn}" ]; then
      local cur started1
      cur="$(inner_container "${VALIDATOR_NODE}" validator)"
      if [ -n "${cur}" ]; then
        started1="$(container_started_at "${VALIDATOR_NODE}" "${cur}")"
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
  div="$(val_metric_req validator_divergence_total 'validator-join divergence==0 assert')"
  [ "${div}" -eq 0 ] || { val_debug; fail "validator-join: ${div} divergence(s) after join"; }
  root_blk="$(val_metric validator_state_root_block)"; root_blk="${root_blk:-0}"
  [ "${root_blk}" -gt 0 ] \
    || { val_debug; fail "validator-join: no MPT state-root observation after join (trie dead?)"; }
  log "validator-join PASS: fresh validator adopted a peer checkpoint, bootstrapped the trie, caught up (lag $(( e_now - v1 ))), verifying live (verified=${vf1}), root observed at block ${root_blk}, 0 divergences"
}

# --- cpu-squeeze: whole-stack CPU-starvation drill ---------------------------

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
  wait_validator_verifying_live 150 \
    || fail "cpu-squeeze: validator never verifying live within ${WARM_T}s (verified=${WARM_VERIFIED} block=${WARM_BLOCK} exec=${WARM_EXEC})"
  log "cpu-squeeze: warmed up (verified=${WARM_VERIFIED} block=${WARM_BLOCK} exec=${WARM_EXEC}) after ${WARM_T}s"

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
  wait_validator_verifying_live "${SQUEEZE_RECOVER_S}" \
    || fail "cpu-squeeze: validator not verifying live within ${SQUEEZE_RECOVER_S}s of restore (verified=${WARM_VERIFIED} block=${WARM_BLOCK} exec=${WARM_EXEC})"

  # THE invariant: zero divergences — metric AND logs (the metric resets if
  # the validator restarted mid-squeeze; a pre-restart divergence still shows
  # in the old alloc's log, exactly the 2026-08-03 signature). The alloc-log
  # scan is the SHARED implementation in validator-verdict.sh (ci-cluster.sh's
  # §7c verdict runs the same one; SIGPIPE doctrine documented there).
  local div
  div="$(val_metric validator_divergence_total)"; div="$(printf '%.0f' "${div:-0}")"
  [ "${div}" -eq 0 ] || fail "cpu-squeeze: validator counted ${div} divergence(s) under starvation"
  if divergence_scan; then
    divergence_dump_context
    fail "cpu-squeeze: validator diverged under starvation (alloc ${DIVERGENCE_ALLOC}; context above)"
  fi
  log "cpu-squeeze PASS: ${n_count} nodes starved ${SQUEEZE_S}s at ${SQUEEZE_CPUS_PER_NODE} CPUs, validator recovered (verified=${WARM_VERIFIED}, lag $(( WARM_EXEC - WARM_BLOCK ))), 0 divergences"
}

# --- case entry points (dispatched from run_case) ----------------------------

case_validator_lapse() {
  # No component killed: pause the (off-hot-path) validator and assert it
  # resumes verifying with coverage held — the live term buffer redelivers
  # the paused window on resume, and the catch-up skip (#78) bounds
  # anything that aged out. All validator-specific asserts live in the
  # helper.
  run_validator_lapse
}

case_cpu_squeeze() {
  # Whole-stack CPU starvation (no kills): throttle every node container
  # at once and assert the invariant that starvation may slow the
  # pipeline but never fork the validator's verdict. All squeeze
  # mechanics + asserts live in the helper.
  run_cpu_squeeze
}

case_validator_join() {
  # Fresh validator joins the running chain mid-run: wipe + restart, must
  # adopt an executor peer checkpoint (#143 cold-start half, incl. the
  # trie bootstrap), catch up, and resume VERIFIED execution with zero
  # divergences. All asserts in the helper.
  run_validator_join
}
