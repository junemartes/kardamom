# shellcheck shell=bash
# =============================================================================
# chaos-cases-validator.sh — validator-centric cases (lapse, fresh join,
# whole-stack CPU squeeze) + the shared warm-up gate and SIGSTOP freeze/thaw.
# =============================================================================
# This file is sourced into chaos.sh's shell, never run as a child
# process. This file must not install traps; chaos.sh owns the single
# EXIT trap.

# --- shared warm-up gate -----------------------------------------------------

# Wait until the validator has caught up and is verifying live: its
# blocks_verified counter is advancing, and its lag to the executors is
# small. A validator re-executes from genesis. Blocks that passed
# before it started have no BAL, and commit unverified during a fast
# catch-up. So it reaches the live head, and only then verifies
# steadily. A lapse or squeeze case must hit a verifying validator, not
# one still catching up. This replaces a warm-up loop that used to
# repeat in 4 places. Returns 0 once warmed, 1 on timeout. Either way,
# the last observations land in WARM_VERIFIED/WARM_BLOCK/WARM_EXEC/
# WARM_T, so callers can log or fail with the real numbers.
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

# Use SIGSTOP with a verified freeze. `docker pause` silently no-ops
# in the nested-DinD freezer, so a "paused" task can keep running, and
# every assert then passes against a victim that never lapsed. This
# happened on both the validator-lapse and sequencer-lapse cases.
# Signals hit the task's PID 1 regardless of freezer delegation, and
# the mid-freeze probe rules out a silent no-op: a frozen process
# cannot answer HTTP, so if metrics still respond, the case fails
# loudly instead of asserting against a replica that never lapsed.
# This function sleeps 3s before probing; callers subtract 3 from
# their freeze window. $5 (bridge ip) is optional. When set, the probe
# tries the bridge first, like fetch_metrics. When empty, it uses
# docker-exec only, for loopback exporters.
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

# Send SIGCONT to a frozen inner container, with a bound and no
# output. Returns non-zero on failure. A failed CONT is not
# automatically a case error: the frozen task can get replaced
# mid-freeze by a supervisor action, or the node exec can be
# transiently wedged. Each case decides what that means.
thaw_container() { # <node> <inner-container>
  timeout 20 docker exec "$1" docker kill -s CONT "$2" >/dev/null 2>&1
}

# --- validator-lapse ---------------------------------------------------------

# validator-lapse case: pause the validator process for a window,
# under sustained load, then resume. The validator's live tx_bal
# multicast image lapses during the pause. On resume, the missed BALs
# are still sitting in the live multicast term buffer, so the
# validator drains them and keeps verifying. The catch-up skip bounds
# the cost of anything that aged out of the term buffer; those blocks
# commit unverified instead of blocking 5s each. There is no
# side-stream refetch mechanism; that prototype was discarded, since a
# co-located recorder plus follow-live replay starves the live poll
# path. Asserts: verification coverage holds (bal_missing did not grow
# much), the validator kept verifying past the pre-pause count, caught
# back up, and saw no divergence. The pipeline itself is untouched, since
# the validator is off the hot path, so the standard load and progress
# verdicts still apply.
LAPSE_S="${LAPSE_S:-30}"

# Print forensics for validator-lapse failures. The validator lives
# alone on the aux tier, and the generic failure dump does not cover
# it. Capture the nomad view, the node's container states, and the
# validator's own log tail, so a dark-endpoint verdict can point to a
# cause: process dead, exec wedged, or supervisor not restarting.
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

  # Warm up. The lapse must hit a verifying validator, not one still
  # catching up. Pausing a validator that never verified would fail
  # later with a misleading "did not resume verifying". Fail here with
  # the real reason.
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
    # A failed CONT is not a case error by itself. The frozen task can
    # get replaced mid-freeze by a supervisor action, which is the
    # newborn path, or the node exec can be transiently wedged. The
    # sampling loop below decides the verdict, based on the end state:
    # verifying live, caught up, zero divergences. The thaw mechanics
    # must never abort the case.
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

  # This is a verified thaw, mirroring the verified freeze. A CONT
  # that silently misses leaves a frozen orphan squatting the metrics
  # port. Every supervisor replacement then dies instantly on
  # EADDRINUSE, fatal in kardamom_obs::init, which burns the restart
  # budget, and mode=fail strands the validator permanently. Within a
  # grace window, the endpoint must answer, or the container must have
  # been replaced. Otherwise, kill the frozen container, so the port
  # frees and the supervisor restarts into clean air.
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

  # After thaw, container identity decides the contract. A ${LAPSE_S}s
  # freeze exceeds the media driver's client-liveness timeout. The
  # validator's aeron client gets evicted, and the process fail-stops
  # on thaw. Nomad restarts it, into the designed recovery loop:
  # persisted-cursor resume, archive replay-merge, and catch-up mode.
  # This is the crash-only path production actually takes, and this
  # case is the first to assert it end to end. Either path must end
  # the same way: verifying live again, caught up, zero divergences.
  #   - Survivor (container StartedAt unchanged, a freeze shorter than
  #     eviction): the original term-buffer contract holds. verified
  #     advances past the pre-freeze count, and bal_missing growth
  #     stays within tolerance.
  #   - Newborn (StartedAt changed): counters reset. Catch-up commits
  #     the freeze-window backlog unverified, by design, so bal_missing
  #     is not comparable across the restart. Assert that it verifies
  #     live from the fresh counter, catches up, and reports zero
  #     divergences.
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

# validator-join: a fresh validator joins a chain already in
# progress. Stop the running validator, wipe its state and checkpoint
# staging, and let Nomad restart it with nothing. The newborn must
# adopt an executor peer checkpoint, the cold-start half of the
# replay-unavailable fallback, bootstrap the hashed mirror and trie
# from that trie-off image, catch up to the live head, and resume
# verified execution with zero divergences. This proves both sync and
# state correctness: the divergence latch re-executes and cross-checks
# every post-join block against the executors' BAL and receipts, and
# the MPT root advancing proves the bootstrapped trie is coherent.
# Executors checkpoint every 20s from bring-up, so a peer checkpoint
# always exists. The adoption log grep keeps the case non-vacuous; a
# genesis-replay join would not print it.
run_validator_join() {
  local inner
  inner="$(inner_container "${VALIDATOR_NODE}" validator)"
  [ -n "${inner}" ] || fail "validator-join: no inner validator container on ${VALIDATOR_NODE}"

  # Warm up. The chain must be far enough along that adoption skips
  # real work, and the pre-join validator must be verifying live, so
  # the case-end state ("verifying again") is meaningful.
  wait_validator_verifying_live 150 \
    || fail "validator-join: cluster never warmed up within ${WARM_T}s (verified=${WARM_VERIFIED} block=${WARM_BLOCK} exec=${WARM_EXEC})"
  log "validator-join: warmed up (verified=${WARM_VERIFIED} block=${WARM_BLOCK} exec=${WARM_EXEC}); wiping the validator for a fresh join"

  # Container names survive a task restart (task-<alloc-id>), so
  # newborn identity is the docker StartedAt timestamp. This is the
  # validator-lapse case's lesson, learned again here: sync succeeded,
  # but name-based newborn detection never fired.
  local started0
  started0="$(container_started_at "${VALIDATOR_NODE}" "${inner}")"

  # Kill, then wipe inside the restart delay. The job's restart
  # stanza waits 15s before the replacement container starts, so the
  # wipe wins the race. A wipe-first order would instead race the
  # live mdbx.
  docker exec "${VALIDATOR_NODE}" docker kill "${inner}" >/dev/null \
    || fail "validator-join: kill failed"
  timeout 15 docker exec "${VALIDATOR_NODE}" sh -c \
      'rm -rf /opt/kardamom/state/validator /opt/kardamom/checkpoints/*' \
    || fail "validator-join: state wipe failed"
  log "validator-join: validator killed, state + checkpoint staging wiped"

  # The newborn must appear, adopt a peer checkpoint, catch up, and
  # verify.
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

  # Non-vacuity check: the join must have gone through adoption,
  # including the trie bootstrap of the trie-off executor image. A
  # genesis replay would satisfy the sync asserts without exercising
  # either.
  local nlogs
  nlogs="$(timeout 20 docker exec "${VALIDATOR_NODE}" docker logs "${newborn}" 2>&1 | tail -400 || true)"
  has_line "${nlogs}" "adopted state from checkpoint" \
    || { val_debug; fail "validator-join: newborn did not adopt a peer checkpoint (genesis replay? peers unreachable?)"; }
  has_line "${nlogs}" "trie bootstrap complete" \
    || { val_debug; fail "validator-join: adopted state but no trie bootstrap ran (trie-off image not detected?)"; }

  # State correctness: zero divergences across the whole join. The
  # latch covers every post-join block the validator actually
  # verified. The MPT root observation must be advancing, proving the
  # bootstrapped trie is live.
  local div root_blk
  div="$(val_metric_req validator_divergence_total 'validator-join divergence==0 assert')"
  [ "${div}" -eq 0 ] || { val_debug; fail "validator-join: ${div} divergence(s) after join"; }
  root_blk="$(val_metric validator_state_root_block)"; root_blk="${root_blk:-0}"
  [ "${root_blk}" -gt 0 ] \
    || { val_debug; fail "validator-join: no MPT state-root observation after join (trie dead?)"; }
  log "validator-join PASS: fresh validator adopted a peer checkpoint, bootstrapped the trie, caught up (lag $(( e_now - v1 ))), verifying live (verified=${vf1}), root observed at block ${root_blk}, 0 divergences"
}

# --- cpu-squeeze: whole-stack CPU-starvation drill ---------------------------

# This drill recreates the degraded-CI-runner storm on purpose. Every
# kardamom node container is cgroup-throttled at once (docker update
# --cpus), so executors, sealers, ingress, sequencers, and the
# validator all starve together. Aeron sessions lapse, back-pressure
# engages everywhere, and the validator falls into catch-up, just like
# the 4-core GH runners under heavy load. That kind of window once
# produced a load-shard divergence: halt, restart, then a clean
# re-validation of the same blocks, a sign of non-determinism on
# replay (the replay-overlap class). Ambient starvation found it by
# luck; this drill hunts it on purpose. The invariant under squeeze:
# no divergence, ever. Starvation may slow the validator, but must
# never fork its verdict.
SQUEEZE_S="${SQUEEZE_S:-120}"
SQUEEZE_CPUS_PER_NODE="${SQUEEZE_CPUS_PER_NODE:-0.75}"
SQUEEZE_RECOVER_S="${SQUEEZE_RECOVER_S:-180}"
# Oscillation: run N squeeze-and-release cycles instead of one long
# squeeze. The replay-overlap class needs the transition: sessions
# lapse under squeeze, then reconnect and replay-merge on release.
# Repeated cycles exercise that machinery far harder than one
# sustained squeeze of the same total length.
SQUEEZE_CYCLES="${SQUEEZE_CYCLES:-1}"
SQUEEZE_RELEASE_S="${SQUEEZE_RELEASE_S:-30}"

run_cpu_squeeze() {
  # Warm-up gate, the same as validator-lapse. The squeeze must hit a
  # validator verifying live; squeezing one still in catch-up asserts
  # nothing.
  wait_validator_verifying_live 150 \
    || fail "cpu-squeeze: validator never verifying live within ${WARM_T}s (verified=${WARM_VERIFIED} block=${WARM_BLOCK} exec=${WARM_EXEC})"
  log "cpu-squeeze: warmed up (verified=${WARM_VERIFIED} block=${WARM_BLOCK} exec=${WARM_EXEC}) after ${WARM_T}s"

  # Target node containers on the host engine, the DinD outer layer.
  # The cgroup limit cascades to every inner task. Control and
  # registry stay untouched, since the drill starves the stack, not
  # the harness's own probes.
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
    # Verify the squeeze took effect. A silently ignored limit would
    # assert nothing, the same lesson as the validator-lapse
    # docker-pause case. NanoCpus must be non-zero.
    local nano
    nano="$(docker inspect -f '{{.HostConfig.NanoCpus}}' "$(head -1 <<<"${nodes}")")"
    [ "${nano:-0}" -gt 0 ] || fail "cpu-squeeze: throttle did not take (NanoCpus=${nano})"
    log "cpu-squeeze: cycle ${cyc}/${SQUEEZE_CYCLES} squeezing ${SQUEEZE_S}s"
    sleep "${SQUEEZE_S}"

    # Restore CPU, with a best-effort second pass. Leaving a node
    # throttled would poison every later case and assert on this
    # cluster.
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

  # The invariant: zero divergences, checked in both the metric and
  # the logs. The metric resets if the validator restarted
  # mid-squeeze, but a pre-restart divergence still shows in the old
  # alloc's log. The alloc-log scan is the shared implementation in
  # validator-verdict.sh; ci-cluster.sh's §7c verdict runs the same
  # one, and the SIGPIPE doctrine is documented there.
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
  # No component is killed. Pause the validator, which is off the hot
  # path, and check that it resumes verifying with coverage held. The
  # live term buffer redelivers the paused window on resume, and the
  # catch-up skip bounds anything that aged out. All validator-specific
  # asserts live in the helper.
  run_validator_lapse
}

case_cpu_squeeze() {
  # Whole-stack CPU starvation, with no kills. Throttle every node
  # container at once, and check the invariant: starvation may slow
  # the pipeline, but must never fork the validator's verdict. All
  # squeeze mechanics and asserts live in the helper.
  run_cpu_squeeze
}

case_validator_join() {
  # A fresh validator joins the running chain mid-run: wipe and
  # restart. It must adopt an executor peer checkpoint, the cold-start
  # half, including the trie bootstrap, catch up, and resume verified
  # execution with zero divergences. All asserts live in the helper.
  run_validator_join
}
