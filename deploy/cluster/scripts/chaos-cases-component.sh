# shellcheck shell=bash
# =============================================================================
# chaos-cases-component.sh — component-chaos cases (executor/ingress/sequencer/
# sealer kills, node failure, checkpoint-restore drills).
# =============================================================================
# This file is sourced into chaos.sh's shell, never run as a child
# process. The injectors set KILLED_* globals in this shell, and every
# case body needs `local`, so each case gets its own case_<name>()
# function. This file must not install traps; chaos.sh owns the single
# EXIT trap. run_case (in chaos.sh) provides the load and injection
# scaffolding, and the post-case common asserts. These functions only
# hold the injection and case-specific assertions.

case_graceful_executor() {
  inject_graceful executor
  assert_count executor 3 "${CHAOS_RESTART_SLO_S}"
}

case_hard_executor() {
  inject_hard "kardamom-executor-0 kardamom-executor-1 kardamom-executor-2" executor
  assert_count executor 3 "${CHAOS_RESTART_SLO_S}"
}

# This checks a count of 2, not 1. With a killed-marker set,
# assert_count's replacement check then requires the killed replica to
# come back, instead of letting the untouched peer satisfy ">= 1" on
# the first poll.
case_graceful_ingress() {
  inject_graceful ingress
  assert_count ingress 2 "${CHAOS_RESTART_SLO_S}"
}

# The hard-kill victim rotates by run id (INGRESS_VICTIM, in
# chaos.sh). Ingress is active/active and symmetric, and a blast radius
# pinned forever to ingress-0 would never prove the twin can die.
case_hard_ingress() {
  inject_hard "kardamom-ingress-${INGRESS_VICTIM}" ingress
  assert_count ingress 2 "${CHAOS_RESTART_SLO_S}"
}

# Sequencers run 2 racing replicas per shard (job groups seq-a/seq-b, 4
# allocs total). A kill no longer stalls its shard: the twin on the
# other node keeps ordering. So these cases also check live pipeline
# progress.
# The load is pinned to shard 0 (account selection in run_case),
# and the stop targets a seq-a alloc specifically. An arbitrary alloc
# used to mean about half the runs killed a replica the pinned load
# never used.
case_graceful_sequencer() {
  inject_graceful_group sequencer seq-a
  assert_progress
  assert_count sequencer 4 "${CHAOS_RESTART_SLO_S}"
}

# This uses an explicit task name. `name=sequencer` would match both
# the sequencer-a and sequencer-b task containers, and kill an
# arbitrary one.
case_hard_sequencer() {
  inject_hard kardamom-sequencer-0 sequencer-a
  assert_progress
  assert_count sequencer 4 "${CHAOS_RESTART_SLO_S}"
}

case_sequencer_replica_kill() {
  # Hard-kill a specific replica: seq-a on node-0, shard 0's replica A.
  # Its twin is seq-b on node-1. The case's load is pinned to shard 0
  # (see the account selection in run_case), so the assertions actually
  # cover the shard that lost a replica. It must stay live with no
  # stall: the racing twin never stopped, and the cluster dedups its
  # refs. The killed replica restarts to full strength (4/4) and comes
  # back healthy. Established-sender coverage on the rejoiner is a known
  # gap (see assert_replica_healthy).
  inject_hard kardamom-sequencer-0 sequencer-a
  assert_progress
  assert_count sequencer 4 "${CHAOS_RESTART_SLO_S}"
  # seq-a on node-0: sequencer ip lane starts at .21, seq-a metrics :9001.
  assert_replica_healthy kardamom-sequencer-0 192.168.56.21 9001
}

# sealer-graceful and sealer-hard are deleted. They targeted the
# legacy single-sealer job, which no longer deploys. The 3-member Raft
# cluster replaces them: cluster-leader-kill, cluster-follower-kill, and
# cluster-quorum-loss-recover, in chaos-cases-cluster.sh. If ever
# invoked, they would fail on the first `running_alloc sealer`. Keeping
# dead cases invites a future meaningless resurrection.

case_node_failure_executor() {
  # Kill the whole node container. With 3 executor-role nodes and
  # distinct_hosts, the lost replica cannot reschedule onto a peer,
  # since none are free. So the cluster degrades to 2, and must keep
  # progressing. Bringing the node back recovers 3.
  log "node-failure: docker kill kardamom-executor-2 (whole node)"
  kill_node kardamom-executor-2
  # The survivors would satisfy a bare ">= 2" instantly. Observe
  # the outage first, by waiting for the victim's gauge to go dark, or
  # the case never proves a node was actually lost.
  local nf_t=0
  while exec_metrics 2 >/dev/null 2>&1; do
    sleep 3; nf_t=$(( nf_t + 3 ))
    [ "${nf_t}" -ge 60 ] \
      && fail "node-failure: executor-2's exporter still answering ${nf_t}s after the node kill — outage not observed"
  done
  log "node-failure: outage observed (executor-2 exporter dark after ${nf_t}s)"
  assert_count executor 2 "${CHAOS_RESTART_SLO_S}"
  # This case also needs a wide window. Killing a whole node thrashes
  # the runner, through docker teardown and nomad node-down churn,
  # enough that on 4-core CI hosts even the survivors' metric scrapes
  # can black out well past 60 seconds.
  assert_executor_progress 180
  log "node-failure: docker start kardamom-executor-2 (node returns)"
  docker start kardamom-executor-2 >/dev/null || fail "could not restart node kardamom-executor-2"
  assert_count executor 3 "${CHAOS_RESCHEDULE_SLO_S}"
}

case_state_checkpoint_restore() {
  # This is a data-loss drill. Wipe executor-0's state DB and its own
  # checkpoints, then restore from a peer executor's checkpoint.
  # Executor replicas are deterministic state machines at the same
  # block, so executor-1's checkpoint is a valid restore source. On
  # restart, executor-0 finds an empty state DB plus the peer
  # checkpoint, and restores it before opening the env. This replays
  # only the tail, instead of re-syncing from genesis. Expected, in
  # order:
  #   1. The fleet degrades from 3 to 2 and keeps progressing, since
  #      the replicas are deterministic.
  #   2. executor-0 restarts and restores from the checkpoint. This is
  #      checked with the "restored state from checkpoint" log line;
  #      otherwise it silently fell back to a full genesis re-sync,
  #      which this case exists to catch.
  #   3. The executor count returns to 3.
  local scr_r0 scr_now ck_name copied ck_rc
  wait_peer_checkpoint kardamom-executor-1 state-checkpoint-restore
  # Take a count baseline over the alloc log. Earlier cases' restarts
  # also log restores, and the evidence must survive container garbage
  # collection and multiple restart generations. docker logs on the
  # current container alone can miss a generation.
  scr_r0="$(count_log_lines executor 'restored state from checkpoint')"
  log "state-checkpoint-restore: killing executor-0 + wiping its state DB and checkpoints"
  inject_hard kardamom-executor-0 executor
  docker exec kardamom-executor-0 bash -lc 'rm -rf /opt/kardamom/state/* /opt/kardamom/checkpoints/*' \
    || fail "state-checkpoint-restore: could not wipe executor-0 state"
  log "state-checkpoint-restore: re-replicating checkpoints from executor-1"
  # Copy one complete checkpoint, not the whole directory. The writer
  # adds a new checkpoint every interval and prunes old ones, so taring
  # the parent directory races with that churn ("file changed as we
  # read it", exit 1). Visible checkpoint-* directories are immutable:
  # they compact under a temp name, then rename into place when done.
  # The retry only covers the narrow window where the picked checkpoint
  # gets pruned mid-copy.
  copied=0
  for _ in 1 2 3; do
    # This is a self-heal short-circuit. Since recovery-D, the
    # restarted executor fetches a peer checkpoint on cold start, and
    # immediately writes and prunes its own checkpoints, racing this
    # loop for the same directory. A self-healed victim still satisfies
    # this case's real assertion, a checkpoint restore rather than a
    # genesis re-sync, with the same evidence line.
    scr_now="$(count_log_lines executor 'restored state from checkpoint')"
    if [ "${scr_now}" -gt "${scr_r0}" ]; then
      log "state-checkpoint-restore: executor-0 self-healed from a peer before the harness copy landed"
      copied=1
      break
    fi
    ck_name="$(docker exec kardamom-executor-1 bash -lc \
      'ls -d /opt/kardamom/checkpoints/checkpoint-* 2>/dev/null | sort | tail -1' \
      | xargs -rn1 basename)"
    [ -n "${ck_name}" ] || { sleep 2; continue; }
    docker exec kardamom-executor-0 bash -lc 'rm -rf /opt/kardamom/checkpoints/*'
    ck_rc=0
    docker exec kardamom-executor-1 tar -C /opt/kardamom --warning=no-file-changed -cf - "checkpoints/${ck_name}" \
      | docker exec -i kardamom-executor-0 tar -C /opt/kardamom -xf - || ck_rc=$?
    # tar exit code 1 means live-writer drift; restore-side validation
    # and the replay fallback (recovery C/D) are the integrity gate. But
    # a tar that raced the source's prune can still deliver a torn copy,
    # an image without MANIFEST, with an exit code of 1 or less. The
    # executor now refuses and quarantines such a copy, and self-heals
    # from a peer. Still, verify completeness here, so this case
    # exercises the local-restore path it exists for, not the network
    # fallback.
    if [ "${ck_rc}" -le 1 ] && docker exec kardamom-executor-0 bash -lc \
        "test -s '/opt/kardamom/checkpoints/${ck_name}/MANIFEST' && test -s '/opt/kardamom/checkpoints/${ck_name}/mdbx.dat'"; then
      copied=1
      break
    fi
    log "state-checkpoint-restore: copy of ${ck_name} incomplete or failed (raced the writer's prune?); retrying"
    sleep 2
  done
  [ "${copied}" = 1 ] || fail "state-checkpoint-restore: checkpoint copy failed"
  # The surviving replicas must keep the pipeline progressing on 2.
  assert_executor_progress 180
  # executor-0 restarts, restores from the peer checkpoint, rejoins to 3.
  assert_count executor 3 "${CHAOS_RESCHEDULE_SLO_S}"
  wait_log_count_gt executor 'restored state from checkpoint' "${scr_r0}" 120 6 \
    "state-checkpoint-restore: executor-0 did NOT restore from checkpoint — fell back to genesis re-sync"
  log "state-checkpoint-restore: executor-0 restored from checkpoint + rejoined (no genesis re-sync)"
}

case_replay_window_resync() {
  # This is a full-resync drill. Wipe executor-1's state DB and
  # checkpoints, then let the node repair itself, with no harness-side
  # checkpoint copy. A wiped node cannot re-sync from genesis, since the
  # cluster keeps only a bounded canonical window: a REPLAY_FROM below
  # its floor gets refused with REPLAY_UNAVAILABLE. So on restart, the
  # executor must fetch a checkpoint from a peer replica over the
  # checkpoint-serve port (9014) before its first join, restore it, and
  # resume from there. Expected, in order:
  #   1. The fleet degrades from 3 to 2 and keeps progressing.
  #   2. executor-1 restarts, fetches a peer checkpoint (checked with
  #      the "fetched checkpoint from peer" log line, which only this
  #      self-heal path emits), and restores it ("restored state from
  #      checkpoint").
  #   3. The executor count returns to 3, and the fleet converges.
  # The victim is executor-1, not executor-0, so this case stays
  # independent of state-checkpoint-restore when they run back-to-back.
  local ex1_inner ex1_logs t fetched restored
  wait_peer_checkpoint kardamom-executor-0 replay-window-resync
  log "replay-window-resync: killing executor-1 + wiping its state DB and checkpoints"
  inject_hard kardamom-executor-1 executor
  docker exec kardamom-executor-1 bash -lc 'rm -rf /opt/kardamom/state/* /opt/kardamom/checkpoints/*' \
    || fail "replay-window-resync: could not wipe executor-1 state"
  # The surviving replicas must keep the pipeline progressing on 2.
  assert_executor_progress 180
  # executor-1 restarts, self-heals from a peer checkpoint, rejoins to 3.
  assert_count executor 3 "${CHAOS_RESCHEDULE_SLO_S}"
  # Fetch and restore take real time, since a checkpoint image is
  # hundreds of MB and the node tries every peer that advertises
  # something newer. So this polls for the two log lines instead of
  # grepping once; a one-shot grep can miss the restore by a fraction of
  # a second. Logs are captured once per poll and matched in pure bash
  # (has_line), never `docker logs | grep -q`, which can silently miss a
  # match under SIGPIPE and pipefail.
  t=0; fetched=0; restored=0
  while :; do
    ex1_inner="$(inner_container kardamom-executor-1 executor)"
    if [ -n "${ex1_inner}" ]; then
      ex1_logs="$(docker exec kardamom-executor-1 bash -lc \
        "docker logs ${ex1_inner} 2>&1" 2>/dev/null || true)"
      has_line "${ex1_logs}" 'fetched checkpoint from peer' && fetched=1
      has_line "${ex1_logs}" 'restored state from checkpoint' && restored=1
      [ "${fetched}" = 1 ] && [ "${restored}" = 1 ] && break
    fi
    sleep 5; t=$((t+5))
    if [ "${t}" -ge 90 ]; then
      [ "${fetched}" = 1 ] \
        || fail "replay-window-resync: executor-1 did NOT fetch a peer checkpoint (self-heal path not taken)"
      fail "replay-window-resync: executor-1 fetched but did NOT restore the peer checkpoint within 90s"
    fi
  done
  log "replay-window-resync: executor-1 self-healed from a peer checkpoint (fetch + restore + rejoin, ${t}s)"
}
