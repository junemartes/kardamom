# shellcheck shell=bash
# =============================================================================
# chaos-cases-cluster.sh — clustered-sealer (Raft) cases.
# =============================================================================
# This file is sourced into chaos.sh's shell, never run as a child
# process. This file must not install traps; chaos.sh owns the single
# EXIT trap.
#
# This suite measures progress at the executor, since the Java cluster
# node has no Prometheus endpoint. The cluster commits blocks out its
# egress, and the executor applies them. So executor_progress()
# advancing means the cluster is alive.

case_cluster_leader_kill() {
  # Hard-kill the inner cluster container on the current leader's node.
  # The 3-member Raft quorum must survive losing the leader, and keep
  # committing. The executor's block gauge resumes advancing once the
  # cluster has a live leader again. This case replaces an earlier gap:
  # a single sealer crash used to freeze the pipeline; the Raft cluster
  # does not.
  #
  # This case checks that the pipeline keeps progressing, not that the
  # leader's memberId changed. A hard-killed leader's Nomad task can
  # restart fast and win the election again, since it has the most
  # up-to-date log. So requiring a different memberId would be flaky and
  # wrong. "The cluster still commits" is the real proof of resilience:
  # it needs a live leader and quorum, regardless of which member leads.
  local old_leader leader_node
  old_leader="$(cluster_leader)"
  leader_node="kardamom-sealer-${old_leader}"
  log "cluster-leader-kill: current leader memberId=${old_leader} on ${leader_node}; hard-killing its cluster container"
  inject_hard "${leader_node}" "${CLUSTER_TASK}"
  # Quorum re-establishes a leader, either a different member or the
  # restarted one winning again. The pipeline resumes committing.
  # assert_executor_progress polls up to its timeout, covering the
  # election and client redirect window.
  assert_executor_progress
  log "cluster-leader-kill: pipeline resumed committing after leader kill (now leader memberId=$(cluster_leader 2>/dev/null || echo '?'))"
  # The killed member's Nomad task restarts, with force_pull
  # re-pulling the image, and rejoins. The cluster job returns to 3
  # running.
  assert_count "${CLUSTER_TASK}" 3 "${CHAOS_RESTART_SLO_S}"
}

case_cluster_follower_kill() {
  # Hard-kill a member that is not the leader. Quorum (2/3) is
  # unaffected, so the pipeline must keep progressing with no stall; the
  # executor's block gauge advances throughout. No new election is
  # needed, since the leader is untouched. The killed member's task then
  # restarts and rejoins (3/3).
  local leader follower fk_r0 fk_t fk_logs still
  leader="$(cluster_leader)"
  # Pick any memberId in 0..2 that isn't the leader.
  for follower in 0 1 2; do [ "${follower}" != "${leader}" ] && break; done
  # A snapshot must exist before the kill. An intact-dir restart is
  # where the sealer's snapshot restore path actually runs. Aeron 1.44
  # static membership instead replays a blank member from log position
  # 0 (see cluster-member-rejoin). Before the in-process scheduler
  # existed, this path had never run outside unit tests.
  log "cluster-follower-kill: leader=memberId=${leader}; waiting for a cluster snapshot"
  fk_t=0
  while :; do
    fk_logs="$(cluster_alloc_logs)"
    has_line "${fk_logs}" 'cluster SNAPSHOT triggered' && break
    sleep 10; fk_t=$(( fk_t + 10 ))
    [ "${fk_t}" -ge 300 ] \
      && fail "cluster-follower-kill: no snapshot within ${fk_t}s — is the snapshot scheduler running?"
  done
  fk_r0="$(count_log_lines "${CLUSTER_TASK}" "sealer snapshot RESTORED memberId=${follower}" --stdout-only)"
  log "cluster-follower-kill: snapshot present (member ${follower} restore count ${fk_r0}); killing FOLLOWER memberId=${follower} on kardamom-sealer-${follower}"
  inject_hard "kardamom-sealer-${follower}" "${CLUSTER_TASK}"
  # Quorum holds (2/3). The executor must keep applying blocks with no
  # stall.
  assert_executor_progress
  # The leader must stay unchanged; a follower loss does not trigger a
  # re-election.
  still="$(cluster_leader)"
  [ "${still}" = "${leader}" ] \
    && log "cluster-follower-kill: leader unchanged (memberId=${leader}) — quorum held" \
    || log "cluster-follower-kill: WARN leader changed (${leader} -> ${still}); quorum still held, progress OK"
  # Killed follower's task restarts and rejoins (3/3).
  assert_count "${CLUSTER_TASK}" 3 "${CHAOS_RESTART_SLO_S}"
  # The restarted member's directories are intact, so it must recover
  # by loading its local latest snapshot, a bounded restart time, not by
  # replaying the whole lifetime log. This checks a count increase,
  # since earlier restarts in this shard may have already restored.
  wait_log_count_gt "${CLUSTER_TASK}" "sealer snapshot RESTORED memberId=${follower}" \
    "${fk_r0}" 180 10 \
    "cluster-follower-kill: restarted member never logged 'sealer snapshot RESTORED' — the snapshot restore path did not run on an intact-dir restart" \
    --stdout-only
  log "cluster-follower-kill: member ${follower} restored from snapshot on restart"
}

case_cluster_member_rejoin() {
  # This is the blank-member catch-up drill: the "join mid-way with an
  # empty state" edge case for the Raft members themselves. A follower's
  # cluster directory and archive are wiped after its kill, so the
  # restarted member owns nothing. Under Aeron 1.44 static membership, a
  # blank member catches up by replicating and replaying the leader's
  # log from position 0. Snapshots are not transferred to blank members;
  # they only bound the restart time of members whose directories
  # survive, the path cluster-follower-kill checks. Full log replay is
  # deterministic, so the correct outcome here is: a fresh-at-genesis
  # service start, the whole log replayed (proven by the post-wipe
  # snapshot position reaching the wipe-time head), 3/3 running, and the
  # pipeline unaffected throughout, since quorum (2/3) held. This case
  # documents a cost: a blank member's rejoin time grows with the
  # lifetime log. Bounding it needs a log purge after snapshot, tracked
  # as a follow-up.
  local leader follower f0 f1 t head_at_wipe catchup_block first_block=0 moved=0
  leader="$(cluster_leader)"
  for follower in 0 1 2; do [ "${follower}" != "${leader}" ] && break; done

  # Take a baseline before the wipe. This is a count, not a presence
  # check, since bring-up also logs a fresh start.
  f0="$(count_log_lines "${CLUSTER_TASK}" "sealer state FRESH at genesis memberId=${follower}" --stdout-only)"
  # Get the head position at wipe time. This is the position the
  # blank member must replay back to, before this case can pass (see
  # the catch-up proof below). A real reading is required.
  # executor_progress prints empty when every scrape fails, which is
  # routine right after the previous case's kill. Defaulting that to 0
  # would make the proof below meaningless again: "replayed to block >=
  # 0" is true even for a member that has done nothing but log fresh at
  # genesis. Read this before any injection, so a failure here is
  # clean.
  head_at_wipe="$(executor_progress || true)"
  { [ -n "${head_at_wipe}" ] && [ "${head_at_wipe}" -gt 0 ]; } \
    || fail "cluster-member-rejoin: could not read the executor head before the wipe (got '${head_at_wipe}') — refusing to run: the catch-up proof needs a real target position or it proves nothing"

  log "cluster-member-rejoin: leader=memberId=${leader}; killing FOLLOWER memberId=${follower} and WIPING its cluster + archive dirs"
  inject_hard "kardamom-sealer-${follower}" "${CLUSTER_TASK}"
  docker exec "kardamom-sealer-${follower}" bash -lc \
    'rm -rf /opt/kardamom/cluster/* /opt/kardamom/archive/*' \
    || fail "cluster-member-rejoin: could not wipe memberId=${follower} state"

  # Quorum (2/3) holds. The pipeline keeps committing throughout.
  assert_executor_progress
  # The wiped member's task restarts and the job returns to 3/3.
  assert_count "${CLUSTER_TASK}" 3 "${CHAOS_RESTART_SLO_S}"

  # The restarted member must start blank. The fresh-at-genesis count
  # grew, proving the wipe took and this is genuinely the empty-state
  # path. It must also finish the full log replay.
  #
  # This is the catch-up proof. An earlier version of this check looked
  # for a new "snapshot TAKEN" log line, on the theory that members
  # snapshot at the same replicated log position, so a post-rejoin TAKEN
  # line means it replayed to that point. That reasoning was backward,
  # and made the check meaningless. The snapshot action is itself an
  # entry in the replicated log, so a blank member replaying from
  # position 0 re-executes every historical snapshot action, emitting
  # `TAKEN block=30`, `TAKEN block=60`, and so on as it goes. Those
  # lines appear because the member is still catching up, not because
  # it finished.
  #
  # That gap had real consequences. The suite moved on to the next case
  # with one member minutes from converged. cluster-quorum-loss-recover
  # then kills sealer-1 and sealer-2, so when the wipe had landed on
  # sealer-0, the only survivor was the un-caught-up member. The
  # pipeline stayed flat for the rest of the replay, and that case
  # missed its SLO. Which member gets wiped depends on who was leader
  # here, so this failed roughly every other run.
  #
  # The real proof is positional: the member's latest post-wipe snapshot
  # block must reach the live head. This check only looks at lines after
  # the last FRESH marker, so pre-wipe history cannot satisfy it. Replay
  # runs at about 12.5 blocks per second on this host, so
  # CLUSTER_REJOIN_SLO_S covers the log this suite builds. A member that
  # never converges now fails here, where the cause is clear, instead of
  # as a mystery stall in a later case. A member that never converges
  # is stuck in a join wedge: it stays inactive after a fast partial
  # replay and never receives the rest of the log (issue #195).
  t=0
  while :; do
    f1="$(count_log_lines "${CLUSTER_TASK}" "sealer state FRESH at genesis memberId=${follower}" --stdout-only)"
    # Get the latest TAKEN block emitted after the most recent
    # FRESH-at-genesis for this member, so only the post-wipe boot
    # counts.
    catchup_block="$(cluster_alloc_logs | awk -v m="${follower}" '
        $0 ~ ("FRESH at genesis memberId=" m) { seen = 1; last = "" }
        seen && $0 ~ ("snapshot TAKEN memberId=" m) { last = $0 }
        END { print last }' \
      | grep -oE 'block=[0-9]+' | cut -d= -f2 || true)"
    catchup_block="${catchup_block:-0}"
    # Track the first non-zero position, and whether it ever moved. The
    # two failure modes look identical in a single end-of-window number,
    # but need different responses. A converging member reaches the
    # head in 10 to 40 seconds. A wedged member parks on its first
    # position and never moves again.
    [ "${catchup_block}" -gt 0 ] && [ "${first_block:-0}" -eq 0 ] && first_block="${catchup_block}"
    [ "${catchup_block}" -gt "${first_block:-0}" ] && moved=1
    # The member has converged once its replayed position reaches the
    # head observed at wipe time. The head keeps advancing under load,
    # so reaching the wipe-time head proves the whole pre-existing log
    # was replayed.
    [ "${f1}" -gt "${f0}" ] && [ "${catchup_block}" -ge "${head_at_wipe}" ] && break
    sleep 10; t=$(( t + 10 ))
    if [ "${t}" -ge "${CLUSTER_REJOIN_SLO_S}" ]; then
      [ "${f1}" -gt "${f0}" ] \
        || fail "cluster-member-rejoin: restarted member did not start blank (fresh-at-genesis count ${f0} -> ${f1}) — the wipe did not take, this run proved nothing about empty-state rejoin"
      if [ "${moved:-0}" -eq 0 ]; then
        fail "cluster-member-rejoin: blank member FROZE at block ${catchup_block} (head at wipe ${head_at_wipe}) — its replay position never advanced once in ${t}s, so this is a JOIN WEDGE (issue #195), not slow replay: the member stays INACTIVE after a partial replay while reporting healthy to Nomad. Check the member's driver error log and a thread dump of its consensus-module thread"
      fi
      fail "cluster-member-rejoin: blank member replayed to block ${catchup_block} of head-at-wipe ${head_at_wipe} in ${t}s — the position DID keep advancing, so this is genuinely slow catch-up rather than the #195 wedge: blank-member replay is O(lifetime log) and the log is never purged, see the log-purge follow-up in docs/reviews/2026-08-03-chaos-coverage-audit.md"
    fi
  done
  log "cluster-member-rejoin: memberId=${follower} rejoined blank via full log replay (fresh ${f0}->${f1}, replayed to block ${catchup_block} >= head-at-wipe ${head_at_wipe}, ${t}s); leader now memberId=$(cluster_leader 2>/dev/null || echo '?')"
}

case_cluster_quorum_loss_recover() {
  # Kill two whole sealer node containers: docker kill the
  # kardamom-sealer-X containers themselves, not just the inner task.
  # Nomad on those nodes is gone too, so the inner cluster tasks cannot
  # restart there, leaving only 1 member. Raft quorum, which needs 2/3,
  # is lost. The pipeline must stall, with no false progress: the
  # executor's block gauge stays flat. Then bring one node back with
  # docker start. Quorum (2/3) returns, a leader is re-elected, progress
  # resumes, and the backlog drains with no gaps (load verdict PASS).
  # These SLOs are generous, since a node restart re-pulls images, so
  # this uses CHAOS_RESCHEDULE_SLO_S for the rejoin.
  local victims=(kardamom-sealer-1 kardamom-sealer-2)
  log "cluster-quorum-loss-recover: docker kill TWO sealer nodes (${victims[*]}) → quorum lost (1/3 up)"
  kill_node "${victims[@]}"
  # Quorum is lost, so the pipeline must stall: no commits, and the
  # executor gauge stays flat.
  assert_executor_stalled 15
  log "cluster-quorum-loss-recover: docker start ${victims[0]} (quorum 2/3 returns)"
  docker start "${victims[0]}" >/dev/null || fail "could not restart node ${victims[0]}"
  # Its cluster task reschedules and rejoins, restoring quorum. Give
  # it the reschedule SLO, to allow for an image re-pull. count_running
  # counts the `cluster` allocs.
  assert_count "${CLUSTER_TASK}" 2 "${CHAOS_RESCHEDULE_SLO_S}"
  # With quorum back, the executor must resume applying blocks and
  # drain the backlog. This case uses a wide timeout: it is the one case
  # where the clients' cluster sessions die, since an outage over 15
  # seconds exceeds the session timeout (leader-kill instead keeps
  # sessions alive through a NewLeaderEvent redirect). Re-election plus
  # client session re-establishment alone can take about 50 seconds
  # after the node restart. The reopened sessions then replay the
  # canonical stream from the log before new commits show up on the
  # executor's block gauge. 60 seconds timed out reliably in testing;
  # 180 seconds covers re-election, reconnect, and replay with margin.
  assert_executor_progress 180
  # Restore the second node too, so the suite leaves a healthy 3/3
  # cluster for later cases. This is best-effort, and not part of this
  # case's SLO.
  docker start "${victims[1]}" >/dev/null 2>&1 || true
}
