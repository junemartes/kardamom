# shellcheck shell=bash
# =============================================================================
# chaos-cases-cluster.sh — CLUSTERED-SEALER (Raft) cases.
# =============================================================================
# SOURCED into chaos.sh's shell (never executed as a child). This file must
# NOT install traps (chaos.sh owns the single EXIT trap).
#
# Progress is measured at the EXECUTOR (the Java cluster node has no
# Prometheus endpoint); the cluster commits blocks out its egress, the
# executor applies them, so executor_progress() advancing == cluster liveness.

case_cluster_leader_kill() {
  # HARD-kill the inner cluster container on the CURRENT leader's node. The
  # 3-member Raft quorum must survive losing the leader and KEEP COMMITTING — the
  # executor's block gauge resumes advancing once the cluster has a live leader
  # again. This REPLACES the documented single-sealer hard-kill SPOF gap (#58):
  # a single sealer crash froze the pipeline; the Raft cluster does not.
  #
  # NOTE: we assert the pipeline keeps progressing, NOT that the leader's memberId
  # changed. A hard-killed leader's Nomad task can restart fast and RE-WIN the
  # election (it has the most up-to-date log), so requiring a different memberId
  # is racy and wrong — "the cluster still commits" is the real resilience proof
  # (it requires a live leader + quorum regardless of which member leads).
  local old_leader leader_node
  old_leader="$(cluster_leader)"
  leader_node="kardamom-sealer-${old_leader}"
  log "cluster-leader-kill: current leader memberId=${old_leader} on ${leader_node}; hard-killing its cluster container"
  inject_hard "${leader_node}" "${CLUSTER_TASK}"
  # Quorum re-establishes a leader (a different member, or the restarted one
  # re-winning) → the pipeline resumes committing. assert_executor_progress polls
  # up to its timeout, covering the election + client redirect window.
  assert_executor_progress
  log "cluster-leader-kill: pipeline resumed committing after leader kill (now leader memberId=$(cluster_leader 2>/dev/null || echo '?'))"
  # The killed member's Nomad task restarts (force_pull re-pulls the image) and
  # rejoins, returning the cluster job to 3 running.
  assert_count "${CLUSTER_TASK}" 3 "${CHAOS_RESTART_SLO_S}"
}

case_cluster_follower_kill() {
  # HARD-kill a member that is NOT the leader. Quorum (2/3) is unaffected, so
  # the pipeline must keep progressing with NO stall — the executor's block
  # gauge advances throughout. (No new election is required; the leader is
  # untouched.) The killed member's task then restarts and rejoins (3/3).
  local leader follower fk_r0 fk_t fk_logs still
  leader="$(cluster_leader)"
  # Pick any memberId in 0..2 that isn't the leader.
  for follower in 0 1 2; do [ "${follower}" != "${leader}" ] && break; done
  # A snapshot must exist BEFORE the kill: an intact-dir restart is where
  # the sealer's snapshot RESTORE path actually runs (Aeron 1.44 static
  # membership replays a BLANK member from log position 0 instead — see
  # cluster-member-rejoin), and before the in-process scheduler existed
  # this path had never executed outside unit tests.
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
  # Quorum holds (2/3): the executor must keep applying blocks with no stall.
  assert_executor_progress
  # Leader must be UNCHANGED (a follower loss does not trigger re-election).
  still="$(cluster_leader)"
  [ "${still}" = "${leader}" ] \
    && log "cluster-follower-kill: leader unchanged (memberId=${leader}) — quorum held" \
    || log "cluster-follower-kill: WARN leader changed (${leader} -> ${still}); quorum still held, progress OK"
  # Killed follower's task restarts and rejoins (3/3).
  assert_count "${CLUSTER_TASK}" 3 "${CHAOS_RESTART_SLO_S}"
  # The restarted member's dirs are INTACT, so it must recover by loading
  # its local latest snapshot — bounded restart time — not by replaying
  # the whole lifetime log. Count-increase, because earlier restarts in
  # this shard may have restored already.
  wait_log_count_gt "${CLUSTER_TASK}" "sealer snapshot RESTORED memberId=${follower}" \
    "${fk_r0}" 180 10 \
    "cluster-follower-kill: restarted member never logged 'sealer snapshot RESTORED' — the snapshot restore path did not run on an intact-dir restart" \
    --stdout-only
  log "cluster-follower-kill: member ${follower} restored from snapshot on restart"
}

case_cluster_member_rejoin() {
  # BLANK-member catch-up drill — the "join mid-way with an empty state"
  # edge for the RAFT MEMBERS themselves. A follower's cluster dir AND
  # archive are wiped after its kill, so the restarted member owns
  # NOTHING. Under Aeron 1.44 STATIC membership a blank member is caught
  # up by replicating and replaying the leader's LOG FROM POSITION 0 —
  # snapshots are NOT transferred to blank members (they bound the
  # restart time of members whose dirs survive; that path is asserted by
  # cluster-follower-kill). Full log replay is deterministic, so the
  # correct outcome here is: a FRESH-at-genesis service start, the whole
  # log replayed (proven via the post-wipe snapshot position reaching the
  # wipe-time head), 3/3 running,
  # pipeline unaffected throughout (quorum 2/3 held). First run of this case
  # proved exactly that (the wiped member replayed to the live head and
  # resumed serving replay sessions). NOTE the cost this documents: a
  # blank member's rejoin time grows with the lifetime log — bounding it
  # needs log purge after snapshot, tracked as audit follow-up.
  local leader follower f0 f1 t head_at_wipe catchup_block
  leader="$(cluster_leader)"
  for follower in 0 1 2; do [ "${follower}" != "${leader}" ] && break; done

  # Baseline BEFORE the wipe (a count, not presence: bring-up also logs a
  # fresh start).
  f0="$(count_log_lines "${CLUSTER_TASK}" "sealer state FRESH at genesis memberId=${follower}" --stdout-only)"
  # Head at wipe time — the position the blank member must replay back to
  # before this case may pass (see the catch-up proof below).
  head_at_wipe="$(executor_progress || echo 0)"

  log "cluster-member-rejoin: leader=memberId=${leader}; killing FOLLOWER memberId=${follower} and WIPING its cluster + archive dirs"
  inject_hard "kardamom-sealer-${follower}" "${CLUSTER_TASK}"
  docker exec "kardamom-sealer-${follower}" bash -lc \
    'rm -rf /opt/kardamom/cluster/* /opt/kardamom/archive/*' \
    || fail "cluster-member-rejoin: could not wipe memberId=${follower} state"

  # Quorum (2/3) holds: the pipeline keeps committing throughout.
  assert_executor_progress
  # The wiped member's task restarts and the job returns to 3/3.
  assert_count "${CLUSTER_TASK}" 3 "${CHAOS_RESTART_SLO_S}"

  # The restarted member must (a) start BLANK — fresh-at-genesis count
  # grew, proving the wipe took and this is genuinely the empty-state
  # path — and (b) finish the full log replay.
  #
  # CATCH-UP PROOF (corrected 2026-08-07). The old proof was "the member
  # logged a NEW 'snapshot TAKEN'", justified as "members snapshot at the
  # same replicated log position, so a post-rejoin TAKEN means it replayed
  # to there". That reasoning is INVERTED, and it made this case vacuous.
  # The SNAPSHOT action is itself an entry in the replicated log, so a
  # blank member replaying from position 0 RE-EXECUTES every historical
  # snapshot action and emits `TAKEN block=30`, `TAKEN block=60`, ... as it
  # goes. Those lines are emitted BECAUSE it is still catching up, not
  # because it finished. Measured: the case passed in 0s while the member
  # was ~10 minutes of replay behind the head.
  #
  # That vacuity had teeth: the suite moved on to the next case with one
  # member minutes from converged. cluster-quorum-loss-recover then kills
  # sealer-1+sealer-2 (hardcoded), so when the wipe had landed on sealer-0
  # the ONLY survivor was the un-caught-up member — and the pipeline stayed
  # flat for the whole remaining replay, blowing that case's SLO. Which
  # member got wiped depends on who was leader here, so it failed roughly
  # every other run: the coin flip behind the rotating-shard flakiness.
  #
  # The real proof is positional: the member's LATEST POST-WIPE snapshot
  # block must reach the live head. Scoped to lines after the last FRESH
  # marker so pre-wipe history cannot satisfy it. Replay measured at ~12.5
  # blocks/s on this host, so CLUSTER_REJOIN_SLO_S covers the log this
  # suite builds — and a member that never converges now fails HERE, where
  # the diagnosis is obvious, instead of as a mystery stall two cases
  # later. (What "never converges" looks like in practice: issue #195, a
  # join wedge in Election.init/awaitLocalSocketsClosed with the member
  # frozen INACTIVE after a fast partial replay.)
  t=0
  while :; do
    f1="$(count_log_lines "${CLUSTER_TASK}" "sealer state FRESH at genesis memberId=${follower}" --stdout-only)"
    # Latest TAKEN block emitted AFTER the most recent FRESH-at-genesis for
    # this member (i.e. from the post-wipe boot only).
    catchup_block="$(cluster_alloc_logs | awk -v m="${follower}" '
        $0 ~ ("FRESH at genesis memberId=" m) { seen = 1; last = "" }
        seen && $0 ~ ("snapshot TAKEN memberId=" m) { last = $0 }
        END { print last }' \
      | grep -oE 'block=[0-9]+' | cut -d= -f2 || true)"
    catchup_block="${catchup_block:-0}"
    # Converged when the replayed position reaches the head observed at
    # wipe time (the head keeps advancing under load; reaching the wipe-time
    # head proves the whole pre-existing log was replayed).
    [ "${f1}" -gt "${f0}" ] && [ "${catchup_block}" -ge "${head_at_wipe}" ] && break
    sleep 10; t=$(( t + 10 ))
    if [ "${t}" -ge "${CLUSTER_REJOIN_SLO_S}" ]; then
      [ "${f1}" -gt "${f0}" ] \
        || fail "cluster-member-rejoin: restarted member did not start blank (fresh-at-genesis count ${f0} -> ${f1}) — the wipe did not take, this run proved nothing about empty-state rejoin"
      fail "cluster-member-rejoin: blank member did not replay to the head within ${t}s (post-wipe snapshot block ${catchup_block}, head at wipe ${head_at_wipe}) — join wedged (issue #195) or replay outran its budget; blank-member catch-up is O(lifetime log) and the log is never purged, see the log-purge follow-up in docs/reviews/2026-08-03-chaos-coverage-audit.md"
    fi
  done
  log "cluster-member-rejoin: memberId=${follower} rejoined blank via full log replay (fresh ${f0}->${f1}, replayed to block ${catchup_block} >= head-at-wipe ${head_at_wipe}, ${t}s); leader now memberId=$(cluster_leader 2>/dev/null || echo '?')"
}

case_cluster_quorum_loss_recover() {
  # Kill TWO WHOLE sealer NODE containers (docker kill the kardamom-sealer-X
  # containers themselves, not the inner task): Nomad on those nodes is gone
  # too, so the inner cluster tasks CANNOT restart there → only 1 member left →
  # Raft quorum (needs 2/3) is LOST. The pipeline MUST stall (no false progress:
  # the executor's block gauge stays flat). Then bring ONE node back (docker
  # start): quorum (2/3) returns, a leader is re-elected, progress RESUMES, and
  # the backlog drains gaplessly (load verdict PASS). Generous SLOs: a node
  # restart re-pulls images, so use CHAOS_RESCHEDULE_SLO_S for the rejoin.
  local victims=(kardamom-sealer-1 kardamom-sealer-2)
  log "cluster-quorum-loss-recover: docker kill TWO sealer nodes (${victims[*]}) → quorum lost (1/3 up)"
  docker kill "${victims[@]}" >/dev/null || fail "could not kill sealer nodes ${victims[*]}"
  # Quorum lost → the pipeline must STALL (no commits, executor gauge flat).
  assert_executor_stalled 15
  log "cluster-quorum-loss-recover: docker start ${victims[0]} (quorum 2/3 returns)"
  docker start "${victims[0]}" >/dev/null || fail "could not restart node ${victims[0]}"
  # Its cluster task reschedules + rejoins → quorum restored. Give it the
  # reschedule SLO (image re-pull). count_running counts the `cluster` allocs.
  assert_count "${CLUSTER_TASK}" 2 "${CHAOS_RESCHEDULE_SLO_S}"
  # With quorum back, the executor must resume applying blocks (drains backlog).
  # WIDE timeout — this is the one case where the clients' cluster SESSIONS
  # die (a >15s total outage exceeds the session timeout; leader-kill keeps
  # sessions alive via NewLeaderEvent redirect). Observed on CI: re-election
  # + client session re-establishment alone takes ~50s after the node
  # restart, and the reopened sessions then replay the canonical stream from
  # the log before NEW commits surface on the executor's block gauge. 60s
  # timed out reproducibly (3/3 runs, sessions reopening ~40s in); 180s
  # covers re-election + reconnect + replay with margin.
  assert_executor_progress 180
  # Restore the second node too so the suite leaves a healthy 3/3 cluster for
  # any subsequent cases (best-effort; not asserted as part of this case's SLO).
  docker start "${victims[1]}" >/dev/null 2>&1 || true
}
