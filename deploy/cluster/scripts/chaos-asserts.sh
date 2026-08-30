# shellcheck shell=bash
# =============================================================================
# chaos-asserts.sh — injectors, evidence sources and assertions for chaos.sh.
# =============================================================================
# This file is sourced into chaos.sh's shell, never run as a child
# process. The injectors write the KILLED_* globals that assert_count
# reads, and fail() must abort the one suite process. This file must
# not install traps; chaos.sh owns the single EXIT trap. It needs
# lib.sh (on_control, count_running, log, fail), lib-topology.sh, and
# chaos-probes.sh.

# --- injectors --------------------------------------------------------------

# The injectors record what they killed: the stopped alloc id, or the
# killed inner container id and where. This lets assert_count require
# observed replacement, not just a running count. Right after a kill,
# the doomed alloc can still report ClientStatus==running on the first
# poll. So a bare count>=N check could pass instantly against the old
# alloc, without ever observing the outage, a vacuous "recovered within
# SLO". assert_count reads and clears these.
KILLED_ALLOC=""
KILLED_NODE=""; KILLED_TASK=""; KILLED_CID=""

inject_graceful() { # <job>
  local alloc; alloc="$(running_alloc "$1")"
  [ -n "${alloc}" ] || fail "no running alloc to stop for job $1"
  log "graceful: nomad alloc stop ${alloc} (job $1)"
  on_control 'nomad alloc stop "$1"' "${alloc}" >/dev/null
  KILLED_ALLOC="${alloc}"
}

inject_graceful_group() { # <job> <task-group>
  # The `|| true` here is load-bearing, the same doctrine as
  # val_metric. Under `set -eo pipefail`, a non-zero docker exec would
  # turn this assignment into a silent exit, with no CHAOS FAIL line.
  # awk reads to EOF on purpose. An early `exit` would close the pipe
  # under the writer and fail it with EPIPE.
  local alloc
  alloc="$(on_control 'nomad job allocs -t "{{range .}}{{.ID}} {{.TaskGroup}} {{.ClientStatus}}{{\"\n\"}}{{end}}" "$1"' "$1" 2>/dev/null \
    | awk -v g="$2" '$2==g && $3=="running" && !found {found=$1} END {if (found) print found}' || true)"
  [ -n "${alloc}" ] || fail "no running $2 alloc to stop for job $1"
  log "graceful: nomad alloc stop ${alloc} (job $1, group $2)"
  on_control 'nomad alloc stop "$1"' "${alloc}" >/dev/null
  KILLED_ALLOC="${alloc}"
}

inject_hard() { # <node-container(s), space-separated candidates> <task-name>
  # Multiple candidates cover tasks whose placement can move between
  # cases. For example, after graceful-executor, the replacement
  # alloc's container is not always back on executor-0 when
  # hard-executor probes it. This kills the first node actually
  # running the task. If no node is running it, that is still a loud
  # failure, never a vacuous pass.
  local node cid=""
  for node in $1; do
    cid="$(docker exec "${node}" sh -c 'docker ps --filter name='"$2"' -q | head -1' 2>/dev/null)"
    [ -n "${cid}" ] && break
  done
  [ -n "${cid}" ] || fail "no running ${2} container to hard-kill on any of: ${1}"
  log "hard: docker kill inner ${2} container ${cid} on ${node}"
  docker exec "${node}" docker kill "${cid}" >/dev/null \
    || fail "could not hard-kill ${2} (${cid}) on ${node}"
  KILLED_NODE="${node}"; KILLED_TASK="$2"; KILLED_CID="${cid}"
}

kill_node() { # <node-container(s)> — docker kill that trusts container state over the exit event
  docker kill "$@" >/dev/null 2>&1 && return 0
  # On a thrashed host, docker kill can report "did not receive an
  # exit event" after the SIGKILL already landed. dockerd's wait for
  # the containerd exit event times out while the victim still dies
  # with exit 137. This has happened for real: both victims' FinishedAt
  # landed within a second of the kill, but docker still reported an
  # error, and the case failed with the victims already dead. Judge the
  # kill by container state, not by the event. Only a victim genuinely
  # still running after a grace window is a failure.
  local n t
  for n in "$@"; do
    t=0
    while [ "$(docker inspect -f '{{.State.Running}}' "${n}" 2>/dev/null || echo unknown)" != "false" ]; do
      if [ "${t}" -ge 30 ]; then fail "could not kill node ${n} (still running ${t}s after docker kill)"; fi
      sleep 2; t=$(( t + 2 ))
    done
    log "kill_node: ${n} — exit event was late but the kill took (state=exited)"
  done
}

# --- alloc-log evidence sources ---------------------------------------------

# Get the concatenated logs of every alloc of a Nomad job, running or
# not. This is the evidence source for lines that straddle a task
# restart. Nomad's docker driver garbage-collects the dead container on
# an in-place restart, so `docker logs` on the node silently loses
# everything the dying generation said. On an early retention-overrun
# run, the sealer provably refused the replay and the executor provably
# self-repaired, but the refusal, fetch, and park lines lived in the
# reaped container, and the case read "never happened". Nomad's own
# alloc log files persist across restarts of the same alloc, so
# pre-kill and post-rejoin lines land in the same stream. Callers
# assert on counts increasing, not mere presence.
#   $2 = --stdout-only: skip the stderr stream. The cluster job's role
#        and snapshot lines print to stdout; its stderr is JVM noise.
job_alloc_logs() { # <nomad-job> [--stdout-only]
  local job="$1" flag="${2:-}" allocs alloc
  allocs="$(on_control 'nomad job allocs -t "{{range .}}{{.ID}}{{\"\n\"}}{{end}}" "$1"' "${job}" 2>/dev/null || true)"
  while read -r alloc; do
    [ -n "${alloc}" ] || continue
    if [ "${flag}" = "--stdout-only" ]; then
      on_control 'nomad alloc logs "$1" 2>/dev/null' "${alloc}" 2>/dev/null || true
    else
      on_control 'nomad alloc logs "$1" 2>/dev/null; nomad alloc logs -stderr "$1" 2>/dev/null' \
        "${alloc}" 2>/dev/null || true
    fi
  done <<<"${allocs}"
}

# Get the concatenated stdout of every cluster alloc, the uniform
# evidence source for snapshot, restore, and role lines. This used to
# be a near-copy of job_alloc_logs; now it is a flag.
cluster_alloc_logs() { job_alloc_logs "${CLUSTER_TASK}" --stdout-only; }

# Count alloc-log lines containing a fixed needle. `grep -c` reads all
# input, with no early exit, so no SIGPIPE under pipefail, unlike
# `grep -q`. `|| true` keeps a zero-match exit status from killing the
# suite.
count_log_lines() { # <job> <needle> [--stdout-only] -> count
  job_alloc_logs "$1" "${3:-}" | grep -c -- "$2" || true
}

# Poll until the job's alloc-log count of <needle> exceeds <baseline>.
# This checks counts, not presence, since bring-up and earlier cases
# also log the same lines. Calls fail() on timeout. Call this function
# directly, never inside $(...): a fail() in a command substitution
# only exits the subshell, and the suite would sail on past a dead
# assert.
wait_log_count_gt() { # <job> <needle> <baseline> <timeout-s> <interval-s> <fail-msg> [--stdout-only]
  local job="$1" needle="$2" base="$3" timeout_s="$4" interval="$5" msg="$6" flag="${7:-}"
  local t=0 now=0
  while :; do
    now="$(count_log_lines "${job}" "${needle}" "${flag}")"; now="${now:-0}"
    if [ "${now}" -gt "${base}" ]; then
      log "alloc-log count for '${needle}' (job ${job}) advanced ${base} -> ${now} after ${t}s"
      return 0
    fi
    sleep "${interval}"; t=$(( t + interval ))
    [ "${t}" -ge "${timeout_s}" ] && fail "${msg} (log count ${base} -> ${now} over ${t}s)"
  done
}

# Wait for a checkpoint to exist on a peer or donor node. This is the
# repeated checkpoint-donor gate. Recovery-D fetches from a peer at or
# above the post-freeze floor. Donors checkpoint every 20s while live,
# but only once the chain is moving. Waiting here, instead of failing
# later with a misleading "resync did not complete", names the real
# precondition.
wait_peer_checkpoint() { # <node> <case-context>
  local node="$1" ctx="${2:-checkpoint-wait}" _i
  log "${ctx}: waiting for a checkpoint on ${node}"
  for _i in $(seq 1 15); do
    docker exec "${node}" bash -lc \
      'ls /opt/kardamom/checkpoints/checkpoint-* >/dev/null 2>&1' && return 0
    sleep 5
  done
  docker exec "${node}" bash -lc \
    'ls /opt/kardamom/checkpoints/checkpoint-* >/dev/null 2>&1' \
    || fail "${ctx}: ${node} produced no checkpoint"
}

# --- cluster leadership detection -------------------------------------------

# Return the memberId (0, 1, or 2) of the current Raft leader. This
# greps the last `cluster role=LEADER memberId=N` line across the 3
# `cluster` allocs' stdout. SealerClusteredService.onRoleChange() prints
# that line to stdout on every role transition, deliberately to stdout
# and not slf4j, so it is easy to grep. So the last such line in an
# alloc's log is that member's most recent role, and the member whose
# last role line is LEADER is the current leader. memberId N equals
# kardamom-sealer-N.
#
# This tolerates election windows with a bounded retry loop ($1 = max
# seconds, default CHAOS_LEADER_SLO_S). Right after a kill, there may
# briefly be no leader line yet. Prints the leader memberId on stdout,
# and nothing else. Fails if none is found in time. Reads logs through
# the control node's `nomad alloc logs`, the same access pattern the
# rest of the suite uses.
cluster_leader() { # [max-secs]
  local max="${1:-${CHAOS_LEADER_SLO_S}}" t=0 allocs alloc lastrole leader
  while :; do
    leader=""
    # All cluster allocs, running or not. A just-killed member's log
    # may still show its last role line, but this only trusts a
    # member whose last line is LEADER.
    allocs="$(on_control 'nomad job allocs -t "{{range .}}{{.ID}}{{\"\n\"}}{{end}}" "$1"' "${CLUSTER_TASK}" 2>/dev/null || true)"
    while read -r alloc; do
      [ -n "${alloc}" ] || continue
      # Last role line in this alloc's stdout. Empty if it never logged a role.
      lastrole="$(on_control 'nomad alloc logs "$1" 2>/dev/null | grep -E "cluster role=[A-Z]+ memberId=[0-9]+" | tail -1' "${alloc}" 2>/dev/null || true)"
      case "${lastrole}" in
        *role=LEADER*)
          leader="$(printf '%s' "${lastrole}" | sed -nE 's/.*memberId=([0-9]+).*/\1/p')"
          ;;
      esac
    done <<<"${allocs}"
    [ -n "${leader}" ] && { printf '%s' "${leader}"; return 0; }
    sleep 3; t=$((t+3))
    [ "${t}" -ge "${max}" ] && fail "no cluster leader observed within ${max}s (checked alloc logs for 'role=LEADER memberId=')"
  done
}

# --- assertions -------------------------------------------------------------

assert_count() { # <job> <min-running> <slo-secs>
  # Besides count>=N, require evidence the recovery actually happened,
  # when the killed target is known. A gracefully stopped alloc must
  # be replaced by a different running alloc id. A hard-killed inner
  # task container must be running again under a new container id.
  # Nomad restarts the task in place, so the alloc id survives, but a
  # docker restart always creates a new container id.
  local killed_alloc="${KILLED_ALLOC}" killed_node="${KILLED_NODE}"
  local killed_task="${KILLED_TASK}" killed_cid="${KILLED_CID}"
  KILLED_ALLOC=""; KILLED_NODE=""; KILLED_TASK=""; KILLED_CID=""
  local t=0
  while :; do
    if [ "$(count_running "$1")" -ge "$2" ]; then
      if [ -n "${killed_alloc}" ]; then
        # Replacement leg: >=N running allocs, not counting the stopped
        # one.
        if [ "$(running_allocs "$1" | grep -cv "^${killed_alloc}\$")" -ge "$2" ]; then
          log "$1 has >= $2 running alloc(s) after ${t}s (stopped alloc ${killed_alloc} replaced)"
          return 0
        fi
      elif [ -n "${killed_cid}" ]; then
        # Restart leg: same task name running again under a new container id.
        local cid_now
        cid_now="$(docker exec "${killed_node}" sh -c 'docker ps --filter name='"${killed_task}"' -q | head -1' 2>/dev/null || true)"
        if [ -n "${cid_now}" ] && [ "${cid_now}" != "${killed_cid}" ]; then
          log "$1 has >= $2 running alloc(s) after ${t}s (${killed_task} restarted on ${killed_node}: ${killed_cid} -> ${cid_now})"
          return 0
        fi
      else
        log "$1 has >= $2 running alloc(s) after ${t}s"
        return 0
      fi
    fi
    sleep 3; t=$((t+3))
    [ "${t}" -ge "$3" ] && fail "$1 did not reach >= $2 running (with the killed alloc/task replaced) within $3s (have $(count_running "$1"))"
  done
}

assert_progress() {
  # This is the pipeline-progress probe for the component-chaos
  # cases. Prefer the sealer's boundary counter, re-exported by the
  # executors, when reachable. It ticks on every boundary, about 4
  # times a second, even with no load. If no executor exporter
  # answers, fall back to the executor's committed-block gauge, the
  # same signal the cluster cases use. This function keeps its name so
  # the ported component-chaos cases need no edits.
  local b0 b1
  b0="$(sealer_boundaries || true)"
  if [ -n "${b0}" ]; then
    sleep 10
    b1="$(sealer_boundaries || true)"; b1="${b1:-0}"
    awk "BEGIN{exit !(${b1}>${b0})}" \
      && log "pipeline progressing (sealer boundaries ${b0} -> ${b1})" \
      || fail "pipeline NOT progressing after recovery (sealer boundaries ${b0} -> ${b1})"
    return 0
  fi
  # No executor exporter answered: poll the executor block gauge instead.
  assert_executor_progress "$@"
}

# This is the cluster-mode analogue of assert_progress: the executor's
# committed-block gauge must advance. It polls until progress shows or
# a timeout hits. Recovery is not instant after a leader kill: the
# cluster client must redirect to the newly elected leader and resume
# egress before the executor applies the next block. So a single fixed
# window is flaky. This succeeds as soon as progress is observed. $1 =
# timeout in seconds (default 60).
assert_executor_progress() {
  local timeout="${1:-60}" e0 e1 t=0
  e0="$(executor_progress || true)"; e0="${e0:-0}"
  while :; do
    sleep 5; t=$((t + 5))
    e1="$(executor_progress || true)"; e1="${e1:-0}"
    awk "BEGIN{exit !(${e1}>${e0})}" \
      && { log "pipeline progressing (executor block ${e0} -> ${e1} after ${t}s)"; return 0; }
    [ "${t}" -ge "${timeout}" ] \
      && fail "pipeline NOT progressing (executor block ${e0} -> ${e1} over ${timeout}s)"
  done
}

# This is the per-replica recovery verdict. Every executor must
# individually converge to the fleet head before a case may pass. The
# max-across-replicas progress probe above keeps transient scrape gaps
# from failing a healthy run, but it also masks a replica that never
# recovered, wedged or crash-looping behind the fleet. The suite once
# stayed green for months while every restarted executor was
# permanently broken, because one untouched replica carried every
# probe. A resilience suite that cannot see two-thirds of the fleet
# dead proves nothing. So every case now ends with this convergence
# check: within EXEC_CONVERGE_SLO_S, every executor's own gauge must be
# scrapeable and within EXEC_CONVERGE_LAG blocks of the fleet head.
EXEC_CONVERGE_SLO_S="${EXEC_CONVERGE_SLO_S:-150}"
EXEC_CONVERGE_LAG="${EXEC_CONVERGE_LAG:-50}"
assert_executors_converged() { # <case>
  local case_name="$1" t=0 i v max ok bad lag
  local -a blk
  while :; do
    max=""; bad=""; blk=()
    for i in "${!EXECUTOR_NODES[@]}"; do
      # `|| true` is load-bearing here, the same class as val_metric's.
      # Under `set -euo pipefail`, a failed scrape, for example the
      # just-returned node's exporter not up yet, would otherwise kill
      # the whole script silently, with no fail() message, since
      # pipefail makes the assignment itself fail.
      v="$(prom_value "$(exec_metrics "${i}" || true)" "${EXECUTOR_BLOCK_METRIC}" first \
        || true)"
      if [ -n "${v}" ]; then
        blk[i]="${v}"
        if [ -z "${max}" ] || [ "${v}" -gt "${max}" ]; then max="${v}"; fi
      else
        bad="${bad} ${EXECUTOR_NODES[$i]}=unreachable"
      fi
    done
    if [ -z "${bad}" ] && [ -n "${max}" ]; then
      ok=1
      for i in "${!EXECUTOR_NODES[@]}"; do
        lag=$(( max - blk[i] ))
        [ "${lag}" -le "${EXEC_CONVERGE_LAG}" ] \
          || { ok=0; bad="${bad} ${EXECUTOR_NODES[$i]}=lag:${lag}"; }
      done
      if [ "${ok}" -eq 1 ]; then
        log "${case_name}: all ${#EXECUTOR_NODES[@]} executors converged (head ${max}, per-replica lag <= ${EXEC_CONVERGE_LAG}) after ${t}s"
        return 0
      fi
    fi
    [ "${t}" -ge "${EXEC_CONVERGE_SLO_S}" ] \
      && fail "${case_name}: executor fleet NOT fully recovered within ${EXEC_CONVERGE_SLO_S}s (head=${max:-?};${bad# })"
    sleep 5; t=$(( t + 5 ))
  done
}

# A bare `assert_count ingress 2`, after the killed-markers are
# consumed, passes even if nothing ever died. Require both ingress
# replicas' exporters to actually answer, real liveness, not a nomad
# row count.
assert_ingress_pair_live() { # <case>
  local t=0 n v ok
  while :; do
    ok=1
    for n in "${INGRESS_NODES[@]}"; do
      v="$(fetch_metrics '' "${n}" "${INGRESS_PORT}" 2>/dev/null | head -1 || true)"
      [ -n "${v}" ] || { ok=0; break; }
    done
    [ "${ok}" -eq 1 ] && { log "$1: both ingress exporters live"; return 0; }
    sleep 5; t=$(( t + 5 ))
    [ "${t}" -ge 120 ] && fail "$1: ingress replica ${n} exporter dark after ${t}s (pair not fully recovered)"
  done
}

# This is the cluster-mode "must not progress" check. The executor's
# block gauge must stay flat over a window: quorum lost means no new
# commits, so there must be no false progress. $1 = window.
assert_executor_stalled() {
  # Both samples must be real scrapes. Defaulting to 0 made this
  # pass vacuously (0 <= 0), exactly when a two-node kill blacked out
  # the exporters, the one situation the quorum-loss case exists to
  # observe. The executors themselves survive a sealer-quorum loss, so
  # an unreachable gauge here is a harness failure, not an expected
  # outage. Retry, then fail loudly.
  local window="${1:-15}" e0="" e1="" i
  for i in 1 2 3 4 5; do
    e0="$(executor_progress || true)"; [ -n "${e0}" ] && break; sleep 3
  done
  [ -n "${e0}" ] || fail "stall assert: no executor gauge scrapeable BEFORE the window — cannot observe the stall (scrape-failure-as-zero would pass vacuously)"
  sleep "${window}"
  for i in 1 2 3 4 5; do
    e1="$(executor_progress || true)"; [ -n "${e1}" ] && break; sleep 3
  done
  [ -n "${e1}" ] || fail "stall assert: no executor gauge scrapeable AFTER the window — cannot observe the stall"
  awk "BEGIN{exit !(${e1}<=${e0})}" \
    && log "pipeline correctly STALLED (executor block ${e0} -> ${e1} over ${window}s, no false progress)" \
    || fail "pipeline UNEXPECTEDLY progressed while quorum lost (executor block ${e0} -> ${e1})"
}

assert_load_pass() { # <json> <case>
  if grep -q '"pass": true' "$1" 2>/dev/null; then
    log "load verdict PASS for case $2"
  else
    echo "----- load report ($2) [$1] -----" >&2
    if [ -s "$1" ]; then
      sed -n '1,80p' "$1" >&2
    else
      echo "(report JSON missing/empty — kardamom-load did not finish writing it)" >&2
    fi
    echo "----- kardamom-load stdout/stderr (/tmp/chaos-$2.load.log) -----" >&2
    tail -n 80 "/tmp/chaos-$2.load.log" 2>/dev/null >&2 || echo "(no load log)" >&2
    fail "load verdict not PASS for case $2"
  fi
}

# This is a relaxed load verdict for the total-outage case
# (cluster-quorum-loss-recover). A full quorum loss fairly stops the
# sequencer from accepting some txs offered during the outage. They
# surface as seq_dropped or past-nonce, and were never accepted, so
# they are not a delivery gap. The guarantee that must still hold is
# gapless delivery of every accepted tx (missing == 0), which the
# cluster meets on recovery. So this asserts missing==0, instead of the
# strict all-delivered verdict.
assert_accepted_delivered() { # <json> <case>
  if grep -qE '"missing":[[:space:]]*0[[:space:]]*,' "$1" 2>/dev/null; then
    log "load OK for case $2: every ACCEPTED tx receipted (missing=0); seq_dropped tolerated during total outage"
  else
    echo "----- load report ($2) [$1] -----" >&2
    [ -s "$1" ] && sed -n '1,80p' "$1" >&2 || echo "(report JSON missing/empty)" >&2
    fail "accepted txs NOT all delivered (missing>0) for case $2"
  fi
}

# This is the restarted-replica health probe. Known limitation: a
# restarted replica hydrates nonce floors from an
# empty state database. So for established senders, it buffers refs as
# "future" and publishes nothing until a global hydration signal
# exists. The shard runs at P=1 for those senders; the racing twin
# covers them. Fresh nonce-0 senders hydrate at floor 0 and are covered
# right away. A nonce-floor fast-forward once made a rejoiner republish
# for established senders, but it was reverted: under overload, it
# forged canonical nonce gaps (nonces dropped before tx_data are
# invisible to both replicas), and crashed every executor. See
# docs/reviews/2026-07-17-30-commit-review/fixes-CI-replay-loop.md. So
# this probe asserts what is guaranteed: the restarted replica is alive
# and scrapeable, with its exporter up and session established, not
# that it republishes for the pinned established-sender load.
assert_replica_healthy() { # <node-container> <node-ip> <metrics-port> [slo-secs]
  local node="$1" ip="$2" port="$3" slo="${4:-90}" t=0 v
  while :; do
    # Try the bridge IP directly first; exporters bind 0.0.0.0 since
    # the replica-metrics fix. Fall back to docker exec, the same
    # reasoning as exec_metrics().
    v="$(fetch_metrics "${ip}" "${node}" "${port}" 2>/dev/null \
        | awk '/^kardamom_sequencer_/{ n++ } END { printf "%d", n }' || true)"
    if [ -n "${v}" ] && [ "${v}" -gt 0 ]; then
      log "restarted replica on ${node} (:${port}) is up and exporting (${v} sequencer metrics; established-sender coverage stays on the twin — re-opened F02.1)"
      return 0
    fi
    [ "${t}" -ge "${slo}" ] \
      && fail "restarted replica on ${node} (:${port}) never came up: metrics unscrapable within ${slo}s of restart"
    sleep 5; t=$(( t + 5 ))
  done
}
