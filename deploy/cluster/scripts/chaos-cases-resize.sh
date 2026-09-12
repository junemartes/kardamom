# shellcheck shell=bash
# =============================================================================
# chaos-cases-resize.sh — dynamic sequencer sizing cases (milestone 7 of
# docs/specs/dynamic-sequencer-sizing.md).
# =============================================================================
# This file is sourced into chaos.sh's shell, never run as a child
# process. It must not install traps; chaos.sh owns the single EXIT
# trap. run_case (in chaos.sh) provides the load and the injection
# scaffolding, and the post-case common asserts.
#
# Both cases run LAST in their shard. The resize leaves the shard map
# at a later version, so ACCT_SHARD no longer pins the cases after it.

# --- probes ------------------------------------------------------------------

# Running allocs of one task group of a job.
count_running_group() { # <job> <task-group>
  on_control 'nomad job allocs -t "{{range .}}{{.TaskGroup}} {{.ClientStatus}}{{\"\n\"}}{{end}}" "$1"' "$1" 2>/dev/null \
    | awk -v g="$2" '$1==g && $2=="running" {n++} END {printf "%d", n}' || printf "0"
}

# Sum of a labeled counter on one sequencer replica, filtered by a label
# fragment such as `outcome="ok"`.
seq_metric_where() { # <ip> <node> <port> <metric> <label-fragment> -> integer (empty on scrape failure)
  local body
  body="$(fetch_metrics "$1" "$2" "$3" || true)"
  [ -n "${body}" ] || return 0
  printf '%s\n' "${body}" | awk -v m="$4" -v f="$5" \
    '$0 ~ "^"m"[{ ]" && $0 !~ /^#/ && index($0, f) > 0 { s += $NF; n++ } END { if (n) printf "%d", s; else printf "0" }'
}

# The installed shard map version on one ingress replica. The ingress
# exporter binds loopback, so this goes through the node container.
ingress_map_version() { # <ingress-node-container>
  local body
  body="$(fetch_metrics "" "$1" 9006 || true)"
  prom_value "${body}" kardamom_ingress_shard_map_version
}

wait_until() { # <what> <timeout-s> <cmd...> — the command's stdout must equal "ok"
  local what="$1" timeout="$2"; shift 2
  local t=0
  while :; do
    [ "$("$@" 2>/dev/null || true)" = "ok" ] && return 0
    [ "${t}" -ge "${timeout}" ] && fail "${what}: not reached within ${timeout}s"
    sleep 3; t=$(( t + 3 ))
  done
}

# True when funded account <n> moves to a new lane under the next map
# (the fewest-moves render of scripts/render-shard-map.py). run_case
# uses this to pin the resize case's load to a moved sender.
acct_moves_on_scale_out() { # <acct-index> <target-lanes>
  local vslot="${ACCT_VSLOT[$1]}"
  python3 - "${SCRIPT_DIR}" "$2" "${vslot}" <<'PY'
import re, subprocess, sys
scripts, target, vslot = sys.argv[1], sys.argv[2], int(sys.argv[3])
cur = f"{scripts}/../config/shard-map.toml"
out = subprocess.run([sys.executable, f"{scripts}/render-shard-map.py", "--from", cur, "--lanes", target],
                     capture_output=True, text=True, check=True).stdout
new = [int(x) for x in re.findall(r"\d+", re.search(r"table\s*=\s*\[([^\]]*)\]", out, re.S).group(1))]
old = [int(x) for x in re.findall(r"\d+", re.search(r"table\s*=\s*\[([^\]]*)\]", open(cur).read(), re.S).group(1))]
sys.exit(0 if new[vslot] != old[vslot] else 1)
PY
}

# --- cases ------------------------------------------------------------------------

# resize-scale-out-in: scripts/scale-sequencers.sh from 2 to 3 lanes
# under load, with one replica of the new lane hard-killed during the
# overlap (its twin covers), then from 3 back to 2. The case's load is a
# sender whose vslot moves to the new lane (the account selection in
# run_case), with a wide submit retry so it rides the ingress roll. The
# load verdict checks gapless delivery of every accepted transaction
# for that sender: the moved-sender nonce-gap assertion of the spec.
case_resize_scale_out_in() {
  local cluster_dir="${SCRIPT_DIR}/.."
  local out_log="/tmp/chaos-resize-out.log" in_log="/tmp/chaos-resize-in.log"
  [ "$(ingress_map_version kardamom-ingress-0)" = "0" ] \
    || fail "resize: ingress-0 does not run map version 0 before the case"

  # --- scale out, in the background, so the overlap can be attacked. ---
  ( cd "${cluster_dir}" && ./scripts/scale-sequencers.sh 3 ) >"${out_log}" 2>&1 &
  local scale_pid=$!
  local t=0
  while [ "$(count_running_group sequencer seq-2)" -lt 2 ]; do
    kill -0 "${scale_pid}" 2>/dev/null \
      || fail "resize: scale-out exited before lane 2 ran: $(tail -n 30 "${out_log}")"
    [ "${t}" -ge 300 ] && fail "resize: lane 2 did not reach 2 running replicas within 300s"
    sleep 3; t=$(( t + 3 ))
  done
  log "resize: lane 2 runs in shadow mode; hard-killing one of its replicas during the overlap"
  inject_hard "kardamom-sequencer-0 kardamom-sequencer-1" sequencer-2
  assert_count sequencer 6 "${CHAOS_RESTART_SLO_S}"
  wait "${scale_pid}" || fail "resize: scale-out failed: $(tail -n 40 "${out_log}")"
  log "resize: scale-out 2 -> 3 done ($(grep -c '==>' "${out_log}") steps)"
  wait_until "ingress-0 on map version 1" 60 ingress_map_version_is kardamom-ingress-0 1
  wait_until "ingress-1 on map version 1" 60 ingress_map_version_is kardamom-ingress-1 1
  assert_progress

  # --- scale in. -----------------------------------------------------------
  ( cd "${cluster_dir}" && ./scripts/scale-sequencers.sh 2 ) >"${in_log}" 2>&1 \
    || fail "resize: scale-in failed: $(tail -n 40 "${in_log}")"
  log "resize: scale-in 3 -> 2 done"
  wait_until "lane 2 group stopped" 120 group_is_gone sequencer seq-2
  assert_count sequencer 4 "${CHAOS_RESTART_SLO_S}"
  wait_until "ingress-0 on map version 2" 60 ingress_map_version_is kardamom-ingress-0 2
  assert_progress
}
ingress_map_version_is() { [ "$(ingress_map_version "$1")" = "$2" ] && echo ok; }
group_is_gone() { [ "$(count_running_group "$1" "$2")" = "0" ] && echo ok; }

# lookup-blackout: every executor is unreachable while a cold replica
# asks for a sender's committed nonce. From the sequencer node, the
# executor addresses are blackholed. Lane 0's replica on node-0 is
# hard-killed and comes back cold; its lookups for the pinned sender
# fail (the failure outcomes count), the twin keeps the lane live, and
# the log stays correct. Then the routes return, and the replica is
# killed once more: the newborn's first park is answered by the lookup
# (the ok outcome counts), the parked transaction drains, and the lane
# makes progress.
case_lookup_blackout() {
  local node="kardamom-sequencer-0" ip="192.168.56.21" port=9001
  local executors="192.168.56.41 192.168.56.42 192.168.56.43"
  local e
  # Both phases hard-kill the replica, and the replacement is a new
  # process whose counters start at zero. So the baseline for each wait
  # is zero, not the killed process's count. A baseline read from the
  # old process fails the wait when it is 1 or more: an answered lookup
  # is a one-off (the floor is known after it), so the newborn's "ok"
  # count stays at exactly one and never rises past the old count.
  #
  # The twin stays up in both phases. The newborn's first park asks an
  # executor at once, because the twin's receipts take longer than the
  # first ingress envelope to arrive (measured: every park in the first
  # seconds requests a lookup; no receipt sets the floor in that window).
  for e in ${executors}; do
    docker exec "${node}" ip route add blackhole "${e}/32" \
      || fail "lookup-blackout: could not blackhole ${e} on ${node}"
  done
  log "lookup-blackout: executors blackholed from ${node}; hard-killing lane 0's replica there"
  # The restore must happen even when an assert fails. chaos.sh owns the
  # one EXIT trap, so this case restores on its own error path. The
  # asserts run in a subshell: `fail` exits the shell it runs in, so a
  # subshell turns that exit into a non-zero status here, and the
  # snapshot and the restore still run.
  restore_routes() { local x; for x in ${executors}; do docker exec "${node}" ip route del blackhole "${x}/32" 2>/dev/null || true; done; }
  local failed=0
  (
    inject_hard "${node}" sequencer-0
    assert_progress
    assert_count sequencer 4 "${CHAOS_RESTART_SLO_S}"
    wait_until "cold replica's lookups failed with no executor reachable" 120 \
      lookups_failed_past "${ip}" "${node}" "${port}" 0
  ) || failed=1
  lookup_snapshot "${ip}" "${node}" "${port}" "lookup-blackout: blackout phase"
  restore_routes
  [ "${failed}" = "0" ] || fail "lookup-blackout: blackout phase failed"
  log "lookup-blackout: routes restored; hard-killing the replica again for an answered lookup"
  (
    inject_hard "${node}" sequencer-0
    assert_count sequencer 4 "${CHAOS_RESTART_SLO_S}"
    wait_until "cold replica's lookup answered" 120 lookups_ok_past "${ip}" "${node}" "${port}" 0
  ) || failed=1
  lookup_snapshot "${ip}" "${node}" "${port}" "lookup-blackout: answered phase"
  if [ "${failed}" != "0" ]; then
    ingress_snapshot "lookup-blackout: answered phase"
    replica_log_tail "${node}" sequencer-0 "lookup-blackout: answered phase"
    load_tail "lookup-blackout: answered phase"
    fail "lookup-blackout: answered-lookup phase failed"
  fi
  assert_progress
}
# One log line of a replica's nonce and lookup counters. The waits above
# watch one counter. On a red run, this line tells the cases apart: no
# park at all (tx_ingested and tx_buffered_future stay at zero), a park
# with the floor already known (requests stay at zero), or a lookup that
# ran (requests and lookups rise). Never fails the case.
lookup_snapshot() { # <ip> <node> <port> <ctx>
  local body
  body="$(fetch_metrics "$1" "$2" "$3" || true)"
  [ -n "${body}" ] || { log "$4: no metrics from $2:$3"; return 0; }
  log "$4: $(printf '%s\n' "${body}" | awk '
    /^kardamom_sequencer_(tx_ingested|tx_buffered_future|tx_dropped_past|tx_published_to_b|pending_evictions|pending_expired|nonce_lookup_requests|nonce_lookups|wrong_shard_dropped|receipt_floor_advances)_total/ ||
    /^kardamom_sequencer_(nonce_lookups_in_flight|pending_depth|receipt_floor_senders)[{ ]/ {
      sub(/^kardamom_sequencer_/, "", $1); printf "%s=%s ", $1, $NF }')"
}
# One log line per ingress replica: received, accepted, and rejected by
# reason. A stalled lane shows up here as timeouts or as
# partition-unavailable rejects. Never fails the case.
ingress_snapshot() { # <ctx>
  local n body
  for n in "${INGRESS_NODES[@]}"; do
    body="$(fetch_metrics '' "${n}" "${INGRESS_PORT}" || true)"
    log "$1: ${n}: $(printf '%s\n' "${body}" | awk '
      /^kardamom_ingress_(tx_received|tx_accepted|tx_rejected)_total/ {
        sub(/^kardamom_ingress_/, "", $1); printf "%s=%s ", $1, $NF }')"
  done
}
# The tail of a replica's own log, through the node's docker. The
# failure dump keeps only a short tail per allocation, and the newborn's
# lines can fall out of it. Never fails the case.
replica_log_tail() { # <node> <task-prefix> <ctx>
  local inner
  inner="$(inner_container "$1" "$2")"
  [ -n "${inner}" ] || { log "$3: no inner $2 container on $1"; return 0; }
  log "$3: log tail of ${inner} on $1"
  timeout 20 docker exec "$1" docker logs --tail 40 "${inner}" 2>&1 || true
}
# The tail of the background load's log. `logf` is run_case's local,
# visible here through bash's dynamic scope. Silent when unset.
load_tail() { # <ctx>
  [ -n "${logf:-}" ] && [ -r "${logf}" ] || return 0
  log "$1: kardamom-load log tail (${logf})"
  tail -n 40 "${logf}"
}
lookups_failed_past() { # <ip> <node> <port> <baseline>
  local t o
  t="$(seq_metric_where "$1" "$2" "$3" kardamom_sequencer_nonce_lookups_total 'outcome="timeout"' || echo 0)"
  o="$(seq_metric_where "$1" "$2" "$3" kardamom_sequencer_nonce_lookups_total 'outcome="error"' || echo 0)"
  [ $(( ${t:-0} + ${o:-0} )) -gt "$4" ] && echo ok
}
lookups_ok_past() { # <ip> <node> <port> <baseline>
  local o
  o="$(seq_metric_where "$1" "$2" "$3" kardamom_sequencer_nonce_lookups_total 'outcome="ok"' || echo 0)"
  [ "${o:-0}" -gt "$4" ] && echo ok
}
