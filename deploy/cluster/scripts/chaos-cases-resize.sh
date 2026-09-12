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
# the log stays correct. Then the routes return, the replica is killed
# once more, and its first park gets an answer (the ok outcome counts).
case_lookup_blackout() {
  local node="kardamom-sequencer-0" ip="${NODE_IP[kardamom-sequencer-0]}" port=9001
  local executors="${NODE_IP[executor-0]} ${NODE_IP[executor-1]} ${NODE_IP[executor-2]}"
  local base_fail base_ok e
  base_fail="$(seq_metric_where "${ip}" "${node}" "${port}" kardamom_sequencer_nonce_lookups_total 'outcome="timeout"' || true)"
  base_fail=$(( ${base_fail:-0} + $(seq_metric_where "${ip}" "${node}" "${port}" kardamom_sequencer_nonce_lookups_total 'outcome="error"' || echo 0) ))
  for e in ${executors}; do
    docker exec "${node}" ip route add blackhole "${e}/32" \
      || fail "lookup-blackout: could not blackhole ${e} on ${node}"
  done
  log "lookup-blackout: executors blackholed from ${node}; hard-killing lane 0's replica there"
  # The restore must happen even when an assert fails. chaos.sh owns the
  # one EXIT trap, so this case restores on its own error path.
  restore_routes() { local x; for x in ${executors}; do docker exec "${node}" ip route del blackhole "${x}/32" 2>/dev/null || true; done; }
  local failed=0
  {
    inject_hard "${node}" sequencer-0
    assert_progress
    assert_count sequencer 4 "${CHAOS_RESTART_SLO_S}"
    wait_until "cold replica's lookups failed with no executor reachable" 120 \
      lookups_failed_past "${ip}" "${node}" "${port}" "${base_fail}"
  } || failed=1
  restore_routes
  [ "${failed}" = "0" ] || fail "lookup-blackout: blackout phase failed"
  log "lookup-blackout: routes restored; hard-killing the replica again for an answered lookup"
  base_ok="$(seq_metric_where "${ip}" "${node}" "${port}" kardamom_sequencer_nonce_lookups_total 'outcome="ok"' || echo 0)"
  inject_hard "${node}" sequencer-0
  assert_count sequencer 4 "${CHAOS_RESTART_SLO_S}"
  wait_until "cold replica's lookup answered" 120 lookups_ok_past "${ip}" "${node}" "${port}" "${base_ok:-0}"
  assert_progress
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
