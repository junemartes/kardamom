#!/usr/bin/env bash
# Resize the sequencer tier to <target-lanes> active lanes. This is the
# runbook of docs/specs/dynamic-sequencer-sizing.md, section 3.5, as one
# script. Every step is a config render plus a Nomad job run, or a wait.
#
#   1. Render the next shard map (fewest moves) and the overlap job: the
#      lanes that gain vslots read the old lanes too, and start those
#      vslots in shadow mode. Run the job.
#   2. Wait for the warm-up: the shadow gauge of every shadow replica
#      reads 0.
#   3. Install the next map as the ingress map, and run the ingress job.
#      Nomad rolls the two allocs one at a time. The graceful drain in the
#      ingress, with the job's kill_timeout, lets parked submits finish.
#   4. Wait tx_ttl, then require zero parked entries on every replica of
#      the lanes that lose vslots.
#   5. Render the steady job for the next map and run it. Nomad restarts
#      the changed groups one replica at a time, and stops a leaving lane.
#
# The script refuses to start while a resize is in flight (the marker
# file config/shard-map.next.toml exists), and while any sequencer replica
# is in resync mode or holds parked entries (sealer backpressure). It
# leaves config/shard-map.toml and nomad/sequencer.nomad.hcl updated in
# the working tree; commit them.
#
# Usage (from the host, like deploy.sh):
#   deploy/cluster/scripts/scale-sequencers.sh <target-lanes>
#   NOMAD_ADDR=http://192.168.56.10:4646 deploy/cluster/scripts/scale-sequencers.sh 3
#   DRY_RUN=1 deploy/cluster/scripts/scale-sequencers.sh 3   # render only
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
GV="${CLUSTER_DIR}/ansible/group_vars/all.yml"
MAP="${CLUSTER_DIR}/config/shard-map.toml"
NEXT_MAP="${CLUSTER_DIR}/config/shard-map.next.toml"
JOB="${CLUSTER_DIR}/nomad/sequencer.nomad.hcl"
DRY_RUN="${DRY_RUN:-0}"

usage() { echo "usage: $0 <target-lanes>" >&2; exit 2; }
[[ $# -eq 1 ]] || usage
TARGET="$1"
[[ "${TARGET}" =~ ^[1-8]$ ]] || { echo "target lanes must be 1..8" >&2; exit 2; }

log() { echo "==> $*"; }
fail() { echo "SCALE FAIL: $*" >&2; exit 1; }

# --- contract values -------------------------------------------------------
gv_scalar() { sed -n -E "s/^$1:[[:space:]]*([^#]+).*/\1/p" "${GV}" | head -1 | tr -d ' "'; }
TX_TTL_MS="$(gv_scalar tx_ttl_ms)"
IP_PREFIX="$(gv_scalar ip_prefix)"
CONTROL_IP="$(gv_scalar control_ip)"
NOMAD_HTTP="$(sed -n -E 's/^  nomad_http:[[:space:]]*([0-9]+).*/\1/p' "${GV}" | head -1)"
export NOMAD_ADDR="${NOMAD_ADDR:-http://${CONTROL_IP}:${NOMAD_HTTP}}"
SEQ_COUNT="$(sed -n -E 's/^  sequencer:[[:space:]]*\{[^}]*count:[[:space:]]*([0-9]+).*/\1/p' "${GV}" | head -1)"
SEQ_IP_START="$(sed -n -E 's/^  sequencer:[[:space:]]*\{[^}]*ip_start:[[:space:]]*([0-9]+).*/\1/p' "${GV}" | head -1)"
[[ -n "${TX_TTL_MS}" && -n "${IP_PREFIX}" && -n "${SEQ_COUNT}" && -n "${SEQ_IP_START}" ]] \
  || fail "could not read tx_ttl_ms, ip_prefix, or node_classes.sequencer from ${GV}"
TX_TTL_S=$(( (TX_TTL_MS + 999) / 1000 ))
METRICS_BASE=9001
SEQ_IPS=()
for ((i = 0; i < SEQ_COUNT; i++)); do SEQ_IPS+=("${IP_PREFIX}.$((SEQ_IP_START + i))"); done

# --- image pin, as deploy.sh does --------------------------------------------
IMAGE_REF_ARGS=()
DIGEST_MANIFEST="${DIGEST_MANIFEST:-${CLUSTER_DIR}/images.digests}"
if [[ -f "${DIGEST_MANIFEST}" ]]; then
  ref="$(awk '$1 == "sequencer" {print $2; exit}' "${DIGEST_MANIFEST}")"
  [[ -n "${ref}" ]] && IMAGE_REF_ARGS=(-var "image_ref=${ref}")
fi

# --- helpers -------------------------------------------------------------------
map_lanes() { # <map file> -> active lane count
  python3 - "$1" <<'PY'
import re, sys
t = open(sys.argv[1]).read()
tab = [int(x) for x in re.findall(r"\d+", re.search(r"table\s*=\s*\[([^\]]*)\]", t, re.S).group(1))]
print(max(tab) + 1)
PY
}
lanes_of() { # <map file> <lane> -> "a,b,c" vslots
  python3 - "$1" "$2" <<'PY'
import re, sys
t = open(sys.argv[1]).read(); lane = int(sys.argv[2])
tab = [int(x) for x in re.findall(r"\d+", re.search(r"table\s*=\s*\[([^\]]*)\]", t, re.S).group(1))]
print(",".join(str(v) for v, l in enumerate(tab) if l == lane))
PY
}
metric() { # <ip> <port> <metric> [label-filter] -> summed value (empty when unreachable)
  curl -fsS --max-time 5 "http://$1:$2/metrics" 2>/dev/null \
    | awk -v m="$3" -v f="${4:-}" '
        index($0, m) == 1 && (f == "" || index($0, f) > 0) { s += $NF; n++ }
        END { if (n) printf "%s", s }'
}
wait_metric() { # <ip> <port> <metric> <want> <what> <timeout-s>
  local t=0 v
  while :; do
    v="$(metric "$1" "$2" "$3" || true)"
    [[ -n "${v}" && "${v}" == "$4" ]] && return 0
    (( t >= $6 )) && fail "$5: $1:$2 $3 = '${v}', wanted $4 after $6s"
    sleep 2; t=$(( t + 2 ))
  done
}
run_job() { # <file> <what>
  log "nomad job run $1 ($2)"
  if [[ "${DRY_RUN}" == "1" ]]; then return 0; fi
  ( cd "${CLUSTER_DIR}" && nomad job run "${IMAGE_REF_ARGS[@]+"${IMAGE_REF_ARGS[@]}"}" "$1" ) \
    || fail "nomad job run $1 failed"
}
wait_running() { # <job> <timeout-s>
  if [[ "${DRY_RUN}" == "1" ]]; then return 0; fi
  local t=0 statuses running pending
  while :; do
    statuses="$(nomad job allocs -t '{{range .}}{{.ClientStatus}}{{"\n"}}{{end}}' "$1" 2>/dev/null || true)"
    running="$(grep -cx running <<<"${statuses}" || true)"
    pending="$(grep -cx pending <<<"${statuses}" || true)"
    (( running >= 1 && pending == 0 )) && return 0
    (( t >= $2 )) && fail "job $1 did not converge in $2s"
    sleep 5; t=$(( t + 5 ))
  done
}

# --- guards ----------------------------------------------------------------------
[[ -f "${MAP}" ]] || fail "no ${MAP}; render one with scripts/render-shard-map.py --identity <lanes>"
[[ ! -f "${NEXT_MAP}" ]] || fail "a resize is in flight (${NEXT_MAP} exists); finish or remove it first"
CURRENT="$(map_lanes "${MAP}")"
[[ "${CURRENT}" != "${TARGET}" ]] || fail "already at ${TARGET} lanes"
if [[ "${DRY_RUN}" != "1" ]]; then
  for ((lane = 0; lane < CURRENT; lane++)); do
    port=$(( METRICS_BASE + 10 * lane ))
    for ip in "${SEQ_IPS[@]}"; do
      v="$(metric "${ip}" "${port}" kardamom_sequencer_resync_mode || true)"
      [[ -z "${v}" || "${v}" == "0" ]] || fail "replica ${ip}:${port} is in resync mode (sealer backpressure); not resizing"
      v="$(metric "${ip}" "${port}" kardamom_sequencer_pending_depth || true)"
      [[ -z "${v}" || "${v}" == "0" ]] || fail "replica ${ip}:${port} holds ${v} parked entries; not resizing"
    done
  done
fi

# --- step 1: the next map and the overlap job ------------------------------------
log "resize ${CURRENT} -> ${TARGET} lanes (tx_ttl ${TX_TTL_S}s)"
python3 "${SCRIPT_DIR}/render-shard-map.py" --from "${MAP}" --lanes "${TARGET}" > "${NEXT_MAP}"
python3 "${SCRIPT_DIR}/render-sequencer-job.py" --map "${NEXT_MAP}" --from "${MAP}" > "${JOB}"
GAINING=()
for ((lane = 0; lane < TARGET; lane++)); do
  grep -q "\"--shadow-vslots\"" <(sed -n "/group \"seq-${lane}\"/,/^  }/p" "${JOB}") && GAINING+=("${lane}")
done
log "lanes that gain vslots (shadow first): ${GAINING[*]:-none}"
run_job nomad/sequencer.nomad.hcl "overlap: new lanes in shadow mode"
wait_running sequencer 300

# --- step 2: warm-up ----------------------------------------------------------------
if [[ "${DRY_RUN}" != "1" ]]; then
  for lane in "${GAINING[@]}"; do
    port=$(( METRICS_BASE + 10 * lane ))
    for ip in "${SEQ_IPS[@]}"; do
      # Not every node runs every lane. An unreachable port is skipped.
      [[ -n "$(metric "${ip}" "${port}" kardamom_sequencer_shadow_vslots || true)" ]] || continue
      wait_metric "${ip}" "${port}" kardamom_sequencer_shadow_vslots 0 \
        "warm-up of lane ${lane}" $(( TX_TTL_S + 60 ))
    done
  done
fi

# --- step 3: switch the ingress -------------------------------------------------------
cp "${NEXT_MAP}" "${MAP}"
run_job nomad/ingress.nomad.hcl "ingress on map $(sed -n -E 's/^version = //p' "${MAP}")"
wait_running ingress 300

# --- step 4: drain ----------------------------------------------------------------------
log "drain: waiting tx_ttl (${TX_TTL_S}s)"
[[ "${DRY_RUN}" == "1" ]] || sleep "${TX_TTL_S}"
if [[ "${DRY_RUN}" != "1" ]]; then
  for ((lane = 0; lane < CURRENT; lane++)); do
    port=$(( METRICS_BASE + 10 * lane ))
    for ip in "${SEQ_IPS[@]}"; do
      [[ -n "$(metric "${ip}" "${port}" kardamom_sequencer_pending_depth || true)" ]] || continue
      wait_metric "${ip}" "${port}" kardamom_sequencer_pending_depth 0 \
        "drain of lane ${lane}" $(( TX_TTL_S + 60 ))
    done
  done
fi

# --- step 5: the steady job -------------------------------------------------------------
python3 "${SCRIPT_DIR}/render-sequencer-job.py" --map "${MAP}" > "${JOB}"
run_job nomad/sequencer.nomad.hcl "steady: final vslot sets"
wait_running sequencer 300
rm -f "${NEXT_MAP}"
log "done: ${TARGET} active lanes."
# check-contract.py requires partition_count and the ingress --shards to
# equal the active lane count of the committed map, and accepts a steady
# count of 1, 2, 4, or 8 only. A committed resize that leaves them
# behind fails the contract check. Say so here, so the operator commits
# all four files together.
case "${TARGET}" in
  1|2|4|8)
    log "commit together: config/shard-map.toml, nomad/sequencer.nomad.hcl,"
    log "  ansible/group_vars/all.yml (partition_count: ${TARGET}),"
    log "  nomad/ingress.nomad.hcl (\"--shards\", \"${TARGET}\")." ;;
  *)
    log "${TARGET} lanes is a transient count: the contract accepts a steady count of 1, 2, 4, or 8."
    log "  Resize again toward one of those before you commit the map and the job." ;;
esac
