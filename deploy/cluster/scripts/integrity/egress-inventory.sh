#!/usr/bin/env bash
# =============================================================================
# egress-inventory.sh — live network peers per node vs the expected-peer set.
# =============================================================================
#
# Snapshots the sockets of every cluster node and normalizes them into an
# inventory of who talks to whom. Every kardamom task runs with
# network_mode = "host" inside its DinD node, so the task's netns IS the
# node's netns: the snapshot is taken once per NODE (docker exec + the node
# image's iproute2 `ss`/`ip`), and per-service attribution comes from the
# owning process name `ss -p` reports (kardamom-ingres..., java, consul, ...).
#
# Normalized inventory, one 4-field line per entry (sorted, deduped):
#   <node> tcp-in  :<port> <proc>          accepted conn on a service port
#   <node> tcp-out <peer-ip>:<port> <proc> outbound established connection
#   <node> udp-bind <local>:<port> <proc>  bound UDP socket (Aeron endpoints)
#   <node> udp-peer <peer-ip>:<port> <proc> connected-UDP peer (rare)
#   <node> mcast   <group> -               multicast group membership
# (in/out split by the ephemeral-port heuristic: local port >= 32768 means
# this node is the client. tcp-in drops the peer's ephemeral port; tcp-out
# drops our own.)
#
# Modes:
#   (no mode)          print the normalized inventory to stdout
#   --generate         ALSO write it as the baseline expected-peers file
#   --expected FILE    diff the live inventory against FILE: every live line
#                      must match (glob, field-wise) some line in FILE;
#                      unexpected peers => report + nonzero exit. Lines in
#                      FILE with no live match are NOT findings (an idle
#                      channel is not a violation).
#
# Expected-file format: 4 glob fields matching the inventory fields, e.g.
#   kardamom-*      tcp-out 192.168.56.10:5000 *        # registry pulls
#   kardamom-aux-0  tcp-out 192.168.56.10:8546 kardamom-*  # L1 RPC
# '#' comments and blank lines are ignored. See expected-peers.tpl for the
# seed derived from channels.toml.tpl + the nomad job specs.
#
# This is INVENTORY, not enforcement (plan P0.2): per-task egress rules come
# after the inventory is quiet.
#
# Usage:
#   integrity/egress-inventory.sh [--generate] [--expected FILE] [--output FILE]
#   --generate        write the live inventory as the baseline (default
#                     output: integrity/expected-peers.txt)
#   --expected FILE   compare mode (mutually exclusive with --generate)
#   --output FILE     baseline path for --generate
#   -h | --help       this text
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=deploy/cluster/scripts/lib.sh
source "${SCRIPT_DIR}/../lib.sh"
# shellcheck source=deploy/cluster/scripts/lib-topology.sh
source "${SCRIPT_DIR}/../lib-topology.sh"
# shellcheck source=deploy/cluster/scripts/integrity/integrity-lib.sh
source "${SCRIPT_DIR}/integrity-lib.sh"

MODE="print"
EXPECTED=""
OUTPUT="${SCRIPT_DIR}/expected-peers.txt"

usage() { sed -n '/^# Usage:/,/^set -euo/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --generate) MODE="generate"; shift ;;
    --expected) MODE="compare"; EXPECTED="${2:?--expected needs a file}"; shift 2 ;;
    --output)   OUTPUT="${2:?--output needs a file}"; shift 2 ;;
    -h|--help)  usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done
[[ "${MODE}" == "compare" && ! -f "${EXPECTED}" ]] && fail "expected-peers file not found: ${EXPECTED}"

# shellcheck disable=SC2119  # topology_load's optional group_vars arg is deliberately defaulted
topology_load || fail "could not load cluster topology from group_vars"

# First process name out of ss's users:(("name",pid=..,fd=..),...) blob; '-'
# when ss printed none (kernel sockets, or no -p permission).
proc_of() {
  local blob="$1"
  case "${blob}" in
    *'users:(("'*) blob="${blob#*users:((\"}"; echo "${blob%%\"*}" ;;
    *) echo "-" ;;
  esac
}

# Normalized snapshot of one node on stdout.
snapshot_node() {
  local node="$1" state local_a peer_a rest proc
  # TCP, established only. -H no header, -n numeric, -a all, -p processes.
  # Field layout with an explicit state column: State Recv-Q Send-Q Local Peer [Process].
  while read -r state _ _ local_a peer_a rest; do
    [[ "${state}" == "ESTAB" ]] || continue
    proc="$(proc_of "${rest:-}")"
    local lport="${local_a##*:}"
    if [[ "${lport}" =~ ^[0-9]+$ ]] && (( lport >= 32768 )); then
      echo "${node} tcp-out ${peer_a} ${proc}"
    else
      echo "${node} tcp-in :${lport} ${proc}"
    fi
  done < <(docker exec "${node}" ss -Htnap 2>/dev/null || true)
  # UDP sockets: Aeron's data plane is unconnected UDP (multicast + unicast
  # endpoints), so the meaningful inventory is the BINDS (who has opened which
  # endpoint), plus any connected UDP peers.
  while read -r state _ _ local_a peer_a rest; do
    proc="$(proc_of "${rest:-}")"
    case "${state}" in
      UNCONN) echo "${node} udp-bind ${local_a} ${proc}" ;;
      ESTAB)  echo "${node} udp-bind ${local_a} ${proc}"
              echo "${node} udp-peer ${peer_a} ${proc}" ;;
    esac
  done < <(docker exec "${node}" ss -Hunap 2>/dev/null || true)
  # Multicast group memberships (the Aeron channel groups from channels.toml).
  while read -r group; do
    [[ -n "${group}" ]] && echo "${node} mcast ${group} -"
  done < <(docker exec "${node}" sh -c "ip maddr show 2>/dev/null | awk '/inet /{print \$2}'" || true)
}

INVENTORY="$(
  for node in $(integrity_nodes); do
    if ! node_reachable "${node}"; then
      log "${node}: unreachable — skipped" >&2
      continue
    fi
    snapshot_node "${node}"
  done | sort -u
)"

case "${MODE}" in
  print)
    echo "${INVENTORY}"
    ;;
  generate)
    {
      echo "# expected-peers baseline generated by egress-inventory.sh --generate"
      echo "# on $(date -u +%Y-%m-%dT%H:%M:%SZ). Fields: node kind addr proc (globs)."
      echo "# Review before trusting: a baseline taken from a compromised cluster"
      echo "# blesses the compromise."
      echo "${INVENTORY}"
    } >"${OUTPUT}"
    echo "${INVENTORY}"
    log "baseline written: ${OUTPUT} ($(grep -c . <<<"${INVENTORY}") entries)"
    ;;
  compare)
    findings=0
    # Expected globs, minus comments/blanks.
    mapfile -t exp_lines < <(grep -vE '^\s*(#|$)' "${EXPECTED}" | sed 's/[[:space:]]*#.*$//')
    while read -r n kind addr proc; do
      [[ -n "${n}" ]] || continue
      matched=0
      for e in "${exp_lines[@]}"; do
        read -r en ekind eaddr eproc _ <<<"${e}"
        # shellcheck disable=SC2053  # deliberate glob matches
        if [[ "${n}" == ${en} && "${kind}" == ${ekind} && "${addr}" == ${eaddr} && "${proc}" == ${eproc} ]]; then
          matched=1; break
        fi
      done
      if [[ "${matched}" == 0 ]]; then
        echo "UNEXPECTED PEER: ${n} ${kind} ${addr} ${proc}"
        findings=$((findings + 1))
      fi
    done <<<"${INVENTORY}"
    echo
    if [[ "${findings}" -gt 0 ]]; then
      echo "egress-inventory: ${findings} peer(s) outside ${EXPECTED}"
      exit 1
    fi
    log "egress-inventory: OK — every live peer matches ${EXPECTED}"
    ;;
esac
