#!/usr/bin/env bash
# =============================================================================
# export-trust-bundle.sh — export the SPIRE server's CA bundle (P1).
# =============================================================================
#
# Fetches the trust bundle (PEM) from the running spire-server. Two consumers:
#   * agent bootstrap: copy to /opt/spire/bootstrap.crt on every workload
#     node BEFORE starting spire-agent (runbook step 2);
#   * peers: hand the bundle to whoever must verify our SVID-backed
#     endpoints (first: peer chains verifying the interop feed WS certs).
#
# Reaches the spire-server CLI inside the server's task container on the
# control node (docker exec — the chaos-suite access pattern); --exec
# overrides for other topologies.
#
# Usage:
#   spire/export-trust-bundle.sh [--output FILE] [--distribute] [--exec "CMD"]
#   --output FILE   write the PEM bundle to FILE (default: stdout)
#   --distribute    ALSO copy the bundle to /opt/spire/bootstrap.crt on every
#                   non-control cluster node (docker cp via each node
#                   container) — the agent bring-up prerequisite
#   --exec "CMD"    command prefix that reaches the spire-server binary
#   -h | --help     this text
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=deploy/cluster/scripts/lib.sh
source "${SCRIPT_DIR}/../scripts/lib.sh"
# shellcheck source=deploy/cluster/scripts/lib-topology.sh
source "${SCRIPT_DIR}/../scripts/lib-topology.sh"

OUTPUT=""
DISTRIBUTE=0
EXEC_OVERRIDE=""

usage() { sed -n '/^# Usage:/,/^set -euo/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output)     OUTPUT="${2:?}"; shift 2 ;;
    --distribute) DISTRIBUTE=1; shift ;;
    --exec)       EXEC_OVERRIDE="${2:?}"; shift 2 ;;
    -h|--help)    usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

server_cmd() {
  if [[ -n "${EXEC_OVERRIDE}" ]]; then
    # shellcheck disable=SC2086  # deliberate word-split of the exec prefix
    ${EXEC_OVERRIDE} "$@"
  else
    local cid
    cid="$(docker exec "${CONTROL}" sh -c 'docker ps --format "{{.ID}} {{.Image}}" | awk "/spire-server/{print \$1; exit}"')"
    [[ -n "${cid}" ]] || fail "no spire-server container found on ${CONTROL} (is the spire-server job running?)"
    docker exec "${CONTROL}" docker exec "${cid}" /opt/spire/bin/spire-server "$@"
  fi
}

bundle="$(server_cmd bundle show -format pem)"
[[ "${bundle}" == *"BEGIN CERTIFICATE"* ]] || fail "bundle show returned no PEM certificate"

if [[ -n "${OUTPUT}" ]]; then
  printf '%s\n' "${bundle}" >"${OUTPUT}"
  log "trust bundle written: ${OUTPUT}"
else
  printf '%s\n' "${bundle}"
fi

if [[ "${DISTRIBUTE}" == 1 ]]; then
  # shellcheck disable=SC2119  # topology_load's optional group_vars arg is deliberately defaulted
  topology_load || fail "could not load cluster topology from group_vars"
  for n in "${NODES[@]}"; do
    [[ "${NODE_TIER[${n}]}" == "control" ]] && continue
    node="kardamom-${n}"
    if ! docker exec "${node}" true 2>/dev/null; then
      log "${node}: unreachable — skipped (re-run --distribute when it returns)"
      continue
    fi
    printf '%s\n' "${bundle}" | docker exec -i "${node}" sh -c 'mkdir -p /opt/spire && cat > /opt/spire/bootstrap.crt'
    log "${node}: /opt/spire/bootstrap.crt updated"
  done
fi
