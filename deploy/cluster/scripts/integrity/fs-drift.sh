#!/usr/bin/env bash
# =============================================================================
# fs-drift.sh — docker diff per kardamom container vs the writable allowlist.
# =============================================================================
#
# `docker diff` lists every path a container has Added/Changed/Deleted in its
# OWN rootfs layers since start. With digest-pinned images and readonly_rootfs
# on the Rust services, the expected steady state is an EMPTY diff — the
# services' real data paths (aeron dir, state, checkpoints, archive, batcher
# cursor/DA store) are bind mounts that docker diff does not see. Every diff
# line is therefore either allowlisted (fs-allowlist.txt: /tmp for the JVMs,
# see the file's header) or a finding: report + nonzero exit.
#
# A finding means "something wrote into the image filesystem that the deploy
# did not declare" — a dropped tool, a modified binary, an unpacked payload,
# or a legitimate writable path nobody has declared yet. During rollout, noisy
# paths get either an explicit writable mount in the job spec or a fix in the
# service; the allowlist is not a dumping ground.
#
# Usage:
#   integrity/fs-drift.sh [--allowlist FILE]
#   --allowlist FILE   expected-writable paths (default: integrity/fs-allowlist.txt)
#   -h | --help        this text
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# shellcheck source=deploy/cluster/scripts/lib.sh
source "${SCRIPT_DIR}/../lib.sh"
# shellcheck source=deploy/cluster/scripts/lib-topology.sh
source "${SCRIPT_DIR}/../lib-topology.sh"
# shellcheck source=deploy/cluster/scripts/integrity/integrity-lib.sh
source "${SCRIPT_DIR}/integrity-lib.sh"

ALLOWLIST="${SCRIPT_DIR}/fs-allowlist.txt"

usage() { sed -n '/^# Usage:/,/^set -euo/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --allowlist) ALLOWLIST="${2:?--allowlist needs a file}"; shift 2 ;;
    -h|--help)   usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -f "${ALLOWLIST}" ]] || fail "allowlist not found: ${ALLOWLIST}"
# shellcheck disable=SC2119  # topology_load's optional group_vars arg is deliberately defaulted
topology_load || fail "could not load cluster topology from group_vars"

# Allowlist into parallel arrays (service-glob, path-prefix).
ALLOW_SVC=()
ALLOW_PATH=()
while read -r a_svc a_path _; do
  [[ -z "${a_svc}" || "${a_svc}" == \#* ]] && continue
  ALLOW_SVC+=("${a_svc}")
  ALLOW_PATH+=("${a_path}")
done <"${ALLOWLIST}"

# allowed <svc> <change-type A|C|D> <path> -> 0 if tolerated.
allowed() {
  local svc="$1" ctype="$2" path="$3" i
  for i in "${!ALLOW_SVC[@]}"; do
    # shellcheck disable=SC2053  # deliberate glob match of svc against the pattern
    [[ "${svc}" == ${ALLOW_SVC[$i]} ]] || continue
    local p="${ALLOW_PATH[$i]}"
    # Under (or at) an allowed prefix.
    [[ "${path}" == "${p}" || "${path}" == "${p}"/* ]] && return 0
    # A 'C' on an ANCESTOR directory of an allowed prefix is the parent-dir
    # mtime ripple of an allowed write (writing /tmp/x also reports C /tmp's
    # ancestors) — tolerated for C lines only.
    [[ "${ctype}" == "C" && "${p}" == "${path}"/* ]] && return 0
  done
  return 1
}

findings=0
checked=0

for node in $(integrity_nodes); do
  if ! node_reachable "${node}"; then
    log "${node}: unreachable (down or not a DinD node) — skipped"
    continue
  fi
  while IFS='|' read -r cid name image; do
    [[ -n "${cid}" ]] || continue
    checked=$((checked + 1))
    svc="$(svc_from_image "${image}")"
    diff_out="$(docker exec "${node}" docker diff "${cid}" 2>/dev/null || true)"
    [[ -z "${diff_out}" ]] && continue
    while read -r ctype path; do
      [[ -n "${path}" ]] || continue
      if ! allowed "${svc}" "${ctype}" "${path}"; then
        echo "DRIFT: ${node}/${name} (${svc}): ${ctype} ${path}"
        findings=$((findings + 1))
      fi
    done <<<"${diff_out}"
  done < <(kardamom_containers "${node}")
done

echo
if [[ "${findings}" -gt 0 ]]; then
  echo "fs-drift: ${findings} unexpected rootfs change(s) across ${checked} container(s)"
  echo "          (either declare the path — job-spec mount + allowlist entry — or treat as compromise evidence)"
  exit 1
fi
log "fs-drift: OK — ${checked} container(s), no rootfs writes outside the allowlist"
