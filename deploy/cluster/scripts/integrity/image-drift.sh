#!/usr/bin/env bash
# =============================================================================
# image-drift.sh — running image digests vs the deployed digest manifest.
# =============================================================================
#
# For every kardamom task container on every cluster node, compare what is
# ACTUALLY running against what the last deploy PUSHED:
#
#   deployed  = deploy/cluster/images.digests, the per-deploy manifest the
#               image push step writes ("<svc> <repo>:<tag>@sha256:..." per
#               image) and deploy.sh passes into every job's image_ref
#               variable. The manifest is the source of truth here — not the
#               Nomad job spec — because it is written at PUSH time by the
#               pipeline that owns the bytes; a job spec re-registered by
#               hand (the :dev fallback) is exactly the situation this sweep
#               must flag.
#   running   = (b, authoritative) the RepoDigests of the image BACKING the
#               container (resolved from its image ID) must contain the
#               deployed digest — catches stale caches, re-pushed tags, and
#               unpinned starts alike; PLUS (a, best-effort) when the
#               container records a REF in .Config.Image, it must normalize
#               to the deployed ref. Nomad 1.9.5 creates task containers by
#               IMAGE ID (drivers/docker/driver.go: `Image: imageID`), so on
#               this cluster .Config.Image is sha256:<id> and check (a) is
#               skipped — (b) alone decides.
#
# Any container whose running digest differs from the manifest — or that
# cannot be verified against it — is a finding: report + nonzero exit.
# (Refs are normalized before comparison: the manifest's combined
# repo:tag@digest form vs dockerd's bare repo@digest RepoDigests entries.)
#
# Usage:
#   integrity/image-drift.sh [--manifest FILE]
#   --manifest FILE   digest manifest (default: deploy/cluster/images.digests)
#   -h | --help       this text
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# shellcheck source=deploy/cluster/scripts/lib.sh
source "${SCRIPT_DIR}/../lib.sh"
# shellcheck source=deploy/cluster/scripts/lib-topology.sh
source "${SCRIPT_DIR}/../lib-topology.sh"
# shellcheck source=deploy/cluster/scripts/integrity/integrity-lib.sh
source "${SCRIPT_DIR}/integrity-lib.sh"

MANIFEST="${DIGEST_MANIFEST:-${CLUSTER_DIR}/images.digests}"

usage() { sed -n '/^# Usage:/,/^set -euo/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --manifest) MANIFEST="${2:?--manifest needs a file}"; shift 2 ;;
    -h|--help)  usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -f "${MANIFEST}" ]] || fail "digest manifest not found: ${MANIFEST} (run a pinned deploy first)"
# shellcheck disable=SC2119  # topology_load's optional group_vars arg is deliberately defaulted
topology_load || fail "could not load cluster topology from group_vars"

# expected ref per service key, from the manifest (last line wins).
declare -A EXPECTED=()
while read -r svc ref _; do
  [[ -n "${svc}" && "${svc}" != \#* ]] && EXPECTED[${svc}]="${ref}"
done <"${MANIFEST}"

findings=0
checked=0
finding() { echo "DRIFT: $*"; findings=$((findings + 1)); }

for node in $(integrity_nodes); do
  if ! node_reachable "${node}"; then
    log "${node}: unreachable (down or not a DinD node) — skipped"
    continue
  fi
  while IFS='|' read -r cid name svc; do
    [[ -n "${cid}" ]] || continue
    checked=$((checked + 1))
    expected="${EXPECTED[${svc}]:-}"
    if [[ -z "${expected}" ]]; then
      finding "${node}/${name}: service '${svc}' has no line in ${MANIFEST} — running image cannot be verified"
      continue
    fi
    expected_bare="$(bare_digest_ref "${expected}")"
    started_ref="$(docker exec "${node}" docker inspect -f '{{.Config.Image}}' "${cid}" 2>/dev/null || true)"
    image_id="$(docker exec "${node}" docker inspect -f '{{.Image}}' "${cid}" 2>/dev/null || true)"
    repo_digests="$(docker exec "${node}" docker image inspect -f '{{join .RepoDigests "\n"}}' "${image_id}" 2>/dev/null || true)"
    # (a) best-effort: only when the container was created from a REF (Nomad
    # 1.9.5 creates task containers by image ID — sha256:<id> — which proves
    # nothing about pinning either way; (b) below decides).
    if [[ -n "${started_ref}" && "${started_ref}" != sha256:* ]]; then
      if [[ "$(bare_digest_ref "${started_ref}")" != "${expected_bare}" ]]; then
        finding "${node}/${name}: started from '${started_ref}', deploy pinned '${expected}' (tag-fallback or manual run?)"
      fi
    fi
    # (b) authoritative: the backing image must carry the deployed digest.
    if ! grep -qxF "${expected_bare}" <<<"${repo_digests}"; then
      finding "${node}/${name}: running image ${image_id} carries digests [$(tr '\n' ' ' <<<"${repo_digests}")], none is the deployed ${expected_bare}"
    fi
  done < <(kardamom_containers "${node}")
done

echo
if [[ "${findings}" -gt 0 ]]; then
  echo "image-drift: ${findings} finding(s) across ${checked} container(s) — the fleet does NOT match ${MANIFEST}"
  exit 1
fi
log "image-drift: OK — ${checked} container(s) all match ${MANIFEST}"
