#!/usr/bin/env bash
# =============================================================================
# image-drift.sh — running image digests vs the deployed digest manifest.
# =============================================================================
#
# For every kardamom task container on every cluster node, compare what is
# ACTUALLY running against what the last deploy PUSHED:
#
#   deployed  = deploy/cluster/images.digests, the per-deploy manifest the
#               image push step writes ("<svc> <repo>@sha256:..." per image)
#               and deploy.sh passes into every job's image_ref variable.
#               The manifest is the source of truth here — not the Nomad job
#               spec — because it is written at PUSH time by the pipeline
#               that owns the bytes; a job spec re-registered by hand (the
#               :dev fallback) is exactly the situation this sweep must flag.
#   running   = (a) the image ref the container was STARTED from
#               (.Config.Image), and (b) the RepoDigests of the image the
#               container is USING (resolved from its image ID) — (b) catches
#               a re-tagged/re-pushed image behind a stale ref, (a) catches a
#               task that was never pinned at all.
#
# Any container whose running digest differs from the manifest — or that
# cannot be verified against it — is a finding: report + nonzero exit.
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
  while IFS='|' read -r cid name image; do
    [[ -n "${cid}" ]] || continue
    checked=$((checked + 1))
    svc="$(svc_from_image "${image}")"
    expected="${EXPECTED[${svc}]:-}"
    if [[ -z "${expected}" ]]; then
      finding "${node}/${name}: service '${svc}' has no line in ${MANIFEST} — running image cannot be verified (ref: ${image})"
      continue
    fi
    # (a) the ref the task was started from should BE the pinned ref.
    started_ref="$(docker exec "${node}" docker inspect -f '{{.Config.Image}}' "${cid}" 2>/dev/null || true)"
    # (b) the digests of the image actually backing the container.
    image_id="$(docker exec "${node}" docker inspect -f '{{.Image}}' "${cid}" 2>/dev/null || true)"
    repo_digests="$(docker exec "${node}" docker image inspect -f '{{join .RepoDigests "\n"}}' "${image_id}" 2>/dev/null || true)"
    if [[ "${started_ref}" != "${expected}" ]]; then
      finding "${node}/${name}: started from '${started_ref}', deploy pinned '${expected}' (tag-fallback or manual run?)"
    fi
    if ! grep -qxF "${expected}" <<<"${repo_digests}"; then
      finding "${node}/${name}: running image ${image_id} carries digests [$(tr '\n' ' ' <<<"${repo_digests}")], none is the deployed ${expected}"
    fi
  done < <(kardamom_containers "${node}")
done

echo
if [[ "${findings}" -gt 0 ]]; then
  echo "image-drift: ${findings} finding(s) across ${checked} container(s) — the fleet does NOT match ${MANIFEST}"
  exit 1
fi
log "image-drift: OK — ${checked} container(s) all match ${MANIFEST}"
