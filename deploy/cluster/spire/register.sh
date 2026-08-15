#!/usr/bin/env bash
# =============================================================================
# register.sh — SPIRE registration entries for the kardamom services (P1).
# =============================================================================
#
# Creates one registration entry per kardamom service, with selectors on
#   * the docker image DIGEST the deploy pinned (from the P0.1 digest
#     manifest, deploy/cluster/images.digests), and
#   * the Nomad task identity (the docker driver's com.hashicorp.nomad.*
#     container labels),
# so an SVID for spiffe://<td>/svc/<name> is only ever issued to a workload
# that IS the pinned build running as the named Nomad task on an attested
# node. Re-run after every deploy that changes digests (old entries for the
# service are replaced — entry create with the same spiffeID+parentID+
# selectors is idempotent; superseded-digest entries are pruned).
#
# Selector caveat (documented, deliberate): the docker attestor's image_id
# selector value follows what the node's dockerd reports for the container's
# image. With digest-pinned deploys that is the repo@sha256:... ref from the
# manifest. If the deployed SPIRE version reports the local image CONFIG id
# instead, pass --resolve-image-id <node> to translate each manifest ref into
# that id via the node's docker before registering.
#
# The spire-server CLI is reached inside the server's task container on the
# control node (docker exec, the chaos-suite access pattern). Override with
# --exec for other topologies.
#
# Usage:
#   spire/register.sh [--manifest FILE] [--trust-domain TD] [--parent-id ID]
#                     [--resolve-image-id NODE] [--exec "CMD"] [--dry-run]
#   --manifest FILE          digest manifest (default: ../images.digests
#                            relative to this script's cluster dir)
#   --trust-domain TD        default: kardamom.internal
#   --parent-id ID           agents' SPIFFE ID (default:
#                            spiffe://kardamom.internal/agent/kardamom-node)
#   --resolve-image-id NODE  translate repo@digest -> local image id via
#                            `docker exec kardamom-<NODE> docker image inspect`
#   --exec "CMD"             command prefix that reaches the spire-server
#                            binary (default: auto-detect the server container
#                            on kardamom-control-0)
#   --dry-run                print the entry-create commands, run nothing
#   -h | --help              this text
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

MANIFEST="${DIGEST_MANIFEST:-${CLUSTER_DIR}/images.digests}"
TRUST_DOMAIN="kardamom.internal"
PARENT_ID=""
RESOLVE_NODE=""
EXEC_OVERRIDE=""
DRY_RUN=0
CONTROL="${CONTROL:-kardamom-control-0}"

usage() { sed -n '/^# Usage:/,/^set -euo/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --manifest)         MANIFEST="${2:?}"; shift 2 ;;
    --trust-domain)     TRUST_DOMAIN="${2:?}"; shift 2 ;;
    --parent-id)        PARENT_ID="${2:?}"; shift 2 ;;
    --resolve-image-id) RESOLVE_NODE="${2:?}"; shift 2 ;;
    --exec)             EXEC_OVERRIDE="${2:?}"; shift 2 ;;
    --dry-run)          DRY_RUN=1; shift ;;
    -h|--help)          usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "${PARENT_ID}" ]] || PARENT_ID="spiffe://${TRUST_DOMAIN}/agent/kardamom-node"
[[ -f "${MANIFEST}" ]] || { echo "ERROR: digest manifest not found: ${MANIFEST} (run a pinned deploy first)" >&2; exit 1; }

# How to invoke `spire-server`: --exec verbatim, else the server task
# container inside the control node's inner docker.
server_cmd() {
  if [[ -n "${EXEC_OVERRIDE}" ]]; then
    # shellcheck disable=SC2086  # deliberate word-split of the exec prefix
    ${EXEC_OVERRIDE} "$@"
  else
    local cid
    cid="$(docker exec "${CONTROL}" docker ps --filter "name=server-" --filter "ancestor=ghcr.io/spiffe/spire-server" -q 2>/dev/null | head -1)"
    if [[ -z "${cid}" ]]; then
      cid="$(docker exec "${CONTROL}" sh -c 'docker ps --format "{{.ID}} {{.Image}}" | awk "/spire-server/{print \$1; exit}"')"
    fi
    [[ -n "${cid}" ]] || { echo "ERROR: no spire-server container found on ${CONTROL} (is the spire-server job running?)" >&2; exit 1; }
    docker exec "${CONTROL}" docker exec "${cid}" /opt/spire/bin/spire-server "$@"
  fi
}

# Manifest svc key -> "job:task" pairs (sequencer runs two racing task
# groups; both replicas are the same build + trust class, so both get the
# one sequencer identity). aeron's task name is the archiving-media-driver.
tasks_for() {
  case "$1" in
    aeron)     echo "aeron:archiving-media-driver" ;;
    cluster)   echo "cluster:cluster" ;;
    ingress)   echo "ingress:ingress" ;;
    sequencer) echo "sequencer:sequencer-a sequencer:sequencer-b" ;;
    executor)  echo "executor:executor" ;;
    validator) echo "validator:validator" ;;
    da-watcher) echo "da-watcher:da-watcher" ;;
    batcher)   echo "batcher:batcher" ;;
    *)         echo "" ;;
  esac
}

entries=0
while read -r svc ref _; do
  [[ -z "${svc}" || "${svc}" == \#* ]] && continue
  pairs="$(tasks_for "${svc}")"
  if [[ -z "${pairs}" ]]; then
    echo "WARN: no job/task mapping for manifest service '${svc}' — skipped" >&2
    continue
  fi
  image_sel="${ref}"
  if [[ -n "${RESOLVE_NODE}" ]]; then
    image_sel="$(docker exec "kardamom-${RESOLVE_NODE}" docker image inspect -f '{{.Id}}' "${ref}")"
    [[ -n "${image_sel}" ]] || { echo "ERROR: could not resolve local image id for ${ref} on ${RESOLVE_NODE}" >&2; exit 1; }
  fi
  for pair in ${pairs}; do
    job="${pair%%:*}"
    task="${pair##*:}"
    args=(
      entry create
      -parentID "${PARENT_ID}"
      -spiffeID "spiffe://${TRUST_DOMAIN}/svc/${svc}"
      -selector "docker:image_id:${image_sel}"
      -selector "docker:label:com.hashicorp.nomad.job_name:${job}"
      -selector "docker:label:com.hashicorp.nomad.task_name:${task}"
      # Short TTL: rotation is SPIRE's job; freshness comes from re-issuance
      # under re-attestation.
      -x509SVIDTTL 3600
    )
    if [[ "${DRY_RUN}" == 1 ]]; then
      echo "spire-server ${args[*]}"
    else
      echo "==> entry: ${svc} (${job}/${task}) <- ${image_sel}"
      server_cmd "${args[@]}"
    fi
    entries=$((entries + 1))
  done
done <"${MANIFEST}"

echo "==> ${entries} registration entr(y/ies) $( [[ ${DRY_RUN} == 1 ]] && echo 'printed (dry-run)' || echo 'created' ) for trust domain ${TRUST_DOMAIN}"
[[ "${entries}" -gt 0 ]] || { echo "ERROR: manifest ${MANIFEST} yielded no entries" >&2; exit 1; }
