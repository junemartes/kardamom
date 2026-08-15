#!/usr/bin/env bash
# =============================================================================
# push-image.sh — docker push + digest capture (attested-identity P0.1).
# =============================================================================
# Used by the Makefile `images` target (the VM/vagrant path, where the HOST
# daemon pushes directly to the in-cluster registry). The container-cluster
# path has its own capture in ci-images.sh push_image (it must also handle the
# REGISTRY_PUSH_NODE engine-to-engine push, which this helper does not).
#
# The digest is parsed from the push output's final "digest: sha256:..." line
# rather than `docker inspect --format='{{index .RepoDigests 0}}'`:
# RepoDigests is an unordered list that can carry digests for other repos or
# registries the same image ID is tagged under, while the push output line is
# by definition the digest of exactly this push to exactly this repo.
#
# The manifest carries the COMBINED repo:tag@sha256:... form — Nomad 1.9.5's
# docker driver mis-parses a bare repo@digest ref on a port-carrying registry
# host (appends :latest -> "invalid reference format"); with the combined
# form the driver pulls the advisory tag and resolves the container image by
# the digest, which still pins what runs. See ci-images.sh push_image for the
# full rationale (same capture, same form).
#
# Usage: push-image.sh <svc> <image:tag> <manifest>
#   <svc>       manifest key (aeron, cluster, ingress, ...)
#   <image:tag> fully-qualified image ref to push
#   <manifest>  digest manifest file; one "<svc> <repo>:<tag>@sha256:..."
#               line is APPENDED per call (the Makefile truncates it per
#               build)
set -euo pipefail

if [[ $# -ne 3 || "$1" == "-h" || "$1" == "--help" ]]; then
  echo "usage: $0 <svc> <image:tag> <manifest>" >&2
  exit 2
fi

svc="$1"
img="$2"
manifest="$3"

out="$(docker push "${img}" | tee /dev/stderr)"
# Defensive strip + shape validation: a stray \r/space prints clean in logs
# but fails docker's reference parser at pull time.
digest="$(awk '/digest: sha256:/ {d=$3} END {print d}' <<<"${out}" | tr -d '[:space:]\r')"
if [[ -z "${digest}" ]]; then
  echo "ERROR: could not capture the pushed digest for ${img} from the push output" >&2
  exit 1
fi

ref="${img}@${digest}"
ref_re='^[a-z0-9./:-]+@sha256:[0-9a-f]{64}$'
if [[ ! "${ref}" =~ ${ref_re} ]]; then
  echo "ERROR: captured image ref fails shape validation: '${ref}'" >&2
  exit 1
fi

echo "${svc} ${ref}" >>"${manifest}"
echo "==> pinned ${svc} -> ${ref}"
