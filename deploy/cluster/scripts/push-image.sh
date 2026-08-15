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
# Usage: push-image.sh <svc> <image:tag> <manifest>
#   <svc>       manifest key (aeron, cluster, ingress, ...)
#   <image:tag> fully-qualified image ref to push
#   <manifest>  digest manifest file; one "<svc> <repo>@sha256:..." line is
#               APPENDED per call (the Makefile truncates it per build)
set -euo pipefail

if [[ $# -ne 3 || "$1" == "-h" || "$1" == "--help" ]]; then
  echo "usage: $0 <svc> <image:tag> <manifest>" >&2
  exit 2
fi

svc="$1"
img="$2"
manifest="$3"

out="$(docker push "${img}" | tee /dev/stderr)"
digest="$(awk '/digest: sha256:/ {d=$3} END {print d}' <<<"${out}")"
if [[ -z "${digest}" ]]; then
  echo "ERROR: could not capture the pushed digest for ${img} from the push output" >&2
  exit 1
fi

repo="${img%:*}"
echo "${svc} ${repo}@${digest}" >>"${manifest}"
echo "==> pinned ${svc} -> ${repo}@${digest}"
