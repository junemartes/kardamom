#!/usr/bin/env bash
# =============================================================================
# push-image.sh — pushes a docker image and captures its digest.
# =============================================================================
# The Makefile `images` target uses this script, on the VM/vagrant path. On
# that path, the host daemon pushes directly to the in-cluster registry. The
# container-cluster path has its own capture, in ci-images.sh push_image.
# That path must also handle the REGISTRY_PUSH_NODE engine-to-engine push,
# which this script does not.
#
# This script reads the digest from the push output's last
# "digest: sha256:..." line. It does not use
# `docker inspect --format='{{index .RepoDigests 0}}'`. RepoDigests is an
# unordered list. It can hold digests for other repos or registries that
# share the same image ID. The push output line always names the digest of
# this exact push, to this exact repo.
#
# The manifest stores the combined repo:tag@sha256:... form. Nomad 1.9.5's
# docker driver cannot parse a bare repo@digest ref on a registry host with a
# port. It appends :latest and fails with "invalid reference format". With
# the combined form, the driver pulls the advisory tag, but still resolves
# the container image by digest. This still pins what runs. See
# ci-images.sh push_image for the full reasoning; it uses the same capture
# and form.
#
# Usage: push-image.sh <svc> <image:tag> <manifest>
#   <svc>       manifest key (aeron, cluster, ingress, ...)
#   <image:tag> fully-qualified image ref to push
#   <manifest>  digest manifest file. Each call appends one
#               "<svc> <repo>:<tag>@sha256:..." line. The Makefile truncates
#               the file once per build.
set -euo pipefail

if [[ $# -ne 3 || "$1" == "-h" || "$1" == "--help" ]]; then
  echo "usage: $0 <svc> <image:tag> <manifest>" >&2
  exit 2
fi

# Signing: the script does keyless cosign signing of the pushed digest
# when it runs in CI with OIDC. Otherwise, it logs a
# clear skip line, since this Makefile/VM path is normally local dev. The
# Makefile signs the completed manifest after the last image, using
# scripts/sign-manifest.sh.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/cluster/scripts/lib-signing.sh
source "${SCRIPT_DIR}/lib-signing.sh"

svc="$1"
img="$2"
manifest="$3"

out="$(docker push "${img}" | tee /dev/stderr)"
# Strip stray characters and validate the shape. A stray \r or space prints
# clean in logs, but fails docker's reference parser at pull time.
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
sign_pushed_image "${ref}"
