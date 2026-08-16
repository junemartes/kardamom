#!/usr/bin/env bash
# =============================================================================
# sign-manifest.sh — keyless-sign the COMPLETED digest manifest (P0.5).
# =============================================================================
# Thin executable wrapper over lib-signing.sh's sign_digest_manifest for
# callers that cannot source bash libs (the Makefile `images` target). Run it
# ONCE, after the last push-image.sh of a build: the manifest blob signature
# covers the whole manifest, and a bundle over a half-written one would
# verify and still lie.
#
# In CI with OIDC (ACTIONS_ID_TOKEN_REQUEST_URL set): cosign sign-blob --yes,
# keyless, logged to the PUBLIC Rekor; the offline-verifiable bundle lands at
# <manifest>.sigbundle. Outside CI: skipped with a log line (local deploys
# are a documented dev affordance).
#
# Usage: sign-manifest.sh <manifest>
set -euo pipefail

if [[ $# -ne 1 || "$1" == "-h" || "$1" == "--help" ]]; then
  echo "usage: $0 <manifest>" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/cluster/scripts/lib-signing.sh
source "${SCRIPT_DIR}/lib-signing.sh"

sign_digest_manifest "$1"
