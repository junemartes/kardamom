#!/usr/bin/env bash
# =============================================================================
# sign-manifest.sh — signs the completed digest manifest with keyless signing.
# =============================================================================
# This is a thin wrapper over lib-signing.sh's sign_digest_manifest function.
# Use it when a caller cannot source bash libraries, such as the Makefile
# `images` target. Run it once, after the last push-image.sh of a build. The
# manifest signature covers the whole manifest. A signature over a
# half-written manifest would still verify, but it would lie about the
# content.
#
# In CI with OIDC (the ACTIONS_ID_TOKEN_REQUEST_URL variable set), the script
# runs `cosign sign-blob --yes`. This does keyless signing and logs to the
# public Rekor log. The offline-verifiable bundle lands at
# <manifest>.sigbundle. Outside CI, the script skips signing and logs a
# message. Local deploys without signing are a supported case for developers.
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
