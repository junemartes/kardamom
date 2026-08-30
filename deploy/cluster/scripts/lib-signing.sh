# shellcheck shell=bash
# =============================================================================
# lib-signing.sh — cosign signing + verification for the deploy digest
# manifest and the images it lists.
# =============================================================================
# This file is sourced, never run directly, by ci-cluster.sh,
# push-image.sh, sign-manifest.sh, deploy.sh, and signing-selftest.sh. It
# installs no traps, and sets no shell options; callers own set -e. Every
# function here has no dependency on lib.sh.
#
# The signing model (infra decision log 2026-08-16, item 1):
#   - In CI, when GitHub Actions provides OIDC, every pushed image is
#     signed keyless by digest. The completed digest manifest is also
#     signed as a blob. Both log to the public Sigstore Rekor log. The
#     trade-off: deploy cadence and image digests become public
#     metadata. The gain: an externally witnessed log whose entries also
#     serve as provenance evidence.
#   - Outside CI, signing is skipped, with a log line. Local deploys are
#     a supported case for developers, like the manifest's :dev-tag
#     fallback in deploy.sh.
#   - A deploy-time signature proves origin, not current health.
#     Verification happens at deploy time (deploy.sh, with
#     KARDAMOM_REQUIRE_SIGNED=1), and continuously in the private repo's
#     image-drift sweep. That sweep verifies the manifest signature
#     before it trusts the manifest as its expected-state anchor.
#
# cosign is pinned to a version and release-binary sha256. The script
# downloads it into a cache directory when it is not already installed,
# and never runs curl piped into bash. The signer and verifier must
# agree on the bundle format, and v3 changed that format. So a
# fail-closed gate cannot accept "whatever cosign happens to be
# installed" on the verify side.

# --- pinned cosign -----------------------------------------------------------
KARDAMOM_COSIGN_VERSION="${KARDAMOM_COSIGN_VERSION:-v3.1.3}"
# sha256 of the cosign-linux-amd64 release binary for the version above,
# from the release's cosign_checksums.txt. Update this together with the
# version pin.
KARDAMOM_COSIGN_SHA256="${KARDAMOM_COSIGN_SHA256:-4629c757b7618056f8ddd7e2625ae9fdd94c0372a65049520bc7d9df9efc7f71}"
KARDAMOM_COSIGN_CACHE="${KARDAMOM_COSIGN_CACHE:-${XDG_CACHE_HOME:-${HOME}/.cache}/kardamom/cosign}"

_signing_log() { echo "==> signing: $*"; }

# --- identity constants ------------------------------------------------
# This is the one place that holds these constants on the public side.
# The private repo's integrity-lib.sh deliberately keeps its own copy,
# so the auditor's trust anchor never comes from the tree it audits.
#
# The keyless certificate identity is the GitHub Actions workflow ref of
# the run that signed, for example:
#   https://github.com/junemartes/kardamom/.github/workflows/cluster-e2e.yml@refs/heads/main
# The default pattern matches the org, repo, and workflows directory. It
# accepts any workflow file and any ref, including PR merge refs.
# Operators can narrow this to one workflow and ref by exporting
# KARDAMOM_CERT_IDENTITY_RE.
signing_defaults() {
  KARDAMOM_CERT_IDENTITY_RE="${KARDAMOM_CERT_IDENTITY_RE:-^https://github\.com/junemartes/kardamom/\.github/workflows/[^@]+@refs/}"
  KARDAMOM_CERT_OIDC_ISSUER="${KARDAMOM_CERT_OIDC_ISSUER:-https://token.actions.githubusercontent.com}"
}

# True when this process can request a GitHub Actions OIDC token. This
# means the workflow granted `id-token: write`, and Actions exported
# ACTIONS_ID_TOKEN_REQUEST_URL. This is the sign-or-skip switch. A runner
# without that grant, and every local shell, skips signing.
signing_active() { [[ -n "${ACTIONS_ID_TOKEN_REQUEST_URL:-}" ]]; }

# Print the path to a usable cosign binary. If none is on PATH, install
# the pinned release into the cache directory. Fails, with a message on
# stderr, rather than falling back to an unverified download.
signing_cosign() {
  local bin
  if bin="$(command -v cosign 2>/dev/null)"; then
    echo "${bin}"
    return 0
  fi
  local cached="${KARDAMOM_COSIGN_CACHE}/${KARDAMOM_COSIGN_VERSION}/cosign"
  if [[ -x "${cached}" ]]; then
    echo "${cached}"
    return 0
  fi
  if [[ "$(uname -s)/$(uname -m)" != "Linux/x86_64" ]]; then
    echo "ERROR: no cosign on PATH and the pinned auto-install only covers linux-amd64." >&2
    echo "       Install cosign ${KARDAMOM_COSIGN_VERSION} for this platform manually." >&2
    return 1
  fi
  local url="https://github.com/sigstore/cosign/releases/download/${KARDAMOM_COSIGN_VERSION}/cosign-linux-amd64"
  local tmp="${cached}.tmp.$$"
  _signing_log "installing pinned cosign ${KARDAMOM_COSIGN_VERSION} -> ${cached}" >&2
  mkdir -p "$(dirname "${cached}")"
  if ! curl -fsSL --retry 3 -o "${tmp}" "${url}"; then
    rm -f "${tmp}"
    echo "ERROR: could not download ${url}" >&2
    return 1
  fi
  # Check the checksum before the binary becomes executable or visible.
  # There is no fallback. A mismatch means a supply-chain problem or a
  # stale pin, and both need a person to fix them.
  if ! echo "${KARDAMOM_COSIGN_SHA256}  ${tmp}" | sha256sum -c - >/dev/null 2>&1; then
    rm -f "${tmp}"
    echo "ERROR: cosign download failed its pinned sha256 (${KARDAMOM_COSIGN_SHA256})." >&2
    echo "       Refusing to install it. If the version pin was just bumped, update" >&2
    echo "       KARDAMOM_COSIGN_SHA256 from the release's cosign_checksums.txt." >&2
    return 1
  fi
  chmod +x "${tmp}"
  mv -f "${tmp}" "${cached}"
  echo "${cached}"
}

# Sign one pushed image, by its digest ref (repo:tag@sha256:...), with
# keyless signing. This is a logged no-op outside CI. In CI, a signing
# failure fails the build, since callers run set -e. Once the workflow
# grants id-token, signing is part of the push contract. Falling back to
# unsigned-but-green would hide the failure. --allow-http-registry: the
# in-cluster registry is plain HTTP on the bridge IP. The signature lands
# in that registry, tag-addressed next to the image, plus a transparency
# entry in the public Rekor log. The Docker Desktop REGISTRY_PUSH_NODE
# push path never reaches this function with signing active; that path
# is local dev by definition, with no OIDC.
sign_pushed_image() {
  local ref="$1" cosign
  if ! signing_active; then
    _signing_log "skip image sign for ${ref} (no CI OIDC — local dev affordance)"
    return 0
  fi
  cosign="$(signing_cosign)" || return 1
  _signing_log "cosign sign (keyless, public Rekor): ${ref}"
  "${cosign}" sign --yes --allow-http-registry "${ref}"
}

# Sign the completed digest manifest as a blob, with keyless signing.
# The Sigstore bundle (certificate, signature, and Rekor proof, all
# offline-verifiable) is written next to it as <manifest>.sigbundle. Call
# this exactly once, after the last push_image of a build. A signature
# over a half-written manifest would still verify, but it would lie.
sign_digest_manifest() {
  local manifest="$1" cosign
  if ! signing_active; then
    _signing_log "skip manifest sign for ${manifest} (no CI OIDC — local dev affordance)"
    return 0
  fi
  if [[ ! -s "${manifest}" ]]; then
    echo "ERROR: refusing to sign a missing/empty digest manifest: ${manifest}" >&2
    return 1
  fi
  cosign="$(signing_cosign)" || return 1
  _signing_log "cosign sign-blob (keyless, public Rekor): ${manifest} -> ${manifest}.sigbundle"
  "${cosign}" sign-blob --yes --bundle "${manifest}.sigbundle" "${manifest}"
}

# Verify the manifest blob signature bundle. Fail closed: a missing
# bundle is a failure, not a skip. The caller only calls this function
# when the operator required signatures, with KARDAMOM_REQUIRE_SIGNED=1.
#
# Test hook (signing-selftest.sh): when KARDAMOM_SIGNING_TEST_KEY names
# a cosign public key, verification uses that explicit key and skips the
# transparency log. This exercises the same code path without CI OIDC.
# Production signing is keyless. This hook logs loudly when active, and
# sits behind an env var the operator controls, like every verification
# parameter (decision log, item 4).
verify_manifest_signature() {
  local manifest="$1" bundle="$1.sigbundle" cosign
  if [[ ! -f "${bundle}" ]]; then
    echo "ERROR: signature bundle not found: ${bundle}" >&2
    echo "       The manifest cannot be verified. Signed manifests come from the CI" >&2
    echo "       push path (ci-images.sh with OIDC); refusing to proceed." >&2
    return 1
  fi
  cosign="$(signing_cosign)" || return 1
  signing_defaults
  if [[ -n "${KARDAMOM_SIGNING_TEST_KEY:-}" ]]; then
    _signing_log "TEST MODE: verifying ${manifest} with explicit key ${KARDAMOM_SIGNING_TEST_KEY} (prod is keyless)"
    "${cosign}" verify-blob --key "${KARDAMOM_SIGNING_TEST_KEY}" \
      --bundle "${bundle}" --insecure-ignore-tlog=true "${manifest}"
    return
  fi
  _signing_log "cosign verify-blob: ${manifest} (identity ~ ${KARDAMOM_CERT_IDENTITY_RE})"
  "${cosign}" verify-blob --bundle "${bundle}" \
    --certificate-identity-regexp "${KARDAMOM_CERT_IDENTITY_RE}" \
    --certificate-oidc-issuer "${KARDAMOM_CERT_OIDC_ISSUER}" \
    "${manifest}"
}

# Verify one image's keyless signature against the same pinned identity.
# This needs registry reachability, which the deploy host has since it
# is about to pull these images, and the public Rekor log.
verify_image_signature() {
  local ref="$1" cosign
  cosign="$(signing_cosign)" || return 1
  signing_defaults
  _signing_log "cosign verify: ${ref}"
  "${cosign}" verify --allow-http-registry \
    --certificate-identity-regexp "${KARDAMOM_CERT_IDENTITY_RE}" \
    --certificate-oidc-issuer "${KARDAMOM_CERT_OIDC_ISSUER}" \
    "${ref}" >/dev/null
}
