# shellcheck shell=bash
# =============================================================================
# lib-signing.sh — cosign signing + verification for the deploy digest
# manifest and the images it lists (attested-identity P0.5).
# =============================================================================
# SOURCED (never executed) by ci-cluster.sh, push-image.sh, sign-manifest.sh,
# deploy.sh and signing-selftest.sh. Installs no traps, sets no shell options
# (callers own set -e); every function is dependency-free of lib.sh.
#
# The model (attested-identity plan P0.5; infra decision log 2026-08-16 #1):
#   - In CI (GitHub Actions with OIDC available) every pushed image is signed
#     KEYLESS by digest, and the completed digest manifest is signed as a
#     blob, both logged to the PUBLIC Sigstore Rekor. Accepted trade: deploy
#     cadence and image digests become public metadata; gained: an
#     externally-witnessed log whose entries double as provenance evidence.
#   - Outside CI, signing is SKIPPED with a log line. Local deploys are a
#     documented dev affordance, exactly like the manifest's :dev-tag
#     fallback in deploy.sh.
#   - A deploy-time signature is a birth certificate, not a pulse:
#     verification happens at deploy time (deploy.sh, KARDAMOM_REQUIRE_SIGNED
#     =1) AND continuously in the private-repo image-drift sweep, which
#     verifies the manifest signature before trusting it as its
#     expected-state anchor.
#
# cosign is PINNED — version + release-binary sha256, downloaded into a cache
# dir when not already installed (never curl|bash): signer and verifier must
# agree on bundle formats, and v3 changed them, so "whatever cosign is lying
# around" is not acceptable on the verify side of a fail-closed gate.

# --- pinned cosign -----------------------------------------------------------
KARDAMOM_COSIGN_VERSION="${KARDAMOM_COSIGN_VERSION:-v3.1.3}"
# sha256 of the cosign-linux-amd64 release binary for the version above (from
# the release's cosign_checksums.txt). MUST be updated together with the
# version pin.
KARDAMOM_COSIGN_SHA256="${KARDAMOM_COSIGN_SHA256:-4629c757b7618056f8ddd7e2625ae9fdd94c0372a65049520bc7d9df9efc7f71}"
KARDAMOM_COSIGN_CACHE="${KARDAMOM_COSIGN_CACHE:-${XDG_CACHE_HOME:-${HOME}/.cache}/kardamom/cosign}"

_signing_log() { echo "==> signing: $*"; }

# --- identity constants (THE one public-side place; see also the private
# repo's integrity-lib.sh, which deliberately carries its own copy so the
# auditor's trust anchor never comes from the tree it audits) ----------------
# The keyless certificate identity is the GitHub Actions WORKFLOW REF of the
# run that signed, e.g.
#   https://github.com/junemartes/kardamom/.github/workflows/cluster-e2e.yml@refs/heads/main
# The default regexp pins org/repo + the workflows dir and accepts any
# workflow file and any ref (PR merge refs included). Operators can tighten
# to one workflow/ref by exporting KARDAMOM_CERT_IDENTITY_RE.
signing_defaults() {
  KARDAMOM_CERT_IDENTITY_RE="${KARDAMOM_CERT_IDENTITY_RE:-^https://github\.com/junemartes/kardamom/\.github/workflows/[^@]+@refs/}"
  KARDAMOM_CERT_OIDC_ISSUER="${KARDAMOM_CERT_OIDC_ISSUER:-https://token.actions.githubusercontent.com}"
}

# True when this process can mint a GitHub Actions OIDC token — i.e. the
# workflow granted `id-token: write` (then Actions exports
# ACTIONS_ID_TOKEN_REQUEST_URL). This is the sign/skip switch: a runner
# without the grant, and every local shell, skips signing.
signing_active() { [[ -n "${ACTIONS_ID_TOKEN_REQUEST_URL:-}" ]]; }

# Print the path of a usable cosign binary, installing the pinned release
# into the cache dir if none is on PATH. Fails (nonzero, message on stderr)
# rather than falling back to an unverified download.
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
  # Checksum gate BEFORE the binary becomes executable/visible. No fallback:
  # a mismatch means a supply-chain problem or a stale pin, both human work.
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

# Keyless-sign one pushed image by its digest ref (repo:tag@sha256:...).
# No-op with a log line outside CI. In CI a signing failure FAILS the build
# (callers run set -e): once the workflow granted id-token, signing is part
# of the push contract — falling back to unsigned-but-green would be silent.
# --allow-http-registry: the in-cluster registry is plain HTTP on the bridge
# IP; the signature lands in that registry (tag-addressed alongside the
# image) and the transparency entry in the PUBLIC Rekor. (The Docker-Desktop
# REGISTRY_PUSH_NODE push path never reaches here with signing active — it is
# local-dev by definition, no OIDC.)
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

# Keyless-sign the COMPLETED digest manifest as a blob; the Sigstore bundle
# (cert + signature + Rekor proof, offline-verifiable) is written alongside
# it as <manifest>.sigbundle. Call this exactly once, after the LAST
# push_image of a build — a bundle over a half-written manifest would verify
# and still lie.
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

# Verify the manifest blob signature bundle. Fail closed: a missing bundle is
# a failure, not a skip — the caller only invokes this when the operator
# demanded signatures (KARDAMOM_REQUIRE_SIGNED=1).
#
# TEST HOOK (signing-selftest.sh): with KARDAMOM_SIGNING_TEST_KEY set to a
# cosign public key, verification uses that explicit key and skips the
# transparency log — exercising this exact code path without CI OIDC. Prod
# is KEYLESS; the hook is announced loudly and sits behind an env var the
# operator controls, like every verification parameter (decision log #4).
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
# Needs registry reachability (the deploy host has it — it is about to pull
# these very images) and the public Rekor.
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
