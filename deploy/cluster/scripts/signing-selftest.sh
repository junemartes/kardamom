#!/usr/bin/env bash
# =============================================================================
# signing-selftest.sh — self-test for the P0.5 signing/verification wiring.
# =============================================================================
# Exercises lib-signing.sh's REAL code paths without CI OIDC:
#
#   1. local no-op contract: outside CI, sign_pushed_image /
#      sign_digest_manifest skip with a log line and rc 0 (local deploys are
#      a documented dev affordance — this run must not need a network or a
#      registry, let alone credentials);
#   2. pinned-cosign resolution (PATH, cache, or checksum-verified download);
#   3. verify round trip with a LOCALLY-GENERATED key pair: cosign
#      generate-key-pair + an offline key-based sign-blob stands in for the
#      CI keyless signer, then verify_manifest_signature — the exact function
#      deploy.sh's gate calls — verifies via its KARDAMOM_SIGNING_TEST_KEY
#      hook. PROD IS KEYLESS (Fulcio cert + public Rekor); the key pair here
#      only lets the verify path run hermetically.
#   4. fail-closed: a tampered manifest and a missing bundle must both be
#      REFUSED by verify_manifest_signature;
#   5. deploy.sh refusal: with KARDAMOM_REQUIRE_SIGNED=1 and an unsigned
#      manifest, deploy.sh must exit nonzero BEFORE any nomad job run
#      (guarded: skipped when no nomad CLI is on PATH — deploy.sh checks for
#      it before the gate).
#
# Usage: deploy/cluster/scripts/signing-selftest.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/cluster/scripts/lib-signing.sh
source "${SCRIPT_DIR}/lib-signing.sh"

TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

pass=0
ok()   { echo "PASS: $*"; pass=$((pass + 1)); }
die()  { echo "SELFTEST FAIL: $*" >&2; exit 1; }

# --- 1. local no-op: signing skips cleanly without CI OIDC -------------------
# (env -u: even if THIS run happens inside a workflow that granted id-token,
# the no-op case must test the no-OIDC branch.)
out="$(env -u ACTIONS_ID_TOKEN_REQUEST_URL bash -c "
  source '${SCRIPT_DIR}/lib-signing.sh'
  sign_pushed_image 'registry.example/kardamom-x:dev@sha256:$(printf '0%.0s' {1..64})'
  sign_digest_manifest '${TMP}/absent-manifest'
")" || die "signing functions must be rc-0 no-ops without CI OIDC"
grep -q "skip image sign" <<<"${out}" || die "expected a 'skip image sign' log line, got: ${out}"
grep -q "skip manifest sign" <<<"${out}" || die "expected a 'skip manifest sign' log line, got: ${out}"
ok "outside CI, image + manifest signing are logged no-ops"

# --- 2. pinned cosign resolves ----------------------------------------------
COSIGN="$(signing_cosign)" || die "signing_cosign could not provide a cosign binary"
"${COSIGN}" version >/dev/null 2>&1 || die "resolved cosign is not runnable: ${COSIGN}"
ok "cosign resolved: ${COSIGN}"

# --- 3. key round trip through the real verify path --------------------------
cd "${TMP}"
export COSIGN_PASSWORD=""
"${COSIGN}" generate-key-pair >/dev/null 2>&1 || die "cosign generate-key-pair failed"
MANIFEST="${TMP}/images.digests"
printf 'aeron 192.168.56.10:5000/kardamom-aeron:dev@sha256:%s\n' "$(printf '1%.0s' {1..64})" >"${MANIFEST}"
printf 'ingress 192.168.56.10:5000/kardamom-ingress:dev@sha256:%s\n' "$(printf '2%.0s' {1..64})" >>"${MANIFEST}"
# Offline key-based stand-in for the CI keyless sign_digest_manifest (no
# transparency-log upload: hermetic, and the public Rekor is not a test
# dumping ground). Prod signing is sign_digest_manifest's keyless call.
"${COSIGN}" sign-blob --yes --key "${TMP}/cosign.key" \
  --bundle "${MANIFEST}.sigbundle" \
  --use-signing-config=false --tlog-upload=false \
  "${MANIFEST}" >/dev/null 2>&1 || die "key-based sign-blob failed"

export KARDAMOM_SIGNING_TEST_KEY="${TMP}/cosign.pub"
verify_manifest_signature "${MANIFEST}" >/dev/null 2>&1 \
  || die "verify_manifest_signature rejected a correctly signed manifest"
ok "verify_manifest_signature accepts the signed manifest (explicit-key test mode)"

# --- 4a. fail closed: tampered manifest --------------------------------------
cp "${MANIFEST}" "${MANIFEST}.orig"
printf 'evil 192.168.56.10:5000/evil:dev@sha256:%s\n' "$(printf '3%.0s' {1..64})" >>"${MANIFEST}"
if verify_manifest_signature "${MANIFEST}" >/dev/null 2>&1; then
  die "verify_manifest_signature ACCEPTED a tampered manifest"
fi
ok "tampered manifest is refused"
mv "${MANIFEST}.orig" "${MANIFEST}"

# --- 4b. fail closed: missing bundle -----------------------------------------
rm -f "${MANIFEST}.sigbundle"
if verify_manifest_signature "${MANIFEST}" >/dev/null 2>&1; then
  die "verify_manifest_signature ACCEPTED a manifest with no signature bundle"
fi
ok "missing signature bundle is refused"

# --- 5. deploy.sh refuses an unsigned deploy when the gate is on -------------
if command -v nomad >/dev/null 2>&1; then
  rc=0
  out="$(cd "${SCRIPT_DIR}/.." \
    && KARDAMOM_REQUIRE_SIGNED=1 DIGEST_MANIFEST="${MANIFEST}" \
       ./scripts/deploy.sh 2>&1)" || rc=$?
  [[ "${rc}" -ne 0 ]] || die "deploy.sh proceeded with KARDAMOM_REQUIRE_SIGNED=1 and no signature"
  grep -q "refusing to" <<<"${out}" || die "deploy.sh refusal did not state its reason; output: ${out}"
  # It must have died at the gate, before any job run.
  if grep -q "nomad run" <<<"${out}"; then
    die "deploy.sh reached a nomad job run despite failing verification"
  fi
  ok "deploy.sh (gate on) refuses an unsigned manifest before any job run"
else
  echo "SKIP: nomad CLI not on PATH — deploy.sh refusal case not run"
fi

echo
echo "signing-selftest PASSED (${pass} checks). Reminder: production signing is"
echo "KEYLESS (CI OIDC -> Fulcio cert, public Rekor); the local key pair above"
echo "exists only to drive the verify code path hermetically."
