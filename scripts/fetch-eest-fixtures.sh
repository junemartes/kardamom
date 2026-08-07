#!/usr/bin/env bash
# Fetch the pinned execution-spec-tests fixture release (W1 of
# docs/agents/l1-client-suite-port-spec.md).
#
# THE fixture pin lives here, and only here: bump EEST_TAG together with
# `SPEC_ID` in crates/exec-core/src/block_env.rs and `FORK` in
# crates/exec-core/tests/eest_state.rs (the fork-bump procedure in the spec's
# hardfork-policy section).
#
# Usage: scripts/fetch-eest-fixtures.sh [dest-root]
#   Downloads + unpacks to <dest-root>/<tag>/ (default
#   ~/.cache/kardamom/eest), skipping work if already present.
#   Prints the fixture directory path on stdout as the last line.
set -euo pipefail

EEST_TAG="tests@v20.0.1"

dest_root="${1:-${HOME}/.cache/kardamom/eest}"
dest="${dest_root}/${EEST_TAG}"

if [[ -d "${dest}/fixtures" ]]; then
    echo "eest fixtures ${EEST_TAG} already present" >&2
    echo "${dest}/fixtures"
    exit 0
fi

# '@' must be URL-encoded in the release-asset path.
url="https://github.com/ethereum/execution-specs/releases/download/${EEST_TAG/@/%40}/fixtures.tar.gz"
mkdir -p "${dest}"
echo "fetching ${url}" >&2
curl -fsSL --retry 3 "${url}" -o "${dest}/fixtures.tar.gz"
# Only state_tests (+ release metadata) are consumed; the blockchain/engine
# formats need a header chain / Engine API kardamom does not have (spec
# non-goals) and would bloat the CI cache ~6x (8.1G → 1.5G).
tar -xzf "${dest}/fixtures.tar.gz" -C "${dest}" \
    "fixtures/state_tests" "fixtures/.meta"
rm "${dest}/fixtures.tar.gz"
[[ -d "${dest}/fixtures" ]] || {
    echo "unexpected tarball layout under ${dest}" >&2
    exit 1
}
echo "${dest}/fixtures"
