#!/usr/bin/env bash
# Fetch the pinned execution-spec-tests fixture release. See W1 of
# docs/agents/l1-client-suite-port-spec.md.
#
# This file holds the only fixture pin. When you bump EEST_TAG, also update
# `SPEC_ID` in crates/exec-core/src/block_env.rs and `FORK` in
# crates/exec-core/tests/eest_state.rs. Follow the fork-bump procedure in the
# spec's hardfork-policy section.
#
# Usage: scripts/fetch-eest-fixtures.sh [dest-root]
#   Downloads and unpacks fixtures to <dest-root>/<tag>/. The default root is
#   ~/.cache/kardamom/eest. The script skips the download if files exist.
#   It prints the fixture directory path as the last line of output.
set -euo pipefail

EEST_TAG="tests@v20.0.1"

dest_root="${1:-${HOME}/.cache/kardamom/eest}"
dest="${dest_root}/${EEST_TAG}"

if [[ -d "${dest}/fixtures" ]]; then
    echo "eest fixtures ${EEST_TAG} already present" >&2
    echo "${dest}/fixtures"
    exit 0
fi

# URL-encode '@' in the release-asset path.
url="https://github.com/ethereum/execution-specs/releases/download/${EEST_TAG/@/%40}/fixtures.tar.gz"
mkdir -p "${dest}"
echo "fetching ${url}" >&2
curl -fsSL --retry 3 "${url}" -o "${dest}/fixtures.tar.gz"
# This script keeps only state_tests and release metadata. The blockchain and
# engine formats need a header chain and an Engine API. Kardamom does not
# have these (see the spec's non-goals). The full download is about 8.1 GB.
# The filtered set is about 1.5 GB.
tar -xzf "${dest}/fixtures.tar.gz" -C "${dest}" \
    "fixtures/state_tests" "fixtures/.meta"
rm "${dest}/fixtures.tar.gz"
[[ -d "${dest}/fixtures" ]] || {
    echo "unexpected tarball layout under ${dest}" >&2
    exit 1
}
echo "${dest}/fixtures"
