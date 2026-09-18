#!/usr/bin/env bash
# Mechanical checks for docs/STYLE.md. Exit 0 when every check passes.
# The judgment rules (R1, R5, R6, R7, R9, R10, R14, R15, R16) are not
# checked here. Read the diff against docs/STYLE.md for those.
set -uo pipefail
cd "$(dirname "$0")/.."

failed=0
step() {
    echo "==> $1"
    if ! "${@:2}"; then
        echo "FAILED: $1" >&2
        failed=1
    fi
}

# R11, R2 (too_many_lines at the threshold in clippy.toml), R8 (unreachable_pub).
step "clippy pedantic" cargo clippy --workspace --all-targets --all-features --locked -- \
    -D warnings -W clippy::pedantic -D unreachable_pub

step "rustfmt" cargo fmt --all -- --check

# R9, R13, R6, R11: patterns that a lint cannot express. Test files are exempt.
forbidden() {
    local hits
    hits="$(grep -rnE --include='*.rs' \
        -e 'debug_assert!' -e '\.max\(1\)' -e '\bdyn\b' -e 'allow\(clippy::too_many_arguments\)' \
        crates guest 2>/dev/null \
        | grep -vE '/tests?/|_tests?\.rs:|/tests\.rs:|/test_support' || true)"
    # `Box<` and `dyn` split over two lines by rustfmt: report the file and
    # the line of the `Box<`.
    local wrapped
    wrapped="$(grep -rnE --include='*.rs' -A1 -e 'Box<$' crates guest 2>/dev/null \
        | grep -E -B1 '^[^:]+-[0-9]+-\s*dyn ' \
        | grep -E ':[0-9]+:' \
        | grep -vE '/tests?/|_tests?\.rs:|/tests\.rs:|/test_support' || true)"
    hits="${hits}${wrapped:+$'\n'$wrapped}"
    if [ -n "$hits" ]; then
        echo "$hits"
        return 1
    fi
}
step "forbidden patterns (debug_assert!, .max(1), dyn, allow(too_many_arguments))" forbidden

if [ "$failed" -ne 0 ]; then
    echo "style check failed. See docs/STYLE.md." >&2
    exit 1
fi
echo "style check passed."
