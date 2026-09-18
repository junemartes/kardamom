# Kardamom

An Ethereum rollup framework. The Rust workspace is under `crates/`, the zk guest under
`guest/`, the Java sealer under `cluster/sealer-service/`, Solidity under `contracts/`.
See `README.md` for the build and `docs/failure-modes.md` for the failure model.

## Style

Every change follows `docs/STYLE.md`. Read it before you write code. The sixteen rules
there are the review bar for this repository.

Before you open a pull request, or push a branch that a pull request will use:

1. Run `just style`. It must pass with no warnings.
2. Read the diff against the judgment rules in `docs/STYLE.md`: self-contained comments,
   no mutex where a channel or ownership works, no `dyn`, no defensive checks, methods
   instead of standalone functions, no nested loops, no duplicated shape.
3. Say in the pull request which rules the change touched and how.

A pull request that fails `just style` is not ready.

## Version control

This repository uses jj. There is no branch protection. `gh pr merge --auto` merges at
once. Commits are immutable after push; use a merge commit to fix a pushed change.
