# Vendored signet-libmdbx

## Upstream

- Crate: `signet-libmdbx` 0.8.3 from crates.io.
- Archive SHA-256: `bd334ebaa7603e0ba8c9d701c883b3d264232e539860b6ff751c94aa959f6f0d`.
- The root `Cargo.toml` uses this directory through `[patch.crates-io]`.
- The native code comes from `signet-mdbx-sys` on crates.io. The stored format does not change.

## Why this copy exists

MDBX frees a transaction on every commit result except `MDBX_THREAD_MISMATCH`
(`mdbx.h`). The upstream wrapper does not mark the handle ended when the commit
fails. Its `Drop` then aborts the freed handle. `commit(self)` consumes the
transaction, so a caller cannot stop the second abort.

## Changed files

- `src/tx/access.rs`: a new default method `end_failed_commit` on the pointer
  trait. It calls `mark_committed()` on every error except `MDBX_THREAD_MISMATCH`.
- `src/tx/impl.rs`: each `commit_inner` (the `PtrSync` path and the `PtrUnsync`
  path) adds `.inspect_err(|error| self.txn.end_failed_commit(error))`.

## Removed from the archive

The build does not use these files, so this copy does not keep them:

- `benches/`, `tests/`, and the `[[test]]`, `[[bench]]`, dev-dependency and
  profile sections of `Cargo.toml`.
- `.claude/`, `.github/`, `CLAUDE.md`, `CONTRIBUTING.md`, `Dockerfile`,
  `dependabot.yml`, `clippy.toml`, `rustfmt.toml`, `.gitignore`.
- `Cargo.lock`, `Cargo.toml.orig`, `.cargo_vcs_info.json`.

## Test and removal

The real state-volume ENOSPC component test uses the `PtrSync` writer:
`just test-component state-enospc`. It requires error 28, no new snapshot or
receipt, the exact prior state after a reopen, and a good replay after space is
free. Remove this copy when an upstream release has the same cleanup and passes
the same failure and normal-commit checks.
