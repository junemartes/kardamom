# Pinned dependency patches

`signet-libmdbx` is the unmodified 0.8.3 crates.io archive except for the commit
error cleanup in `src/tx/access.rs` and `src/tx/impl.rs`, plus its note in
`CLAUDE.md`. The archive SHA-256 is
`bd334ebaa7603e0ba8c9d701c883b3d264232e539860b6ff751c94aa959f6f0d`.
The upstream MIT and Apache licenses remain in the package.

MDBX ends a transaction when commit returns an error other than thread mismatch.
The wrapper must mark that handle ended before propagating the error, so Drop
does not abort it again. Both synchronized and unsynchronized commit paths use
this rule. The success path keeps its existing completion marker. The native
MDBX dependency and stored format are unchanged.

The real state-volume ENOSPC component test exercises the synchronized writer:
`just test-component state-enospc`. It requires error 28, no new snapshot or receipt,
exact prior state after reopening, and successful replay after space is freed.
Remove the patch once an upstream release contains equivalent cleanup and passes
the same failure and normal-commit checks.
