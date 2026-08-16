# kardamom-zk-guest

The SP1 zkVM guest program (spec: `docs/agents/no-std-exec-core-spec.md`,
phase 3c). Reads one rkyv `ProverInput` frame, runs the exec core's
`execute_block_anchored` — the same monomorphized function the live
validator's stateless re-execution runs — and commits the 104-byte
`PublicOutputs` tuple.

## Building

Requires the SP1 toolchain (operator-installed: `sp1up`; this crate was
scaffolded against SP1 v6.4.0 / rustup toolchain `succinct`):

```
cargo prove build
```

Deliberately OUTSIDE the workspace: own lockfile (committed — alloy pinned
to the workspace's proven versions; the succinct rustc trails stable, so
`cargo update` here needs matching `--precise` pins), own toolchain, own
target. The execution crates are path dependencies, UNMODIFIED — the
one-code-path invariant.

SP1 precompile accelerator patches (sha3/sha2/k256, the rsp reth-block set,
pinned in Cargo.toml) cut the cycle count ~13.5x (7.9M -> 587k for a
3-tx anchored block) — keccak dominates because every witness trie node is
hashed. Correctness is identical (precompiles are drop-in); the win is what
makes CPU groth16 proving tractable. A guest change invalidates the vkey
and any committed proof fixture.
