# kardamom-zk-guest

`kardamom-zk-guest` is the SP1 zkVM guest program. The spec is [`docs/agents/no-std-exec-core-spec.md`](../../docs/agents/no-std-exec-core-spec.md).

The guest does these steps:

1. It reads one rkyv `ProverInput` frame.
2. It runs `execute_block_anchored` of the exec core. The live validator runs the same monomorphized function in its stateless re-execution.
3. It commits the 104-byte `PublicOutputs` tuple.

## Building

The build needs the SP1 toolchain. The operator installs it with `sp1up`. This crate was scaffolded against SP1 v6.4.0 and the rustup toolchain `succinct`.

```
cargo prove build
```

The crate is outside the Cargo workspace on purpose.

- It has its own lockfile, which is committed. The alloy versions match the workspace versions.
- It has its own toolchain and its own target.
- The `succinct` rustc trails stable. A `cargo update` here needs matching `--precise` pins.
- The execution crates are path dependencies and are not modified. The live validator and the guest therefore run one code path.

## Precompile patches

`Cargo.toml` pins three SP1 precompile accelerator patches: `sha3`, `sha2` and `k256`. They are the set of the rsp reth block prover.

- They cut the cycle count by about 13.5 times (7.9M to 587k for an anchored block with 3 transactions).
- Keccak dominates the cycle count, because the guest hashes every witness trie node.
- The precompiles are drop-in. The results are identical.
- The lower cycle count makes CPU groth16 proving practical.

A change to the guest invalidates the vkey and every committed proof fixture.
