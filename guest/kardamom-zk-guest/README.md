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

SP1's precompile accelerator patches (sha3/k256/bls12-381) are not yet
pinned — perf optimization, chosen against the prover SDK version when the
harness lands (3c step 2).
