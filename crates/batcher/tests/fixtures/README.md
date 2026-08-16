# Real SP1 proof fixtures (PR 5 — on-chain verification e2e)

`optimistic_proof_e2e.rs` verifies a REAL SP1 Groth16 proof against the
REAL vendored SP1 verifier (contracts/test/sp1/, circuit v6.1.0) on anvil —
the full false-claim → challenge → on-chain-accept path, plus the
honest-claim-can't-be-griefed opposite.

The test reads committed fixtures (fast, reproducible, CI-runnable); it
does NOT prove at test time. Regenerate them after a guest or SP1 change:

    # Single-block frame (block 1) from the anchoring pipeline test:
    KARDAMOM_EMIT_PROVER_FIXTURE=/tmp/fx cargo test -p kardamom-validator \
        --test witness_anchoring
    # Real groth16 proof + on-chain bytes + vkey (needs the SP1 toolchain
    # + circuit artifacts; ~minutes on CPU):
    cd guest/kardamom-zk-guest && cargo prove build && cd ../kardamom-zk-host
    KARDAMOM_GROTH16=1 cargo run --release -- --prove /tmp/fx
    # Copy into place:
    cp /tmp/fx/expected-outputs.bin  <repo>/crates/batcher/tests/fixtures/public-values.bin
    cp /tmp/fx/proof-onchain.bin     <repo>/crates/batcher/tests/fixtures/proof.bin
    cp /tmp/fx/vkey.hex              <repo>/crates/batcher/tests/fixtures/vkey.hex

The proof binds to the guest ELF's vkey; a guest change invalidates it.
