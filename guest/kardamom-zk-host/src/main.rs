//! Execute the kardamom guest ELF in SP1's executor (no proving) against a
//! fixture emitted by the validator's `witness_anchoring` test
//! (`KARDAMOM_EMIT_PROVER_FIXTURE=dir cargo test -p kardamom-validator
//! --test witness_anchoring`), and assert the guest's committed public
//! values equal the host-side expectation BYTE FOR BYTE — the guest/host
//! round-trip contract of phase 3c.
//!
//! Usage: kardamom-zk-host [--prove] <fixture-dir> [elf-path]
//! Exit 0 = outputs identical; nonzero = divergence or execution failure.
//!
//! `--prove` generates and VERIFIES a real SP1 core proof instead of just
//! executing — the first actual validity proof of a kardamom block. The
//! proof is written to `<fixture-dir>/proof.bin`. CPU proving of an
//! unpatched ~8M-cycle block is minutes-scale; this mode is a milestone
//! and a benchmark, not the production prover loop.

use std::sync::Arc;

use anyhow::{Context, bail};
use sp1_sdk::ProvingKey;
use sp1_sdk::blocking::{Elf, ProveRequest, Prover, ProverClient};
use sp1_sdk::SP1Stdin;

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1).peekable();
    let prove_mode = args.peek().is_some_and(|a| a == "--prove");
    if prove_mode {
        args.next();
    }
    let dir = args
        .next()
        .context("usage: kardamom-zk-host [--prove] <fixture-dir> [elf-path]")?;
    let elf_path = args.next().unwrap_or_else(|| {
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../kardamom-zk-guest/target/elf-compilation/",
            "riscv64im-succinct-zkvm-elf/release/kardamom-zk-guest"
        )
        .to_string()
    });

    let input = std::fs::read(format!("{dir}/prover-input.rkyv")).context("fixture input")?;
    let expected =
        std::fs::read(format!("{dir}/expected-outputs.bin")).context("fixture expected")?;
    let elf = std::fs::read(&elf_path).with_context(|| format!("guest ELF at {elf_path}"))?;

    let mut stdin = SP1Stdin::new();
    stdin.write_vec(input);

    let client = ProverClient::builder().cpu().build();
    let elf = Elf::Dynamic(Arc::from(elf.into_boxed_slice()));

    if prove_mode {
        let setup_t = std::time::Instant::now();
        let pk = client.setup(elf).context("prover setup")?;
        let setup_dt = setup_t.elapsed();
        let prove_t = std::time::Instant::now();
        let proof = client.prove(&pk, stdin).run().context("core proving")?;
        let prove_dt = prove_t.elapsed();
        let verify_t = std::time::Instant::now();
        client
            .verify(&proof, pk.verifying_key(), None)
            .context("proof verification")?;
        let verify_dt = verify_t.elapsed();

        let got = proof.public_values.as_slice();
        if got != expected.as_slice() {
            bail!("PROVEN public values diverge from the host expectation");
        }
        let proof_path = format!("{dir}/proof.bin");
        proof.save(&proof_path).context("save proof")?;
        println!(
            "PROOF OK: core proof generated, verified, public values identical.\n\
             setup {setup_dt:?}, prove {prove_dt:?}, verify {verify_dt:?}; saved to {proof_path}"
        );
        return Ok(());
    }

    let (public_values, report) = client
        .execute(elf, stdin)
        .run()
        .context("guest execution")?;

    let got = public_values.as_slice();
    if got != expected.as_slice() {
        bail!(
            "guest/host divergence: guest committed {} bytes {:02x?}..., host expected {:02x?}...",
            got.len(),
            &got[..got.len().min(8)],
            &expected[..8]
        );
    }
    println!(
        "round-trip OK: {} bytes of public values identical; {} cycles",
        got.len(),
        report.total_instruction_count()
    );
    Ok(())
}
