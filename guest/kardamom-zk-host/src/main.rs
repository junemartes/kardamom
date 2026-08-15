//! Execute the kardamom guest ELF in SP1's executor (no proving) against a
//! fixture emitted by the validator's `witness_anchoring` test
//! (`KARDAMOM_EMIT_PROVER_FIXTURE=dir cargo test -p kardamom-validator
//! --test witness_anchoring`), and assert the guest's committed public
//! values equal the host-side expectation BYTE FOR BYTE — the guest/host
//! round-trip contract of phase 3c.
//!
//! Usage: kardamom-zk-host <fixture-dir> [elf-path]
//! Exit 0 = outputs identical; nonzero = divergence or execution failure.

use std::sync::Arc;

use anyhow::{Context, bail};
use sp1_sdk::blocking::{Elf, Prover, ProverClient};
use sp1_sdk::SP1Stdin;

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let dir = args.next().context("usage: kardamom-zk-host <fixture-dir> [elf-path]")?;
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
    let (public_values, report) = client
        .execute(Elf::Dynamic(Arc::from(elf.into_boxed_slice())), stdin)
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
