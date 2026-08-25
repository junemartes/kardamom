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
    if args.peek().is_some_and(|a| a == "batch") {
        args.next();
        return batch_main(args);
    }
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


fn batch_main(
    mut args: std::iter::Peekable<impl Iterator<Item = String>>,
) -> anyhow::Result<()> {
    use kardamom_types::{
        BatchProverInput, BatchPublicOutputs, BlockRecordsDigest, ProverInput, ProverRecord,
        PublicOutputs, batch_records_commitment,
    };

    let prove_mode = args.peek().is_some_and(|a| a == "--prove");
    if prove_mode {
        args.next();
    }
    let usage = "usage: kardamom-zk-host batch [--prove] <spool-dir> <first> <last> [elf-path]";
    let spool = args.next().context(usage)?;
    let first: u64 = args.next().context(usage)?.parse().context("first block")?;
    let last: u64 = args.next().context(usage)?.parse().context("last block")?;
    let elf_path = args.next().unwrap_or_else(|| {
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../kardamom-zk-guest/target/elf-compilation/",
            "riscv64im-succinct-zkvm-elf/release/batch"
        )
        .to_string()
    });
    anyhow::ensure!(first >= 1 && last >= first, "bad block range");

    // Assemble the batch input + derive the expected outputs from the
    // spooled per-block frames (the submitter's cross-check; the guest
    // recomputes everything independently).
    let mut blocks = Vec::new();
    let mut digests = Vec::new();
    let mut pre_root = None;
    let mut post_root = None;
    for n in first..=last {
        let frame = std::fs::read(format!("{spool}/block-{n}/prover-input.rkyv"))
            .with_context(|| format!("spool frame for block {n}"))?;
        let input: ProverInput = rkyv::from_bytes::<ProverInput, rkyv::rancor::Error>(&frame)
            .with_context(|| format!("decode frame {n}"))?;
        let expected_bytes = std::fs::read(format!("{spool}/block-{n}/expected-outputs.bin"))
            .with_context(|| format!("expected outputs for block {n}"))?;
        let expected =
            PublicOutputs::decode(&expected_bytes).context("expected-outputs layout")?;
        if n == first {
            pre_root = Some(expected.pre_state_root);
        }
        post_root = Some(expected.post_state_root);
        let mut digest = BlockRecordsDigest::new(n);
        for r in &input.records {
            if let ProverRecord::Tx { envelope, .. } = r {
                digest.add_tx(&envelope.raw_tx);
            }
        }
        digests.push(digest.finish());
        blocks.push(input);
    }
    let expected = BatchPublicOutputs {
        pre_state_root: pre_root.expect("nonempty range"),
        post_state_root: post_root.expect("nonempty range"),
        first_block: first,
        last_block: last,
        records_commitment: batch_records_commitment(digests),
    };

    let batch_input = BatchProverInput { blocks };
    let input_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&batch_input)
        .map_err(|e| anyhow::anyhow!("serialize batch input: {e}"))?;
    let elf = std::fs::read(&elf_path).with_context(|| format!("batch ELF at {elf_path}"))?;
    let mut stdin = SP1Stdin::new();
    stdin.write_vec(input_bytes.to_vec());

    let out_dir = format!("{spool}/batch-{first}-{last}");
    std::fs::create_dir_all(&out_dir)?;
    let client = ProverClient::builder().cpu().build();
    let elf = Elf::Dynamic(Arc::from(elf.into_boxed_slice()));

    if prove_mode {
        let pk = client.setup(elf).context("prover setup")?;
        let t = std::time::Instant::now();
        let proof = client.prove(&pk, stdin).run().context("batch proving")?;
        let dt = t.elapsed();
        client
            .verify(&proof, pk.verifying_key(), None)
            .context("proof verification")?;
        anyhow::ensure!(
            proof.public_values.as_slice() == expected.encode(),
            "PROVEN batch public values diverge from the spool expectation"
        );
        std::fs::write(format!("{out_dir}/public-values.bin"), expected.encode())?;
        proof.save(format!("{out_dir}/proof.bin")).context("save proof")?;
        println!("BATCH PROOF OK: blocks {first}..={last}, prove {dt:?}; saved to {out_dir}");
    } else {
        let (public_values, report) = client
            .execute(elf, stdin)
            .run()
            .context("batch guest execution")?;
        anyhow::ensure!(
            public_values.as_slice() == expected.encode(),
            "batch guest/host divergence"
        );
        std::fs::write(format!("{out_dir}/public-values.bin"), expected.encode())?;
        println!(
            "BATCH round-trip OK: blocks {first}..={last}, {} bytes public values, {} cycles",
            public_values.as_slice().len(),
            report.total_instruction_count()
        );
    }
    Ok(())
}