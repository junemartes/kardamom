//! Execute the kardamom guest ELF in SP1's executor (no proving) against a
//! fixture emitted by the validator's `witness_anchoring` test
//! (`KARDAMOM_EMIT_PROVER_FIXTURE=dir cargo test -p kardamom-validator
//! --test witness_anchoring`), and check that the guest's committed public
//! values equal the host-side expectation byte for byte: the guest and
//! host must commit identical public values.
//!
//! Usage: kardamom-zk-host [--prove] <fixture-dir> [elf-path]
//! Exit 0 means the outputs are identical; nonzero means divergence or an
//! execution failure.
//!
//! `--prove` generates and verifies a real SP1 core proof instead of just
//! executing: the first actual validity proof of a kardamom block. The
//! proof is written to `<fixture-dir>/proof.bin`. CPU proving of an
//! unpatched block of about 8M cycles takes minutes; this mode is a
//! milestone and a benchmark, not the production prover loop.

use std::sync::Arc;

use anyhow::{bail, Context};
use sp1_sdk::blocking::{CpuProver, Elf, ProveRequest, Prover, ProverClient};
use sp1_sdk::SP1Stdin;
use sp1_sdk::{HashableKey, ProvingKey};

fn main() -> anyhow::Result<()> {
    sp1_sdk::utils::setup_logger();
    let mut args = std::env::args().skip(1).peekable();
    if args.peek().is_some_and(|a| a == "batch") {
        args.next();
        return BatchRun::parse(args)?.run(&ProverClient::builder().cpu().build());
    }

    SingleRun::parse(args)?.run(&ProverClient::builder().cpu().build())
}

/// A guest ELF's default path, by binary name (`kardamom-zk-guest` or
/// `batch`), next to the guest crate.
fn default_elf(bin: &str) -> String {
    format!(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../kardamom-zk-guest/target/elf-compilation/",
            "riscv64im-succinct-zkvm-elf/release/{}"
        ),
        bin
    )
}

/// One single-block execute-or-prove run: a fixture directory and an ELF
/// to run it against.
struct SingleRun {
    prove_mode: bool,
    dir: String,
    elf_path: String,
}

impl SingleRun {
    /// Parse the single-block subcommand's arguments: `[--prove]
    /// <fixture-dir> [elf-path]`. `elf_path` defaults to the guest's
    /// release ELF next to this crate.
    fn parse(mut args: std::iter::Peekable<impl Iterator<Item = String>>) -> anyhow::Result<Self> {
        let prove_mode = args.peek().is_some_and(|a| a == "--prove");
        if prove_mode {
            args.next();
        }
        let dir = args
            .next()
            .context("usage: kardamom-zk-host [--prove] <fixture-dir> [elf-path]")?;
        let elf_path = args
            .next()
            .unwrap_or_else(|| default_elf("kardamom-zk-guest"));
        Ok(Self {
            prove_mode,
            dir,
            elf_path,
        })
    }

    fn run(self, client: &CpuProver) -> anyhow::Result<()> {
        let input =
            std::fs::read(format!("{}/prover-input.rkyv", self.dir)).context("fixture input")?;
        let expected = std::fs::read(format!("{}/expected-outputs.bin", self.dir))
            .context("fixture expected")?;
        let elf = std::fs::read(&self.elf_path)
            .with_context(|| format!("guest ELF at {}", self.elf_path))?;

        let mut stdin = SP1Stdin::new();
        stdin.write_vec(input);
        let elf = Elf::Dynamic(Arc::from(elf.into_boxed_slice()));

        if self.prove_mode {
            return self.run_prove(client, elf, stdin, &expected);
        }
        Self::run_execute(client, elf, stdin, &expected)
    }

    /// Generate and verify a real SP1 proof over the single-block
    /// fixture, check its public values against `expected`, and save it
    /// to `<dir>/proof.bin` (plus the on-chain artifacts, under
    /// `KARDAMOM_GROTH16=1`).
    #[allow(
        clippy::similar_names,
        reason = "the `_t`/`_dt` (start time / elapsed duration) naming pairs are the intentional convention for each of the three timed phases below"
    )]
    fn run_prove(
        &self,
        client: &CpuProver,
        elf: Elf,
        stdin: SP1Stdin,
        expected: &[u8],
    ) -> anyhow::Result<()> {
        let setup_t = std::time::Instant::now();
        let pk = client.setup(elf).context("prover setup")?;
        let setup_dt = setup_t.elapsed();
        let prove_t = std::time::Instant::now();
        let g16 = std::env::var("KARDAMOM_GROTH16").is_ok_and(|v| v == "1");
        let proof = if g16 {
            client
                .prove(&pk, stdin)
                .groth16()
                .run()
                .context("groth16 proving")?
        } else {
            client.prove(&pk, stdin).run().context("core proving")?
        };
        let prove_dt = prove_t.elapsed();
        let verify_t = std::time::Instant::now();
        client
            .verify(&proof, pk.verifying_key(), None)
            .context("proof verification")?;
        let verify_dt = verify_t.elapsed();

        let got = proof.public_values.as_slice();
        if got != expected {
            bail!("PROVEN public values diverge from the host expectation");
        }
        let proof_path = format!("{}/proof.bin", self.dir);
        proof.save(&proof_path).context("save proof")?;
        if g16 {
            std::fs::write(format!("{}/proof-onchain.bin", self.dir), proof.bytes())
                .context("write on-chain proof bytes")?;
            std::fs::write(
                format!("{}/vkey.hex", self.dir),
                pk.verifying_key().bytes32(),
            )
            .context("write vkey")?;
        }
        println!(
            "PROOF OK ({}): setup {setup_dt:?}, prove {prove_dt:?}, verify {verify_dt:?}; \
             saved to {proof_path}{}",
            if g16 {
                "groth16, on-chain-ready"
            } else {
                "core"
            },
            if g16 {
                format!(" (+ proof-onchain.bin, vkey.hex in {})", self.dir)
            } else {
                String::new()
            }
        );
        Ok(())
    }

    /// Execute (no proving) the single-block fixture, and check the
    /// guest's committed public values against `expected` byte for byte.
    fn run_execute(
        client: &CpuProver,
        elf: Elf,
        stdin: SP1Stdin,
        expected: &[u8],
    ) -> anyhow::Result<()> {
        let (public_values, report) = client
            .execute(elf, stdin)
            .run()
            .context("guest execution")?;

        let got = public_values.as_slice();
        if got != expected {
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
}

/// A validated, ordered, non-empty block range `[first, last]`. Blocks
/// are numbered from 1 (block 0 is genesis, never proven).
struct BlockRange {
    first: u64,
    last: u64,
}

impl BlockRange {
    fn new(first: u64, last: u64) -> anyhow::Result<Self> {
        anyhow::ensure!(first >= 1 && last >= first, "bad block range");
        Ok(Self { first, last })
    }
}

/// A batch's expected boundary roots: the first block's pre-state root
/// and the last block's post-state root.
struct BoundaryRoots {
    pre: alloy_primitives::B256,
    post: alloy_primitives::B256,
}

/// One batch execute-or-prove run: a spool directory, the block range to
/// assemble from it, and an ELF to run it against.
struct BatchRun {
    prove_mode: bool,
    spool: String,
    range: BlockRange,
    elf_path: String,
}

impl BatchRun {
    fn parse(mut args: std::iter::Peekable<impl Iterator<Item = String>>) -> anyhow::Result<Self> {
        let prove_mode = args.peek().is_some_and(|a| a == "--prove");
        if prove_mode {
            args.next();
        }
        let usage = "usage: kardamom-zk-host batch [--prove] <spool-dir> <first> <last> [elf-path]";
        let spool = args.next().context(usage)?;
        let first: u64 = args.next().context(usage)?.parse().context("first block")?;
        let last: u64 = args.next().context(usage)?.parse().context("last block")?;
        let elf_path = args.next().unwrap_or_else(|| default_elf("batch"));
        let range = BlockRange::new(first, last)?;
        Ok(Self {
            prove_mode,
            spool,
            range,
            elf_path,
        })
    }

    /// Read and decode every block's spool frame in `self.range`, in
    /// order.
    fn load_spool_frames(&self) -> anyhow::Result<Vec<kardamom_types::ProverInput>> {
        (self.range.first..=self.range.last)
            .map(|n| {
                let frame = std::fs::read(format!("{}/block-{n}/prover-input.rkyv", self.spool))
                    .with_context(|| format!("spool frame for block {n}"))?;
                rkyv::from_bytes::<kardamom_types::ProverInput, rkyv::rancor::Error>(&frame)
                    .with_context(|| format!("decode frame {n}"))
            })
            .collect()
    }

    /// One block's records digest, folded over its tx records.
    fn block_records_digest(input: &kardamom_types::ProverInput) -> alloy_primitives::B256 {
        let mut digest = kardamom_types::BlockRecordsDigest::new(input.boundary.block_number);
        for r in &input.records {
            if let kardamom_types::ProverRecord::Tx { envelope, .. } = r {
                digest.add_tx(&envelope.raw_tx);
            }
        }
        digest.finish()
    }

    /// Read every block's `expected-outputs.bin` in `self.range`
    /// (validating that each one decodes), and return the batch's
    /// boundary roots: the first block's pre-state root and the last
    /// block's post-state root.
    fn expected_boundary_roots(&self) -> anyhow::Result<BoundaryRoots> {
        let mut pre = None;
        let mut post = None;
        for n in self.range.first..=self.range.last {
            let expected_bytes =
                std::fs::read(format!("{}/block-{n}/expected-outputs.bin", self.spool))
                    .with_context(|| format!("expected outputs for block {n}"))?;
            let expected = kardamom_types::PublicOutputs::decode(&expected_bytes)
                .context("expected-outputs layout")?;
            if n == self.range.first {
                pre = Some(expected.pre_state_root);
            }
            post = Some(expected.post_state_root);
        }
        Ok(BoundaryRoots {
            pre: pre.expect("BlockRange guarantees at least one block"),
            post: post.expect("BlockRange guarantees at least one block"),
        })
    }

    fn run(self, client: &CpuProver) -> anyhow::Result<()> {
        use kardamom_types::{batch_records_commitment, BatchProverInput};

        // Assemble the batch input and derive the expected outputs from
        // the spooled per-block frames (the submitter's cross-check; the
        // guest recomputes everything independently).
        let blocks = self.load_spool_frames()?;
        let digests: Vec<_> = blocks.iter().map(Self::block_records_digest).collect();
        let roots = self.expected_boundary_roots()?;
        let expected = kardamom_types::BatchPublicOutputs {
            pre_state_root: roots.pre,
            post_state_root: roots.post,
            first_block: self.range.first,
            last_block: self.range.last,
            records_commitment: batch_records_commitment(digests),
        };

        let batch_input = BatchProverInput { blocks };
        let input_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&batch_input)
            .map_err(|e| anyhow::anyhow!("serialize batch input: {e}"))?;
        let elf = std::fs::read(&self.elf_path)
            .with_context(|| format!("batch ELF at {}", self.elf_path))?;
        let mut stdin = SP1Stdin::new();
        stdin.write_vec(input_bytes.to_vec());

        let out_dir = format!(
            "{}/batch-{}-{}",
            self.spool, self.range.first, self.range.last
        );
        std::fs::create_dir_all(&out_dir)?;
        let elf = Elf::Dynamic(Arc::from(elf.into_boxed_slice()));

        self.prove_or_execute(client, elf, stdin, &expected, &out_dir)
    }

    /// Prove-and-verify or execute the assembled batch input, check its
    /// public values against `expected`, and save the artifacts to
    /// `out_dir`.
    fn prove_or_execute(
        &self,
        client: &CpuProver,
        elf: Elf,
        stdin: SP1Stdin,
        expected: &kardamom_types::BatchPublicOutputs,
        out_dir: &str,
    ) -> anyhow::Result<()> {
        if self.prove_mode {
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
            proof
                .save(format!("{out_dir}/proof.bin"))
                .context("save proof")?;
            println!(
                "BATCH PROOF OK: blocks {}..={}, prove {dt:?}; saved to {out_dir}",
                self.range.first, self.range.last
            );
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
                "BATCH round-trip OK: blocks {}..={}, {} bytes public values, {} cycles",
                self.range.first,
                self.range.last,
                public_values.as_slice().len(),
                report.total_instruction_count()
            );
        }
        Ok(())
    }
}
