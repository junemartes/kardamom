//! Shared per-block record processing between the single-block guest
//! (`src/main.rs`) and the batch guest (`src/bin/batch.rs`).

use alloy_primitives::B256;
use alloy_rlp::Decodable;
use kardamom_exec_core::block_env::ExecEnv;
use kardamom_exec_core::exec_types::TxIndex;
use kardamom_exec_core::stateless::{execute_block_anchored, AnchoredBlockOutput, BufferedRecord};
use kardamom_types::{BlockRecordsDigest, ProverInput, ProverRecord};

/// One block's records, decoded from the wire and ready for execution,
/// paired with the digest folded from them as they were built. Both
/// guest binaries (`main.rs`, `bin/batch.rs`) build one of these per
/// block, so they share this one conversion.
pub struct GuestBlock {
    pub digest: BlockRecordsDigest,
    pub records: Vec<BufferedRecord>,
}

/// The output of one [`GuestBlock::run`]: the anchored execution result,
/// paired with the block's records digest.
pub struct GuestRun {
    pub anchored: AnchoredBlockOutput,
    pub records_digest: B256,
}

impl GuestBlock {
    /// Convert one block's [`ProverRecord`]s into [`BufferedRecord`]s
    /// ready for execution, folding each tx's raw bytes into a
    /// [`BlockRecordsDigest`] as they go.
    #[must_use]
    pub fn from_records(block_number: u64, records: Vec<ProverRecord>) -> Self {
        let mut digest = BlockRecordsDigest::new(block_number);
        let records = records
            .into_iter()
            .map(|r| match r {
                ProverRecord::Tx {
                    tx_idx,
                    envelope,
                    position,
                } => {
                    digest.add_tx(&envelope.raw_tx);
                    BufferedRecord::Tx {
                        tx_idx: TxIndex(tx_idx),
                        envelope,
                        position,
                    }
                }
                ProverRecord::Deposit {
                    tx_idx,
                    deposit,
                    position,
                } => BufferedRecord::Deposit {
                    tx_idx: TxIndex(tx_idx),
                    deposit,
                    position,
                },
            })
            .collect();
        Self { digest, records }
    }

    /// Run one block through [`execute_block_anchored`]: rebuild the
    /// records and digest, rebuild `ExecEnv` from the boundary and chain
    /// id, decode the published BAL RLP, and execute. Both guest
    /// binaries (`main.rs`, `bin/batch.rs`) panic identically on any of
    /// the three failure modes (BAL decode, anchored execution), so the
    /// `.expect(...)` calls live here instead of being written twice.
    ///
    /// # Panics
    ///
    /// Panics (the guest's fail-closed posture) when the published BAL
    /// frame fails to decode, or when anchored stateless execution fails
    /// (identity forgery, witness incompleteness, or BAL inequality).
    #[must_use]
    pub fn run(input: ProverInput) -> GuestRun {
        let block = Self::from_records(input.boundary.block_number, input.records);
        let env = ExecEnv::new(input.chain_id, &input.boundary);
        let mut bal_slice: &[u8] = &input.bal_rlp;
        let expected_bal = alloy_eip7928::BlockAccessList::decode(&mut bal_slice)
            .expect("published BAL frame decodes");
        let anchored = execute_block_anchored(
            &input.witness,
            &input.proofs,
            None,
            &block.records,
            env,
            &expected_bal,
            input.granularity,
        )
        .expect("anchored stateless execution");
        GuestRun {
            anchored,
            records_digest: block.digest.finish(),
        }
    }
}
