//! The kardamom batch guest: one proof per
//! posted batch. Reads a rkyv [`BatchProverInput`], runs the same anchored
//! per-block execution as the single-block guest over each frame, and
//! chains the roots internally: block i's claimed `pre_state_root` must
//! equal block i-1's recomputed post root, so the proof's outer roots
//! attest the whole contiguous range. It folds the batch records
//! commitment (L2 txs only; deposits are L1-originated and excluded from
//! DA batches by design) and commits the 160-byte [`BatchPublicOutputs`]
//! layout the `KardamomProofOracle` decodes.
//!
//! [`BatchProverInput`]: kardamom_types::BatchProverInput
//! [`BatchPublicOutputs`]: kardamom_types::BatchPublicOutputs

#![no_main]
sp1_zkvm::entrypoint!(main);

use alloy_rlp::Decodable;
use kardamom_exec_core::block_env::ExecEnv;
use kardamom_exec_core::exec_types::TxIndex;
use kardamom_exec_core::stateless::{BufferedRecord, execute_block_anchored};
use kardamom_types::{
    BatchProverInput, BatchPublicOutputs, BlockRecordsDigest, ProverRecord,
    batch_records_commitment,
};

pub fn main() {
    let input_bytes = sp1_zkvm::io::read_vec();
    let input: BatchProverInput =
        rkyv::from_bytes::<BatchProverInput, rkyv::rancor::Error>(&input_bytes)
            .expect("batch prover input frame");
    assert!(!input.blocks.is_empty(), "empty batch");

    let first_block = input.blocks.first().expect("nonempty").boundary.block_number;
    let last_block = input.blocks.last().expect("nonempty").boundary.block_number;
    let batch_pre_root = input
        .blocks
        .first()
        .expect("nonempty")
        .witness
        .pre_state_root
        .expect("anchored input");

    let mut running_root = batch_pre_root;
    let mut block_digests = Vec::with_capacity(input.blocks.len());
    for (i, block) in input.blocks.into_iter().enumerate() {
        let number = block.boundary.block_number;
        assert_eq!(
            number,
            first_block + i as u64,
            "batch blocks must be contiguous"
        );
        assert_eq!(
            block.witness.pre_state_root,
            Some(running_root),
            "root chain broken at block {number}"
        );

        let mut digest = BlockRecordsDigest::new(number);
        let records: Vec<BufferedRecord> = block
            .records
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
        block_digests.push(digest.finish());

        let env = ExecEnv::new(block.chain_id, &block.boundary);
        let mut bal_slice: &[u8] = &block.bal_rlp;
        let expected_bal = alloy_eip7928::BlockAccessList::decode(&mut bal_slice)
            .expect("published BAL frame decodes");
        let anchored = execute_block_anchored(
            &block.witness,
            &block.proofs,
            None,
            &records,
            env,
            &expected_bal,
            block.granularity,
        )
        .expect("anchored stateless execution");
        running_root = anchored.post_state_root;
    }

    let outputs = BatchPublicOutputs {
        pre_state_root: batch_pre_root,
        post_state_root: running_root,
        first_block,
        last_block,
        records_commitment: batch_records_commitment(block_digests),
    };
    sp1_zkvm::io::commit_slice(&outputs.encode());
}
