//! The kardamom zkVM guest (spec: no-std-exec-core, phase 3c).
//!
//! Reads one rkyv [`ProverInput`] frame, rebuilds the exec-core record
//! list, and runs [`execute_block_anchored`] — the SAME monomorphized
//! function the validator's stateless re-execution runs. Everything
//! fail-closed happens in there (record identity, witness completeness,
//! BAL equality, MPT anchoring on both ends); this file is I/O.
//!
//! Committed output: the 104-byte [`PublicOutputs`] layout —
//! `pre_state_root || post_state_root || bal_commitment || block_number`.
//!
//! [`ProverInput`]: kardamom_types::ProverInput
//! [`PublicOutputs`]: kardamom_types::PublicOutputs
//! [`execute_block_anchored`]: kardamom_exec_core::stateless::execute_block_anchored

#![no_main]
sp1_zkvm::entrypoint!(main);

use alloy_rlp::Decodable;
use kardamom_exec_core::block_env::ExecEnv;
use kardamom_exec_core::exec_types::TxIndex;
use kardamom_exec_core::stateless::{BufferedRecord, execute_block_anchored};
use kardamom_types::{BlockRecordsDigest, ProverInput, ProverRecord, PublicOutputs};

pub fn main() {
    let input_bytes = sp1_zkvm::io::read_vec();
    let input: ProverInput = rkyv::from_bytes::<ProverInput, rkyv::rancor::Error>(&input_bytes)
        .expect("prover input frame");

    let mut digest = BlockRecordsDigest::new(input.boundary.block_number);
    let records: Vec<BufferedRecord> = input
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

    let env = ExecEnv::new(input.chain_id, &input.boundary);
    let mut bal_slice: &[u8] = &input.bal_rlp;
    let expected_bal = alloy_eip7928::BlockAccessList::decode(&mut bal_slice)
        .expect("published BAL frame decodes");

    let anchored = execute_block_anchored(
        &input.witness,
        &input.proofs,
        None,
        &records,
        env,
        &expected_bal,
        input.granularity,
    )
    .expect("anchored stateless execution");

    let outputs = PublicOutputs {
        pre_state_root: anchored.pre_state_root,
        post_state_root: anchored.post_state_root,
        block_number: anchored.block_number,
        records_digest: digest.finish(),
        bal_commitment: anchored.bal_commitment,
    };
    sp1_zkvm::io::commit_slice(&outputs.encode());
}
