//! The kardamom zkVM guest.
//!
//! Reads one rkyv [`ProverInput`] frame, rebuilds the exec-core record
//! list, and runs [`execute_block_anchored`]: the same monomorphized
//! function the validator's stateless re-execution runs. All fail-closed
//! logic (record identity, witness completeness, BAL equality, MPT
//! anchoring on both ends) lives there. This file only handles I/O.
//!
//! Committed output: the 104-byte [`PublicOutputs`] layout,
//! `pre_state_root || post_state_root || bal_commitment || block_number`.
//!
//! [`ProverInput`]: kardamom_types::ProverInput
//! [`PublicOutputs`]: kardamom_types::PublicOutputs
//! [`execute_block_anchored`]: kardamom_exec_core::stateless::execute_block_anchored

#![no_main]
sp1_zkvm::entrypoint!(main);

use kardamom_types::{ProverInput, PublicOutputs};

/// # Panics
///
/// Panics (the guest's fail-closed posture) when `input_bytes` fails to
/// decode as a [`ProverInput`], when the published BAL frame fails to
/// decode, or when anchored stateless execution fails (identity forgery,
/// witness incompleteness, or BAL inequality).
pub fn main() {
    let input_bytes = sp1_zkvm::io::read_vec();
    let input: ProverInput = rkyv::from_bytes::<ProverInput, rkyv::rancor::Error>(&input_bytes)
        .expect("prover input frame");

    let run = kardamom_zk_guest::GuestBlock::run(input);

    let outputs = PublicOutputs {
        pre_state_root: run.anchored.pre_state_root,
        post_state_root: run.anchored.post_state_root,
        block_number: run.anchored.block_number,
        records_digest: run.records_digest,
        bal_commitment: run.anchored.bal_commitment,
    };
    sp1_zkvm::io::commit_slice(&outputs.encode());
}
