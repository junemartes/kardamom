//! Fixtures shared by more than one integration test binary in this crate.
//!
//! `witness_anchoring.rs` and `prover_spool.rs` both build one block
//! against the same chain id, the same recipient, and the same
//! zeroing-storage contract, signing each transaction the same way. This
//! module is that one shared shape; each binary still owns its own
//! block-specific setup (seeding, the writer, the spool) on top of it.

use alloy_primitives::{Address, B256, address};
use alloy_signer_local::PrivateKeySigner;
use kardamom_engine::actor::BufferedRecord;
use kardamom_engine::actor::fixtures::LegacyTx;
use kardamom_engine::exec_types::TxIndex;
use kardamom_types::{BPosition, TxEnvelope};

pub(crate) const CHAIN_ID: u64 = 412_346;
pub(crate) const RECIPIENT: Address = address!("000000000000000000000000000000000000dEaD");

/// SSTORE(0, 0); STOP. Zeroes slot 0: the storage-deletion shape.
pub(crate) const ZEROER: Address = address!("00000000000000000000000000000000000000Aa");
pub(crate) const ZEROER_CODE: [u8; 6] = [0x60, 0x00, 0x60, 0x00, 0x55, 0x00];

pub(crate) const S0: B256 = B256::with_last_byte(0);
pub(crate) const S1: B256 = B256::with_last_byte(1);

/// A signed legacy transfer from `signer` to `to`, as a canonical-record
/// `BufferedRecord::Tx` at bal index `i`.
pub(crate) fn tx(
    signer: &PrivateKeySigner,
    to: Address,
    nonce: u64,
    value: u64,
    i: u64,
) -> BufferedRecord {
    let envelope = LegacyTx {
        chain_id: CHAIN_ID,
        to,
        nonce,
        value,
        gas_limit: 300_000,
        gas_price: 0,
    }
    .sign(signer);
    BufferedRecord::Tx {
        tx_idx: TxIndex(i),
        position: BPosition {
            term_id: 0,
            term_offset: i32::try_from(i * 64).expect("fixture index"),
        },
        envelope: TxEnvelope {
            correlation_id: i,
            ..envelope
        },
    }
}
