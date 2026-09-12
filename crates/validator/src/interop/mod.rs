//! The validator's interop role: one binary, roles by config.
//!
//! - [`verify`] — destination side: the [`RemoteEpochVerifier`] wired on the
//!   engine's `RemoteEpochObserver` seam (inline pair-sequence checks;
//!   content-vs-origin is not checked here — see the module docs).
//! - [`extract`] — origin side: decode `MessageSent` from the re-executed
//!   receipts with the recompute-and-reject discipline, cross-check the
//!   `sentMessages` BAL claim.
//! - [`sink`] — the engine seam feeding extraction per block boundary.
//! - [`store`] — the retained serving stores (outbox lanes, attestations).
//! - [`serve`] — the config-gated jsonrpsee WS surfaces
//!   (`kardamom_subscribeOutbox`, `kardamom_subscribeAttestations`).

pub mod extract;
pub mod serve;
pub mod sink;
pub mod state_rpc;
pub mod store;
pub mod verify;

pub use serve::{
    DEFAULT_FEED_MAX_SUBSCRIPTIONS, DEFAULT_FEED_MAX_SUBSCRIPTIONS_PER_DEST, FeedServerLimits,
    FeedServerState, start_feed_server,
};
pub use sink::ExtractingReceiptSink;
pub use store::{AttestationStore, FeedStore, RetentionBlocks};
pub use verify::RemoteEpochVerifier;

/// `block` as a `u8`, for a repeat-byte fixture hash. Shared by
/// [`outbox_msg`] and [`store`]'s attestation-ring test — every test fixture
/// in this crate keeps block numbers under 256.
#[cfg(test)]
pub(crate) fn fixture_block_byte(block: u64) -> u8 {
    u8::try_from(block).expect("fixture block < 256")
}

/// A fixture `OutboxMessage`, shared by [`serve`] and [`store`]'s test
/// modules.
#[cfg(test)]
pub(crate) fn outbox_msg(dest: u64, seq: u64, block: u64) -> kardamom_types::xchain::OutboxMessage {
    let block_byte = fixture_block_byte(block);
    kardamom_types::xchain::OutboxMessage {
        origin_block_number: block,
        origin_block_hash: alloy_primitives::B256::repeat_byte(block_byte),
        dest_chain_id: dest,
        seq,
        sender: alloy_primitives::Address::repeat_byte(0xA1),
        target: alloy_primitives::Address::repeat_byte(0xB2),
        value: 0,
        gas_limit: 100_000,
        data: alloy_primitives::Bytes::default(),
        callback: None,
    }
}
