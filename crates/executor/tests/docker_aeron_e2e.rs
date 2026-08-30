//! Real-Aeron Docker end-to-end test for the executor.
//!
//! Status: deferred to a follow-up PR. It needs `kardamom-log` to publish
//! the tx_ordering/C wrappers, under the `aeron-live` and `testing`
//! features. Today `kardamom-log` ships only:
//!
//! - `kardamom_log::testing::AeronTestCluster` (a testcontainers harness).
//! - `kardamom_log::testing::Fake*` in-memory pub/sub stand-ins.
//! - `kardamom_log::codec::{encode, access, materialize}` for rkyv to bytes.
//!
//! The tx_ordering/C publishers and subscribers are still to do in
//! `kardamom-log`. They must wrap the real rusteron handles into
//! `TxOrderingSubscription` and `TxReceiptsPublication` adapters. Once they
//! land, the e2e test follows this outline:
//!
//! 1. Call `AeronTestCluster::single_node().await?`.
//! 2. Open a publisher on tx_ordering and a subscriber on tx_receipts.
//! 3. Publish N rkyv-encoded `TxEnvelope`s and a `BlockBoundaryStart`.
//! 4. Wire up the executor's `TxOrderingSubscription` and
//!    `TxReceiptsPublication` adapters, then run.
//! 5. Drain tx_receipts. Check the receipts and the slim boundary.
//!
//! Tracking: a follow-up must expose `RealTxOrderingPublication`,
//! `RealTxOrderingSubscription`, `RealTxReceiptsPublication`, and
//! `RealTxReceiptsSubscription` (gated by `aeron-live`).
//!
//! Until then, the in-memory `FakePublication` and `FakeTypedSubscription`
//! fakes from `kardamom_log::testing` already check the wire format, through
//! the unit tests in this crate. The real-Aeron coverage instead checks
//! back-pressure, fsync, and image-availability behavior. This only shows up
//! against the real Java Media Driver and Archive.

#[test]
fn docker_aeron_e2e_pending_real_channel_wrappers() {
    // Placeholder test. It skips itself for now. Fill in the end-to-end
    // scenario when the real channel wrappers ship.
    eprintln!(
        "docker_aeron_e2e: pending Real* tx_ordering / tx_receipts wrappers \
         in `log`. See module docs."
    );
}
