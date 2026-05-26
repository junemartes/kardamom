//! Real-Aeron Docker e2e for the executor.
//!
//! Status: **deferred to a follow-up PR** pending S3 publishing the
//! tx_ordering/C wrappers under the `aeron-live` + `testing` features of
//! `kardamom-log`. Today S3 only ships:
//!
//! - `log::testing::AeronTestCluster` (testcontainers harness),
//! - `log::testing::Fake*` in-memory pub/sub stand-ins,
//! - `log::codec::{encode, access, materialize}` for rkyv ↔ bytes.
//!
//! The tx_ordering/C publishers and subscribers that wrap the real rusteron
//! handles into `TxOrderingSubscription` / `TxReceiptsPublication` adapters are
//! still TBD in `kardamom-log`. Once those land, the e2e test follows the
//! Task 19 outline in ``:
//!
//! 1. `AeronTestCluster::single_node().await?`
//! 2. Open a publisher on tx_ordering, a subscriber on tx_receipts.
//! 3. Publish N rkyv-encoded `TxEnvelope`s + a `BlockBoundaryStart`.
//! 4. Wire the executor's `TxOrderingSubscription` / `TxReceiptsPublication`
//!    adapters and run.
//! 5. Drain tx_receipts, assert receipts and the slim boundary.
//!
//! Tracking: follow-up to expose `RealTxOrderingPublication`,
//! `RealTxOrderingSubscription`, `RealTxReceiptsPublication`,
//! `RealTxReceiptsSubscription` (gated by `aeron-live`).
//!
//! Until then, the in-memory `FakePublication` / `FakeTypedSubscription`
//! fakes shipped by `log::testing` already give us the wire-format
//! check via the unit tests in this crate; the real-Aeron coverage is
//! about back-pressure / fsync / image-availability behaviour that only
//! emerges against the real Java Media Driver + Archive.

#[test]
fn docker_aeron_e2e_pending_real_channel_wrappers() {
    // Self-skipping placeholder. When the real channel wrappers ship,
    // populate this with the end-to-end scenario.
    eprintln!(
        "docker_aeron_e2e: pending Real* tx_ordering / tx_receipts wrappers \
         in `log`. See module docs."
    );
}
