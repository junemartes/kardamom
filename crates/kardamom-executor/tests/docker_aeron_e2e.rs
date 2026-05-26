//! Real-Aeron Docker e2e for the executor.
//!
//! Status: **deferred to a follow-up PR** pending S3 publishing the
//! tx_ordering/C wrappers under the `aeron-live` + `testing` features of
//! `kardamom-log`. Today S3 only ships:
//!
//! - `kardamom_log::testing::AeronTestCluster` (testcontainers harness),
//! - `kardamom_log::testing::Fake*` in-memory pub/sub stand-ins,
//! - `kardamom_log::codec::{encode, access, materialize}` for rkyv ↔ bytes.
//!
//! The tx_ordering/C publishers and subscribers that wrap the real rusteron
//! handles into `TxOrderingSubscription` / `ChannelCPublication` adapters are
//! still TBD in `kardamom-log`. Once those land, the e2e test follows the
//! Task 19 outline in `docs/plans/2026-05-23-S4-v0-sequential-executor.md`:
//!
//! 1. `AeronTestCluster::single_node().await?`
//! 2. Open a publisher on tx_ordering, a subscriber on tx_receipts.
//! 3. Publish N rkyv-encoded `TxEnvelope`s + a `BlockBoundaryStart`.
//! 4. Wire the executor's `TxOrderingSubscription` / `ChannelCPublication`
//!    adapters and run.
//! 5. Drain tx_receipts, assert receipts and the slim boundary.
//!
//! Tracking: S3 follow-up PR to expose `RealChannelBPublication`,
//! `RealTxOrderingSubscription`, `RealChannelCPublication`,
//! `RealChannelCSubscription` (gated by `aeron-live`).
//!
//! Until then, the in-memory `FakePublication` / `FakeTypedSubscription`
//! fakes shipped by `kardamom-log::testing` already give us the wire-format
//! check via the unit tests in this crate; the real-Aeron coverage is
//! about back-pressure / fsync / image-availability behaviour that only
//! emerges against the real Java Media Driver + Archive.

#[test]
fn docker_aeron_e2e_pending_s3_channel_wrappers() {
    // Self-skipping placeholder. When S3 ships the real channel wrappers,
    // populate this with the scenario from the S4 plan Task 19.
    eprintln!(
        "docker_aeron_e2e: pending S3 publishing Real*ChannelB/C* wrappers \
         in kardamom-log under the `aeron-live` feature. See module docs."
    );
}
