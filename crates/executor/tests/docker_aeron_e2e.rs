//! Real-Aeron Docker end-to-end test for the executor.
//!
//! This test needs `kardamom-log` to publish real `TxOrderingSubscription`
//! and `TxReceiptsPublication` adapters over rusteron, under the
//! `aeron-live` and `testing` features. Until those land, the in-memory
//! `FakePublication` and `FakeTypedSubscription` fakes from
//! `kardamom_log::testing` check the wire format, through the unit tests
//! in this crate. The real-Aeron coverage this test would add checks
//! back-pressure, fsync, and image-availability behavior, which only
//! shows up against the real Java Media Driver and Archive.

#[test]
#[ignore = "needs real Aeron TxOrdering/TxReceipts adapters in kardamom-log"]
fn docker_aeron_e2e_pending_real_channel_wrappers() {}
