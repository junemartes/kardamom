//! High-level real-Aeron channel adapters that are `Send`-friendly for
//! tokio consumers.
//!
//! ## Why this module exists
//!
//! The raw rusteron types (`rusteron_client::Aeron`, `AeronPublication`,
//! `AeronSubscription`, `rusteron_archive::AeronArchive`) wrap raw FFI
//! pointers into a thread-confined C client, so they are `!Send + !Sync`.
//! Production consumers (the proxy/ingress, sequencer, executor, sealer,
//! state writer, batcher) all live in multi-threaded tokio runtimes. They
//! need `Send + Sync` handles they can stash in `Arc`s or move into
//! spawned tasks.
//!
//! This module bridges the gap with a dedicated Aeron thread per
//! [`AeronRuntime`]. The thread owns the `Rc<Aeron>` and every publication
//! or subscription opened from it. All cross-thread communication flows
//! through `crossbeam_channel` (outbound publish requests from many tokio
//! tasks to the one Aeron thread) and
//! `tokio::sync::mpsc::UnboundedSender` (inbound messages from the Aeron
//! thread to the registered subscriber task).
//!
//! ## Threading rules
//!
//! 1. `Aeron` and `AeronArchive` are `!Send + !Sync`. Never move them
//!    across threads.
//! 2. Use `Rc`, not `Arc`. The Aeron loop runs in a dedicated
//!    `std::thread::spawn` OS thread.
//! 3. Use `crossbeam::channel` or `tokio::sync::mpsc`/`broadcast` for
//!    cross-thread communication. Never send an Aeron handle across
//!    threads.
//! 4. Tokio multi-thread runtimes silently move tasks across worker
//!    threads at await points, so the Aeron loop is plain `std::thread`,
//!    not tokio.
//!
//! ## Handle set
//!
//! Maps the MDS channel topology onto Send-friendly handles:
//! - `TxData{Publisher,Subscriber}Handle`: per-shard envelope channel. The
//!   proxy/ingress publishes; sequencers, executors, and batchers
//!   subscribe.
//! - `TxOrdering{Publisher,Subscriber}Handle`: canonical orderer of tiny
//!   `TxOrderingMessage` records (`TxRef | BoundaryStart`). Sequencers
//!   race to publish, the sealer also publishes boundaries, and the
//!   executor/batcher subscribe.
//! - `TxReceipts{Publisher,ReceiptSubscriber,BoundarySubscriber}Handle`:
//!   receipts plus slim boundaries (not recorded). The executor
//!   publishes; the proxy/state writer subscribe.
//! - `ReceiptCache{Publisher,Subscriber}Handle`: the proxy-executor
//!   receipt cache (not recorded).
//! - `FsyncWatermark{Publisher,Subscriber}Handle`: per-recorder fsync
//!   watermark streams feeding the quorum aggregator.
//! - `Quorum{Publisher,Subscriber}Handle`: the aggregated quorum
//!   watermark.
//!
//! This module has an unconditional dependency on rusteron.
//!
//! ## Module layout
//!
//! - `runtime`: [`AeronRuntime`] (the command bus and spawn/open API) and
//!   [`PubHandle`].
//! - `thread`: the dedicated Aeron thread's poll loop and its
//!   publication/subscription tables.
//! - `pending`: the parked-publish retry scheduler ([`IdleBackoff`],
//!   `drain_pending`) and its unit tests.
//! - `handles`: the typed per-channel publisher/subscriber handle pairs.
//!
//! Everything public is re-exported here. Downstream imports are always
//! `kardamom_log::aeron_live::<Name>`.

mod handles;
mod pending;
mod runtime;
mod thread;

pub use handles::simple::{
    FsyncWatermarkPublisherHandle, FsyncWatermarkSubscriberHandle, QuorumPublisherHandle,
    QuorumSubscriberHandle, TxDepositsPublisherHandle, TxDepositsSubscriberHandle,
    TxErrorsPublisherHandle, TxErrorsSubscriberHandle, TxRemoteEpochsPublisherHandle,
    TxRemoteEpochsSubscriberHandle,
};
pub use handles::tx_data::{TxDataPublisherHandle, TxDataSubscriberHandle};
pub use handles::tx_receipts::{
    TxReceiptsBoundarySubscriberHandle, TxReceiptsPublisherHandle, TxReceiptsSubscriberHandle,
};
pub use pending::IdleBackoff;
pub use runtime::{AeronRuntime, PubHandle};

use std::time::Duration;

use kardamom_types::BPosition;

type AeronClient = rusteron_client::Aeron;
type Pub = rusteron_client::AeronPublication;
type Sub = rusteron_client::AeronSubscription;
type Header = rusteron_client::AeronHeader;

/// Closure that decodes one Aeron fragment, position, and publisher
/// `session_id`, and forwards the decoded value (or its raw bytes)
/// somewhere Send-friendly. Boxed so different message types can share the
/// subscription registration path. Most consumers ignore `session_id`. The
/// tx_data subscription uses it to build a [`kardamom_types::TxDataLoc`],
/// so concurrent ingress publishers on one shard stay distinct.
pub type DeliverFn = Box<dyn FnMut(&[u8], BPosition, i32) + Send>;

const ADD_PUB_TIMEOUT: Duration = Duration::from_secs(5);
const ADD_SUB_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a command round trip ([`runtime`]'s `request`) waits for the
/// Aeron thread's ack. This covers control-plane opens and
/// `PubHandle::publish_bytes` alike. It must stay well above
/// [`crate::offer_retry::OFFER_TIMEOUT`] (the per-frame queue deadline,
/// enforced for every queued frame on each drain pass). That ordering
/// guarantees the publish ack (delivered or expired) always resolves
/// before this timeout fires, so a caller can never give up on a frame
/// that is later delivered behind its back.
const ACK_TIMEOUT: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// Send/Sync compile-time assertions.
// ---------------------------------------------------------------------------

#[allow(dead_code)]
fn assert_send_sync<T: Send + Sync>() {}

#[allow(dead_code)]
fn assert_send<T: Send>() {}

const _: fn() = || {
    assert_send_sync::<AeronRuntime>();
    assert_send_sync::<PubHandle>();
    assert_send_sync::<TxDataPublisherHandle>();
    assert_send::<TxDataSubscriberHandle>();
    assert_send_sync::<TxReceiptsPublisherHandle>();
    assert_send::<TxReceiptsSubscriberHandle>();
    assert_send::<TxReceiptsBoundarySubscriberHandle>();
    assert_send_sync::<TxErrorsPublisherHandle>();
    assert_send::<TxErrorsSubscriberHandle>();
    assert_send_sync::<TxDepositsPublisherHandle>();
    assert_send::<TxDepositsSubscriberHandle>();
    assert_send_sync::<FsyncWatermarkPublisherHandle>();
    assert_send::<FsyncWatermarkSubscriberHandle>();
    assert_send_sync::<QuorumPublisherHandle>();
    assert_send::<QuorumSubscriberHandle>();
};
