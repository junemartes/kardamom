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
//! Maps the channel topology onto Send-friendly handles:
//! - `TxData{Publisher,Subscriber}Handle`: per-lane envelope channel. The
//!   ingress publishes; sequencers, executors, validators, and batchers
//!   subscribe.
//! - `TxReceipts{Publisher,Subscriber,BoundarySubscriber}Handle`:
//!   receipts plus slim boundaries (not recorded). The executor
//!   publishes; the ingress, sequencers, and validators subscribe.
//! - `TxErrors`, `TxDeposits`, `TxRemoteEpochs`, `FsyncWatermark`
//!   publisher/subscriber pairs: one stream each (see `handles::simple`).
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
    FsyncWatermarkPublisherHandle, FsyncWatermarkSubscriberHandle, TxDepositsPublisherHandle,
    TxDepositsSubscriberHandle, TxErrorsPublisherHandle, TxErrorsSubscriberHandle,
    TxRemoteEpochsPublisherHandle, TxRemoteEpochsSubscriberHandle,
};
pub use handles::tx_data::{TxDataPublisherHandle, TxDataSubscriberHandle};
pub use handles::tx_receipts::{
    TxReceiptsBoundarySubscriberHandle, TxReceiptsPublisherHandle, TxReceiptsReceiver,
    TxReceiptsSubscriberHandle,
};
pub use pending::IdleBackoff;
pub use runtime::{
    AeronRuntime, Destinations, PollRecv, PubHandle, TxDataSubscription, TypedSubscription,
};

use std::time::Duration;

use kardamom_types::BPosition;

type AeronClient = rusteron_client::Aeron;
type Pub = rusteron_client::AeronPublication;
type Sub = rusteron_client::AeronSubscription;
type Header = rusteron_client::AeronHeader;

/// One undecoded fragment delivered off a subscription: the raw payload
/// bytes plus the stream position and publisher `session_id` from the
/// same header read (see `thread::header_loc`). Most consumers ignore
/// `session`. The `tx_data` subscription uses it to build a
/// [`kardamom_types::TxDataLoc`], so concurrent ingress publishers on one
/// shard stay distinct.
///
/// This is the concrete, non-generic payload that crosses the
/// `OpenSubscription` command to the dedicated Aeron thread: nothing
/// downstream of the command channel is erased into a trait object.
/// Decoding into a caller's own message type — over any
/// [`crate::codec::WireMessage`] a downstream crate defines — happens on
/// the consumer side, in [`runtime::TypedSubscription::recv`] and its
/// siblings, not on the Aeron thread. `bytes` is copied out of the
/// fragment assembler's buffer once per delivery (`buffer.to_vec()`), so
/// the decode can run after the callback returns.
pub struct RawFrame {
    pub bytes: Vec<u8>,
    pub pos: BPosition,
    pub session: i32,
}

/// Where an [`AeronRuntime::open_subscription_raw`]-family call sends its
/// [`RawFrame`]s. Exactly two shapes exist in this crate, both concrete,
/// with no dynamic dispatch:
///
/// - `Tokio`: an async consumer, decoded lazily on `recv`/`try_recv`
///   (every subscriber handle in [`handles`]).
/// - `Crossbeam`: a plain OS thread that waits on this subscription
///   alongside other crossbeam channels via `crossbeam_channel::Select`
///   (`kardamom_cluster_adapter`'s session thread). Tokio's channels do
///   not implement crossbeam's `SelectHandle`, so a `Select`-based
///   consumer needs this variant instead of the `Tokio` one.
///
/// Both variants forward to the same fragment callback
/// ([`thread::AssembledDeliver`]); the enum, not a boxed closure, is what
/// lets one non-generic `RuntimeCmd::OpenSubscription` field carry either.
pub(super) enum FrameSink {
    Tokio(tokio::sync::mpsc::UnboundedSender<RawFrame>),
    Crossbeam(crossbeam_channel::Sender<RawFrame>),
}

impl FrameSink {
    /// Best effort: a closed receiver (the consumer dropped its side)
    /// just means this frame is discarded, the same as every other
    /// dropped-receiver send in this module.
    fn send(&self, frame: RawFrame) {
        match self {
            FrameSink::Tokio(tx) => {
                let _ = tx.send(frame);
            }
            FrameSink::Crossbeam(tx) => {
                let _ = tx.send(frame);
            }
        }
    }
}

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

#[allow(
    dead_code,
    reason = "called only from the const-eval assertions below; never \
              run, so the compiler cannot see a live call"
)]
fn assert_send_sync<T: Send + Sync>() {}

#[allow(
    dead_code,
    reason = "called only from the const-eval assertions below; never \
              run, so the compiler cannot see a live call"
)]
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
    assert_send_sync::<TxRemoteEpochsPublisherHandle>();
    assert_send::<TxRemoteEpochsSubscriberHandle>();
};
