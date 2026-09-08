//! The five structurally identical single-stream handle pairs: `TxErrors`,
//! `TxDeposits`, `TxRemoteEpochs`, `FsyncWatermark`, Quorum. Each is a publisher
//! wrapping one [`PubHandle`] plus a subscriber wrapping one typed receiver, differing
//! only in message type, channel/stream selection, and the publisher's
//! publish surface. [`declare_channel_handles!`] stamps out the
//! boilerplate (structs, `open`, `recv`/`try_recv`). The public type-name
//! pairs and their method signatures are exactly what the hand-written
//! copies exposed.

use super::super::{AeronRuntime, PubHandle, TypedSubscription};
use crate::config::ChannelsConfig;
use crate::error::LogError;
use kardamom_types::xchain::RemoteEpochRecord;
use kardamom_types::{BPosition, EpochRecord, FsyncWatermark, QuorumWatermark, TxError};

/// Declare a publisher/subscriber handle pair over one config-selected
/// `(channel, stream_id)`:
///
/// - a `Clone` publisher struct wrapping a [`PubHandle`], with an
///   `open(rt, ch, ...)` constructor and the caller-supplied publish
///   methods pasted verbatim into its `impl` (so `publish`'s exact
///   signature stays per type);
/// - a subscriber struct wrapping a `TypedSubscription<$msg>` with the
///   same-shaped `open` plus the standard `recv` and `try_recv`.
///
/// `open(ch, ...)` binds the `ChannelsConfig` parameter name (and any
/// extra parameters) used by the `$channel`/`$stream` selection
/// expressions. Both the publisher's and the subscriber's `open` share them.
macro_rules! declare_channel_handles {
    (
        $(#[$pub_doc:meta])*
        publisher $pub_name:ident { $($pub_methods:tt)* }
        $(#[$sub_doc:meta])*
        subscriber $sub_name:ident($msg:ty);
        open($ch:ident $(, $arg:ident: $argty:ty)*) = ($channel:expr, $stream:expr);
    ) => {
        $(#[$pub_doc])*
        #[derive(Clone)]
        pub struct $pub_name {
            inner: PubHandle,
        }

        impl $pub_name {
            /// # Errors
            ///
            /// Returns an error if the Aeron thread fails to add the
            /// publication (for example a malformed channel URI, or the
            /// driver's `add_publication` timeout elapsing).
            pub fn open(
                rt: &AeronRuntime,
                $ch: &ChannelsConfig
                $(, $arg: $argty)*
            ) -> Result<Self, LogError> {
                Ok(Self {
                    inner: rt.open_publication(&$channel, $stream)?,
                })
            }

            $($pub_methods)*
        }

        $(#[$sub_doc])*
        pub struct $sub_name {
            rx: TypedSubscription<$msg>,
        }

        impl $sub_name {
            /// # Errors
            ///
            /// Returns an error if the Aeron thread fails to add the
            /// subscription (for example a malformed channel URI, or the
            /// driver's `add_subscription` timeout elapsing).
            pub fn open(
                rt: &AeronRuntime,
                $ch: &ChannelsConfig
                $(, $arg: $argty)*
            ) -> Result<Self, LogError> {
                Ok(Self {
                    rx: rt.open_subscription::<$msg>(&$channel, $stream)?,
                })
            }

            pub async fn recv(&mut self) -> Option<(BPosition, $msg)> {
                self.rx.recv().await
            }

            pub fn try_recv(&mut self) -> Option<(BPosition, $msg)> {
                self.rx.try_recv()
            }
        }
    };

    // Second arm: a subscriber whose item type is not `(BPosition, $msg)`
    // (for example `TxData`'s `(TxDataLoc, TxEnvelope)`, which carries the
    // publisher `session_id` too) and whose open call is not the generic
    // `open_subscription::<$msg>`. `subscribe` names an `AeronRuntime`
    // method with signature `fn(&self, uri: &str, stream_id: i32) ->
    // Result<$rx, LogError>`, called through its type path so the
    // receiver (`rt`) passes as the first argument. `$rx` owns its
    // decode, so `recv`/`try_recv` here are plain delegation, not a
    // channel read.
    (
        $(#[$pub_doc:meta])*
        publisher $pub_name:ident { $($pub_methods:tt)* }
        $(#[$sub_doc:meta])*
        subscriber $sub_name:ident(item = $item:ty, rx = $rxty:ty, subscribe = $subscribe:path);
        open($ch:ident $(, $arg:ident: $argty:ty)*) = ($channel:expr, $stream:expr);
    ) => {
        $(#[$pub_doc])*
        #[derive(Clone)]
        pub struct $pub_name {
            inner: PubHandle,
        }

        impl $pub_name {
            /// # Errors
            ///
            /// Returns an error if the Aeron thread fails to add the
            /// publication (for example a malformed channel URI, or the
            /// driver's `add_publication` timeout elapsing).
            pub fn open(
                rt: &AeronRuntime,
                $ch: &ChannelsConfig
                $(, $arg: $argty)*
            ) -> Result<Self, LogError> {
                Ok(Self {
                    inner: rt.open_publication(&$channel, $stream)?,
                })
            }

            $($pub_methods)*
        }

        $(#[$sub_doc])*
        pub struct $sub_name {
            rx: $rxty,
        }

        impl $sub_name {
            /// # Errors
            ///
            /// Returns an error if the Aeron thread fails to add the
            /// subscription (for example a malformed channel URI, or the
            /// driver's `add_subscription` timeout elapsing).
            pub fn open(
                rt: &AeronRuntime,
                $ch: &ChannelsConfig
                $(, $arg: $argty)*
            ) -> Result<Self, LogError> {
                Ok(Self {
                    rx: $subscribe(rt, &$channel, $stream)?,
                })
            }

            pub async fn recv(&mut self) -> Option<$item> {
                self.rx.recv().await
            }

            pub fn try_recv(&mut self) -> Option<$item> {
                self.rx.try_recv()
            }
        }
    };
}

// Re-exported (not `#[macro_export]`, which would put it at the crate
// root) so `tx_data.rs` can use the second arm for its non-`BPosition`
// item type.
pub(crate) use declare_channel_handles;

declare_channel_handles! {
    /// `TxErrors` publisher (sequencer → ingress).
    publisher TxErrorsPublisherHandle {
        /// # Errors
        ///
        /// Returns an error if the underlying Aeron offer fails or times
        /// out (see `PubHandle::publish`).
        pub fn publish(&self, e: &TxError) -> Result<BPosition, LogError> {
            self.inner.publish(e)
        }
    }
    /// `TxErrors` subscriber (sequencer → ingress).
    subscriber TxErrorsSubscriberHandle(TxError);
    open(ch) = (ch.tx_errors_channel, ch.tx_errors_stream_id);
}

declare_channel_handles! {
    /// `TxDeposits` publisher (DA watcher → sequencer).
    publisher TxDepositsPublisherHandle {
        /// # Errors
        ///
        /// Returns an error if the underlying Aeron offer fails or times
        /// out (see `PubHandle::publish`).
        pub fn publish(&self, e: &EpochRecord) -> Result<BPosition, LogError> {
            self.inner.publish(e)
        }
    }
    /// `TxDeposits` subscriber (DA watcher → sequencer).
    subscriber TxDepositsSubscriberHandle(EpochRecord);
    open(ch) = (ch.tx_deposits_channel, ch.tx_deposits_stream_id);
}

declare_channel_handles! {
    /// `TxRemoteEpochs` publisher (interop watcher → sequencer).
    publisher TxRemoteEpochsPublisherHandle {
        /// # Errors
        ///
        /// Returns an error if the underlying Aeron offer fails or times
        /// out (see `PubHandle::publish`).
        pub fn publish(&self, r: &RemoteEpochRecord) -> Result<BPosition, LogError> {
            self.inner.publish(r)
        }
    }
    /// `TxRemoteEpochs` subscriber (interop watcher → sequencer).
    subscriber TxRemoteEpochsSubscriberHandle(RemoteEpochRecord);
    open(ch) = (ch.tx_remote_epochs_channel, ch.tx_remote_epochs_stream_id);
}

declare_channel_handles! {
    /// Per-recorder fsync watermark publisher.
    publisher FsyncWatermarkPublisherHandle {
        /// # Errors
        ///
        /// Returns an error if the underlying Aeron offer fails or times
        /// out (see `PubHandle::publish`).
        pub fn publish(&self, w: &FsyncWatermark) -> Result<(), LogError> {
            self.inner.publish(w).map(|_| ())
        }
    }
    /// Per-recorder fsync watermark subscriber.
    subscriber FsyncWatermarkSubscriberHandle(FsyncWatermark);
    open(ch, recorder_id: u8) = (
        ch.fsync_watermark_channel(recorder_id),
        ch.fsync_watermark_stream_id
    );
}

declare_channel_handles! {
    /// Aggregated quorum watermark publisher.
    publisher QuorumPublisherHandle {
        /// # Errors
        ///
        /// Returns an error if the underlying Aeron offer fails or times
        /// out (see `PubHandle::publish`).
        pub fn publish(&self, q: &QuorumWatermark) -> Result<(), LogError> {
            self.inner.publish(q).map(|_| ())
        }
    }
    /// Aggregated quorum watermark subscriber.
    subscriber QuorumSubscriberHandle(QuorumWatermark);
    open(ch) = (ch.quorum_watermark_channel, ch.quorum_watermark_stream_id);
}
