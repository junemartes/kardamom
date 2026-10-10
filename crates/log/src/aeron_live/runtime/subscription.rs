//! The subscription side of [`AeronRuntime`]: the opens, the attached
//! destinations, and the close.

use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

use super::{
    AeronRuntime, Destinations, RuntimeCmd, TxDataSubscription, TypedSubscription, request,
    request_within,
};
use crate::aeron_live::add_wait::AddWait;
use crate::aeron_live::{ADD_SUB_TIMEOUT, FrameSink, RawFrame};
use crate::error::LogError;

impl AeronRuntime {
    /// Open a subscription, returning its raw undecoded fragment stream
    /// (as [`RawFrame`]s) plus the assigned `sub_id` (used to attach source
    /// endpoints; most callers ignore it). Used by adapters that
    /// decode or demultiplex fragments themselves, on the consumer side
    /// rather than on the Aeron thread — see [`FrameSink`].
    ///
    /// # Errors
    ///
    /// Returns an error if the Aeron thread fails to add the subscription
    /// (a malformed channel URI, or the driver's `add_subscription`
    /// timeout elapsing), or if the command round trip itself times out.
    pub fn open_subscription_raw(
        &self,
        uri: &str,
        stream_id: i32,
    ) -> Result<(u32, UnboundedReceiver<RawFrame>), LogError> {
        let (tx, rx) = unbounded_channel();
        let sub_id = self.open_subscription_sink(uri, stream_id, FrameSink::Tokio(tx))?;
        Ok((sub_id, rx))
    }

    /// Like [`open_subscription_raw`](Self::open_subscription_raw), but
    /// for a plain OS thread that waits on this subscription alongside
    /// other crossbeam channels via `crossbeam_channel::Select`
    /// (`kardamom_cluster_adapter`'s session thread is the one consumer
    /// today). Tokio's channels do not implement crossbeam's
    /// `SelectHandle`, so that consumer needs a crossbeam receiver, not
    /// the tokio one every other subscriber handle in this crate uses.
    ///
    /// # Errors
    ///
    /// Returns an error if the Aeron thread fails to add the subscription,
    /// or if the command round trip itself times out.
    pub fn open_subscription_raw_crossbeam(
        &self,
        uri: &str,
        stream_id: i32,
    ) -> Result<(u32, crossbeam_channel::Receiver<RawFrame>), LogError> {
        let (tx, rx) = crossbeam_channel::unbounded();
        let sub_id = self.open_subscription_sink(uri, stream_id, FrameSink::Crossbeam(tx))?;
        Ok((sub_id, rx))
    }

    /// Shared by every `open_subscription*` method: register the
    /// subscription and point its delivery at `sink`. Several calls can
    /// share one clone of the same [`FrameSink::Tokio`] sender to merge
    /// multiple subscriptions into one raw stream (see
    /// [`open_subscription_merged`](Self::open_subscription_merged)).
    fn open_subscription_sink(
        &self,
        uri: &str,
        stream_id: i32,
        sink: FrameSink,
    ) -> Result<u32, LogError> {
        self.open_subscription_sink_within(uri, stream_id, sink, AddWait::run_time(ADD_SUB_TIMEOUT))
    }

    /// [`Self::open_subscription_sink`] with an add that waits as `wait`
    /// says.
    fn open_subscription_sink_within(
        &self,
        uri: &str,
        stream_id: i32,
        sink: FrameSink,
        wait: AddWait,
    ) -> Result<u32, LogError> {
        let uri = uri.to_string();
        let reply_wait = wait.reply_wait();
        request_within(
            &self.cmd_tx,
            |ack| RuntimeCmd::OpenSubscription {
                uri,
                stream_id,
                sink,
                wait,
                ack,
            },
            "open_subscription",
            reply_wait,
        )
    }

    /// [`Self::open_subscription_raw`] with an add that waits as `wait`
    /// says: a start-up open waits [`Self::start_open_limit`], and a stop
    /// ends it at once.
    ///
    /// # Errors
    ///
    /// Returns an error if the Aeron thread fails to add the subscription
    /// (a malformed channel URI, no answer of the driver within the wait,
    /// or a stop), or if the command round trip itself times out.
    pub fn open_subscription_raw_within(
        &self,
        uri: &str,
        stream_id: i32,
        wait: AddWait,
    ) -> Result<(u32, UnboundedReceiver<RawFrame>), LogError> {
        let (tx, rx) = unbounded_channel();
        let sub_id =
            self.open_subscription_sink_within(uri, stream_id, FrameSink::Tokio(tx), wait)?;
        Ok((sub_id, rx))
    }

    /// Attach a source endpoint to the subscription `sub_id`. The endpoint
    /// gets its own Aeron subscription, which feeds the frame stream of
    /// `sub_id`. No Aeron multi-destination subscription is used: the
    /// Java media driver fails the removal of some of its destinations.
    /// The add waits the run-time add timeout, and a wait with no answer
    /// cancels it. Idempotent: re-adding an already-attached `uri` is a
    /// no-op.
    ///
    /// # Errors
    ///
    /// Returns an error if `sub_id` is unknown, if the driver rejects or
    /// times out the destination attach, or if the command round trip
    /// itself times out.
    pub fn add_destination(&self, sub_id: u32, uri: &str) -> Result<(), LogError> {
        let uri = uri.to_string();
        request(
            &self.cmd_tx,
            |ack| RuntimeCmd::SubAddDestination { sub_id, uri, ack },
            "add_destination",
        )
    }

    /// Detach a previously-attached source endpoint: close its own Aeron
    /// subscription.
    ///
    /// # Errors
    ///
    /// Returns an error if the command round trip to the Aeron thread
    /// times out.
    pub fn remove_destination(&self, sub_id: u32, uri: &str) -> Result<(), LogError> {
        let uri = uri.to_string();
        request(
            &self.cmd_tx,
            |ack| RuntimeCmd::SubRemoveDestination { sub_id, uri, ack },
            "remove_destination",
        )
    }

    /// Close a subscription opened by one of the `open_subscription*`
    /// methods. The driver releases its images; the receiver side of the
    /// frame channel sees the end of the stream. A short-lived
    /// subscription, such as one bounded archive replay, closes here
    /// rather than stay in the thread's table for the process lifetime.
    ///
    /// # Errors
    ///
    /// Returns an error if `sub_id` is unknown or already closed, or if
    /// the command round trip times out.
    pub fn close_subscription(&self, sub_id: u32) -> Result<(), LogError> {
        request(
            &self.cmd_tx,
            |ack| RuntimeCmd::CloseSubscription { sub_id, ack },
            "close_subscription",
        )
    }

    /// Open a typed subscription, returning a [`TypedSubscription`] that
    /// decodes each fragment as `T` when the consumer calls
    /// `recv`/`try_recv`.
    ///
    /// # Errors
    ///
    /// Returns an error if the Aeron thread fails to add the subscription
    /// (see [`open_subscription_merged`](Self::open_subscription_merged)).
    pub fn open_subscription<T>(
        &self,
        uri: &str,
        stream_id: i32,
    ) -> Result<TypedSubscription<T>, LogError>
    where
        T: crate::codec::WireMessage,
    {
        self.open_subscription_merged(uri, &[], stream_id)
    }

    /// Open one or more subscriptions on the same `stream_id`, all feeding
    /// a single [`TypedSubscription`]. Each URI becomes its own Aeron
    /// subscription (its own `SubEntry`), sharing one clone of the same
    /// raw-frame sender, so fragments from every one merge into the
    /// returned stream in the Aeron thread's poll order.
    ///
    /// This is the `tx_ordering` MDC subscriber primitive: the executor
    /// passes one MDC control URI per publisher (the sealer and each
    /// sequencer), and the downstream reader sees a single ordered
    /// `(BPosition, T)` stream, exactly as before. With no `rest` URIs it
    /// is identical to [`Self::open_subscription`].
    ///
    /// Takes `first` plus `rest` instead of one slice, so the empty-URI
    /// case cannot be constructed and needs no runtime check.
    ///
    /// # Errors
    ///
    /// Returns an error if the Aeron thread fails to add any of the
    /// underlying subscriptions (see
    /// [`open_subscription_raw`](Self::open_subscription_raw)).
    pub fn open_subscription_merged<T>(
        &self,
        first: &str,
        rest: &[&str],
        stream_id: i32,
    ) -> Result<TypedSubscription<T>, LogError>
    where
        T: crate::codec::WireMessage,
    {
        let (frames_tx, frames_rx) = unbounded_channel();
        for uri in std::iter::once(first).chain(rest.iter().copied()) {
            self.open_subscription_sink(uri, stream_id, FrameSink::Tokio(frames_tx.clone()))?;
        }
        Ok(TypedSubscription::new(frames_rx))
    }

    /// Like [`open_subscription`](Self::open_subscription), but also
    /// returns the `sub_id`, so the caller can attach source endpoints
    /// with [`add_destination`](Self::add_destination). A
    /// `control-mode=manual` channel opens a subscription that receives
    /// only through its attached endpoints.
    ///
    /// # Errors
    ///
    /// Returns an error if the Aeron thread fails to add the subscription
    /// (see [`open_subscription_raw`](Self::open_subscription_raw)).
    pub fn open_subscription_with_id<T>(
        &self,
        uri: &str,
        stream_id: i32,
    ) -> Result<(u32, TypedSubscription<T>), LogError>
    where
        T: crate::codec::WireMessage,
    {
        let (sub_id, rx) = self.open_subscription_raw(uri, stream_id)?;
        Ok((sub_id, TypedSubscription::new(rx)))
    }

    /// A command-only handle on the destinations of subscription
    /// `sub_id`. Unlike an [`AeronRuntime`] clone it does not own the
    /// Aeron thread, so a long-lived task can hold it without keeping the
    /// runtime alive past the last owner's drop.
    #[must_use]
    pub fn destinations(&self, sub_id: u32) -> Destinations {
        Destinations {
            cmd_tx: self.cmd_tx.clone(),
            sub_id,
        }
    }

    /// Open a `tx_data` subscription yielding `(TxDataLoc, TxEnvelope)`,
    /// pairing each envelope with its Aeron publisher `session_id`. The
    /// session id keeps concurrent (active/active) ingress publishers on
    /// one shard distinct. It is what the sequencer stamps into
    /// `TxRef.tx_data_session_id`, and what the executor keys its join
    /// buffer on. With a single publisher, every fragment carries the same
    /// session id.
    ///
    /// # Errors
    ///
    /// Returns an error if the Aeron thread fails to add the subscription
    /// (see [`open_subscription_raw`](Self::open_subscription_raw)).
    pub fn open_tx_data_subscription(
        &self,
        uri: &str,
        stream_id: i32,
    ) -> Result<TxDataSubscription, LogError> {
        let (_sub_id, rx) = self.open_subscription_raw(uri, stream_id)?;
        Ok(TxDataSubscription { rx })
    }

    /// [`open_tx_data_subscription`](Self::open_tx_data_subscription),
    /// also returning the `sub_id` that [`close_subscription`](Self::close_subscription)
    /// takes.
    ///
    /// # Errors
    ///
    /// Returns an error if the Aeron thread fails to add the subscription.
    pub fn open_tx_data_subscription_with_id(
        &self,
        uri: &str,
        stream_id: i32,
    ) -> Result<(u32, TxDataSubscription), LogError> {
        let (sub_id, rx) = self.open_subscription_raw(uri, stream_id)?;
        Ok((sub_id, TxDataSubscription { rx }))
    }
}
