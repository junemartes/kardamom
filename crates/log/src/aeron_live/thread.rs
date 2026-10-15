//! The dedicated Aeron thread: the poll/command loop that owns every
//! `!Send` rusteron object (client, publications, subscriptions, and the
//! subscriptions of attached destinations). Everything here runs on the
//! one `kardamom-aeron` OS thread; the rest of the module talks to it
//! exclusively through [`RuntimeCmd`]s.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::ops::ControlFlow;
use std::rc::Rc;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver as CbReceiver, Sender as CbSender, TryRecvError};
use tracing::warn;

use super::add_wait::{AddWait, PubAdd, SubAdd};
use super::bound::BoundControl;
use super::destination::Destination;
use super::image_log::ImageHandlers;
use super::pending::{IdleBackoff, PendingPublish, PubEntry, drain_pending};
use super::runtime::{OpenedPub, RuntimeCmd};
use super::table_pub::TablePub;
use super::{ADD_PUB_TIMEOUT, ADD_SUB_TIMEOUT, AeronClient, FrameSink, Header, RawFrame, Sub};
use crate::error::LogError;
use crate::offer_retry::OFFER_TIMEOUT;
use crate::term_layout::TermLayout;
use kardamom_types::BPosition;

/// Adapter between rusteron's fragment-handler callback and a
/// [`FrameSink`], sitting behind an
/// [`rusteron_client::AeronFragmentAssembler`]. It runs once per complete
/// message, with multi-fragment messages (any frame larger than one Aeron
/// MTU, about 1.4 KB) already reassembled. Without the assembler,
/// `aeron_subscription_poll` hands over raw fragments, and every oversized
/// frame (for example a `ReceiptBatch` that crosses the MTU) fails
/// to decode at the consumer. The header passed through belongs to the
/// final fragment. Both ends of a position-keyed stream (the `tx_data`
/// join) go through this same path, so position derivation stays
/// consistent.
struct AssembledDeliver {
    sink: FrameSink,
}

impl rusteron_client::AeronFragmentHandlerCallback for AssembledDeliver {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: Header) {
        if let Some((pos, session)) = header_loc(&header) {
            self.sink.send(RawFrame {
                bytes: buffer.to_vec(),
                pos,
                session,
            });
        }
    }
}

/// [`AeronThread::try_next_cmd`]'s outcome, for [`AeronThread::drain_commands`]'s loop.
enum CmdStep {
    /// A command was handled; keep draining.
    Handled,
    /// The channel has nothing queued right now.
    Empty,
    /// `Shutdown`, or the channel disconnected: the thread must stop.
    Stop,
}

/// One row in the Aeron thread's subscription table: the subscription,
/// and the stream id and the sink that each of its destinations opens
/// its own subscription with.
struct SubRow {
    entry: SubEntry,
    stream_id: i32,
    sink: FrameSink,
}

/// One open Aeron subscription behind a fragment assembler: a row of the
/// subscription table, or an attached destination.
pub(super) struct SubEntry {
    sub: Sub,
    /// Assembler-wrapped handler passed to `poll` (owns per-session
    /// assembly buffers). Delegates complete messages to `inner`.
    assembler: rusteron_client::Handler<rusteron_client::AeronFragmentAssembler>,
    /// The leaked delegate the assembler forwards to. Retained so it can
    /// be released when the subscription row is dropped.
    inner: rusteron_client::Handler<AssembledDeliver>,
    /// Set once `poll` errors, cleared once it succeeds again. Latches
    /// the warning to one line per transition, since `poll` runs on
    /// every pass of the thread's hot loop.
    poll_failed: bool,
}

impl Drop for SubEntry {
    fn drop(&mut self) {
        self.assembler.release();
        self.inner.release();
    }
}

impl SubEntry {
    /// One [`AeronThread::poll_subscriptions`] poll. Returns whether this
    /// entry delivered at least one fragment. A poll error counts as no
    /// fragments: one failed image must not stop polling the others. Logs
    /// once on the transition into the error state, not on every failed
    /// poll, since this runs on every pass of the thread's hot loop.
    fn poll_once(&mut self) -> bool {
        match self.sub.poll(Some(&self.assembler), 64) {
            Ok(fragments) => {
                self.poll_failed = false;
                fragments > 0
            }
            Err(e) => {
                self.note_poll_failure(&e);
                false
            }
        }
    }

    /// The count of images of the subscription.
    pub(super) fn image_count(&self) -> Result<i32, rusteron_client::AeronCError> {
        self.sub.image_count()
    }

    /// Log on the transition into the poll-failed state, then latch it. A
    /// run of failing polls on the thread's hot loop then logs once, not
    /// on every pass.
    fn note_poll_failure(&mut self, e: &impl std::fmt::Debug) {
        if self.poll_failed {
            return;
        }
        warn!(error = ?e, "subscription poll failed");
        self.poll_failed = true;
    }
}

/// Entry point called from [`super::runtime::AeronRuntime::spawn_with_dir`]
/// on the dedicated OS thread. Builds the thread's state and runs its
/// poll/command loop until `Shutdown` or the command channel disconnects.
///
/// `aeron` and `cmd_rx` are taken by value on purpose: this is the Aeron
/// thread's whole-lifetime body, and [`AeronThread`] is the sole,
/// exclusive owner of both for as long as it runs.
pub(super) fn run_aeron_thread(
    aeron: Rc<AeronClient>,
    cmd_rx: CbReceiver<RuntimeCmd>,
    linger: Duration,
) -> Result<(), LogError> {
    AeronThread::new(aeron, cmd_rx, linger).run()
}

/// The Aeron thread's whole state: every `!Send` rusteron object it owns,
/// plus the retry queue and idle cadence. Every command handler and poll
/// step is a method here, so a step's inputs are struct fields, not
/// parameters threaded through free functions.
struct AeronThread {
    aeron: Rc<AeronClient>,
    cmd_rx: CbReceiver<RuntimeCmd>,
    /// Indexed by `pub_id`. A closed publication leaves a `None` slot,
    /// so the ids of the open ones stay valid.
    pubs: Vec<Option<PubEntry>>,
    /// The attached destinations of the rows in `subs`, and the detached
    /// ones that linger.
    dests: Vec<Destination>,
    /// How long a detached destination with an image stays open: the
    /// stall budget of the client, which is at least the image liveness
    /// timeout of the driver (10 s by default). A publisher that stops
    /// loses its image within that time.
    linger: Duration,
    /// Indexed by `sub_id`. A closed subscription leaves a `None` slot,
    /// so the ids of the open ones stay valid.
    subs: Vec<Option<SubRow>>,
    pending: VecDeque<PendingPublish>,
    /// The image log handlers of every subscription. They live as long
    /// as the process (see [`ImageHandlers`]).
    image_log: ImageHandlers,
    /// Escalating idle wait for the busy branch: base 100 microseconds (the
    /// established sub-poll/retry cadence), cap 1 ms (the empty-branch
    /// cadence), grace 10 (about 1 ms of consecutive emptiness before the
    /// first escalation).
    backoff: IdleBackoff,
}

impl AeronThread {
    fn new(aeron: Rc<AeronClient>, cmd_rx: CbReceiver<RuntimeCmd>, linger: Duration) -> Self {
        Self {
            aeron,
            cmd_rx,
            linger,
            pubs: Vec::new(),
            subs: Vec::new(),
            pending: VecDeque::new(),
            dests: Vec::new(),
            image_log: ImageHandlers::leak(),
            backoff: IdleBackoff::new(Duration::from_micros(100), Duration::from_millis(1), 10),
        }
    }

    /// The poll/command loop. Runs until `Shutdown` or the command channel
    /// disconnects.
    fn run(mut self) -> Result<(), LogError> {
        loop {
            if let ControlFlow::Break(()) = self.step() {
                return Ok(());
            }
        }
    }

    /// One pass of the poll/command loop: drain commands, retry pending
    /// publishes, poll every subscription, then idle-wait for the next
    /// command at a cadence that escalates while nothing has work.
    /// `Break(())` means the thread must stop (`Shutdown`, or the command
    /// channel disconnected).
    fn step(&mut self) -> ControlFlow<()> {
        // Whether this pass did anything: handled a command, or polled at
        // least one fragment. Drives the idle backoff. An empty streak
        // escalates the wait; any work snaps it back to base.
        let mut worked = match self.drain_commands() {
            ControlFlow::Break(()) => return ControlFlow::Break(()),
            ControlFlow::Continue(handled_any) => handled_any,
        };

        // 2. Attempt one offer per pending publish, preserving
        //    per-publication FIFO order. Successful or expired entries
        //    are removed.
        drain_pending(&self.pubs, &mut self.pending);

        // 3. Poll every subscription. This runs on every pass, even while
        //    a publish is back-pressured in `pending`, so a slow or
        //    stalled publish can never starve a subscription's image.
        worked |= self.poll_subscriptions();
        self.close_departed();

        // 4. Idle. Block only when there is genuinely nothing to do:
        //    nothing to poll and nothing pending. Otherwise wait at the
        //    poll/retry cadence without busy-spinning a core.
        if self.subs.iter().flatten().next().is_none() && self.pending.is_empty() {
            return match self.wait_for_cmd(Duration::from_millis(1)) {
                ControlFlow::Break(()) => ControlFlow::Break(()),
                ControlFlow::Continue(_) => ControlFlow::Continue(()),
            };
        }

        // Keep the 100 microsecond sub-poll/retry cadence while traffic
        // flows, but wake immediately on a new command instead of
        // blocking on `recv_timeout` for the full interval. With any
        // subscription open (always, in the services) this branch is the
        // steady state. Blocking the full 100 microseconds under every
        // ack-waited publish caps a serialized publisher's rate (the
        // sequencer's offer path first among them), so the wait uses
        // `recv_timeout` to wake early on a command. When quiet, the wait
        // escalates toward 1 ms (`IdleBackoff`), since a fixed 100
        // microsecond wake dominates a quiet loop's CPU with crossbeam's
        // pre-park spin, not work. A non-empty `pending` pins the base
        // cadence, because the retry timing of a back-pressured offer
        // must not degrade.
        let pending_or_worked = worked || !self.pending.is_empty();
        if pending_or_worked {
            self.backoff.reset();
        }
        let wait = if pending_or_worked {
            Duration::from_micros(100)
        } else {
            self.backoff.idle_wait()
        };
        let outcome = self.wait_for_cmd(wait);
        self.finish_wait(outcome)
    }

    /// Turn [`Self::wait_for_cmd`]'s outcome into `step`'s outcome. A stop
    /// signal passes through unchanged. A handled command also resets the
    /// idle backoff, matching every other work path this pass took.
    fn finish_wait(&mut self, outcome: ControlFlow<(), bool>) -> ControlFlow<()> {
        let ControlFlow::Continue(handled) = outcome else {
            return ControlFlow::Break(());
        };
        if handled {
            self.backoff.reset();
        }
        ControlFlow::Continue(())
    }

    /// Drain every queued command (non-blocking). Publishes are enqueued
    /// onto `pending`, never offered inline, so a back-pressured offer can
    /// never block this loop (see [`PendingPublish`]).
    ///
    /// `Break(())` means the thread must stop (`Shutdown`, or the command
    /// channel disconnected); the caller returns at that exact point.
    /// `Continue(true)` means at least one command was handled.
    fn drain_commands(&mut self) -> ControlFlow<(), bool> {
        let mut worked = false;
        loop {
            match self.try_next_cmd() {
                CmdStep::Handled => worked = true,
                CmdStep::Empty => return ControlFlow::Continue(worked),
                CmdStep::Stop => return ControlFlow::Break(()),
            }
        }
    }

    /// One non-blocking command-channel poll, for [`drain_commands`]'s loop.
    fn try_next_cmd(&mut self) -> CmdStep {
        match self.cmd_rx.try_recv() {
            Ok(RuntimeCmd::Shutdown) | Err(TryRecvError::Disconnected) => CmdStep::Stop,
            Ok(cmd) => {
                self.handle_cmd(cmd);
                CmdStep::Handled
            }
            Err(TryRecvError::Empty) => CmdStep::Empty,
        }
    }

    /// Poll every subscription and every destination for fragments.
    /// Returns whether any subscription delivered at least one fragment
    /// this pass. This is the per-iteration hot path, so it must poll
    /// every subscription and cannot short-circuit on the first one that
    /// has work.
    fn poll_subscriptions(&mut self) -> bool {
        let mut worked = false;
        let dests = self.dests.iter_mut().map(|d| &mut d.entry);
        let rows = self.subs.iter_mut().flatten().map(|r| &mut r.entry);
        for entry in rows.chain(dests) {
            worked |= entry.poll_once();
        }
        worked
    }

    /// Block waiting for the next command, up to `wait`. `Break(())` means
    /// the thread must stop. `Continue(true)` means a command arrived and
    /// was handled; `Continue(false)` means the wait timed out with
    /// nothing to do. The caller uses that distinction to decide whether
    /// to reset the idle backoff.
    fn wait_for_cmd(&mut self, wait: Duration) -> ControlFlow<(), bool> {
        match self.cmd_rx.recv_timeout(wait) {
            Ok(RuntimeCmd::Shutdown) | Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                ControlFlow::Break(())
            }
            Ok(cmd) => {
                self.handle_cmd(cmd);
                ControlFlow::Continue(true)
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => ControlFlow::Continue(false),
        }
    }

    /// Enqueue a publish onto the retry queue. Never offered inline here:
    /// it is enqueued and retried by `drain_pending`, so a back-pressured
    /// offer can never block the poll loop. `ack` is `Some` for an
    /// acknowledged publish, `None` for best effort.
    fn enqueue_publish(
        &mut self,
        pub_id: u32,
        bytes: rkyv::util::AlignedVec,
        ack: Option<CbSender<Result<BPosition, LogError>>>,
    ) {
        self.pending.push_back(PendingPublish {
            stream_id: self.pub_entry(pub_id).map(|e| e.stream_id),
            pub_id,
            bytes,
            ack,
            deadline: Instant::now() + OFFER_TIMEOUT,
        });
    }

    /// Make one offer of a lossy frame. A refused offer, or an unknown
    /// publication, drops the frame and counts it under the stream id.
    fn offer_lossy(&self, pub_id: u32, bytes: &rkyv::util::AlignedVec) {
        let Some(entry) = self.pub_entry(pub_id) else {
            return;
        };
        let code = entry.publication.offer(bytes.as_slice());
        if code < 0 {
            metrics::counter!(
                super::BEST_EFFORT_DROPPED_TOTAL,
                "stream_id" => entry.stream_id.to_string()
            )
            .increment(1);
        }
    }

    /// The open publication at `pub_id`, if any.
    fn pub_entry(&self, pub_id: u32) -> Option<&PubEntry> {
        self.pubs.get(pub_id as usize).and_then(Option::as_ref)
    }

    /// Close a publication: drop the row, which closes the Aeron
    /// publication. The slot stays `None`.
    fn cmd_close_publication(&mut self, pub_id: u32) -> Result<(), LogError> {
        let slot = self.pubs.get_mut(pub_id as usize).ok_or_else(|| {
            LogError::Aeron(format!("close publication: unknown pub_id {pub_id}"))
        })?;
        slot.take().ok_or_else(|| {
            LogError::Aeron(format!("close publication: pub_id {pub_id} is closed"))
        })?;
        Ok(())
    }

    /// Open a publication and append it to `pubs`, replying with its
    /// index and its Aeron session id.
    fn cmd_open_publication(&mut self, uri: &str, stream_id: i32) -> Result<OpenedPub, LogError> {
        let publication = self.open_pub(uri, stream_id, &AddWait::run_time(ADD_PUB_TIMEOUT))?;
        self.push_pub(TablePub::Shared(publication), stream_id)
    }

    /// Open an exclusive publication and append it to `pubs`, replying
    /// with its index and its own Aeron session id.
    fn cmd_open_exclusive_publication(
        &mut self,
        uri: &str,
        stream_id: i32,
    ) -> Result<OpenedPub, LogError> {
        let c = crate::ffi::c_uri(uri, "uri")?;
        let publication = self
            .aeron
            .add_exclusive_publication(c.as_c_str(), stream_id, ADD_PUB_TIMEOUT)
            .map_err(|e| LogError::Aeron(format!("add_exclusive_publication {uri}: {e}")))?;
        self.push_pub(TablePub::Exclusive(publication), stream_id)
    }

    /// Open a dynamic MDC publication whose control endpoint names port
    /// 0, wait for the control address the driver bound, and append the
    /// publication to `pubs`. A publication with no bound address drops
    /// here and never enters the table. The add waits as `wait` says.
    fn cmd_open_mdc_publication(
        &mut self,
        uri: &str,
        stream_id: i32,
        wait: &AddWait,
    ) -> Result<(OpenedPub, SocketAddr), LogError> {
        let publication = self.open_pub(uri, stream_id, wait)?;
        let control = BoundControl::new(&publication, uri).wait()?;
        Ok((
            self.push_pub(TablePub::Shared(publication), stream_id)?,
            control,
        ))
    }

    /// Append `publication` to `pubs` and return its index and its Aeron
    /// session id. Also reads the publication's term layout once (its
    /// `position_bits_to_shift` and `initial_term_id`), so later offer
    /// decodes never re-derive it.
    fn push_pub(&mut self, publication: TablePub, stream_id: i32) -> Result<OpenedPub, LogError> {
        let constants = publication.constants()?;
        let layout = TermLayout::from_publication(&constants)?;
        let session_id = constants.session_id();
        let pub_id = u32::try_from(self.pubs.len())
            .map_err(|_| LogError::Aeron("publication table exceeds u32::MAX entries".into()))?;
        self.pubs.push(Some(PubEntry {
            publication,
            layout,
            stream_id,
        }));
        Ok(OpenedPub {
            pub_id,
            session_id,
            layout,
        })
    }

    /// Open a subscription behind a fragment assembler, and append it to
    /// `subs`, replying with its index. The add waits as `wait` says.
    fn cmd_open_subscription(
        &mut self,
        uri: &str,
        stream_id: i32,
        sink: FrameSink,
        wait: &AddWait,
    ) -> Result<u32, LogError> {
        let entry = self.open_entry(uri, stream_id, sink.clone(), wait)?;
        let id = u32::try_from(self.subs.len())
            .map_err(|_| LogError::Aeron("subscription table exceeds u32::MAX entries".into()))?;
        self.subs.push(Some(SubRow {
            entry,
            stream_id,
            sink,
        }));
        Ok(id)
    }

    /// Open a subscription on `uri` behind a fragment assembler that sends
    /// every complete message to `sink`. The add waits as `wait` says.
    fn open_entry(
        &self,
        uri: &str,
        stream_id: i32,
        sink: FrameSink,
        wait: &AddWait,
    ) -> Result<SubEntry, LogError> {
        let sub = self.open_sub(uri, stream_id, wait)?;
        let (assembler, inner) =
            rusteron_client::Handler::leak_with_fragment_assembler(AssembledDeliver { sink })
                .map_err(|e| LogError::Aeron(format!("fragment assembler: {e:?}")))?;
        Ok(SubEntry {
            sub,
            assembler,
            inner,
            poll_failed: false,
        })
    }

    /// Close a subscription: its destinations drop first, then the row
    /// itself. Each drop releases the handlers and closes the Aeron
    /// subscription of the row. The slot stays `None`.
    fn cmd_close_subscription(&mut self, sub_id: u32) -> Result<(), LogError> {
        let slot = self.subs.get_mut(sub_id as usize).ok_or_else(|| {
            LogError::Aeron(format!("close subscription: unknown sub_id {sub_id}"))
        })?;
        let entry = slot.take().ok_or_else(|| {
            LogError::Aeron(format!("close subscription: sub_id {sub_id} is closed"))
        })?;
        self.dests.retain(|d| d.sub_id != sub_id);
        drop(entry);
        Ok(())
    }

    /// Detach a destination from a subscription. The destination
    /// lingers until its image goes (see [`Destination`]).
    fn cmd_remove_destination(&mut self, sub_id: u32, uri: &str) -> Result<(), LogError> {
        let dest = self
            .dests
            .iter_mut()
            .find(|d| d.is(sub_id, uri))
            .ok_or_else(|| {
                LogError::Aeron(format!(
                    "remove destination: no attached {uri} on sub {sub_id}"
                ))
            })?;
        dest.leave(Instant::now(), self.linger);
        Ok(())
    }

    /// Close every detached destination whose image went or whose linger
    /// ended.
    fn close_departed(&mut self) {
        let now = Instant::now();
        self.dests.retain(|d| !d.closes(now));
    }

    fn handle_cmd(&mut self, cmd: RuntimeCmd) {
        match cmd {
            RuntimeCmd::Publish { pub_id, bytes, ack } => {
                self.enqueue_publish(pub_id, bytes, Some(ack));
            }
            RuntimeCmd::PublishBestEffort { pub_id, bytes } => {
                self.enqueue_publish(pub_id, bytes, None);
            }
            RuntimeCmd::PublishLossy { pub_id, bytes } => {
                self.offer_lossy(pub_id, &bytes);
            }
            RuntimeCmd::OpenPublication {
                uri,
                stream_id,
                ack,
            } => {
                let _ = ack.send(self.cmd_open_publication(&uri, stream_id));
            }
            RuntimeCmd::OpenExclusivePublication {
                uri,
                stream_id,
                ack,
            } => {
                let _ = ack.send(self.cmd_open_exclusive_publication(&uri, stream_id));
            }
            RuntimeCmd::OpenMdcPublication {
                uri,
                stream_id,
                wait,
                ack,
            } => {
                let _ = ack.send(self.cmd_open_mdc_publication(&uri, stream_id, &wait));
            }
            RuntimeCmd::OpenSubscription {
                uri,
                stream_id,
                sink,
                wait,
                ack,
            } => {
                let _ = ack.send(self.cmd_open_subscription(&uri, stream_id, sink, &wait));
            }
            RuntimeCmd::SubAddDestination { sub_id, uri, ack } => {
                let _ = ack.send(self.add_sub_destination(sub_id, &uri));
            }
            RuntimeCmd::SubRemoveDestination { sub_id, uri, ack } => {
                let _ = ack.send(self.cmd_remove_destination(sub_id, &uri));
            }
            RuntimeCmd::CloseSubscription { sub_id, ack } => {
                let _ = ack.send(self.cmd_close_subscription(sub_id));
            }
            RuntimeCmd::ClosePublication { pub_id, ack } => {
                let _ = ack.send(self.cmd_close_publication(pub_id));
            }
            RuntimeCmd::Shutdown => {}
        }
    }

    /// Add a publication, and wait for it as `wait` says.
    fn open_pub(&self, uri: &str, stream_id: i32, wait: &AddWait) -> Result<super::Pub, LogError> {
        let c = crate::ffi::c_uri(uri, "uri")?;
        let poller = self
            .aeron
            .async_add_publication(c.as_c_str(), stream_id)
            .map_err(|e| LogError::Aeron(format!("add_publication {uri}: {e}")))?;
        let add = PubAdd {
            aeron: &self.aeron,
            poller,
        };
        wait.complete(&add, "add_publication", uri)
    }

    /// Add a subscription with the image log of this thread, and wait for
    /// it as `wait` says.
    fn open_sub(&self, uri: &str, stream_id: i32, wait: &AddWait) -> Result<Sub, LogError> {
        let c = crate::ffi::c_uri(uri, "uri")?;
        let poller = self
            .aeron
            .async_add_subscription(
                c.as_c_str(),
                stream_id,
                Some(self.image_log.available),
                Some(self.image_log.unavailable),
            )
            .map_err(|e| LogError::Aeron(format!("add_subscription {uri}: {e}")))?;
        let add = SubAdd {
            aeron: &self.aeron,
            poller,
        };
        wait.complete(&add, "add_subscription", uri)
    }

    /// Attach a source endpoint (`uri`, for example
    /// `aeron:udp?endpoint=10.0.0.5:9000`) to the subscription `sub_id`:
    /// open a subscription on `uri` with the stream id and the sink of
    /// `sub_id`. Idempotent. A detached destination that still lingers
    /// is attached again with its open subscription and image.
    fn add_sub_destination(&mut self, sub_id: u32, uri: &str) -> Result<(), LogError> {
        let sub = self
            .subs
            .get(sub_id as usize)
            .and_then(Option::as_ref)
            .ok_or_else(|| LogError::Aeron(format!("add destination: unknown sub_id {sub_id}")))?;
        if let Some(dest) = self.dests.iter_mut().find(|d| d.is(sub_id, uri)) {
            dest.stay();
            return Ok(());
        }
        let wait = AddWait::run_time(ADD_SUB_TIMEOUT);
        let entry = self.open_entry(uri, sub.stream_id, sub.sink.clone(), &wait)?;
        self.dests.push(Destination::new(sub_id, uri, entry));
        Ok(())
    }
}

/// Read the fragment-start [`BPosition`] and the Aeron publisher
/// `session_id` from a single `get_values()` FFI call, the hottest
/// per-fragment path. They must come from the same header read: returning
/// them together guarantees the session belongs to the position's
/// fragment, and avoids a divergent second-read error path where a
/// transient failure could mint a spurious session 0 that collides with a
/// genuine session-0 publisher's join key.
fn header_loc(h: &Header) -> Option<(BPosition, i32)> {
    let v = h.get_values().ok()?;
    let frame = v.frame();
    let pos = BPosition {
        term_id: frame.term_id(),
        term_offset: frame.term_offset(),
    };
    Some((pos, frame.session_id()))
}
