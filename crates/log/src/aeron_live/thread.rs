//! The dedicated Aeron thread: the poll/command loop that owns every
//! `!Send` rusteron object (client, publications, subscriptions, MDS
//! destinations). Everything here runs on the one `kardamom-aeron` OS
//! thread; the rest of the module talks to it exclusively through
//! [`RuntimeCmd`]s.

use std::collections::VecDeque;
use std::ops::ControlFlow;
use std::rc::Rc;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver as CbReceiver, Sender as CbSender, TryRecvError};
use tracing::warn;

use super::pending::{IdleBackoff, PendingPublish, PubEntry, drain_pending};
use super::runtime::RuntimeCmd;
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
/// frame (for example a `Vec<Receipt>` batch that crosses the MTU) fails
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

/// One row in the Aeron thread's subscription table.
struct SubEntry {
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
) -> Result<(), LogError> {
    AeronThread::new(aeron, cmd_rx).run()
}

/// Live MDS destination attachment, keyed by `(sub_id, uri)` for removal.
/// `_handle`, the rusteron `AeronAsyncDestination`, removes its
/// destination when dropped, so this must be retained for as long as the
/// attachment should stay active.
struct Destination {
    sub_id: u32,
    uri: String,
    // RAII: dropping this field issues the async remove-destination
    // command to the driver. The field is never read; its only purpose
    // is the drop.
    _handle: rusteron_client::AeronAsyncDestination,
}

/// The Aeron thread's whole state: every `!Send` rusteron object it owns,
/// plus the retry queue and idle cadence. Every command handler and poll
/// step is a method here, so a step's inputs are struct fields, not
/// parameters threaded through free functions.
struct AeronThread {
    aeron: Rc<AeronClient>,
    cmd_rx: CbReceiver<RuntimeCmd>,
    pubs: Vec<PubEntry>,
    subs: Vec<SubEntry>,
    pending: VecDeque<PendingPublish>,
    dests: Vec<Destination>,
    /// Escalating idle wait for the busy branch: base 100 microseconds (the
    /// established sub-poll/retry cadence), cap 1 ms (the empty-branch
    /// cadence), grace 10 (about 1 ms of consecutive emptiness before the
    /// first escalation).
    backoff: IdleBackoff,
}

impl AeronThread {
    fn new(aeron: Rc<AeronClient>, cmd_rx: CbReceiver<RuntimeCmd>) -> Self {
        Self {
            aeron,
            cmd_rx,
            pubs: Vec::new(),
            subs: Vec::new(),
            pending: VecDeque::new(),
            dests: Vec::new(),
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

        // 4. Idle. Block only when there is genuinely nothing to do:
        //    nothing to poll and nothing pending. Otherwise wait at the
        //    poll/retry cadence without busy-spinning a core.
        if self.subs.is_empty() && self.pending.is_empty() {
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

    /// Poll every subscription for fragments. Returns whether any
    /// subscription delivered at least one fragment this pass. This is
    /// the per-iteration hot path, so it must poll every subscription and
    /// cannot short-circuit on the first one that has work.
    fn poll_subscriptions(&mut self) -> bool {
        let mut worked = false;
        for entry in &mut self.subs {
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
            pub_id,
            bytes,
            ack,
            deadline: Instant::now() + OFFER_TIMEOUT,
        });
    }

    /// Open a publication and append it to `pubs`, replying with its
    /// index. Also reads the publication's term layout once (its
    /// `position_bits_to_shift` and `initial_term_id`), so later offer
    /// decodes never re-derive it.
    fn cmd_open_publication(&mut self, uri: &str, stream_id: i32) -> Result<u32, LogError> {
        let publication = self.open_pub(uri, stream_id)?;
        let layout = TermLayout::from_publication(&publication)?;
        let id = u32::try_from(self.pubs.len())
            .map_err(|_| LogError::Aeron("publication table exceeds u32::MAX entries".into()))?;
        self.pubs.push(PubEntry {
            publication,
            layout,
        });
        Ok(id)
    }

    /// Open a subscription behind a fragment assembler, and append it to
    /// `subs`, replying with its index.
    fn cmd_open_subscription(
        &mut self,
        uri: &str,
        stream_id: i32,
        sink: FrameSink,
    ) -> Result<u32, LogError> {
        let sub = self.open_sub(uri, stream_id)?;
        let (assembler, inner) =
            rusteron_client::Handler::leak_with_fragment_assembler(AssembledDeliver { sink })
                .map_err(|e| LogError::Aeron(format!("fragment assembler: {e:?}")))?;
        let id = u32::try_from(self.subs.len())
            .map_err(|_| LogError::Aeron("subscription table exceeds u32::MAX entries".into()))?;
        self.subs.push(SubEntry {
            sub,
            assembler,
            inner,
            poll_failed: false,
        });
        Ok(id)
    }

    /// Detach a source endpoint from an MDS subscription. Dropping the
    /// retained `AeronAsyncDestination` issues the async remove command to
    /// the driver. Best effort: a removed source's image also times out
    /// on its own.
    fn cmd_remove_destination(&mut self, sub_id: u32, uri: &str) -> Result<(), LogError> {
        let before = self.dests.len();
        self.dests.retain(|d| !(d.sub_id == sub_id && d.uri == uri));
        if self.dests.len() < before {
            Ok(())
        } else {
            Err(LogError::Aeron(format!(
                "remove destination: no attached {uri} on sub {sub_id}"
            )))
        }
    }

    fn handle_cmd(&mut self, cmd: RuntimeCmd) {
        match cmd {
            RuntimeCmd::Publish { pub_id, bytes, ack } => {
                self.enqueue_publish(pub_id, bytes, Some(ack));
            }
            RuntimeCmd::PublishBestEffort { pub_id, bytes } => {
                self.enqueue_publish(pub_id, bytes, None);
            }
            RuntimeCmd::OpenPublication {
                uri,
                stream_id,
                ack,
            } => {
                let _ = ack.send(self.cmd_open_publication(&uri, stream_id));
            }
            RuntimeCmd::OpenSubscription {
                uri,
                stream_id,
                sink,
                ack,
            } => {
                let _ = ack.send(self.cmd_open_subscription(&uri, stream_id, sink));
            }
            RuntimeCmd::SubAddDestination { sub_id, uri, ack } => {
                let _ = ack.send(self.add_sub_destination(sub_id, &uri));
            }
            RuntimeCmd::SubRemoveDestination { sub_id, uri, ack } => {
                let _ = ack.send(self.cmd_remove_destination(sub_id, &uri));
            }
            RuntimeCmd::Shutdown => {}
        }
    }

    fn open_pub(&self, uri: &str, stream_id: i32) -> Result<super::Pub, LogError> {
        let c = crate::ffi::c_uri(uri, "uri")?;
        self.aeron
            .add_publication(c.as_c_str(), stream_id, ADD_PUB_TIMEOUT)
            .map_err(|e| LogError::Aeron(format!("add_publication {uri}: {e}")))
    }

    fn open_sub(&self, uri: &str, stream_id: i32) -> Result<Sub, LogError> {
        let c = crate::ffi::c_uri(uri, "uri")?;
        self.aeron
            .add_subscription(
                c.as_c_str(),
                stream_id,
                rusteron_client::Handlers::no_available_image_handler(),
                rusteron_client::Handlers::no_unavailable_image_handler(),
                ADD_SUB_TIMEOUT,
            )
            .map_err(|e| LogError::Aeron(format!("add_subscription {uri}: {e}")))
    }

    /// Attach a source endpoint (`uri`, for example
    /// `aeron:udp?endpoint=10.0.0.5:9000`) to a `control-mode=manual` MDS
    /// subscription, and retain the returned `AeronAsyncDestination` so
    /// the attachment stays live (dropping it issues the async remove).
    /// Idempotent. Blocks the Aeron thread only briefly to poll the
    /// driver's async completion, since destination changes are rare
    /// (membership churn), unlike steady-state publishing.
    fn add_sub_destination(&mut self, sub_id: u32, uri: &str) -> Result<(), LogError> {
        let sub = self
            .subs
            .get(sub_id as usize)
            .ok_or_else(|| LogError::Aeron(format!("add destination: unknown sub_id {sub_id}")))?;
        if self
            .dests
            .iter()
            .any(|d| d.sub_id == sub_id && d.uri == uri)
        {
            return Ok(()); // already attached
        }
        let c = crate::ffi::c_uri(uri, "destination uri")?;
        let dest =
            rusteron_client::AeronAsyncDestination::aeron_subscription_async_add_destination(
                &self.aeron,
                &sub.sub,
                c.as_c_str(),
            )
            .map_err(|e| LogError::Aeron(format!("add destination {uri}: {e}")))?;
        poll_until_attached(&dest, Instant::now(), uri)?;
        self.dests.push(Destination {
            sub_id,
            uri: uri.to_string(),
            _handle: dest,
        });
        Ok(())
    }
}

/// Block until `dest`'s attach completes or `uri`'s add-destination call
/// times out (measured from `start`). Polls at a fixed 2 ms cadence.
fn poll_until_attached(
    dest: &rusteron_client::AeronAsyncDestination,
    start: Instant,
    uri: &str,
) -> Result<(), LogError> {
    loop {
        if let ControlFlow::Break(result) = poll_attach_step(dest, start, uri) {
            return result;
        }
    }
}

/// One attach-poll for [`poll_until_attached`]'s loop. `Break` carries the
/// answer: ready, a poll error, or a timeout past `ADD_SUB_TIMEOUT`.
/// `Continue` means the caller polls again, after this waits at the
/// fixed 2 ms cadence.
fn poll_attach_step(
    dest: &rusteron_client::AeronAsyncDestination,
    start: Instant,
    uri: &str,
) -> ControlFlow<Result<(), LogError>> {
    match dest.aeron_subscription_async_destination_poll() {
        Ok(1) => return ControlFlow::Break(Ok(())),
        Ok(_) => {}
        Err(e) => {
            return ControlFlow::Break(Err(LogError::Aeron(format!(
                "destination poll {uri}: {e}"
            ))));
        }
    }
    if start.elapsed() > ADD_SUB_TIMEOUT {
        return ControlFlow::Break(Err(LogError::Aeron(format!(
            "add destination {uri} timed out"
        ))));
    }
    std::thread::sleep(Duration::from_millis(2));
    ControlFlow::Continue(())
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
