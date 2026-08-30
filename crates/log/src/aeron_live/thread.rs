//! The dedicated Aeron thread: the poll/command loop that owns every
//! `!Send` rusteron object (client, publications, subscriptions, MDS
//! destinations). Everything here runs on the one `kardamom-aeron` OS
//! thread; the rest of the module talks to it exclusively through
//! [`RuntimeCmd`]s.

use std::collections::VecDeque;
use std::ffi::CString;
use std::rc::Rc;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver as CbReceiver, TryRecvError};

use super::pending::{IdleBackoff, PendingPublish, drain_pending};
use super::runtime::RuntimeCmd;
use super::{ADD_PUB_TIMEOUT, ADD_SUB_TIMEOUT, AeronClient, DeliverFn, Header, Pub, Sub};
use crate::error::LogError;
use crate::offer_retry::OFFER_TIMEOUT;
use kardamom_types::BPosition;

/// Adapter between rusteron's fragment-handler callback and a
/// [`DeliverFn`], sitting behind an
/// [`rusteron_client::AeronFragmentAssembler`]. It runs once per complete
/// message, with multi-fragment messages (any frame larger than one Aeron
/// MTU, about 1.4 KB) already reassembled. Without the assembler,
/// `aeron_subscription_poll` hands over raw fragments, and every oversized
/// frame fails to decode at the consumer. This once happened live when
/// `Vec<Receipt>` batch frames crossed the MTU: ingress, sequencer, and
/// validator all logged decode failures, the lost receipts left parked
/// submits hanging to their 60 s timeout, and the load edge collapsed to
/// 500 tx/s. The header passed through belongs to the final fragment. Both
/// ends of a position-keyed stream (the tx_data join) go through this same
/// path, so position derivation stays consistent.
struct AssembledDeliver {
    deliver: DeliverFn,
}

impl rusteron_client::AeronFragmentHandlerCallback for AssembledDeliver {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], header: Header) {
        if let Some((pos, session)) = header_loc(&header) {
            (self.deliver)(buffer, pos, session);
        }
    }
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
}

impl Drop for SubEntry {
    fn drop(&mut self) {
        self.assembler.release();
        self.inner.release();
    }
}

pub(super) fn run_aeron_thread(
    aeron: Rc<AeronClient>,
    cmd_rx: CbReceiver<RuntimeCmd>,
) -> Result<(), LogError> {
    let mut pubs: Vec<Pub> = Vec::new();
    let mut subs: Vec<SubEntry> = Vec::new();
    let mut pending: VecDeque<PendingPublish> = VecDeque::new();
    // Live MDS destinations. The rusteron `AeronAsyncDestination` removes
    // its destination when dropped, so this code must retain each one for
    // as long as the attachment should stay active, keyed by (sub_id, uri)
    // for removal.
    let mut dests: Vec<(u32, String, rusteron_client::AeronAsyncDestination)> = Vec::new();
    // Escalating idle wait for the busy branch: base 100 microseconds (the
    // established sub-poll/retry cadence), cap 1 ms (the empty-branch
    // cadence), grace 10 (about 1 ms of consecutive emptiness before the
    // first escalation).
    let mut backoff = IdleBackoff::new(Duration::from_micros(100), Duration::from_millis(1), 10);

    loop {
        // Whether this iteration did anything: handled a command, or
        // polled at least one fragment. Drives the idle backoff. An empty
        // streak escalates the wait; any work snaps it back to base.
        let mut worked = false;

        // 1. Drain all queued commands (non-blocking). Publishes are
        //    enqueued onto `pending`, never offered inline, so a
        //    back-pressured offer can never block this loop (see
        //    `PendingPublish`).
        loop {
            match cmd_rx.try_recv() {
                Ok(RuntimeCmd::Shutdown) => return Ok(()),
                Ok(cmd) => {
                    worked = true;
                    handle_cmd(&aeron, &mut pubs, &mut subs, &mut pending, &mut dests, cmd)?;
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return Ok(()),
            }
        }

        // 2. Attempt one offer per pending publish, preserving
        //    per-publication FIFO order. Successful or expired entries are
        //    removed.
        drain_pending(&pubs, &mut pending);

        // 3. Poll every subscription. This runs on every iteration, even
        //    while a publish is back-pressured in `pending`, so a slow or
        //    stalled publish can never starve a subscription's image. This
        //    is the fix for the cluster `tx_ordering` freeze.
        for entry in subs.iter_mut() {
            let fragments = entry.sub.poll(Some(&entry.assembler), 64);
            worked |= fragments.unwrap_or(0) > 0;
        }

        // 4. Idle. Block only when there is genuinely nothing to do:
        //    nothing to poll and nothing pending. Otherwise wait at the
        //    poll/retry cadence without busy-spinning a core.
        if subs.is_empty() && pending.is_empty() {
            match cmd_rx.recv_timeout(Duration::from_millis(1)) {
                Ok(RuntimeCmd::Shutdown) => return Ok(()),
                Ok(cmd) => handle_cmd(&aeron, &mut pubs, &mut subs, &mut pending, &mut dests, cmd)?,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return Ok(()),
            }
        } else {
            // Keep the 100 microsecond sub-poll/retry cadence while traffic
            // flows, but wake immediately on a new command instead of
            // sleeping through it. With any subscription open (always, in
            // the services) this branch is the steady state, and a plain
            // sleep put up to 100 microseconds of latency under every
            // ack-waited publish, a hard cap of about 10k/s on any
            // serialized publisher (the sequencer's offer path first among
            // them). When quiet, the wait escalates toward 1 ms
            // (IdleBackoff). Waking every 100 microseconds regardless of
            // traffic measured at about 66% of the sequencer's CPU, almost
            // all of it crossbeam's pre-park spin. A non-empty `pending`
            // pins the base cadence, because the retry timing of a
            // back-pressured offer must not degrade.
            if worked || !pending.is_empty() {
                backoff.reset();
            }
            let wait = if worked || !pending.is_empty() {
                Duration::from_micros(100)
            } else {
                backoff.idle_wait()
            };
            match cmd_rx.recv_timeout(wait) {
                Ok(RuntimeCmd::Shutdown) => return Ok(()),
                Ok(cmd) => {
                    backoff.reset();
                    handle_cmd(&aeron, &mut pubs, &mut subs, &mut pending, &mut dests, cmd)?;
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => return Ok(()),
            }
        }
    }
}

fn handle_cmd(
    aeron: &Rc<AeronClient>,
    pubs: &mut Vec<Pub>,
    subs: &mut Vec<SubEntry>,
    pending: &mut VecDeque<PendingPublish>,
    dests: &mut Vec<(u32, String, rusteron_client::AeronAsyncDestination)>,
    cmd: RuntimeCmd,
) -> Result<(), LogError> {
    match cmd {
        // Publishes are never offered here. They are enqueued and retried
        // by `drain_pending`, so a back-pressured offer cannot block the
        // poll loop.
        RuntimeCmd::Publish { pub_id, bytes, ack } => {
            pending.push_back(PendingPublish {
                pub_id,
                bytes,
                ack: Some(ack),
                deadline: Instant::now() + OFFER_TIMEOUT,
            });
        }
        RuntimeCmd::PublishBestEffort { pub_id, bytes } => {
            pending.push_back(PendingPublish {
                pub_id,
                bytes,
                ack: None,
                deadline: Instant::now() + OFFER_TIMEOUT,
            });
        }
        RuntimeCmd::OpenPublication {
            uri,
            stream_id,
            ack,
        } => {
            let res = open_pub(aeron, &uri, stream_id).map(|p| {
                pubs.push(p);
                (pubs.len() - 1) as u32
            });
            let _ = ack.send(res);
        }
        RuntimeCmd::OpenSubscription {
            uri,
            stream_id,
            deliver,
            ack,
        } => {
            let res = open_sub(aeron, &uri, stream_id).and_then(|sub| {
                let (assembler, inner) =
                    rusteron_client::Handler::leak_with_fragment_assembler(AssembledDeliver {
                        deliver,
                    })
                    .map_err(|e| LogError::Aeron(format!("fragment assembler: {e:?}")))?;
                subs.push(SubEntry {
                    sub,
                    assembler,
                    inner,
                });
                Ok((subs.len() - 1) as u32)
            });
            let _ = ack.send(res);
        }
        RuntimeCmd::SubAddDestination { sub_id, uri, ack } => {
            let res = add_sub_destination(aeron, subs, dests, sub_id, &uri);
            let _ = ack.send(res);
        }
        RuntimeCmd::SubRemoveDestination { sub_id, uri, ack } => {
            // Dropping the retained `AeronAsyncDestination` issues the
            // async remove command to the driver. This is best effort: a
            // removed source's image also times out on its own.
            let before = dests.len();
            dests.retain(|(s, u, _)| !(*s == sub_id && *u == uri));
            let res = if dests.len() < before {
                Ok(())
            } else {
                Err(LogError::Aeron(format!(
                    "remove destination: no attached {uri} on sub {sub_id}"
                )))
            };
            let _ = ack.send(res);
        }
        RuntimeCmd::Shutdown => {}
    }
    Ok(())
}

fn open_pub(aeron: &Rc<AeronClient>, uri: &str, stream_id: i32) -> Result<Pub, LogError> {
    let c = CString::new(uri).map_err(|e| LogError::Aeron(format!("uri contains NUL: {e}")))?;
    aeron
        .add_publication(c.as_c_str(), stream_id, ADD_PUB_TIMEOUT)
        .map_err(|e| LogError::Aeron(format!("add_publication {uri}: {e}")))
}

fn open_sub(aeron: &Rc<AeronClient>, uri: &str, stream_id: i32) -> Result<Sub, LogError> {
    let c = CString::new(uri).map_err(|e| LogError::Aeron(format!("uri contains NUL: {e}")))?;
    aeron
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
/// subscription, and retain the returned `AeronAsyncDestination` so the
/// attachment stays live (dropping it issues the async remove).
/// Idempotent. Blocks the Aeron thread only briefly to poll the driver's
/// async completion, since destination changes are rare (membership
/// churn), unlike steady-state publishing.
fn add_sub_destination(
    aeron: &Rc<AeronClient>,
    subs: &[SubEntry],
    dests: &mut Vec<(u32, String, rusteron_client::AeronAsyncDestination)>,
    sub_id: u32,
    uri: &str,
) -> Result<(), LogError> {
    let sub = subs
        .get(sub_id as usize)
        .ok_or_else(|| LogError::Aeron(format!("add destination: unknown sub_id {sub_id}")))?;
    if dests.iter().any(|(s, u, _)| *s == sub_id && u == uri) {
        return Ok(()); // already attached
    }
    let c = CString::new(uri).map_err(|e| LogError::Aeron(format!("destination uri NUL: {e}")))?;
    let dest = rusteron_client::AeronAsyncDestination::aeron_subscription_async_add_destination(
        aeron,
        &sub.sub,
        c.as_c_str(),
    )
    .map_err(|e| LogError::Aeron(format!("add destination {uri}: {e}")))?;
    let start = Instant::now();
    loop {
        match dest.aeron_subscription_async_destination_poll() {
            Ok(1) => break,
            Ok(_) => {}
            Err(e) => return Err(LogError::Aeron(format!("destination poll {uri}: {e}"))),
        }
        if start.elapsed() > ADD_SUB_TIMEOUT {
            return Err(LogError::Aeron(format!("add destination {uri} timed out")));
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    dests.push((sub_id, uri.to_string(), dest));
    Ok(())
}

/// Decode Aeron's packed `offer` return position into a [`BPosition`].
pub(super) fn decode_position(p: i64) -> BPosition {
    let term_id = (p >> 32) as i32;
    let term_offset = (p & 0xFFFF_FFFF) as i32;
    BPosition {
        term_id,
        term_offset,
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
