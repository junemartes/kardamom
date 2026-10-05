//! The two tasks of one owned webhook subscription: the appender, which
//! writes the live events the filter selects to the outbox, and the
//! deliverer, which posts them from a persisted cursor, at least once.
//!
//! The retry schedule is exponential from one second, capped at one
//! minute, for ten attempts. After that the deliverer gives up on the
//! event, counts it, and moves on: one dead endpoint never wedges the
//! outbox behind it.

use std::num::NonZeroUsize;
use std::ops::ControlFlow;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use crate::dto::{StatusFilter, TxStatusEvent};
use crate::metrics as m;
use crate::outbox::{Cursor, OutboxReader, OutboxRecord, OutboxWriter};
use crate::ring::Stamped;
use crate::webhooks::{
    IDEMPOTENCY_HEADER, SIGNATURE_HEADER, SUBSCRIPTION_HEADER, Subscription, WebhookError,
};

/// The retry schedule: `BACKOFF_BASE * 2^(attempt - 1)`, at most
/// `BACKOFF_CAP`, for `MAX_ATTEMPTS` attempts.
const MAX_ATTEMPTS: u32 = 10;
const BACKOFF_BASE: Duration = Duration::from_secs(1);
const BACKOFF_CAP: Duration = Duration::from_secs(60);
/// The cursor is stored after this many deliveries, and whenever the
/// loop idles.
const CURSOR_EVERY: u32 = 64;

/// The idempotency key of one event: `<tx_hash>:<stage>`.
#[must_use]
pub fn idempotency_key(event: &TxStatusEvent) -> String {
    let stage = serde_json::to_value(event.stage)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_default();
    format!("{:#x}:{stage}", event.tx_hash)
}

/// What one worker needs to start.
pub(crate) struct WorkerSpec<'a> {
    pub(crate) sub: &'a Subscription,
    /// The directory of the outbox and the cursor.
    pub(crate) dir: &'a Path,
    pub(crate) http: reqwest::Client,
    pub(crate) retain_bytes: u64,
    pub(crate) queue: NonZeroUsize,
    pub(crate) shutdown: CancellationToken,
}

/// An owned subscription's live side: its filter and the queue ahead of
/// its appender.
pub(crate) struct Worker {
    pub(crate) filter: StatusFilter,
    pub(crate) events: mpsc::Sender<Stamped>,
}

impl WorkerSpec<'_> {
    /// Open the outbox, and start the appender and the deliverer.
    pub(crate) fn start(self) -> Result<Worker, WebhookError> {
        let sub = self.sub;
        let label = format!("{:#x}", sub.id);
        let outbox = self.dir.join(format!("{:#x}.outbox", sub.id));
        let writer = OutboxWriter::open(&outbox)?;
        let cursor = Cursor::new(self.dir.join(format!("{:#x}.cursor", sub.id)));
        // A cursor past the file names a cut the cursor store missed.
        let start = cursor.load().min(writer.len());
        let reader = OutboxReader::open(&outbox, start)?;
        let (len_tx, len_rx) = watch::channel(writer.len());
        let (events_tx, events_rx) = mpsc::channel(self.queue.get());
        let (truncate_tx, truncate_rx) = mpsc::channel(1);
        tokio::spawn(
            Appender {
                writer,
                events: events_rx,
                truncate: truncate_rx,
                len: len_tx,
                label: label.clone(),
            }
            .run(),
        );
        tokio::spawn(
            Deliverer {
                sub: sub.clone(),
                reader,
                cursor,
                len: len_rx,
                truncate: truncate_tx,
                http: self.http,
                retain_bytes: self.retain_bytes,
                dirty: 0,
                label,
                shutdown: self.shutdown,
            }
            .run(),
        );
        Ok(Worker {
            filter: sub.filter,
            events: events_tx,
        })
    }
}

/// A request from the deliverer to cut a fully delivered outbox.
struct Truncate {
    at_len: u64,
    reply: oneshot::Sender<bool>,
}

/// The appender of one owned subscription.
struct Appender {
    writer: OutboxWriter,
    events: mpsc::Receiver<Stamped>,
    truncate: mpsc::Receiver<Truncate>,
    /// The outbox length, published after every append and every cut.
    len: watch::Sender<u64>,
    label: String,
}

enum AppendStep {
    Event(Stamped),
    Truncate(Truncate),
    End,
}

impl Appender {
    async fn run(mut self) {
        while self.step().await.is_continue() {}
    }

    async fn step(&mut self) -> ControlFlow<()> {
        let next = tokio::select! {
            ev = self.events.recv() => ev.map_or(AppendStep::End, AppendStep::Event),
            t = self.truncate.recv() => t.map_or(AppendStep::End, AppendStep::Truncate),
        };
        match next {
            AppendStep::Event(stamped) => self.append(&stamped),
            AppendStep::Truncate(t) => self.cut(t),
            AppendStep::End => return ControlFlow::Break(()),
        }
        ControlFlow::Continue(())
    }

    fn append(&mut self, stamped: &Stamped) {
        let record = OutboxRecord::new(stamped.seq, unix_ms(), &stamped.event);
        match self.writer.append(&record) {
            Ok(len) => {
                metrics::counter!(m::OUTBOX_APPENDED_TOTAL, "subscription" => self.label.clone())
                    .increment(1);
                self.len.send_replace(len);
            }
            Err(e) => {
                tracing::error!(subscription = %self.label, error = %e, "outbox append failed");
            }
        }
    }

    fn cut(&mut self, t: Truncate) {
        let cut = self
            .writer
            .truncate_if_len(t.at_len)
            .inspect_err(|e| {
                tracing::error!(subscription = %self.label, error = %e, "outbox truncate failed");
            })
            .unwrap_or(false);
        if cut {
            self.len.send_replace(0);
        }
        let _ = t.reply.send(cut);
    }
}

/// The delivery loop of one owned subscription.
struct Deliverer {
    sub: Subscription,
    reader: OutboxReader,
    cursor: Cursor,
    len: watch::Receiver<u64>,
    truncate: mpsc::Sender<Truncate>,
    http: reqwest::Client,
    retain_bytes: u64,
    /// Deliveries since the cursor was stored.
    dirty: u32,
    label: String,
    shutdown: CancellationToken,
}

/// How one event's delivery ended.
enum Outcome {
    Delivered,
    GaveUp,
    /// Shutdown fired mid-retry. The cursor stays, so the event goes
    /// again on the next start.
    Interrupted,
}

impl Deliverer {
    async fn run(mut self) {
        while self.step().await.is_continue() {}
        self.store_cursor();
    }

    async fn step(&mut self) -> ControlFlow<()> {
        match self.reader.read_next() {
            Ok(Some(record)) => self.deliver_one(&record).await,
            Ok(None) => self.idle().await,
            Err(e) => {
                tracing::error!(
                    subscription = %self.label,
                    pos = self.reader.pos(),
                    error = %e,
                    "outbox unreadable; delivery stops for this subscription"
                );
                ControlFlow::Break(())
            }
        }
    }

    async fn deliver_one(&mut self, record: &OutboxRecord) -> ControlFlow<()> {
        let outcome = match record.event() {
            Some(event) => self.deliver(&event, record.at_unix_ms).await,
            // A stage code this build does not know: skip the record.
            None => Outcome::GaveUp,
        };
        let label = match outcome {
            Outcome::Delivered => "delivered",
            Outcome::GaveUp => "gave_up",
            Outcome::Interrupted => return ControlFlow::Break(()),
        };
        metrics::counter!(
            m::OUTBOX_DELIVERED_TOTAL,
            "subscription" => self.label.clone(),
            "outcome" => label
        )
        .increment(1);
        // Saturating: the count resets at `CURSOR_EVERY`.
        self.dirty = self.dirty.saturating_add(1);
        if self.dirty >= CURSOR_EVERY {
            self.store_cursor();
        }
        self.report_backlog();
        ControlFlow::Continue(())
    }

    /// Post `event` until it lands or the attempts run out.
    async fn deliver(&self, event: &TxStatusEvent, at_unix_ms: u64) -> Outcome {
        let Ok(body) = serde_json::to_vec(event) else {
            return Outcome::GaveUp;
        };
        let key = idempotency_key(event);
        for attempt in 1..=MAX_ATTEMPTS {
            if let ControlFlow::Break(outcome) =
                self.attempt(attempt, &body, &key, at_unix_ms).await
            {
                return outcome;
            }
        }
        Outcome::GaveUp
    }

    /// One POST. `Break` ends the delivery; `Continue` follows the
    /// retry wait.
    async fn attempt(
        &self,
        attempt: u32,
        body: &[u8],
        key: &str,
        at_unix_ms: u64,
    ) -> ControlFlow<Outcome> {
        if self.post(body, key).await {
            metrics::counter!(m::WEBHOOK_ATTEMPTS_TOTAL, "outcome" => "ok").increment(1);
            metrics::histogram!(m::WEBHOOK_DELIVERY_SECONDS)
                .record(Duration::from_millis(unix_ms().saturating_sub(at_unix_ms)).as_secs_f64());
            return ControlFlow::Break(Outcome::Delivered);
        }
        if attempt >= MAX_ATTEMPTS {
            tracing::warn!(subscription = %self.label, key, "webhook delivery gave up");
            return ControlFlow::Break(Outcome::GaveUp);
        }
        tokio::select! {
            () = tokio::time::sleep(backoff(attempt)) => ControlFlow::Continue(()),
            () = self.shutdown.cancelled() => ControlFlow::Break(Outcome::Interrupted),
        }
    }

    /// One signed POST. True on a 2xx answer.
    async fn post(&self, body: &[u8], key: &str) -> bool {
        let sent = self
            .http
            .post(&self.sub.url)
            .header("content-type", "application/json")
            .header(SIGNATURE_HEADER, self.sub.sign(body))
            .header(IDEMPOTENCY_HEADER, key)
            .header(SUBSCRIPTION_HEADER, format!("{:#x}", self.sub.id))
            .body(body.to_vec())
            .send()
            .await;
        match sent {
            Ok(r) if r.status().is_success() => true,
            Ok(r) => {
                metrics::counter!(m::WEBHOOK_ATTEMPTS_TOTAL, "outcome" => "status").increment(1);
                tracing::debug!(subscription = %self.label, status = %r.status(), "webhook refused");
                false
            }
            Err(e) => {
                metrics::counter!(m::WEBHOOK_ATTEMPTS_TOTAL, "outcome" => "error").increment(1);
                tracing::debug!(subscription = %self.label, error = %e, "webhook unreachable");
                false
            }
        }
    }

    /// Nothing to deliver: store the cursor, cut a long and fully
    /// delivered outbox, then wait for the appender.
    async fn idle(&mut self) -> ControlFlow<()> {
        self.store_cursor();
        self.report_backlog();
        self.maybe_truncate().await;
        let len = *self.len.borrow_and_update();
        if self.reader.pos() < len {
            return ControlFlow::Continue(());
        }
        tokio::select! {
            changed = self.len.changed() => {
                if changed.is_err() {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            }
            () = self.shutdown.cancelled() => ControlFlow::Break(()),
        }
    }

    /// Ask the appender to cut the outbox when every byte is delivered
    /// and the file is past the retention bound. The appender refuses
    /// when it appended since, and the next idle asks again.
    async fn maybe_truncate(&mut self) {
        let len = *self.len.borrow();
        if len < self.retain_bytes || self.reader.pos() < len {
            return;
        }
        let (reply, rx) = oneshot::channel();
        let asked = self
            .truncate
            .send(Truncate { at_len: len, reply })
            .await
            .is_ok();
        if asked && rx.await.unwrap_or(false) {
            self.reader.seek(0);
            self.dirty = self.dirty.saturating_add(1);
            self.store_cursor();
        }
    }

    fn store_cursor(&mut self) {
        if self.dirty == 0 {
            return;
        }
        match self.cursor.store(self.reader.pos()) {
            Ok(()) => self.dirty = 0,
            Err(e) => {
                tracing::error!(subscription = %self.label, error = %e, "cursor store failed");
            }
        }
    }

    fn report_backlog(&self) {
        let backlog = self.len.borrow().saturating_sub(self.reader.pos());
        metrics::gauge!(m::OUTBOX_BACKLOG_BYTES, "subscription" => self.label.clone())
            .set(m::count(usize::try_from(backlog).unwrap_or(usize::MAX)));
    }
}

/// The wait before retry `attempt + 1`: `1s, 2s, 4s, …`, capped at one
/// minute. The shift cannot overflow: attempts stop at ten.
fn backoff(attempt: u32) -> Duration {
    BACKOFF_BASE
        .saturating_mul(1u32 << attempt.saturating_sub(1).min(31))
        .min(BACKOFF_CAP)
}

fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;

    #[test]
    fn the_backoff_doubles_and_caps() {
        assert_eq!(backoff(1), Duration::from_secs(1));
        assert_eq!(backoff(2), Duration::from_secs(2));
        assert_eq!(backoff(6), Duration::from_secs(32));
        assert_eq!(backoff(7), Duration::from_secs(60));
        assert_eq!(backoff(10), Duration::from_secs(60));
    }

    #[test]
    fn the_idempotency_key_is_hash_and_stage() {
        let event = TxStatusEvent {
            tx_hash: B256::repeat_byte(0xab),
            sender: None,
            nonce: None,
            stage: crate::dto::Stage::Sealed,
            status: None,
            reason: None,
            expected_nonce: None,
        };
        assert_eq!(
            idempotency_key(&event),
            format!("{:#x}:sealed", B256::repeat_byte(0xab))
        );
    }
}
