//! The stream publisher thread: it takes the reader's items in order,
//! writes each record to the recorded and the live publication, keeps the
//! locator log, and computes the recorded cursor.

use std::ops::ControlFlow;
use std::thread::JoinHandle;
use std::time::Instant;

use anyhow::{Context, anyhow};
use crossbeam_channel::{Receiver, TryRecvError, select};
use kardamom_engine::ExecStreamItem;
use kardamom_log::error::LogError;
use kardamom_types::ExecTxRecord;
use rkyv::util::AlignedVec;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::cursor::RecordedCursor;
use super::locators::{LOCATOR_EVERY, Locator, LocatorLog};
use super::metrics::ExecStreamMetrics;

/// The two publications of the executor stream.
///
/// The recorded publication is the one the local archive records. It
/// cannot run ahead of the archive, so an offer can be refused while the
/// archive is slow or gone. The live publication is lossy: a consumer
/// repairs a gap from an archive.
pub(crate) trait StreamPublications: Send + 'static {
    /// The Aeron session id of the recorded publication.
    fn session_id(&self) -> i32;

    /// Offer `bytes` once to the recorded publication. Returns the raw
    /// stream position at the end of the record.
    ///
    /// # Errors
    ///
    /// Returns an error when the publication did not take the record. The
    /// record is then not on the stream, and the caller offers it again.
    fn offer_recorded(&self, bytes: &AlignedVec) -> Result<i64, LogError>;

    /// Offer `bytes` once to the live publication. A refused offer drops
    /// the record.
    fn offer_live(&self, bytes: &AlignedVec);
}

/// Everything the publisher thread takes.
pub(crate) struct PublisherInputs<P> {
    /// The reader's records and progress marks, in canonical order.
    pub(crate) items: Receiver<ExecStreamItem>,
    /// The recording positions that the recorder thread reads from the
    /// archive. The first one is the start of this session's recording.
    pub(crate) positions: Receiver<i64>,
    pub(crate) publications: P,
    pub(crate) locators: LocatorLog,
    /// Cancelled at shutdown. It ends a wait for a refused record.
    pub(crate) stop: CancellationToken,
}

/// Whether the publisher loop goes on.
enum Flow {
    Continue,
    Stop,
}

/// The state of the publisher thread. One thread owns it, so the locator
/// log and the cursor need no lock.
pub(crate) struct ExecStreamPublisher<P> {
    inputs: PublisherInputs<P>,
    cursor: RecordedCursor,
    /// The records that this session published.
    records: u64,
}

impl<P: StreamPublications> ExecStreamPublisher<P> {
    /// Spawn the publisher thread.
    ///
    /// # Errors
    ///
    /// Returns the OS error when the thread cannot be spawned.
    pub(crate) fn spawn(
        inputs: PublisherInputs<P>,
    ) -> std::io::Result<JoinHandle<anyhow::Result<()>>> {
        std::thread::Builder::new()
            .name("exec-stream-publisher".into())
            .spawn(move || Self::start(inputs)?.run())
    }

    /// Wait for the first recording position, the start of this session's
    /// recording, and build the state.
    ///
    /// # Errors
    ///
    /// Returns an error when the recorder thread ends before it reports a
    /// position.
    fn start(inputs: PublisherInputs<P>) -> anyhow::Result<Self> {
        let start = inputs
            .positions
            .recv()
            .context("the exec_txs recorder stopped before it reported a position")?;
        let session_id = inputs.publications.session_id();
        ExecStreamMetrics::session(session_id);
        info!(
            session_id,
            start, "exec stream: the recorded publication starts"
        );
        Ok(Self {
            inputs,
            cursor: RecordedCursor::new(start),
            records: 0,
        })
    }

    /// The loop: one [`Self::step`] for each item or position, until the
    /// reader is gone.
    ///
    /// # Errors
    ///
    /// Returns an error when a record fails to encode, when the local
    /// recording ends, or when shutdown ends a wait for a refused record.
    fn run(mut self) -> anyhow::Result<()> {
        while let Flow::Continue = self.step()? {}
        info!(
            records = self.records,
            recorded_through = ?self.cursor.through(),
            "exec stream: the reader is gone; the publisher ends"
        );
        Ok(())
    }

    /// Take the next reader item or recording position.
    fn step(&mut self) -> anyhow::Result<Flow> {
        select! {
            recv(self.inputs.items) -> item => match item {
                Ok(item) => self.on_item(item).map(|()| Flow::Continue),
                Err(_) => Ok(Flow::Stop),
            },
            recv(self.inputs.positions) -> position => {
                let position = position.map_err(|_| Self::recording_ended())?;
                ExecStreamMetrics::recorded(self.cursor.recorded(position));
                Ok(Flow::Continue)
            },
        }
    }

    /// The recorder thread ended: the local recording is lost, or the
    /// archive no longer answers. This executor then has no recorded copy
    /// of what it joins, so the publisher fails. The reader stops, and the
    /// process exits. A restart waits for a new recording.
    fn recording_ended() -> anyhow::Error {
        tracing::error!("exec stream: the local exec_txs recording ended; the executor stops");
        anyhow!("the local exec_txs recording ended")
    }

    /// Take the recording positions that arrived while an offer waits.
    ///
    /// # Errors
    ///
    /// Returns an error when the recorder thread ended.
    fn poll_recorder(&mut self) -> anyhow::Result<()> {
        match self.inputs.positions.try_recv() {
            Ok(position) => ExecStreamMetrics::recorded(self.cursor.recorded(position)),
            Err(TryRecvError::Empty) => (),
            Err(TryRecvError::Disconnected) => return Err(Self::recording_ended()),
        }
        Ok(())
    }

    fn on_item(&mut self, item: ExecStreamItem) -> anyhow::Result<()> {
        match item {
            ExecStreamItem::Record(record) => self.publish(&record),
            ExecStreamItem::Passed(mark) => {
                ExecStreamMetrics::recorded(self.cursor.passed(mark));
                Ok(())
            }
        }
    }

    /// Write one record: first to the recorded publication, with no drop,
    /// then to the live one. The live publication never carries a record
    /// that the recorded one did not take.
    fn publish(&mut self, record: &ExecTxRecord) -> anyhow::Result<()> {
        let bytes = kardamom_log::codec::encode(record).context("encode an ExecTxRecord")?;
        let start = self.cursor.offered_end();
        let end = self.offer_until_taken(&bytes)?;
        self.cursor.offered(end);
        self.inputs.publications.offer_live(&bytes);
        self.note_locator(record.index, start)
    }

    /// Offer `bytes` to the recorded publication until it takes them. The
    /// thread blocks meanwhile, so the reader's channel fills and the
    /// reader blocks too: this executor stalls and drops nothing.
    fn offer_until_taken(&mut self, bytes: &AlignedVec) -> anyhow::Result<i64> {
        let mut wait = RefusedOffers::new();
        loop {
            if let Some(end) = self.offer_once(bytes, &mut wait)? {
                return Ok(end);
            }
        }
    }

    /// One offer of a record. A refused offer also checks shutdown and the
    /// recorder, so a wait ends when the recording ends.
    fn offer_once(
        &mut self,
        bytes: &AlignedVec,
        wait: &mut RefusedOffers,
    ) -> anyhow::Result<Option<i64>> {
        let offer = self.inputs.publications.offer_recorded(bytes);
        if let ControlFlow::Break(end) = wait.settle(offer) {
            return Ok(Some(end));
        }
        if self.inputs.stop.is_cancelled() {
            return Err(anyhow!(
                "shutdown while the exec_txs archive refused a record"
            ));
        }
        self.poll_recorder()?;
        Ok(None)
    }

    /// Append a locator for the first record of the session and for every
    /// [`LOCATOR_EVERY`] records. The entry goes after the offer, so it
    /// never names a record that is not on the stream. A failed append
    /// costs a longer replay, not a stall, so it only logs.
    fn note_locator(&mut self, index: u64, start: i64) -> anyhow::Result<()> {
        if self.records.is_multiple_of(LOCATOR_EVERY) {
            let locator = Locator {
                index,
                session_id: self.inputs.publications.session_id(),
                position: start,
            };
            let _ =
                self.inputs.locators.append(locator).inspect_err(
                    |e| warn!(index, error = %e, "exec stream: locator append failed"),
                );
        }
        self.records = self
            .records
            .checked_add(1)
            .context("the exec stream record count overflows u64")?;
        Ok(())
    }
}

/// The refused offers of one record: it counts the time the publisher
/// waits, and logs the start and the end of the wait once.
struct RefusedOffers {
    last: Instant,
    refused: bool,
}

impl RefusedOffers {
    fn new() -> Self {
        Self {
            last: Instant::now(),
            refused: false,
        }
    }

    /// Settle one offer: `Break` with the end position when it was taken,
    /// else `Continue`.
    fn settle(&mut self, offer: Result<i64, LogError>) -> ControlFlow<i64> {
        let now = Instant::now();
        if offer.is_err() || self.refused {
            ExecStreamMetrics::blocked(now.saturating_duration_since(self.last));
        }
        self.last = now;
        match offer {
            Ok(end) => {
                self.log_taken();
                ControlFlow::Break(end)
            }
            Err(e) => {
                self.log_refused(&e);
                ControlFlow::Continue(())
            }
        }
    }

    fn log_refused(&mut self, e: &LogError) {
        if self.refused {
            return;
        }
        warn!(
            error = %e,
            "exec stream: the recorded publication refuses a record; the executor stalls until the archive takes it"
        );
        self.refused = true;
    }

    fn log_taken(&self) {
        if self.refused {
            info!("exec stream: the recorded publication takes records again");
        }
    }
}
