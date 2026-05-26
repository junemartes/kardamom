//! Offline reader for the tx_ordering recording, used by the L1 batcher.
//!
//! The batcher is temporally decoupled from the live pipeline. It does not
//! subscribe to the tx_ordering live IPC stream; it reads the recorded
//! archive — either via direct segment-file reads (filesystem access
//! required) or via the Aeron Archive replay protocol (works over the
//! network, no filesystem access required).
//!
//! This module exposes both paths behind a single trait so consumers depend
//! on a `Send`-friendly abstraction rather than the underlying rusteron
//! handles (which are thread-confined; see [`crate::aeron_live`] for the
//! threading discipline).
//!
//! Gated behind `feature = "aeron-live"`.

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;

use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
use tracing::error;

use crate::aeron_live::{AeronRuntime, DeliverFn};
use crate::codec;
use crate::error::LogError;
use types::{BPosition, TxOrderingMessage};

type Archive = rusteron_archive::AeronArchive;

/// One message yielded by the archive reader. Mirrors what the live
/// tx_ordering subscription would deliver, plus a `BPosition` cursor.
#[derive(Clone, Debug)]
pub enum ArchiveMessage {
    Msg(TxOrderingMessage),
}

/// Open a replay subscription against the Aeron Archive and return a Send
/// channel of decoded messages. The Aeron thread inside `rt` owns the
/// underlying subscription; the returned receiver is plain `tokio::mpsc`.
///
/// `replay_channel` and `replay_stream_id` are the URI / stream id the
/// archive will write replayed bytes to — they must match the
/// `start_replay` parameters the caller passes to the archive. By
/// convention we use `aeron:ipc?alias=replay-<recording_id>` with a unique
/// stream id.
///
/// The replay subscription stays open until the returned channel is dropped.
pub fn open_replay_subscription(
    rt: &AeronRuntime,
    replay_channel: &str,
    replay_stream_id: i32,
) -> Result<UnboundedReceiver<(BPosition, ArchiveMessage)>, LogError> {
    let (tx, rx) = unbounded_channel::<(BPosition, ArchiveMessage)>();
    let deliver: DeliverFn =
        Box::new(move |bytes: &[u8], pos: BPosition| {
            match codec::materialize::<TxOrderingMessage>(bytes) {
                Ok(msg) => {
                    let _ = tx.send((pos, ArchiveMessage::Msg(msg)));
                }
                Err(e) => {
                    error!(
                        error = %e,
                        len = bytes.len(),
                        "archive replay fragment failed to decode as TxOrderingMessage"
                    );
                }
            }
        });
    rt.open_subscription_with_deliver(replay_channel, replay_stream_id, deliver)?;
    Ok(rx)
}

/// Helper bundle describing a replay range to drive.
#[derive(Clone, Debug)]
pub struct ReplayRange {
    pub recording_id: i64,
    pub from_position: i64,
    /// `i64::MAX` means "until end of recording".
    pub length: i64,
    pub replay_channel: String,
    pub replay_stream_id: i32,
}

impl ReplayRange {
    /// Drive a replay by talking to the supplied `AeronArchive` directly.
    /// **Must** be called from the same thread that owns `archive` (rusteron
    /// archive handles are `!Send + !Sync`).
    ///
    /// Returns the replay session id assigned by the archive.
    pub fn start(&self, archive: &Archive) -> Result<i64, LogError> {
        use std::ffi::CString;
        let c = CString::new(self.replay_channel.as_str())
            .map_err(|e| LogError::Aeron(format!("replay uri NUL: {e}")))?;
        // bounding_limit_counter_id = -1 → no bound.
        // file_io_max_length = 0 → archive default I/O chunk size.
        // replay_token = 0 → no token (used for cluster auth).
        // subscription_registration_id = -1 → no binding.
        let params = rusteron_archive::AeronArchiveReplayParams::new(
            -1,
            0,
            self.from_position,
            self.length,
            0,
            -1,
        )
        .map_err(|e| LogError::Aeron(format!("ReplayParams::new: {e}")))?;
        archive
            .start_replay(
                self.recording_id,
                c.as_c_str(),
                self.replay_stream_id,
                &params,
            )
            .map_err(|e| LogError::Aeron(format!("start_replay: {e}")))
    }
}

/// Direct-from-disk reader stub. The batcher's `batcher` crate ships its own
/// KAR1-internal frame format for tests; this reader operates over the
/// Aeron-native segment frame layout. Implementation is deferred behind the
/// filesystem flag until the cross-pipeline e2e proves it is needed; for the
/// proof-of-pipeline test we use the replay-protocol path (above) which the
/// archive supports without filesystem access.
pub struct SegmentFileReader {
    pub archive_dir: PathBuf,
    pub recording_id: i64,
}

impl SegmentFileReader {
    pub fn new(archive_dir: PathBuf, recording_id: i64) -> Self {
        Self {
            archive_dir,
            recording_id,
        }
    }
}

/// Returns `(start_position, term_buffer_length, segment_file_length)` for a
/// recording. **Must** be called on the thread that owns `archive`.
pub fn fetch_recording_descriptor(
    archive: &Archive,
    recording_id: i64,
) -> Result<(i64, i32, i32), LogError> {
    use rusteron_archive::{
        AeronArchiveRecordingDescriptor, AeronArchiveRecordingDescriptorConsumerFuncCallback,
        Handler,
    };

    #[derive(Default)]
    struct Captured {
        start_position: i64,
        term_buffer_length: i32,
        segment_file_length: i32,
        seen: bool,
    }

    struct Consumer {
        captured: Rc<RefCell<Captured>>,
    }

    impl AeronArchiveRecordingDescriptorConsumerFuncCallback for Consumer {
        fn handle_aeron_archive_recording_descriptor_consumer_func(
            &mut self,
            desc: AeronArchiveRecordingDescriptor,
        ) {
            let mut g = self.captured.borrow_mut();
            g.start_position = desc.start_position();
            g.term_buffer_length = desc.term_buffer_length();
            g.segment_file_length = desc.segment_file_length();
            g.seen = true;
        }
    }

    let captured: Rc<RefCell<Captured>> = Rc::new(RefCell::new(Captured::default()));
    let handler = Handler::leak(Consumer {
        captured: captured.clone(),
    });
    archive
        .list_recording(recording_id, Some(&handler))
        .map_err(|e| LogError::Aeron(format!("list_recording: {e}")))?;
    // Give the C client a tick to invoke the callback if it queues it.
    std::thread::sleep(Duration::from_millis(10));
    let g = captured.borrow();
    if !g.seen {
        return Err(LogError::Aeron(format!(
            "list_recording({recording_id}) returned no descriptor"
        )));
    }
    Ok((
        g.start_position,
        g.term_buffer_length,
        g.segment_file_length,
    ))
}
