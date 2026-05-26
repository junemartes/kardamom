//! Live Aeron Archive reader. (unconditional dep on rusteron.)
//!
//! Same `!Send + !Sync` thread-confinement discipline as
//! `kardamom_log::recorder` — owns its `AeronArchive` on a single thread,
//! never crosses it.
//!
//! v0 implements the **filesystem read path**: the batcher process has direct
//! access to the recorder host's `archive_dir`. We use `AeronArchive` solely
//! for the descriptor fetch + segment-base-position computation, then read
//! segment files via the offline
//! [`crate::archive_reader::TypedSegmentReader`].
//!
//! The replay-protocol path (no filesystem access) is deferred; the
//! filesystem path covers the spec and matches the layout the recorder writes.
//!
//! After the batcher resolves a B-archive recording (tx_ordering,
//! `T = TxOrderingMessage`) plus M per-sequencer A-archive recordings
//! (tx_data[i], `T = TxEnvelope`). This module produces one
//! `LiveSegmentDescriptor` per recording id — callers compose them into the
//! [`crate::multi_archive_reader::MultiArchiveReader`].

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;

use kardamom_types::{TxEnvelope, TxOrderingMessage};

use rusteron_archive::{
    AeronArchive, AeronArchiveRecordingDescriptor,
    AeronArchiveRecordingDescriptorConsumerFuncCallback, Handler,
};

use crate::archive_reader::{TxDataSegmentReader, TxOrderingSegmentReader, TypedSegmentReader};
use crate::error::BatcherError;

/// Resolved on-disk location of a live recording's active segment file,
/// together with the descriptor fields the offline path also wants.
pub struct LiveSegmentDescriptor {
    pub recording_id: i64,
    pub start_position: i64,
    pub term_buffer_length: i32,
    pub segment_file_length: i32,
    pub segment_path: PathBuf,
}

impl LiveSegmentDescriptor {
    /// Resolve the active segment file for `recording_id`, using the same
    /// `<archive_dir>/<recording_id>-<segmentBasePosition>.rec` layout the
    /// recorder writes.
    pub fn resolve(
        archive: &AeronArchive,
        recording_id: i64,
        archive_dir: PathBuf,
    ) -> Result<Self, BatcherError> {
        let (start_position, term_buffer_length, segment_file_length) =
            fetch_descriptor(archive, recording_id)?;
        let cur = archive
            .get_recording_position(recording_id)
            .map_err(|e| BatcherError::Aeron(format!("get_recording_position: {e}")))?;
        let segment_base = AeronArchive::segment_file_base_position(
            start_position,
            cur,
            term_buffer_length,
            segment_file_length,
        );
        let segment_path = TypedSegmentReader::<TxOrderingMessage>::segment_path(
            &archive_dir,
            recording_id,
            segment_base,
        );
        Ok(Self {
            recording_id,
            start_position,
            term_buffer_length,
            segment_file_length,
            segment_path,
        })
    }

    /// Open a tx_ordering typed reader over this descriptor's segment file.
    pub fn open_tx_ordering(&self) -> Result<TxOrderingSegmentReader, BatcherError> {
        TypedSegmentReader::<TxOrderingMessage>::open(&self.segment_path)
    }

    /// Open a tx_data typed reader over this descriptor's segment file.
    /// The caller is responsible for ensuring the underlying recording is
    /// in fact a tx_data recording for some sequencer.
    pub fn open_tx_data(&self) -> Result<TxDataSegmentReader, BatcherError> {
        TypedSegmentReader::<TxEnvelope>::open(&self.segment_path)
    }
}

fn fetch_descriptor(
    archive: &AeronArchive,
    recording_id: i64,
) -> Result<(i64, i32, i32), BatcherError> {
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
        .map_err(|e| BatcherError::Aeron(format!("list_recording: {e}")))?;

    let g = captured.borrow();
    if !g.seen {
        return Err(BatcherError::Aeron(format!(
            "list_recording({recording_id}) returned no descriptor"
        )));
    }
    Ok((
        g.start_position,
        g.term_buffer_length,
        g.segment_file_length,
    ))
}
