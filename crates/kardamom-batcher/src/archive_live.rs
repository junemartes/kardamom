//! Live Aeron Archive reader. Gated behind `feature = "aeron-live"`.
//!
//! Same `!Send + !Sync` thread-confinement discipline as
//! `kardamom_log::recorder` — owns its `AeronArchive` on a single thread,
//! never crosses it.
//!
//! v0 implements the **filesystem read path**: the batcher process has direct
//! access to the recorder host's `archive_dir`. We use `AeronArchive` solely
//! for the descriptor fetch + segment-base-position computation, then read
//! segment files via the offline [`crate::archive_reader::SegmentReader`].
//!
//! The replay-protocol path (no filesystem access) is deferred; the
//! filesystem path covers the spec and matches the layout the recorder writes.

use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;

use rusteron_archive::{
    AeronArchive, AeronArchiveRecordingDescriptor,
    AeronArchiveRecordingDescriptorConsumerFuncCallback, Handler,
};

use crate::archive_reader::SegmentReader;
use crate::error::BatcherError;

pub struct LiveArchiveReader {
    pub segment: SegmentReader,
    pub recording_id: i64,
    pub start_position: i64,
    pub term_buffer_length: i32,
    pub segment_file_length: i32,
}

impl LiveArchiveReader {
    /// Look up the descriptor, compute the active segment's base position, and
    /// open the corresponding `.rec` file via [`SegmentReader`].
    pub fn open(
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
        let segment_path = SegmentReader::segment_path(&archive_dir, recording_id, segment_base);
        let segment = SegmentReader::open(&segment_path)?;
        Ok(Self {
            segment,
            recording_id,
            start_position,
            term_buffer_length,
            segment_file_length,
        })
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
