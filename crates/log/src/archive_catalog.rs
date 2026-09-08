//! Page an Aeron Archive's recording catalog for one stream id.
//!
//! `recorder.rs` and `refetch.rs` both scan `list_recordings_for_uri`,
//! matching by stream id only (an empty channel fragment matches any
//! channel), and both page by recording id because recording ids are
//! archive-global: the newest recording for one stream can sit beyond any
//! single page. [`ArchiveCatalog`] owns that paging loop and its
//! `Handler::leak`/`release()` pair once, so callers cannot forget to
//! release the leaked handler (a bug this crate hit before).
//!
//! This is an extension trait, not an inherent method, because
//! `AeronArchive` is a foreign type (`rusteron_archive` owns it): the
//! orphan rule blocks an inherent `impl` on it from this crate.

use std::ffi::{CStr, CString};
use std::ops::ControlFlow;

use rusteron_archive::{
    AeronArchive, AeronArchiveRecordingDescriptor,
    AeronArchiveRecordingDescriptorConsumerFuncCallback, Handler,
};

use crate::error::LogError;

/// Recordings requested per catalog page. A page shorter than this ends
/// the scan.
const PAGE: i32 = 100;

pub(crate) trait ArchiveCatalog {
    /// Call `on_desc` once for every recording of `stream_id`, in
    /// ascending recording-id order, paging through the whole catalog.
    /// Each page leaks one `Handler` and releases it right after the call
    /// returns, on both the ok and error paths, matching the archive's
    /// "call the consumer synchronously, then drop the pointer" contract.
    ///
    /// # Errors
    ///
    /// Returns an error if `list_recordings_for_uri` fails on any page.
    fn for_each_recording_of_stream(
        &self,
        stream_id: i32,
        on_desc: impl FnMut(&AeronArchiveRecordingDescriptor),
    ) -> Result<(), LogError>;
}

/// One page's descriptor accumulator: tracks the highest recording id seen
/// (the next page's start) and forwards each descriptor to the caller.
/// Generic over the caller's closure type, not a trait object: rusteron's
/// `Handler::leak` takes any `T`, with no object-safety requirement, so a
/// dynamic callback would cost a vtable indirection for nothing.
struct Acc<'a, F: FnMut(&AeronArchiveRecordingDescriptor)> {
    on_desc: &'a mut F,
    max_id: Option<i64>,
}

impl<F: FnMut(&AeronArchiveRecordingDescriptor)> AeronArchiveRecordingDescriptorConsumerFuncCallback
    for Acc<'_, F>
{
    fn handle_aeron_archive_recording_descriptor_consumer_func(
        &mut self,
        desc: AeronArchiveRecordingDescriptor,
    ) {
        let id = desc.recording_id();
        self.max_id = Some(self.max_id.map_or(id, |cur| cur.max(id)));
        (self.on_desc)(&desc);
    }
}

/// One [`ArchiveCatalog::for_each_recording_of_stream`] catalog page,
/// starting at `from_record_id`. `Break` ends the scan: the page was
/// short, or empty. `Continue` carries the next page's start id.
fn fetch_page<F: FnMut(&AeronArchiveRecordingDescriptor)>(
    archive: &AeronArchive,
    on_desc: &mut F,
    stream_id: i32,
    any_channel: &CStr,
    from_record_id: i64,
) -> Result<ControlFlow<(), i64>, LogError> {
    let acc = Acc {
        on_desc,
        max_id: None,
    };
    let mut handler = Handler::leak(acc);
    let res = archive.list_recordings_for_uri(
        from_record_id,
        PAGE,
        any_channel,
        stream_id,
        Some(&handler),
    );
    // Release right after the call, before the `?`, so an error page
    // still frees the leaked handler. The C side calls the consumer
    // synchronously inside `list_recordings_for_uri` and never holds the
    // pointer past that call.
    let max_id = handler.max_id;
    handler.release();
    let count = res.map_err(|e| LogError::Aeron(format!("list_recordings_for_uri: {e}")))?;
    if count < PAGE {
        return Ok(ControlFlow::Break(()));
    }
    // A page that reaches `count == PAGE` (`PAGE > 0`) always delivered at
    // least one descriptor, so `max_id` is always `Some` here.
    let Some(id) = max_id else {
        return Ok(ControlFlow::Break(()));
    };
    let next = id
        .checked_add(1)
        .ok_or_else(|| LogError::Aeron(format!("archive recording id {id} overflows i64")))?;
    Ok(ControlFlow::Continue(next))
}

impl ArchiveCatalog for AeronArchive {
    fn for_each_recording_of_stream(
        &self,
        stream_id: i32,
        mut on_desc: impl FnMut(&AeronArchiveRecordingDescriptor),
    ) -> Result<(), LogError> {
        // An empty channel fragment matches any channel; stream_id narrows
        // the match to the caller's stream. This never fails: an empty
        // string has no NUL byte.
        let any_channel = CString::new("").expect("empty fragment has no NUL");
        let mut from_record_id: i64 = 0;
        loop {
            match fetch_page(self, &mut on_desc, stream_id, &any_channel, from_record_id)? {
                ControlFlow::Break(()) => break,
                ControlFlow::Continue(next) => from_record_id = next,
            }
        }
        Ok(())
    }
}
