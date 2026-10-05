//! The webhook outbox: one append-only file per subscription, in the
//! archive segment frame format, so the tooling that reads archives
//! reads outboxes.
//!
//! ```text
//!   length      u32 LE      total frame length including this header
//!   reserved    u32 zero
//!   term_id     i32 LE      zero: an outbox has no Aeron position
//!   term_offset i32 LE      zero
//!   payload     length - 16 bytes (rkyv archive of OutboxRecord)
//!   pad         zero, to next 8-byte boundary
//! ```
//!
//! The writer appends; the delivery loop reads from a cursor it persists
//! next to the file. A file can end mid-frame after a crash: the reader
//! stops at the last whole frame, and the writer truncates the partial
//! tail before it appends again.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use alloy_primitives::{Address, B256};
use rkyv::with::Map;
use rkyv::{Archive, Deserialize, Serialize, rancor};
use thiserror::Error;

use crate::dto::{Stage, TxStatusEvent};
use kardamom_types::wire::{AddressBytes, B256Bytes};

const FRAME_HEADER_LEN: usize = 16;
const FRAME_ALIGN: u64 = 8;

/// One outbox entry: the event as the client sees it, the hub's
/// sequence number, and the arrival time, for the delivery lag.
#[derive(Clone, Debug, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct OutboxRecord {
    pub at_unix_ms: u64,
    pub seq: u64,
    #[rkyv(with = B256Bytes)]
    pub tx_hash: B256,
    #[rkyv(with = Map<AddressBytes>)]
    pub sender: Option<Address>,
    pub nonce: Option<u64>,
    pub stage: u8,
    pub status: Option<u8>,
    pub reason: Option<String>,
    pub expected_nonce: Option<u64>,
}

impl OutboxRecord {
    #[must_use]
    pub fn new(seq: u64, at_unix_ms: u64, event: &TxStatusEvent) -> Self {
        Self {
            at_unix_ms,
            seq,
            tx_hash: event.tx_hash,
            sender: event.sender,
            nonce: event.nonce,
            stage: stage_code(event.stage),
            status: event.status,
            reason: event.reason.clone(),
            expected_nonce: event.expected_nonce,
        }
    }

    /// The event this record holds. `None` for a stage code no version
    /// of this crate wrote.
    #[must_use]
    pub fn event(&self) -> Option<TxStatusEvent> {
        Some(TxStatusEvent {
            tx_hash: self.tx_hash,
            sender: self.sender,
            nonce: self.nonce,
            stage: stage_from_code(self.stage)?,
            status: self.status,
            reason: self.reason.clone(),
            expected_nonce: self.expected_nonce,
        })
    }
}

fn stage_code(stage: Stage) -> u8 {
    match stage {
        Stage::Offered => 1,
        Stage::Sealed => 2,
        Stage::Executed => 3,
        Stage::Rejected => 4,
    }
}

fn stage_from_code(code: u8) -> Option<Stage> {
    match code {
        1 => Some(Stage::Offered),
        2 => Some(Stage::Sealed),
        3 => Some(Stage::Executed),
        4 => Some(Stage::Rejected),
        _ => None,
    }
}

#[derive(Debug, Error)]
pub enum OutboxError {
    #[error("outbox io: {0}")]
    Io(#[from] std::io::Error),
    #[error("outbox frame: {0}")]
    Corruption(String),
    #[error("outbox record: {0}")]
    Codec(String),
}

/// The appender of one outbox file.
pub struct OutboxWriter {
    file: File,
    len: u64,
}

impl OutboxWriter {
    /// Open `path` for appending. A partial frame at the tail is cut.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or trimmed.
    pub fn open(path: &Path) -> Result<Self, OutboxError> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        let len = whole_frames_len(&file)?;
        file.set_len(len)?;
        Ok(Self { file, len })
    }

    /// Append one record. Returns the file length after it.
    ///
    /// # Errors
    ///
    /// Returns an error if the record fails to encode or the write
    /// fails.
    pub fn append(&mut self, record: &OutboxRecord) -> Result<u64, OutboxError> {
        let payload = rkyv::to_bytes::<rancor::Error>(record)
            .map_err(|e| OutboxError::Codec(e.to_string()))?;
        let total = u32::try_from(FRAME_HEADER_LEN + payload.len())
            .map_err(|_| OutboxError::Codec("frame longer than u32::MAX".to_string()))?;
        let frame_len = usize::try_from(aligned(u64::from(total)))
            .map_err(|_| OutboxError::Codec("frame longer than the address space".to_string()))?;
        let mut frame = Vec::with_capacity(frame_len);
        frame.extend_from_slice(&total.to_le_bytes());
        frame.extend_from_slice(&[0u8; 12]);
        frame.extend_from_slice(payload.as_slice());
        frame.resize(frame_len, 0);
        self.file.write_all(&frame)?;
        self.len = self.len.saturating_add(frame.len() as u64);
        Ok(self.len)
    }

    #[must_use]
    pub fn len(&self) -> u64 {
        self.len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Cut the file to nothing, if nothing was appended since it was
    /// `len` long. Returns whether it did.
    ///
    /// # Errors
    ///
    /// Returns an error if the truncate fails.
    pub fn truncate_if_len(&mut self, len: u64) -> Result<bool, OutboxError> {
        if self.len != len {
            return Ok(false);
        }
        self.file.set_len(0)?;
        self.len = 0;
        Ok(true)
    }
}

/// The length of the whole frames at the front of `file`.
fn whole_frames_len(file: &File) -> Result<u64, OutboxError> {
    let end = file.metadata()?.len();
    let mut pos = 0;
    while let Some(next) = frame_end(file, pos, end)? {
        pos = next;
    }
    Ok(pos)
}

/// The position after the frame at `pos`, or `None` when no whole frame
/// starts there: the file ends, or a zero length marks a cut tail.
fn frame_end(file: &File, pos: u64, end: u64) -> Result<Option<u64>, OutboxError> {
    let Some(len) = read_frame_len(file, pos, end)? else {
        return Ok(None);
    };
    let whole = pos.checked_add(u64::from(len)).is_some_and(|e| e <= end);
    Ok(whole.then(|| pos.saturating_add(aligned(u64::from(len)))))
}

/// The frame length at `pos`, or `None` when the header is absent or
/// zero.
fn read_frame_len(file: &File, pos: u64, end: u64) -> Result<Option<u32>, OutboxError> {
    if pos
        .checked_add(FRAME_HEADER_LEN as u64)
        .is_none_or(|h| h > end)
    {
        return Ok(None);
    }
    let mut header = [0u8; FRAME_HEADER_LEN];
    file.read_exact_at(&mut header, pos)?;
    let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    if len == 0 {
        return Ok(None);
    }
    if (len as usize) < FRAME_HEADER_LEN {
        return Err(OutboxError::Corruption(format!(
            "frame length {len} below header size at offset {pos}"
        )));
    }
    Ok(Some(len))
}

fn aligned(len: u64) -> u64 {
    len.div_ceil(FRAME_ALIGN) * FRAME_ALIGN
}

/// The reader of one outbox file, from a position.
pub struct OutboxReader {
    file: File,
    pos: u64,
}

impl OutboxReader {
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened.
    pub fn open(path: &Path, pos: u64) -> Result<Self, OutboxError> {
        Ok(Self {
            file: File::open(path)?,
            pos,
        })
    }

    /// The next whole record, advancing past it. `None` at the tail.
    ///
    /// # Errors
    ///
    /// Returns an error on a damaged frame or an undecodable record.
    pub fn read_next(&mut self) -> Result<Option<OutboxRecord>, OutboxError> {
        let end = self.file_len()?;
        let Some(len) = read_frame_len(&self.file, self.pos, end)? else {
            return Ok(None);
        };
        if self.pos.checked_add(u64::from(len)).is_none_or(|e| e > end) {
            return Ok(None);
        }
        let mut payload = vec![0u8; len as usize - FRAME_HEADER_LEN];
        self.file.read_exact_at(
            &mut payload,
            self.pos.saturating_add(FRAME_HEADER_LEN as u64),
        )?;
        let record = rkyv::from_bytes::<OutboxRecord, rancor::Error>(&payload)
            .map_err(|e| OutboxError::Codec(e.to_string()))?;
        // Saturating: the frame was read whole below `end`, so the sum
        // fits; the file never reaches u64::MAX bytes.
        self.pos = self.pos.saturating_add(aligned(u64::from(len)));
        Ok(Some(record))
    }

    #[must_use]
    pub fn pos(&self) -> u64 {
        self.pos
    }

    /// Move to `pos`, after the writer cut the file.
    pub fn seek(&mut self, pos: u64) {
        self.pos = pos;
    }

    /// The file's current length.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata read fails.
    pub fn file_len(&self) -> Result<u64, OutboxError> {
        Ok(self.file.metadata()?.len())
    }
}

/// The persisted read position of one outbox, next to it.
pub struct Cursor {
    path: PathBuf,
}

impl Cursor {
    #[must_use]
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// The stored position; zero when none was stored yet.
    #[must_use]
    pub fn load(&self) -> u64 {
        std::fs::read_to_string(&self.path)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0)
    }

    /// Store `pos` atomically: a sibling temp file, then a rename.
    ///
    /// # Errors
    ///
    /// Returns an error if the write or the rename fails.
    pub fn store(&self, pos: u64) -> Result<(), OutboxError> {
        let tmp = self.path.with_extension("cursor.tmp");
        std::fs::write(&tmp, pos.to_string())?;
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "outbox_tests.rs"]
mod tests;
