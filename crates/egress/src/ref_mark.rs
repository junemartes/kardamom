//! [`RefMark`]: the highest transaction reference that the store holds.
//!
//! The store keeps the mark in the side file `ref-mark` of its directory:
//! `[index:u64][block:u64]`, little endian, 16 bytes. The mark only goes
//! up. A write goes to a temporary file, syncs it, renames it over
//! `ref-mark`, and syncs the directory. So a crash leaves the old mark or
//! the new mark, never a part of one.
//!
//! The store writes the mark of a batch before the frames of the batch.
//! So after a crash the mark is at or above every reference that the
//! segments hold. A mark above the segments only delays the move of a
//! reader to the egress. It never lets a reader meet a reference.

use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use kardamom_cluster_adapter::wire::RT_TXREF;

use crate::merge::{Key, RECORD_TYPE_AT};
use crate::store::{StoreError, io};

/// The name of the side file.
const FILE: &str = "ref-mark";
/// The name of the temporary file of a write.
const TEMP: &str = "ref-mark.tmp";

/// The canonical index and the block of the highest stored reference.
/// `(0, 0)` means that the store holds no reference: block 0 holds no
/// record.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct RefMark {
    pub index: u64,
    pub block: u64,
}

impl RefMark {
    /// The mark in `dir`. A missing file is `(0, 0)`.
    ///
    /// # Errors
    ///
    /// Returns an error when the file does not read, or does not hold 16
    /// bytes.
    pub fn read(dir: &Path) -> Result<Self, StoreError> {
        let path = dir.join(FILE);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(e) => return Err(io(&path)(e)),
        };
        let (index, block) = bytes
            .as_slice()
            .split_first_chunk::<8>()
            .and_then(|(index, rest)| Some((*index, *rest.first_chunk::<8>()?)))
            .filter(|_| bytes.len() == 16)
            .ok_or_else(|| {
                io(&path)(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "the reference mark is not 16 bytes",
                ))
            })?;
        Ok(Self {
            index: u64::from_le_bytes(index),
            block: u64::from_le_bytes(block),
        })
    }

    /// Write the mark to `dir`: a temporary file, its sync, the rename over
    /// the mark, and the sync of the directory.
    pub(crate) fn write(self, dir: &Path) -> Result<(), StoreError> {
        let temp = dir.join(TEMP);
        let mut file = File::create(&temp).map_err(io(&temp))?;
        file.write_all(&self.index.to_le_bytes())
            .and_then(|()| file.write_all(&self.block.to_le_bytes()))
            .and_then(|()| file.sync_all())
            .map_err(io(&temp))?;
        let path = dir.join(FILE);
        fs::rename(&temp, &path).map_err(io(&path))?;
        File::open(dir).and_then(|d| d.sync_all()).map_err(io(dir))
    }

    /// The highest reference of `frames`, in stream order from the block
    /// `next_block`. A record belongs to the block of the next boundary.
    /// `None` when no frame is a reference.
    pub(crate) fn top(frames: &[Vec<u8>], next_block: u64) -> Option<Self> {
        frames
            .iter()
            .scan(next_block, |block, frame| {
                let mark = match Key::of(frame) {
                    Some(Key::Boundary(closed)) => {
                        // No chain reaches block `u64::MAX`, so the cap
                        // never applies.
                        *block = closed.saturating_add(1);
                        None
                    }
                    Some(Key::Record(index)) => (frame.get(RECORD_TYPE_AT) == Some(&RT_TXREF))
                        .then_some(Self {
                            index,
                            block: *block,
                        }),
                    None => None,
                };
                Some(mark)
            })
            .flatten()
            .max()
    }

    /// Set the gauges of the mark.
    pub(crate) fn show(self) {
        crate::metrics::set_ref_mark(self);
    }
}

#[cfg(test)]
#[path = "ref_mark_tests.rs"]
mod tests;
