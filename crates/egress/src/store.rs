//! [`Store`]: the merged canonical stream on disk.
//!
//! The store keeps the frames in segment files in one directory. A segment
//! starts with the head of the stream in front of its first frame:
//! `[next_index:u64][next_block:u64][last_kind:u8][last_key:u64]`, where
//! the kind of the last frame is 0 (none), 1 (record) or 2 (boundary). Then
//! each frame follows as
//! `[len:u32][frame]`, little endian. A new segment starts after the
//! boundary that closes `segment_blocks` blocks, so a segment holds whole
//! blocks. The name of a segment is the next block at its start, in 20
//! digits, so the names sort in stream order.
//!
//! At open the store reads every segment once: it rebuilds the head, and it
//! cuts a torn frame at the end of the last segment. A reader that resumes
//! from a cursor reads the frames after it from the segment that holds the
//! cursor.
//!
//! The side file `ref-mark` holds a mark at or above the highest stored
//! transaction reference (see [`crate::ref_mark`]). The head comes from the last segment only, so
//! the mark needs its own file: a reference in an older segment counts too.

use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::num::NonZeroU64;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};

use crate::merge::{Head, Key};
use crate::ref_mark::RefMark;

/// The bytes of the head at the start of each segment.
const SEGMENT_HEAD: usize = 25;
/// The suffix of a segment file.
const SUFFIX: &str = "frames";

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("egress store {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
}

/// What a reader that resumes from a cursor gets.
#[derive(Debug, PartialEq, Eq)]
pub enum Range {
    /// The frames after the cursor, up to the limit, the cursor after the
    /// last of them, and the head of the store when the read ended. A
    /// reader that resumes asks again from `after` until it reaches `head`.
    Frames {
        frames: Vec<Vec<u8>>,
        after: Head,
        head: Head,
    },
    /// The store holds nothing that old: its oldest segment starts at
    /// `floor`. `mark` is the reference mark of the store.
    Unavailable { floor: Head, mark: RefMark },
}

/// One segment: the head at its start and its file.
#[derive(Clone, Debug)]
struct Segment {
    start: Head,
    path: PathBuf,
}

/// The merged stream on disk.
pub struct Store {
    dir: PathBuf,
    segment_blocks: NonZeroU64,
    segments: Vec<Segment>,
    writer: BufWriter<File>,
    /// The boundaries in the open segment.
    blocks_in_segment: u64,
    head: Head,
    /// The highest stored transaction reference.
    mark: RefMark,
    /// The mark that the side file holds: at or above `mark`.
    written: RefMark,
}

impl Store {
    /// Open the store in `dir`, rebuild its head from the segments, and
    /// read its reference mark. An empty directory starts at `genesis`.
    ///
    /// # Errors
    ///
    /// Returns an error when a file does not read or write.
    pub fn open(dir: &Path, segment_blocks: NonZeroU64, genesis: Head) -> Result<Self, StoreError> {
        fs::create_dir_all(dir).map_err(io(dir))?;
        let mut segments = Self::list(dir)?;
        let last = match segments.pop() {
            Some(segment) => segment,
            None => Self::create(dir, genesis)?,
        };
        segments.push(last.clone());
        let (head, blocks, end) = Self::scan(&last)?;
        let file = OpenOptions::new()
            .write(true)
            .open(&last.path)
            .map_err(io(&last.path))?;
        file.set_len(end).map_err(io(&last.path))?;
        let mut writer = BufWriter::new(file);
        writer.seek(SeekFrom::Start(end)).map_err(io(&last.path))?;
        let written = RefMark::read(dir)?;
        let mark = Self::stored_mark(&segments, written.behind())?;
        mark.show();
        Ok(Self {
            dir: dir.to_path_buf(),
            segment_blocks,
            segments,
            writer,
            blocks_in_segment: blocks,
            head,
            mark,
            written,
        })
    }

    /// The highest reference in the segments that can hold one above
    /// `behind`, the file value minus the lead (see [`Self::mark_segments`]).
    /// The side file can hold the mark plus the lead, so the open takes the
    /// exact mark from the frames. It reads one frame at a time, as
    /// [`Self::scan`] does.
    ///
    /// A reference that a prune deleted does not count. A reader below the
    /// floor gets [`Range::Unavailable`], so it cannot meet that reference.
    /// This assumes that no reader is `keep_blocks` behind the head with a
    /// cursor that still needs the pruned frames.
    fn stored_mark(segments: &[Segment], behind: RefMark) -> Result<RefMark, StoreError> {
        Self::mark_segments(segments, behind)
            .iter()
            .try_fold(behind, |mark, segment| {
                let mut reader = Self::reader(segment)?;
                let frames = std::iter::from_fn(|| read_frame(&mut reader));
                Ok(RefMark::top(frames, segment.start.next_block)
                    .map_or(mark, |top| mark.joined(top)))
            })
    }

    /// The segments that can hold a reference above `behind`: from the
    /// first segment whose successor starts above `behind` in the index or
    /// the block. Every frame of an earlier segment is below the start of
    /// its successor in both fields, so no reference there is above
    /// `behind`.
    fn mark_segments(segments: &[Segment], behind: RefMark) -> &[Segment] {
        let skip = segments
            .windows(2)
            .take_while(|pair| {
                pair[1].start.next_index <= behind.index && pair[1].start.next_block <= behind.block
            })
            .count();
        &segments[skip..]
    }

    /// The highest stored transaction reference.
    #[must_use]
    pub fn mark(&self) -> RefMark {
        self.mark
    }

    /// The head of the stream on disk.
    #[must_use]
    pub fn head(&self) -> Head {
        self.head
    }

    /// Append the frames that the merge delivered, in order. The reference
    /// mark of the batch goes to disk first, so no frame on disk is above
    /// the mark.
    ///
    /// # Errors
    ///
    /// Returns an error when a write fails.
    pub fn append(&mut self, frames: &[Vec<u8>]) -> Result<(), StoreError> {
        if let Some(top) = RefMark::top(frames.iter(), self.head.next_block) {
            self.raise_mark(top)?;
        }
        frames.iter().try_for_each(|frame| self.append_one(frame))?;
        self.writer.flush().map_err(io(&self.dir))
    }

    /// Close the store at a clean stop: force the frames to disk, then
    /// write the exact mark over the lead. Every stored reference is then
    /// at or below the mark, so only a crash leaves the lead in the file.
    ///
    /// # Errors
    ///
    /// Returns an error when the sync or the write fails.
    pub fn close(&mut self) -> Result<(), StoreError> {
        self.sync()?;
        if self.written != self.mark {
            self.mark.write(&self.dir)?;
            self.written = self.mark;
        }
        Ok(())
    }

    /// Force the appended frames to disk.
    ///
    /// # Errors
    ///
    /// Returns an error when the sync fails.
    pub fn sync(&mut self) -> Result<(), StoreError> {
        self.writer.flush().map_err(io(&self.dir))?;
        self.writer.get_ref().sync_data().map_err(io(&self.dir))
    }

    /// The frames after the cursor `(from_index, from_block)`, up to about
    /// `limit` bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when a segment does not read.
    pub fn read_from(
        &self,
        from_index: u64,
        from_block: u64,
        limit: usize,
    ) -> Result<Range, StoreError> {
        let Some(at) = self
            .segments
            .iter()
            .rposition(|s| s.start.next_block <= from_block && s.start.next_index <= from_index)
        else {
            return Ok(Range::Unavailable {
                floor: self.segments.first().map_or(self.head, |s| s.start),
                mark: self.mark,
            });
        };
        let cursor = Head {
            next_index: from_index,
            next_block: from_block,
            last: None,
        };
        let mut frames = Vec::new();
        let mut bytes = 0usize;
        self.segments[at..].iter().try_for_each(|segment| {
            Self::read_after(segment, cursor, &mut frames, &mut bytes, limit)
        })?;
        let after = frames.iter().fold(cursor, |mut after, frame| {
            after.advance(frame);
            after
        });
        Ok(Range::Frames {
            frames,
            after,
            head: self.head,
        })
    }

    /// Start the store again at `head`: delete every segment, and open a
    /// new one that starts at `head`. The egress does this when it starts
    /// the stream again after a boundary, with no peer to fill the frames
    /// between its old head and that boundary. A reader of an older
    /// cursor then gets [`Range::Unavailable`] with the new floor, and
    /// reads another instance.
    ///
    /// The reference mark becomes the higher of the mark of the store and
    /// `peer`, the mark of the peer that gave the start. It never goes
    /// down.
    ///
    /// # Errors
    ///
    /// Returns an error when a file does not delete or write.
    pub fn reset(&mut self, head: Head, peer: RefMark) -> Result<(), StoreError> {
        self.raise_mark(peer)?;
        self.writer.flush().map_err(io(&self.dir))?;
        self.segments
            .drain(..)
            .try_for_each(|s| fs::remove_file(&s.path).map_err(io(&s.path)))?;
        self.head = head;
        self.start_segment()
    }

    /// Delete the segments that end below `block`: every frame in them is of
    /// an older block.
    ///
    /// # Errors
    ///
    /// Returns an error when a file does not delete.
    pub fn prune_below(&mut self, block: u64) -> Result<(), StoreError> {
        let keep_from = self
            .segments
            .windows(2)
            .take_while(|pair| pair[1].start.next_block <= block)
            .count();
        self.segments
            .drain(..keep_from)
            .try_for_each(|s| fs::remove_file(&s.path).map_err(io(&s.path)))
    }

    /// Raise the mark to `mark` when it is above the mark. Write the side
    /// file first when `mark` passes it.
    fn raise_mark(&mut self, mark: RefMark) -> Result<(), StoreError> {
        if mark <= self.mark {
            return Ok(());
        }
        if !mark.within(self.written) {
            let ahead = mark.ahead();
            ahead.write(&self.dir)?;
            self.written = ahead;
        }
        self.mark = mark;
        mark.show();
        Ok(())
    }

    fn append_one(&mut self, frame: &[u8]) -> Result<(), StoreError> {
        let len = u32::try_from(frame.len()).map_err(|_| StoreError::Io {
            path: self.dir.clone(),
            source: std::io::Error::other("a frame above 4 GiB"),
        })?;
        self.writer
            .write_all(&len.to_le_bytes())
            .map_err(io(&self.dir))?;
        self.writer.write_all(frame).map_err(io(&self.dir))?;
        self.head.advance(frame);
        if matches!(Key::of(frame), Some(Key::Boundary(_))) {
            self.close_block()?;
        }
        Ok(())
    }

    /// Count one closed block, and start a new segment after the last block
    /// of this one.
    fn close_block(&mut self) -> Result<(), StoreError> {
        self.blocks_in_segment += 1;
        if self.blocks_in_segment < self.segment_blocks.get() {
            return Ok(());
        }
        self.writer.flush().map_err(io(&self.dir))?;
        self.start_segment()
    }

    /// Open a new segment that starts at the head, and write to it.
    fn start_segment(&mut self) -> Result<(), StoreError> {
        let segment = Self::create(&self.dir, self.head)?;
        let file = OpenOptions::new()
            .append(true)
            .open(&segment.path)
            .map_err(io(&segment.path))?;
        self.writer = BufWriter::new(file);
        self.segments.push(segment);
        self.blocks_in_segment = 0;
        Ok(())
    }

    /// The segments of `dir`, in stream order.
    fn list(dir: &Path) -> Result<Vec<Segment>, StoreError> {
        let mut paths: Vec<PathBuf> = fs::read_dir(dir)
            .map_err(io(dir))?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|p| p.extension().is_some_and(|e| e == SUFFIX))
            .collect();
        paths.sort();
        paths
            .into_iter()
            .map(|path| {
                Ok(Segment {
                    start: Self::start_of(&path)?,
                    path,
                })
            })
            .collect()
    }

    fn create(dir: &Path, start: Head) -> Result<Segment, StoreError> {
        let path = dir.join(name(start.next_block));
        let mut file = File::create(&path).map_err(io(&path))?;
        file.write_all(&encode_head(start))
            .and_then(|()| file.sync_all())
            .map_err(io(&path))?;
        Ok(Segment { start, path })
    }

    fn start_of(path: &Path) -> Result<Head, StoreError> {
        let mut head = [0u8; SEGMENT_HEAD];
        File::open(path)
            .and_then(|mut f| f.read_exact(&mut head))
            .map_err(io(path))?;
        Ok(decode_head(&head))
    }

    /// The head after the last whole frame of `segment`, the boundaries in
    /// it, and the byte offset after that frame.
    fn scan(segment: &Segment) -> Result<(Head, u64, u64), StoreError> {
        let mut reader = Self::reader(segment)?;
        let mut head = segment.start;
        let mut blocks = 0;
        let mut end = SEGMENT_HEAD as u64;
        std::iter::from_fn(|| read_frame(&mut reader)).for_each(|frame| {
            end += 4 + frame.len() as u64;
            head.advance(&frame);
            blocks += u64::from(matches!(Key::of(&frame), Some(Key::Boundary(_))));
        });
        Ok((head, blocks, end))
    }

    /// Push the frames of `segment` after `cursor` onto `frames`, while
    /// `bytes` stays under `limit`.
    fn read_after(
        segment: &Segment,
        cursor: Head,
        frames: &mut Vec<Vec<u8>>,
        bytes: &mut usize,
        limit: usize,
    ) -> Result<(), StoreError> {
        let mut reader = Self::reader(segment)?;
        let _ = std::iter::from_fn(|| read_frame(&mut reader))
            .filter(|frame| Key::of(frame).is_some_and(|key| !cursor.passed(key)))
            .try_for_each(|frame| {
                if *bytes >= limit {
                    return ControlFlow::Break(());
                }
                *bytes += frame.len();
                frames.push(frame);
                ControlFlow::Continue(())
            });
        Ok(())
    }

    fn reader(segment: &Segment) -> Result<BufReader<File>, StoreError> {
        let mut file = File::open(&segment.path).map_err(io(&segment.path))?;
        file.seek(SeekFrom::Start(SEGMENT_HEAD as u64))
            .map_err(io(&segment.path))?;
        Ok(BufReader::new(file))
    }
}

/// One whole frame, or `None` at the end or at a torn frame.
fn read_frame(reader: &mut impl Read) -> Option<Vec<u8>> {
    let mut len = [0u8; 4];
    reader.read_exact(&mut len).ok()?;
    let mut frame = vec![0u8; usize::try_from(u32::from_le_bytes(len)).ok()?];
    reader.read_exact(&mut frame).ok()?;
    Some(frame)
}

fn encode_head(head: Head) -> [u8; SEGMENT_HEAD] {
    let (kind, key) = match head.last {
        None => (0u8, 0u64),
        Some(Key::Record(index)) => (1, index),
        Some(Key::Boundary(block)) => (2, block),
    };
    let mut bytes = [0u8; SEGMENT_HEAD];
    bytes[..8].copy_from_slice(&head.next_index.to_le_bytes());
    bytes[8..16].copy_from_slice(&head.next_block.to_le_bytes());
    bytes[16] = kind;
    bytes[17..].copy_from_slice(&key.to_le_bytes());
    bytes
}

fn decode_head(bytes: &[u8; SEGMENT_HEAD]) -> Head {
    let u64_at = |at: usize| {
        bytes[at..]
            .first_chunk::<8>()
            .map_or(0, |b| u64::from_le_bytes(*b))
    };
    let last = match bytes[16] {
        1 => Some(Key::Record(u64_at(17))),
        2 => Some(Key::Boundary(u64_at(17))),
        _ => None,
    };
    Head {
        next_index: u64_at(0),
        next_block: u64_at(8),
        last,
    }
}

fn name(next_block: u64) -> String {
    format!("{next_block:020}.{SUFFIX}")
}

pub(crate) fn io(path: &Path) -> impl Fn(std::io::Error) -> StoreError + '_ {
    move |source| StoreError::Io {
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
#[path = "store_tests.rs"]
mod tests;
