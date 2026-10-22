//! The block log: the write batch of each committed block, in block order.
//!
//! A restore copies a base checkpoint at block `b`, then applies the
//! batches of the blocks after `b` through the writer's `apply`. So a
//! block `n` is a restore point when the log holds each block from `b + 1`
//! to `n`.
//!
//! The log is a set of segment files in one directory. The name of a
//! segment is its first block in 20 digits with the suffix `.blocks`, so
//! the names sort in block order. A segment starts with a head:
//! `[magic:8][version:u32][schema:u32][genesis:32][first_block:u64]`. Then
//! each record follows as `[len:u32][block:u64][crc32:u32][batch]`, little
//! endian. `batch` is the rkyv archive of the [`WriteBatch`], and the CRC
//! covers the block and the batch.
//!
//! The blocks in one segment follow each other with no gap. A block that
//! does not follow the last block starts a new segment, so a gap shows as
//! the start of a segment. A reader stops at the first torn or bad record,
//! and [`BlockLogWriter::open`] cuts the last segment there.

use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use alloy_primitives::B256;
use rkyv::util::AlignedVec;

use crate::error::StateError;
use crate::writer::WriteBatch;

/// The version of the segment head and the record layout.
pub const BLOCK_LOG_VERSION: u32 = 1;
/// The first bytes of each segment.
const MAGIC: [u8; 8] = *b"KARBLOG\0";
/// The bytes of the head at the start of each segment.
const SEGMENT_HEAD: usize = 56;
/// The bytes in front of the batch of each record.
const RECORD_HEAD: usize = 16;
/// The suffix of a segment file.
const SUFFIX: &str = "blocks";
/// The size at which a segment ends, when no setting gives one.
pub const DEFAULT_SEGMENT_BYTES: NonZeroU64 = NonZeroU64::new(64 << 20).unwrap();

/// The chain and the state schema of the batches in a log. A segment of
/// another identity is not part of the log.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogIdentity {
    pub genesis: B256,
    pub schema: u32,
}

/// The head of one segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SegmentHead {
    identity: LogIdentity,
    first_block: u64,
}

impl SegmentHead {
    fn encode(self) -> [u8; SEGMENT_HEAD] {
        let mut bytes = [0u8; SEGMENT_HEAD];
        bytes[..8].copy_from_slice(&MAGIC);
        bytes[8..12].copy_from_slice(&BLOCK_LOG_VERSION.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.identity.schema.to_le_bytes());
        bytes[16..48].copy_from_slice(self.identity.genesis.as_slice());
        bytes[48..].copy_from_slice(&self.first_block.to_le_bytes());
        bytes
    }

    /// The head in `bytes`, or `None` for a file of another format or
    /// version.
    fn decode(bytes: &[u8; SEGMENT_HEAD]) -> Option<Self> {
        let (magic, rest) = bytes.split_first_chunk::<8>()?;
        let (version, rest) = rest.split_first_chunk::<4>()?;
        let (schema, rest) = rest.split_first_chunk::<4>()?;
        let (genesis, rest) = rest.split_first_chunk::<32>()?;
        let first_block = rest.first_chunk::<8>()?;
        (*magic == MAGIC && u32::from_le_bytes(*version) == BLOCK_LOG_VERSION).then(|| Self {
            identity: LogIdentity {
                genesis: B256::from(*genesis),
                schema: u32::from_le_bytes(*schema),
            },
            first_block: u64::from_le_bytes(*first_block),
        })
    }
}

/// One record of the log: a block and the archive of its batch.
#[derive(Debug)]
pub struct Record {
    pub block: u64,
    bytes: AlignedVec,
}

impl Record {
    /// The batch of the block.
    ///
    /// # Errors
    ///
    /// Returns an error when the archive does not decode.
    pub fn batch(&self) -> Result<WriteBatch, StateError> {
        rkyv::from_bytes::<WriteBatch, rkyv::rancor::Error>(&self.bytes).map_err(|e| {
            StateError::RkyvDecode {
                table: "block_log",
                detail: e.to_string(),
            }
        })
    }

    /// The bytes of the record on disk.
    fn len_on_disk(&self) -> u64 {
        // A record holds at most `u32::MAX` bytes of batch, so the sum fits.
        (RECORD_HEAD + self.bytes.len()) as u64
    }

    /// Write the record as it is on disk: `[len][block][crc][batch]`.
    pub(crate) fn write_to(&self, out: &mut impl Write) -> std::io::Result<()> {
        Self::encode(out, self.block, &self.bytes)
    }

    /// Write the record of `block` with the archive `batch`.
    fn encode(out: &mut impl Write, block: u64, batch: &[u8]) -> std::io::Result<()> {
        let len =
            u32::try_from(batch.len()).map_err(|_| std::io::Error::other("a batch above 4 GiB"))?;
        out.write_all(&len.to_le_bytes())?;
        out.write_all(&block.to_le_bytes())?;
        out.write_all(&Self::crc(block, batch).to_le_bytes())?;
        out.write_all(batch)
    }

    fn crc(block: u64, batch: &[u8]) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&block.to_le_bytes());
        hasher.update(batch);
        hasher.finalize()
    }
}

/// The records of a stream, in order, up to the first torn or bad record.
pub(crate) struct Records<R> {
    reader: R,
}

impl<R: Read> Records<R> {
    /// The records in `reader`: a segment after its head, or the body of
    /// a log transfer.
    pub(crate) fn new(reader: R) -> Self {
        Self { reader }
    }

    /// Read the next record. `None` at the end of the file, and at a torn
    /// or bad record.
    fn read(&mut self) -> Option<Record> {
        let mut head = [0u8; RECORD_HEAD];
        self.reader.read_exact(&mut head).ok()?;
        let (len, rest) = head.split_first_chunk::<4>()?;
        let (block, crc) = rest.split_first_chunk::<8>()?;
        let len = usize::try_from(u32::from_le_bytes(*len)).ok()?;
        let block = u64::from_le_bytes(*block);
        let mut bytes = AlignedVec::with_capacity(len);
        bytes.resize(len, 0);
        self.reader.read_exact(&mut bytes).ok()?;
        (Record::crc(block, &bytes).to_le_bytes() == *crc.first_chunk::<4>()?)
            .then_some(Record { block, bytes })
    }
}

impl<R: Read> Iterator for Records<R> {
    type Item = Record;

    fn next(&mut self) -> Option<Record> {
        self.read()
    }
}

/// One segment file and its first block.
#[derive(Clone, Debug)]
struct Segment {
    first_block: u64,
    path: PathBuf,
}

impl Segment {
    /// The segments in `dir`, in block order. A missing directory holds
    /// none.
    fn list(dir: &Path) -> Result<Vec<Self>, StateError> {
        let entries = match fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        let mut segments: Vec<Self> = entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter_map(|path| {
                let first_block = path
                    .extension()
                    .filter(|e| *e == SUFFIX)
                    .and(path.file_stem())?
                    .to_str()?
                    .parse()
                    .ok()?;
                Some(Self { first_block, path })
            })
            .collect();
        segments.sort_by_key(|s| s.first_block);
        Ok(segments)
    }

    fn name(first_block: u64) -> String {
        format!("{first_block:020}.{SUFFIX}")
    }

    /// The records of the segment, when its head is of `identity`. A
    /// segment that does not open, or that has another head, holds none
    /// for this log.
    fn records(&self, identity: LogIdentity) -> Option<Records<BufReader<File>>> {
        let mut reader = File::open(&self.path).map(BufReader::new).ok()?;
        let mut head = [0u8; SEGMENT_HEAD];
        reader.read_exact(&mut head).ok()?;
        SegmentHead::decode(&head)
            .filter(|h| h.identity == identity && h.first_block == self.first_block)
            .map(|_| Records { reader })
    }
}

/// The first block and the bytes of one segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentSize {
    pub first_block: u64,
    pub bytes: u64,
}

impl SegmentSize {
    /// The segments in `dir` with their sizes, in block order. A segment
    /// that is gone is not in the list.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory does not read.
    pub fn list(dir: &Path) -> Result<Vec<Self>, StateError> {
        Ok(Segment::list(dir)?
            .iter()
            .filter_map(|s| {
                let bytes = fs::metadata(&s.path).ok()?.len();
                Some(Self {
                    first_block: s.first_block,
                    bytes,
                })
            })
            .collect())
    }

    /// Delete each segment in `dir` whose blocks are all at or below
    /// `base`: the next segment starts at or below `base + 1`. The last
    /// segment always stays, because the writer appends to it. Returns
    /// how many were deleted.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory does not read or a segment
    /// does not delete.
    pub fn prune_below(dir: &Path, base: u64) -> Result<usize, StateError> {
        Segment::list(dir)?
            .windows(2)
            .take_while(|pair| pair[1].first_block <= base.saturating_add(1))
            .try_fold(0, |deleted, pair| {
                fs::remove_file(&pair[0].path).map(|()| deleted + 1)
            })
            .map_err(StateError::from)
    }
}

/// A reader of the block log in one directory.
pub struct BlockLog {
    identity: LogIdentity,
    segments: Vec<Segment>,
}

impl BlockLog {
    /// The log in `dir`, as its segments are now. A segment that the
    /// writer starts later is not in this reader.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory does not read.
    pub fn open(dir: &Path, identity: LogIdentity) -> Result<Self, StateError> {
        Ok(Self {
            identity,
            segments: Segment::list(dir)?,
        })
    }

    /// The records of the blocks after `base` up to `to`, in order. The
    /// records stop at the first block that is missing: a gap, a torn
    /// record, or a segment that is gone or of another identity.
    pub fn after(&self, base: u64, to: u64) -> impl Iterator<Item = Record> + '_ {
        let start = self
            .segments
            .iter()
            .rposition(|s| s.first_block <= base.saturating_add(1))
            .unwrap_or(self.segments.len());
        let mut expected = base.checked_add(1);
        self.segments[start..]
            .iter()
            .flat_map(|s| s.records(self.identity).into_iter().flatten())
            .skip_while(move |r| r.block <= base)
            .take_while(move |r| {
                let next = expected.filter(|e| r.block == *e && r.block <= to);
                expected = next.and_then(|e| e.checked_add(1));
                next.is_some()
            })
    }
}

/// The open segment of the writer.
struct OpenSegment {
    writer: BufWriter<File>,
    /// The bytes in the file.
    len: u64,
    /// The block that continues this segment.
    next_block: u64,
}

impl OpenSegment {
    fn append(&mut self, block: u64, batch: &[u8]) -> Result<(), StateError> {
        Record::encode(&mut self.writer, block, batch)?;
        self.len = self
            .len
            .checked_add((RECORD_HEAD + batch.len()) as u64)
            .ok_or_else(|| std::io::Error::other("the segment length overflows"))?;
        self.next_block = block
            .checked_add(1)
            .ok_or_else(|| std::io::Error::other("the block number overflows"))?;
        Ok(())
    }

    fn sync(&mut self) -> Result<(), StateError> {
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        Ok(())
    }
}

/// The writer of the block log in one directory.
pub struct BlockLogWriter {
    dir: PathBuf,
    identity: LogIdentity,
    segment_bytes: NonZeroU64,
    open: Option<OpenSegment>,
}

impl BlockLogWriter {
    /// Open the log in `dir`, and continue its last segment after the last
    /// good record. The writer cuts a torn record at the end. A last
    /// segment of another identity stays as it is, and the next batch
    /// starts a new segment.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory or the last segment does not
    /// read or write.
    pub fn open(
        dir: &Path,
        identity: LogIdentity,
        segment_bytes: NonZeroU64,
    ) -> Result<Self, StateError> {
        fs::create_dir_all(dir)?;
        let open = Segment::list(dir)?
            .last()
            .map(|last| Self::resume(last, identity, u64::MAX))
            .transpose()?
            .flatten();
        Ok(Self {
            dir: dir.to_path_buf(),
            identity,
            segment_bytes,
            open,
        })
    }

    /// The block that continues the log. `None` when no segment is open:
    /// the next batch starts a segment at its own block.
    #[must_use]
    pub fn next_block(&self) -> Option<u64> {
        self.open.as_ref().map(|s| s.next_block)
    }

    /// Append the batch of the next committed block. A block after a gap
    /// starts a new segment, and so does a full segment.
    ///
    /// # Errors
    ///
    /// Returns an error for a block at or below the last block of the open
    /// segment, and when a write fails.
    pub fn append(&mut self, batch: &WriteBatch) -> Result<(), StateError> {
        let block = batch.boundary.block_number;
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(batch).map_err(|e| self.refuse(e))?;
        self.segment_for(block)?.append(block, &bytes)
    }

    /// Cut the log after `block`: delete each segment that starts above
    /// it, and cut the last segment after its record of `block`. The log
    /// then continues with the batch of `block + 1`.
    ///
    /// # Errors
    ///
    /// Returns an error when a segment does not sync, delete or cut.
    pub fn cut_after(&mut self, block: u64) -> Result<(), StateError> {
        self.open.take().map(|mut s| s.sync()).transpose()?;
        let (kept, above): (Vec<Segment>, Vec<Segment>) = Segment::list(&self.dir)?
            .into_iter()
            .partition(|s| s.first_block <= block);
        above.iter().try_for_each(|s| fs::remove_file(&s.path))?;
        self.open = kept
            .last()
            .map(|last| Self::resume(last, self.identity, block))
            .transpose()?
            .flatten();
        Ok(())
    }

    /// Give up the open segment after a failed write. The next batch
    /// starts a new segment, and a reader stops at the torn record.
    pub fn abandon(&mut self) {
        self.open = None;
    }

    /// Force the appended records to disk.
    ///
    /// # Errors
    ///
    /// Returns an error when the sync fails.
    pub fn sync(&mut self) -> Result<(), StateError> {
        self.open.as_mut().map_or(Ok(()), OpenSegment::sync)
    }

    /// The open segment that `block` continues, or a new one that starts
    /// at `block`.
    fn segment_for(&mut self, block: u64) -> Result<&mut OpenSegment, StateError> {
        if let Some(next) = self.next_block().filter(|next| block < *next) {
            return Err(self.refuse(format!("block {block} is below the next block {next}")));
        }
        let limit = self.segment_bytes.get();
        let segment = match self.open.take() {
            Some(open) if open.next_block == block && open.len < limit => open,
            ended => self.start(block, ended)?,
        };
        Ok(self.open.insert(segment))
    }

    /// Sync the segment that `ended`, and open a new one that starts at
    /// `block`.
    fn start(&self, block: u64, ended: Option<OpenSegment>) -> Result<OpenSegment, StateError> {
        ended.map(|mut s| s.sync()).transpose()?;
        let path = self.dir.join(Segment::name(block));
        let mut file = File::create(&path)?;
        let head = SegmentHead {
            identity: self.identity,
            first_block: block,
        };
        file.write_all(&head.encode())?;
        file.sync_all()?;
        Ok(OpenSegment {
            writer: BufWriter::new(file),
            len: SEGMENT_HEAD as u64,
            next_block: block,
        })
    }

    /// The open segment after the last good record of `last` up to block
    /// `upto`, cut there. `None` when `last` is of another identity.
    fn resume(
        last: &Segment,
        identity: LogIdentity,
        upto: u64,
    ) -> Result<Option<OpenSegment>, StateError> {
        let Some(records) = last.records(identity) else {
            return Ok(None);
        };
        let (len, next_block) = records
            .take_while(|r| r.block <= upto)
            .try_fold((SEGMENT_HEAD as u64, last.first_block), |(len, _), r| {
                Some((len.checked_add(r.len_on_disk())?, r.block.checked_add(1)?))
            })
            .ok_or_else(|| std::io::Error::other("a record of the last segment overflows"))?;
        let file = OpenOptions::new().write(true).open(&last.path)?;
        file.set_len(len)?;
        let mut writer = BufWriter::new(file);
        std::io::Seek::seek(&mut writer, std::io::SeekFrom::Start(len))?;
        Ok(Some(OpenSegment {
            writer,
            len,
            next_block,
        }))
    }

    fn refuse(&self, detail: impl std::fmt::Display) -> StateError {
        StateError::BlockLog {
            dir: self.dir.display().to_string(),
            detail: detail.to_string(),
        }
    }
}

#[cfg(test)]
#[path = "block_log_tests.rs"]
mod tests;
