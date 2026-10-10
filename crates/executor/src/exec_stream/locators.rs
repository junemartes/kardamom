//! The locator log: where a canonical index is in the recordings of this
//! executor's stream.
//!
//! The archive replays by position, not by content. So the publisher thread
//! appends one locator for the first record of each session, then one for
//! every [`LOCATOR_EVERY`] records. A locator names a position at or before
//! the start of its record, so a replay from it reaches the record.
//!
//! Entry layout, 24 bytes, little-endian:
//! `[index:u64][session_id:i32][position:i64][crc32:u32]`. The CRC covers
//! the first 20 bytes. The file is append-only. A crash can tear the last
//! entry. [`LocatorLog::open`] keeps the longest prefix of whole entries
//! with a good CRC and cuts the rest, so a torn tail costs only that entry:
//! a lookup then takes the previous entry, which is a lower bound.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

use kardamom_state::Locators;

/// The publisher appends a locator for the first record of a session and
/// then for every this many records.
pub(crate) const LOCATOR_EVERY: u64 = 1024;

/// The size of one entry on disk.
const ENTRY_LEN: usize = 24;

/// [`ENTRY_LEN`] as a file length.
const ENTRY_LEN_U64: u64 = 24;

/// The bytes that the CRC of an entry covers.
const BODY_LEN: usize = 20;

/// Where one record is: the session of its recording and a position at or
/// before the start of the record. The answers state of the query endpoint
/// keeps the same value.
pub use kardamom_state::ExecLocator as Locator;

/// One entry on disk.
struct Entry([u8; ENTRY_LEN]);

impl Entry {
    fn encode(locator: Locator) -> Self {
        let mut entry = [0u8; ENTRY_LEN];
        entry[..8].copy_from_slice(&locator.index.to_le_bytes());
        entry[8..12].copy_from_slice(&locator.session_id.to_le_bytes());
        entry[12..BODY_LEN].copy_from_slice(&locator.position.to_le_bytes());
        let crc = crc32fast::hash(&entry[..BODY_LEN]);
        entry[BODY_LEN..].copy_from_slice(&crc.to_le_bytes());
        Self(entry)
    }

    /// Decode one whole entry. `None` when its CRC does not match.
    fn decode(entry: &[u8; ENTRY_LEN]) -> Option<Locator> {
        let (body, crc) = entry.split_at(BODY_LEN);
        let crc = u32::from_le_bytes(crc.try_into().ok()?);
        if crc32fast::hash(body) != crc {
            return None;
        }
        Some(Locator {
            index: u64::from_le_bytes(body[..8].try_into().ok()?),
            session_id: i32::from_le_bytes(body[8..12].try_into().ok()?),
            position: i64::from_le_bytes(body[12..BODY_LEN].try_into().ok()?),
        })
    }
}

/// The locator log of one executor. One thread owns it: the stream
/// publisher thread appends to it, and nothing else writes the file.
pub struct LocatorLog {
    file: File,
    /// Every good entry, in append order.
    entries: Locators,
    /// The length of the good prefix of the file, in bytes.
    len: u64,
}

impl LocatorLog {
    /// Open the log at `path`, create it and its directory when absent,
    /// and cut a torn or corrupt tail.
    ///
    /// # Errors
    ///
    /// Returns the I/O error when the file cannot be created, read, or cut.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let mut entries = Locators::default();
        bytes
            .as_chunks::<ENTRY_LEN>()
            .0
            .iter()
            .map_while(Entry::decode)
            .for_each(|l| entries.push(l));
        let len = u64::try_from(entries.entries().len())
            .ok()
            .and_then(|n| n.checked_mul(ENTRY_LEN_U64))
            .ok_or_else(|| std::io::Error::other("the locator log length overflows u64"))?;
        file.set_len(len)?;
        Ok(Self { file, entries, len })
    }

    /// Append `locator`. A failed write cuts the file back to its good
    /// prefix, so a later entry never lands at a torn offset, and returns
    /// the error. The caller loses only this entry.
    ///
    /// # Errors
    ///
    /// Returns the I/O error of the write.
    pub fn append(&mut self, locator: Locator) -> std::io::Result<()> {
        let len = self
            .len
            .checked_add(ENTRY_LEN_U64)
            .ok_or_else(|| std::io::Error::other("the locator log length overflows u64"))?;
        match self.file.write_all(&Entry::encode(locator).0) {
            Ok(()) => {
                self.len = len;
                self.entries.push(locator);
                Ok(())
            }
            Err(e) => {
                let _ = self.file.set_len(self.len);
                Err(e)
            }
        }
    }

    /// The newest entry whose index is at or below `index`. A replay of its
    /// session from its position reaches `index` when that session holds
    /// it. The entry is a lower bound: the replay can start before the
    /// record.
    #[must_use]
    pub fn lookup(&self, index: u64) -> Option<Locator> {
        self.entries.lookup(index)
    }

    /// Every good entry, in append order.
    #[must_use]
    pub fn entries(&self) -> &[Locator] {
        self.entries.entries()
    }
}
