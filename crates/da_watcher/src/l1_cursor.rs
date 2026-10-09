//! The L1 watcher's durable cursor value: the last L1 block whose epoch
//! the watcher published, by number and by hash.
//!
//! The hash is part of the cursor because the next block must name it as
//! its parent. A restart reads the next block through the same source set,
//! and the watcher refuses it when the link does not hold. A cursor of the
//! number alone would let a restart continue on any chain.

use std::fmt;
use std::num::ParseIntError;
use std::str::FromStr;

use alloy_primitives::B256;

/// The last L1 block whose epoch the watcher published. The next epoch is
/// block `number + 1`, and its parent hash must equal `hash`.
///
/// The file form is one line: the decimal number, one space, and the hash
/// as `0x` and 64 hex digits. An operator can read it and write it by hand.
/// The reader ignores any field after the hash, so a later release can add
/// one at the tail and this release still reads its file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L1Cursor {
    /// The L1 block number.
    pub number: u64,
    /// The hash of that block, as the source set agreed it.
    pub hash: B256,
}

/// Why a cursor line does not parse.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum L1CursorError {
    /// The line holds fewer than the two required fields.
    #[error("an L1 cursor starts with `<number> <hash>`; got {0} fields")]
    Fields(usize),
    /// The first field is not a `u64`.
    #[error("not an L1 block number: {0}")]
    Number(#[from] ParseIntError),
    /// The second field is not a 32-byte hex hash.
    #[error("not an L1 block hash: {0}")]
    Hash(String),
}

impl fmt::Display for L1Cursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.number, self.hash)
    }
}

impl FromStr for L1Cursor {
    type Err = L1CursorError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let fields: Vec<&str> = s.split_ascii_whitespace().collect();
        let [number, hash, ..] = fields.as_slice() else {
            return Err(L1CursorError::Fields(fields.len()));
        };
        Ok(Self {
            number: number.parse()?,
            hash: hash
                .parse()
                .map_err(|e: alloy_primitives::hex::FromHexError| {
                    L1CursorError::Hash(e.to_string())
                })?,
        })
    }
}

#[cfg(test)]
#[path = "l1_cursor_tests.rs"]
mod tests;
