//! The sealer seed: the head of a state rebuilt from L1, in the form a
//! sealer cluster with no state starts from.
//!
//! A sealer cluster that lost all its state cannot start at genesis: the
//! consumers resume at the rebuilt head. The seed gives the sealer that
//! head (block `H`, its canonical end `E_H`, its timestamp and L1 origin)
//! and the next nonce of each sender its nonce guard tracks. The sealer
//! reads it in `SealerSeed.java`; the two layouts must stay equal.
//!
//! Layout, big-endian:
//! `magic "KSED" (4) | version (4) | chain_id (8) | block (8) |
//! end_tx_idx (8) | l2_timestamp (8) | l1_origin (8) | state_root (32) |
//! sender_count (4) | sender_count * (address (20) | next_nonce (8))`.

use std::collections::HashMap;
use std::path::Path;

use alloy_primitives::{Address, B256};
use kardamom_engine::ReplayOutcome;
use kardamom_state::{StateEnvBuilder, StateError, StateSnapshot};
use kardamom_types::{BPosition, StateDatabase};

use crate::ReconstructError;

/// The first four bytes of a seed file.
const MAGIC: [u8; 4] = *b"KSED";
/// The layout version. The sealer refuses any other.
const VERSION: u32 = 1;

/// One sender the sealer's nonce guard tracks, and the nonce its next
/// transaction must carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeedSender {
    pub address: Address,
    pub next_nonce: u64,
}

/// The values a sealer cluster starts from at block `block + 1`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealerSeed {
    pub chain_id: u64,
    /// The rebuilt head `H`. The batcher posted every block up to it.
    pub block: u64,
    /// The canonical end index of `H`: the first index the sealer assigns.
    pub end_tx_idx: u64,
    pub l2_timestamp: u64,
    pub l1_origin: u64,
    pub state_root: B256,
    /// The senders in the order they last sent, the eldest first. The
    /// sealer's guard keeps the most recent senders up to its capacity,
    /// so this order decides which senders it keeps.
    pub senders: Vec<SeedSender>,
}

impl SealerSeed {
    /// The seed file bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the sender count does not fit the 4-byte field.
    pub fn encode(&self) -> Result<Vec<u8>, ReconstructError> {
        let count = u32::try_from(self.senders.len()).map_err(|_| {
            ReconstructError(format!("{} senders do not fit a seed", self.senders.len()))
        })?;
        let header: [&[u8]; 9] = [
            &MAGIC,
            &VERSION.to_be_bytes(),
            &self.chain_id.to_be_bytes(),
            &self.block.to_be_bytes(),
            &self.end_tx_idx.to_be_bytes(),
            &self.l2_timestamp.to_be_bytes(),
            &self.l1_origin.to_be_bytes(),
            self.state_root.as_slice(),
            &count.to_be_bytes(),
        ];
        let senders = self
            .senders
            .iter()
            .flat_map(|s| s.address.iter().copied().chain(s.next_nonce.to_be_bytes()));
        Ok(header
            .into_iter()
            .flatten()
            .copied()
            .chain(senders)
            .collect())
    }

    /// Write the seed file at `path`.
    ///
    /// # Errors
    ///
    /// Returns an error if the write fails.
    pub fn write(&self, path: &Path) -> Result<(), ReconstructError> {
        std::fs::write(path, self.encode()?)
            .map_err(|e| ReconstructError(format!("write the sealer seed {}: {e}", path.display())))
    }
}

/// The distinct senders of a run of transactions, in the order they last
/// sent, the eldest first. This is the order of the sealer's guard, which
/// moves a sender to its newest end on each of its transactions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SenderOrder(Vec<Address>);

impl SenderOrder {
    /// The order of `senders`, which come in canonical order.
    #[must_use]
    pub fn of(senders: impl IntoIterator<Item = Address>) -> Self {
        let last: HashMap<Address, usize> = senders
            .into_iter()
            .enumerate()
            .map(|(position, sender)| (sender, position))
            .collect();
        let mut by_last: Vec<(usize, Address)> = last
            .into_iter()
            .map(|(sender, position)| (position, sender))
            .collect();
        by_last.sort_unstable();
        Self(by_last.into_iter().map(|(_, sender)| sender).collect())
    }
}

/// What a seed reads: the rebuilt state, its replay outcome, and the
/// sender order of the rebuilt blocks.
pub struct SeedInput<'a> {
    pub state_dir: &'a Path,
    pub chain_id: u64,
    pub outcome: &'a ReplayOutcome,
    pub senders: &'a SenderOrder,
}

impl SeedInput<'_> {
    /// The seed of the rebuilt head. It refuses a head that does not come
    /// from L1, and a head whose payload carries no canonical end.
    ///
    /// # Errors
    ///
    /// Returns an error if the state cannot be read, or if the head is not
    /// a block rebuilt from L1 with its canonical end.
    pub fn seed(&self) -> Result<SealerSeed, ReconstructError> {
        let env = StateEnvBuilder::new(self.state_dir)
            .read_only(true)
            .open()
            .map_err(|e| ReconstructError(format!("open state env: {e}")))?;
        let snapshot = StateSnapshot::open(&env)?;
        let block = snapshot.block_number();
        let end_tx_idx = self.rebuilt_end(&snapshot)?;
        let header = snapshot
            .header(block)?
            .ok_or_else(|| ReconstructError(format!("block {block} has no header")))?;
        let state_root = snapshot
            .state_root()?
            .ok_or_else(|| ReconstructError("the rebuilt state holds no state root".into()))?;
        Ok(SealerSeed {
            chain_id: self.chain_id,
            block,
            end_tx_idx,
            l2_timestamp: header.l2_timestamp,
            l1_origin: header.l1_origin,
            state_root,
            senders: self.next_nonces(&snapshot)?,
        })
    }

    /// The canonical end of the head, proven to come from L1: the rebuilt
    /// mark is the last committed end, and the payload carried the end.
    fn rebuilt_end(&self, snapshot: &StateSnapshot) -> Result<u64, ReconstructError> {
        let block = snapshot.block_number();
        let Some(payload_end) = self.outcome.head_end_tx_idx else {
            return Err(ReconstructError(format!(
                "block {block} carries no canonical cursor (a version 2 payload): a sealer cannot start after it"
            )));
        };
        let committed = snapshot.end_tx_position()?;
        let rebuilt = snapshot.l1_rebuilt_end()?;
        if rebuilt != Some(committed) || committed.as_index() != payload_end {
            return Err(ReconstructError(format!(
                "block {block} is not the rebuilt head: committed end {}, rebuilt mark {:?}, payload end {payload_end}",
                committed.as_index(),
                rebuilt.map(BPosition::as_index),
            )));
        }
        Ok(payload_end)
    }

    /// Each sender with the nonce of its rebuilt account: the nonce its
    /// next transaction carries. A sender with no account sends at 0.
    fn next_nonces(&self, snapshot: &StateSnapshot) -> Result<Vec<SeedSender>, ReconstructError> {
        self.senders
            .0
            .iter()
            .map(|&address| {
                let account = snapshot.basic(address)?;
                Ok(SeedSender {
                    address,
                    next_nonce: account.map_or(0, |(nonce, _, _)| nonce),
                })
            })
            .collect()
    }
}

impl From<StateError> for ReconstructError {
    fn from(e: StateError) -> Self {
        Self(format!("read the rebuilt state: {e}"))
    }
}

#[cfg(test)]
#[path = "seed_tests.rs"]
mod tests;
