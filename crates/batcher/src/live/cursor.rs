//! The durable cursor and the L1-truth reconcile it starts from.

use std::path::Path;

use alloy_primitives::Address;
use alloy_provider::Provider;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::l1::read_posted_batches;
use crate::settlement::IKardamomL2Settlement;

/// The durable cursor: the ordering-stream position matching the last
/// confirmed L1 post. `next_index` and `next_block` seed the cluster replay
/// request. `last_batch_index` ties the position to the contract's CAS
/// counter.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BatchCursor {
    pub next_index: u64,
    pub next_block: u64,
    pub last_batch_index: u64,
}

impl BatchCursor {
    /// A fresh consumer: no records seen, the first boundary is block 1,
    /// and nothing is posted (`lastBatchIndex` starts at 0 on-chain; batch
    /// indices start at 1).
    #[must_use]
    pub fn genesis() -> Self {
        Self {
            next_index: 0,
            next_block: 1,
            last_batch_index: 0,
        }
    }

    /// # Errors
    /// Returns an error when `path` exists but cannot be read or parsed.
    pub fn load(path: &Path) -> Result<Option<Self>> {
        match std::fs::read_to_string(path) {
            Ok(raw) => Ok(Some(serde_json::from_str(&raw).with_context(|| {
                format!("parse batcher cursor file {}", path.display())
            })?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).with_context(|| format!("read cursor file {}", path.display())),
        }
    }

    /// An atomic write: write to a temp file, then rename it into place in
    /// the same directory.
    ///
    /// # Errors
    /// Returns an error when serialization, the write, or the rename fails.
    pub fn store(&self, path: &Path) -> Result<()> {
        let tmp = path.with_extension("tmp");
        let bytes = serde_json::to_vec(self).context("serialize cursor")?;
        std::fs::write(&tmp, bytes)
            .with_context(|| format!("write cursor tmp {}", tmp.display()))?;
        std::fs::rename(&tmp, path)
            .with_context(|| format!("rename cursor into place {}", path.display()))?;
        Ok(())
    }
}

/// What L1 says has been posted: the CAS counter, and the block the chain
/// is covered through (the latest `BatchPosted.l2BlockEnd`; 0 when nothing
/// is posted yet, since L2 blocks start at 1).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct L1Truth {
    pub last_batch_index: u64,
    pub covered_through_block: u64,
}

/// The settlement contract's CAS counter (`lastBatchIndex`). Both
/// [`read_l1_truth`] and the offline post path use this. The offline path
/// needs only the counter, not the `BatchPosted` event scan.
///
/// # Errors
/// Returns an error when the contract call fails.
pub async fn read_last_batch_index<P: Provider>(provider: &P, settlement: Address) -> Result<u64> {
    IKardamomL2Settlement::new(settlement, provider)
        .lastBatchIndex()
        .call()
        .await
        .context("read lastBatchIndex")
}

/// Read the settlement contract's view. The event scan runs from L1 block
/// 0. This is fine against the dev-cluster anvil. A long-lived L1 should
/// pass a deployment block hint here.
pub(crate) async fn read_l1_truth<P: Provider>(
    provider: &P,
    settlement: Address,
) -> Result<L1Truth> {
    let last = read_last_batch_index(provider, settlement).await?;
    if last == 0 {
        return Ok(L1Truth {
            last_batch_index: 0,
            covered_through_block: 0,
        });
    }
    let posted = read_posted_batches(provider, settlement, 0)
        .await
        .context("read BatchPosted events")?;
    let head = posted
        .iter()
        .find(|d| d.index == last)
        .with_context(|| format!("lastBatchIndex={last} but no BatchPosted event with it"))?;
    Ok(L1Truth {
        last_batch_index: last,
        covered_through_block: head.l2_block_end,
    })
}

/// Reconcile the cursor file against L1 at startup. Returns the cursor to
/// replay from and the block to skip through (drop without posting).
///
/// - No cursor file: replay from genesis; skip L1 coverage instead of
///   re-posting it.
/// - Cursor behind L1 (a crash between post and cursor write): replay from
///   the cursor, and skip through L1's covered block.
/// - Cursor ahead of L1: the chain regressed under this batcher (for
///   example, an anvil reset while `/opt/kardamom` survived). Stop, and let
///   the operator decide which side is real. Silently re-posting would
///   fork the DA history.
pub(crate) fn reconcile(cursor: Option<BatchCursor>, l1: L1Truth) -> Result<(BatchCursor, u64)> {
    match cursor {
        None => {
            if l1.last_batch_index > 0 {
                warn!(
                    covered_through_block = l1.covered_through_block,
                    "no cursor file but L1 has batches; genesis replay will skip re-posting \
                     (requires cluster retention back to genesis)"
                );
            }
            Ok((BatchCursor::genesis(), l1.covered_through_block))
        }
        Some(c) if c.last_batch_index > l1.last_batch_index => bail!(
            "cursor file says batch {} was posted but L1 lastBatchIndex is {} — the L1 chain \
             regressed under a surviving cursor (anvil reset?); refusing to guess. Delete the \
             cursor file to re-derive from this L1, or restore the L1 state",
            c.last_batch_index,
            l1.last_batch_index,
        ),
        Some(c) => Ok((c, l1.covered_through_block)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_roundtrip_and_missing() {
        let dir = std::env::temp_dir().join(format!("batcher-cursor-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cursor.json");
        assert_eq!(BatchCursor::load(&path).unwrap(), None);
        let c = BatchCursor {
            next_index: 42,
            next_block: 7,
            last_batch_index: 3,
        };
        c.store(&path).unwrap();
        assert_eq!(BatchCursor::load(&path).unwrap(), Some(c));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn reconcile_matrix() {
        let l1_empty = L1Truth {
            last_batch_index: 0,
            covered_through_block: 0,
        };
        let l1_posted = L1Truth {
            last_batch_index: 5,
            covered_through_block: 120,
        };
        // Fresh start, empty chain: genesis, nothing skipped.
        assert_eq!(
            reconcile(None, l1_empty).unwrap(),
            (BatchCursor::genesis(), 0)
        );
        // Lost cursor on a posted chain: genesis replay, skip L1 coverage.
        assert_eq!(
            reconcile(None, l1_posted).unwrap(),
            (BatchCursor::genesis(), 120)
        );
        // Stale cursor (crash between post and cursor write): replay from
        // the cursor, skip through L1's covered block.
        let stale = BatchCursor {
            next_index: 900,
            next_block: 100,
            last_batch_index: 4,
        };
        assert_eq!(reconcile(Some(stale), l1_posted).unwrap(), (stale, 120));
        // Cursor ahead of L1 (chain regressed): fail-stop.
        let ahead = BatchCursor {
            next_index: 2000,
            next_block: 200,
            last_batch_index: 9,
        };
        let err = reconcile(Some(ahead), l1_posted).unwrap_err().to_string();
        assert!(err.contains("regressed"), "unexpected error: {err}");
    }
}
