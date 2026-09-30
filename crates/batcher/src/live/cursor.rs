//! The durable cursor and the L1-truth reconcile it starts from.

use std::path::Path;
use std::time::Duration;

use alloy_primitives::Address;
use alloy_provider::Provider;
use anyhow::{Context, Result, bail};

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::frame::BlockFrame;
use crate::indexer::IndexerClient;
use crate::l1::{read_posted_batches, recover_blocks};
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
/// `from_block`: 0 is fine against the dev-cluster anvil; a long-lived L1
/// takes the settlement's deployment block, so the scan stays bounded.
pub(crate) async fn read_l1_truth<P: Provider>(
    provider: &P,
    settlement: Address,
    from_block: u64,
) -> Result<L1Truth> {
    let last = read_last_batch_index(provider, settlement).await?;
    if last == 0 {
        return Ok(L1Truth::nothing_posted());
    }
    let posted = read_posted_batches(provider, settlement, from_block)
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

impl L1Truth {
    const fn nothing_posted() -> Self {
        Self {
            last_batch_index: 0,
            covered_through_block: 0,
        }
    }

    /// The settlement contract's view through the indexer: the CAS counter
    /// from L1, the covered block from the indexer's copy of that batch.
    /// No event scan. The indexer follows the finalized L1, so a batch
    /// posted in the last minutes is not there yet; this waits for it,
    /// one poll every `poll` up to `polls` times, then gives up.
    ///
    /// # Errors
    /// Returns an error when L1 or the indexer fails, or when the indexer
    /// does not reach the batch in time.
    pub(crate) async fn read_via_indexer<P: Provider>(
        provider: &P,
        settlement: Address,
        indexer: &IndexerClient,
        poll: Duration,
        polls: u32,
    ) -> Result<Self> {
        let last = read_last_batch_index(provider, settlement).await?;
        if last == 0 {
            return Ok(Self::nothing_posted());
        }
        let mut left = polls;
        loop {
            if let Some(truth) = Self::poll_indexer(indexer, last, poll, &mut left).await? {
                return Ok(truth);
            }
        }
    }

    /// One poll: the truth when the indexer has batch `last`; `None` after
    /// a wait when it does not, or an error when no polls are left.
    async fn poll_indexer(
        indexer: &IndexerClient,
        last: u64,
        poll: Duration,
        left: &mut u32,
    ) -> Result<Option<Self>> {
        if let Some(d) = indexer.batch(last).await? {
            return Ok(Some(Self {
                last_batch_index: last,
                covered_through_block: d.l2_block_end,
            }));
        }
        *left = left.checked_sub(1).with_context(|| {
            format!("indexer has not reached batch {last} (it follows the finalized L1)")
        })?;
        warn!(
            last_batch_index = last,
            polls_left = *left,
            "indexer behind L1; waiting"
        );
        tokio::time::sleep(poll).await;
        Ok(None)
    }
}

/// The cursor a fresh batcher resumes from when L1 already holds batches:
/// the position just past the last posted batch, read from that batch's
/// own blobs. The blobs carry each block's cursor (`BlockCursor`), so the
/// replay request names a point of the stream the sealer can check,
/// instead of genesis, which the cluster's retention cannot serve.
///
/// # Errors
/// Returns an error when the indexer or a blob fails verification, or
/// when the batch's last block is not L1's covered block.
pub(crate) async fn resume_from_indexer(
    indexer: &IndexerClient,
    l1: L1Truth,
) -> Result<(BatchCursor, u64)> {
    let descriptor = indexer
        .batch(l1.last_batch_index)
        .await?
        .with_context(|| format!("indexer lost batch {}", l1.last_batch_index))?;
    let blocks = recover_blocks(std::slice::from_ref(&descriptor), indexer)
        .with_context(|| format!("recover batch {} from the indexer", l1.last_batch_index))?;
    resume_from_blocks(&blocks, l1)
}

/// [`resume_from_indexer`] on recovered blocks. A batch of version-2 blobs
/// carries no cursor; then the replay starts at genesis, as without an
/// indexer.
pub(crate) fn resume_from_blocks(blocks: &[BlockFrame], l1: L1Truth) -> Result<(BatchCursor, u64)> {
    let last = blocks
        .last()
        .with_context(|| format!("batch {} holds no block", l1.last_batch_index))?;
    if last.block_number != l1.covered_through_block {
        bail!(
            "batch {} ends at block {} but L1 says it covers through {}",
            l1.last_batch_index,
            last.block_number,
            l1.covered_through_block
        );
    }
    let Some(cursor) = last.cursor else {
        warn!(
            last_batch_index = l1.last_batch_index,
            "last batch carries no block cursor (version 2 blobs); replaying from genesis"
        );
        return Ok(genesis_reconcile(l1));
    };
    let next_block = last
        .block_number
        .checked_add(1)
        .context("block_number overflowed u64")?;
    Ok((
        BatchCursor {
            next_index: cursor.end_tx_idx,
            next_block,
            last_batch_index: l1.last_batch_index,
        },
        l1.covered_through_block,
    ))
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
        None => Ok(genesis_reconcile(l1)),
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

/// Build the starting cursor when no cursor file exists. Warns if L1
/// already has batches: genesis replay then skips re-posting them, which
/// needs cluster retention back to genesis.
fn genesis_reconcile(l1: L1Truth) -> (BatchCursor, u64) {
    if l1.last_batch_index > 0 {
        warn!(
            covered_through_block = l1.covered_through_block,
            "no cursor file but L1 has batches; genesis replay will skip re-posting \
             (requires cluster retention back to genesis)"
        );
    }
    (BatchCursor::genesis(), l1.covered_through_block)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::BlockCursor;

    fn truth(last_batch_index: u64, covered_through_block: u64) -> L1Truth {
        L1Truth {
            last_batch_index,
            covered_through_block,
        }
    }

    fn frame(block_number: u64, cursor: Option<BlockCursor>) -> BlockFrame {
        BlockFrame {
            block_number,
            cursor,
            ..BlockFrame::default()
        }
    }

    #[test]
    fn a_fresh_batcher_resumes_just_past_the_last_posted_batch() {
        let blocks = [
            frame(
                11,
                Some(BlockCursor {
                    end_tx_idx: 30,
                    l1_origin: 5,
                }),
            ),
            frame(
                12,
                Some(BlockCursor {
                    end_tx_idx: 40,
                    l1_origin: 5,
                }),
            ),
        ];
        let (cursor, skip) = resume_from_blocks(&blocks, truth(7, 12)).unwrap();
        assert_eq!(
            cursor,
            BatchCursor {
                next_index: 40,
                next_block: 13,
                last_batch_index: 7,
            }
        );
        assert_eq!(skip, 12);
    }

    #[test]
    fn a_batch_without_block_cursors_replays_from_genesis() {
        let blocks = [frame(12, None)];
        let (cursor, skip) = resume_from_blocks(&blocks, truth(7, 12)).unwrap();
        assert_eq!(cursor, BatchCursor::genesis());
        assert_eq!(skip, 12);
    }

    #[test]
    fn a_batch_that_does_not_end_at_the_covered_block_is_refused() {
        let blocks = [frame(11, Some(BlockCursor::default()))];
        let err = resume_from_blocks(&blocks, truth(7, 12))
            .unwrap_err()
            .to_string();
        assert!(err.contains("ends at block 11"), "{err}");
        assert!(resume_from_blocks(&[], truth(7, 12)).is_err());
    }

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
