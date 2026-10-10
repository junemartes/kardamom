//! The fee history the ingress serves to wallets: `eth_feeHistory`,
//! `eth_maxPriorityFeePerGas`, and `eth_gasPrice`.
//!
//! The ingress holds no block headers. The executors' block boundaries on
//! `tx_receipts` carry each block's base fee and gas used, and every
//! receipt carries the tip rate its transaction paid, so a ring of the
//! last blocks is the whole history a wallet asks for. The reward
//! percentiles are over the block's tip rates, one per transaction. There
//! is no floor on the tip, so a zero suggestion is a valid price at all
//! times.

use std::collections::VecDeque;
use std::sync::Mutex;

use crate::sync_util::LockIgnorePoison;

use alloy_rpc_types_eth::FeeHistory as RpcFeeHistory;
use kardamom_types::BlockBoundary;
use kardamom_types::fees::BaseFeeSchedule;
use kardamom_types::limits::BLOCK_GAS_LIMIT;

/// How many closed blocks the ring keeps: `eth_feeHistory` serves at
/// most this many.
const CAPACITY: usize = 1024;

/// How many of the newest blocks the tip suggestion looks at.
const SUGGESTION_BLOCKS: usize = 20;

/// One block of the ring. The receipts open it; the boundary closes it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FeeBlock {
    number: u64,
    base_fee: u128,
    gas_used: u64,
    /// Whether the boundary arrived. An open block serves no history.
    closed: bool,
    /// The tip rate of every receipt, in arrival order.
    tips: Vec<u128>,
}

impl FeeBlock {
    fn open(number: u64) -> Self {
        Self {
            number,
            base_fee: 0,
            gas_used: 0,
            closed: false,
            tips: Vec::new(),
        }
    }

    /// The base fee of the block after this one.
    fn next_base_fee(&self) -> u128 {
        BaseFeeSchedule::CHAIN.next_base_fee(self.base_fee, self.gas_used)
    }

    /// The reward at each percentile: the tip rate at that rank of the
    /// block's sorted tips. An empty block rewards zero at every rank.
    fn rewards(&self, percentiles: &[f64]) -> Vec<u128> {
        let mut sorted = self.tips.clone();
        sorted.sort_unstable();
        percentiles
            .iter()
            .map(|p| Self::at_percentile(&sorted, *p))
            .collect()
    }

    /// `sorted` is non-empty on this path only when the block had
    /// receipts; the rank is clamped into the vector, so the cast from
    /// a percentile in `0..=100` cannot index out of range.
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss,
        reason = "a percentile rank over at most a few thousand receipts: the float math is exact enough and the index is clamped"
    )]
    fn at_percentile(sorted: &[u128], percentile: f64) -> u128 {
        let Some(last) = sorted.len().checked_sub(1) else {
            return 0;
        };
        let rank = (percentile.clamp(0.0, 100.0) / 100.0 * last as f64).round() as usize;
        sorted[rank.min(last)]
    }
}

/// The ring of recent blocks. One `Mutex`: the receipt watcher and the
/// boundary watcher write from their tasks, and the RPC tasks read; each
/// critical section touches a few entries of a small deque.
#[derive(Debug, Default)]
pub(crate) struct FeeHistory {
    blocks: Mutex<VecDeque<FeeBlock>>,
}

impl FeeHistory {
    /// Record one receipt's tip rate in its block.
    pub(crate) fn on_receipt(&self, block_number: u64, tip_rate: u128) {
        let mut blocks = self.blocks.lock_ignore_poison();
        Self::entry(&mut blocks, block_number).tips.push(tip_rate);
        Self::trim(&mut blocks);
    }

    /// Close a block with its boundary. Every executor publishes the same
    /// boundary, so a repeat only rewrites the same values.
    pub(crate) fn on_boundary(&self, boundary: &BlockBoundary) {
        let mut blocks = self.blocks.lock_ignore_poison();
        let block = Self::entry(&mut blocks, boundary.block_number);
        block.base_fee = boundary.base_fee;
        block.gas_used = boundary.gas_used;
        block.closed = true;
        Self::trim(&mut blocks);
    }

    /// The entry of block `number`, opened at its place in number order
    /// when it is new. The order of arrival is not the order of the
    /// blocks: an executor streams a receipt when it executes the
    /// transaction, but publishes a boundary only when the block is
    /// durable. So the receipts of block N+1 can come before the boundary
    /// of block N, and block N has no entry yet when it has no receipts.
    fn entry(blocks: &mut VecDeque<FeeBlock>, number: u64) -> &mut FeeBlock {
        let at = blocks
            .binary_search_by_key(&number, |b| b.number)
            .unwrap_or_else(|at| {
                blocks.insert(at, FeeBlock::open(number));
                at
            });
        &mut blocks[at]
    }

    /// Drop the oldest blocks past the capacity.
    fn trim(blocks: &mut VecDeque<FeeBlock>) {
        let excess = blocks.len().saturating_sub(CAPACITY);
        blocks.drain(..excess);
    }

    /// The newest closed block's number. Zero before the first boundary.
    pub(crate) fn latest(&self) -> u64 {
        let blocks = self.blocks.lock_ignore_poison();
        blocks
            .iter()
            .rev()
            .find(|b| b.closed)
            .map_or(0, |b| b.number)
    }

    /// The base fee the next block charges, from the newest closed block.
    pub(crate) fn next_base_fee(&self) -> u128 {
        let blocks = self.blocks.lock_ignore_poison();
        blocks
            .iter()
            .rev()
            .find(|b| b.closed)
            .map_or(0, FeeBlock::next_base_fee)
    }

    /// A tip rate a wallet can bid: the median tip of the newest closed
    /// blocks' receipts. Zero with no receipts: there is no floor.
    pub(crate) fn suggested_tip(&self) -> u128 {
        let blocks = self.blocks.lock_ignore_poison();
        let mut tips: Vec<u128> = blocks
            .iter()
            .rev()
            .filter(|b| b.closed)
            .take(SUGGESTION_BLOCKS)
            .flat_map(|b| b.tips.iter().copied())
            .collect();
        tips.sort_unstable();
        tips.get(tips.len() / 2).copied().unwrap_or(0)
    }

    /// The history of up to `count` closed blocks ending at `newest`, in
    /// Ethereum's shape: one base fee per block plus the next block's,
    /// the gas used ratio per block, and the rewards at `percentiles`
    /// when any are asked. `None` when `newest` is not a closed block the
    /// ring holds.
    pub(crate) fn query(
        &self,
        count: u64,
        newest: u64,
        percentiles: &[f64],
    ) -> Option<RpcFeeHistory> {
        let blocks = self.blocks.lock_ignore_poison();
        let end = blocks
            .iter()
            .rposition(|b| b.number == newest && b.closed)?;
        let first = end
            .saturating_add(1)
            .saturating_sub(usize::try_from(count).ok()?);
        let window: Vec<&FeeBlock> = blocks.range(first..=end).collect();
        let newest_block = window.last()?;
        let mut base_fee_per_gas: Vec<u128> = window.iter().map(|b| b.base_fee).collect();
        base_fee_per_gas.push(newest_block.next_base_fee());
        let reward = (!percentiles.is_empty())
            .then(|| window.iter().map(|b| b.rewards(percentiles)).collect());
        Some(RpcFeeHistory {
            base_fee_per_gas,
            gas_used_ratio: window.iter().map(|b| gas_used_ratio(b.gas_used)).collect(),
            base_fee_per_blob_gas: Vec::new(),
            blob_gas_used_ratio: Vec::new(),
            oldest_block: window.first()?.number,
            reward,
        })
    }
}

/// The share of the block gas limit a block used.
#[allow(
    clippy::cast_precision_loss,
    reason = "a ratio for a wallet: a block's gas used is under 2^25, exact in an f64"
)]
fn gas_used_ratio(gas_used: u64) -> f64 {
    gas_used as f64 / BLOCK_GAS_LIMIT as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn boundary(number: u64, base_fee: u128, gas_used: u64) -> BlockBoundary {
        BlockBoundary {
            block_number: number,
            base_fee,
            gas_used,
            ..BlockBoundary::default()
        }
    }

    #[test]
    fn serves_base_fees_ratios_and_rewards_of_closed_blocks() {
        let h = FeeHistory::default();
        h.on_receipt(1, 5);
        h.on_receipt(1, 1);
        h.on_receipt(1, 9);
        h.on_boundary(&boundary(1, 800, 30_000_000));
        h.on_boundary(&boundary(2, 900, 0));
        h.on_receipt(3, 4);
        assert_eq!(h.latest(), 2);
        let history = h.query(2, 2, &[0.0, 50.0, 100.0]).unwrap();
        assert_eq!(history.oldest_block, 1);
        // The two blocks, then the next block's base fee: 900 after an
        // empty block is 788.
        assert_eq!(history.base_fee_per_gas, vec![800, 900, 788]);
        assert_eq!(history.gas_used_ratio, vec![1.0, 0.0]);
        assert_eq!(history.reward, Some(vec![vec![1, 5, 9], vec![0, 0, 0]]));
        assert_eq!(h.next_base_fee(), 788);
        // An open block, and a block the ring does not hold, serve nothing.
        assert!(h.query(1, 3, &[]).is_none());
        assert!(h.query(1, 7, &[]).is_none());
        // No percentiles, no rewards; a count past the ring's start clips.
        let clipped = h.query(10, 2, &[]).unwrap();
        assert_eq!(clipped.oldest_block, 1);
        assert_eq!(clipped.reward, None);
    }

    #[test]
    fn the_suggestion_is_the_median_tip_of_recent_closed_blocks() {
        let h = FeeHistory::default();
        assert_eq!(h.suggested_tip(), 0);
        h.on_receipt(1, 2);
        h.on_receipt(1, 8);
        h.on_boundary(&boundary(1, 1, 0));
        h.on_receipt(2, 4);
        h.on_boundary(&boundary(2, 1, 0));
        assert_eq!(h.suggested_tip(), 4);
        // A late copy of a boundary rewrites the same block.
        h.on_boundary(&boundary(1, 1, 0));
        assert_eq!(h.latest(), 2);
    }

    /// An executor streams a receipt at execution, and publishes the
    /// boundary only when the block is durable. So the receipt of block 3
    /// comes before the boundary of block 2, an empty block. The boundary
    /// of block 2 must close block 2, not block 3.
    #[test]
    fn a_boundary_after_a_newer_receipt_closes_its_own_block() {
        let h = FeeHistory::default();
        h.on_boundary(&boundary(1, 1_000, 0));
        h.on_receipt(3, 4);
        h.on_boundary(&boundary(2, 875, 0));
        assert_eq!(h.latest(), 2);
        assert_eq!(h.query(1, 2, &[]).unwrap().base_fee_per_gas, vec![875, 766]);
        // Block 3 is open until its own boundary comes.
        assert!(h.query(1, 3, &[]).is_none());
        h.on_boundary(&boundary(3, 766, 21_000));
        let history = h.query(3, 3, &[50.0]).unwrap();
        assert_eq!(history.oldest_block, 1);
        assert_eq!(history.base_fee_per_gas, vec![1_000, 875, 766, 671]);
        assert_eq!(history.reward, Some(vec![vec![0], vec![0], vec![4]]));
    }

    /// A late block older than a full ring does not displace a newer one.
    #[test]
    fn a_late_block_older_than_a_full_ring_is_dropped() {
        let h = FeeHistory::default();
        for n in 2..=u64::try_from(CAPACITY + 1).unwrap() {
            h.on_boundary(&boundary(n, 1, 0));
        }
        h.on_boundary(&boundary(1, 1, 0));
        assert!(h.query(1, 1, &[]).is_none());
        assert_eq!(h.query(1, 2, &[]).unwrap().oldest_block, 2);
    }

    #[test]
    fn the_ring_drops_the_oldest_block() {
        let h = FeeHistory::default();
        for n in 1..=u64::try_from(CAPACITY + 5).unwrap() {
            h.on_boundary(&boundary(n, 1, 0));
        }
        assert!(h.query(1, 5, &[]).is_none());
        assert_eq!(h.query(1, 6, &[]).unwrap().oldest_block, 6);
    }
}
