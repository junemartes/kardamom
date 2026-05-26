//! Bootstrap the local `block_number` counter from B's tail.
//!
//! On startup the sealer subscribes to the boundary stream on tx_ordering and
//! drains every `BlockBoundaryStart` already in the recording. The largest
//! `block_number` it sees + 1 becomes the local counter's initial value. If
//! the tail is empty (genesis), `block_number` starts at 1.
//!
//! ## Why a forward scan
//!
//! Aeron streams are append-only and can only be replayed forward. The
//! kardamom-log `testing::FakeTypedSubscription` mirrors that surface (cursor
//! advances; no random access). The sealer's bootstrap therefore drains the
//! recording from the earliest available fragment forward, tracking the max
//! `block_number` it observes. For a long-lived recording the Aeron Archive
//! exposes `replay_range`; the sealer can use that to skip ahead by a fixed
//! lookback (e.g. 30 s of boundaries = 120 markers at 250 ms cadence).
//!
//! The pure helper [`max_block_number_from_iter`] is what the unit tests
//! exercise; the supervisor's bootstrap path uses the same helper after
//! draining the live subscription.

use types::BlockBoundaryStart;

/// Iterator-based helper. Returns the next `block_number` the local emitter
/// should use:
///   - `max(block_number observed) + 1` if any boundary was seen, or
///   - `1` (genesis) otherwise.
pub fn next_block_number_from_iter<I>(boundaries: I) -> u64
where
    I: IntoIterator<Item = BlockBoundaryStart>,
{
    let max = boundaries.into_iter().map(|b| b.block_number).max();
    max.map_or(1, |n| n + 1)
}

/// Same as [`next_block_number_from_iter`] but takes the max directly. Useful
/// when the caller has been streaming boundaries and just wants the
/// next-emitter value.
pub fn next_after(max_seen: Option<u64>) -> u64 {
    max_seen.map_or(1, |n| n + 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use types::BPosition;

    fn bs(n: u64) -> BlockBoundaryStart {
        BlockBoundaryStart {
            block_number: n,
            end_tx_idx: BPosition {
                term_id: 0,
                term_offset: 0,
            },
            l2_timestamp: 0,
        }
    }

    #[test]
    fn empty_tail_returns_genesis() {
        assert_eq!(next_block_number_from_iter(std::iter::empty()), 1);
    }

    #[test]
    fn picks_max_block_plus_one() {
        let scanned = vec![bs(7), bs(8), bs(5)];
        assert_eq!(next_block_number_from_iter(scanned), 9);
    }

    #[test]
    fn out_of_order_boundaries_still_correct() {
        // A later subscriber could observe boundaries in any (per-publisher)
        // order if multiple sealers raced during a leadership flap. The
        // bootstrap helper only cares about the max.
        let scanned = vec![bs(100), bs(99), bs(101), bs(50)];
        assert_eq!(next_block_number_from_iter(scanned), 102);
    }

    #[test]
    fn next_after_helpers_agree() {
        assert_eq!(next_after(None), 1);
        assert_eq!(next_after(Some(0)), 1);
        assert_eq!(next_after(Some(42)), 43);
    }
}
