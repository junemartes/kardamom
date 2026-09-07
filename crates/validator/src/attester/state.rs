//! Pure attestation state: the output-root computation and the
//! cadence/leaf-accumulation state machine. Both are unit-tested in
//! isolation, with no L1 dependency.

use std::collections::BTreeMap;
use std::num::NonZeroU64;

use alloy_primitives::B256;
use kardamom_types::withdrawals;

/// The committed output for a block range: its withdrawals root and the
/// output root posted to L1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Output {
    state_root: B256,
    withdrawals_root: B256,
    pub output_root: B256,
}

/// Build the output root from the committed `state_root` and the range's
/// withdrawal `leaves`.
#[must_use]
pub(crate) fn build_output(state_root: B256, leaves: &[B256]) -> Output {
    let withdrawals_root = withdrawals::withdrawals_root(leaves);
    let output_root = withdrawals::output_root(state_root, withdrawals_root);
    Output {
        state_root,
        withdrawals_root,
        output_root,
    }
}

/// Pure attestation state: pending per-block withdrawal leaves and the
/// posting cadence. The [`spawn_attester`](super::spawn_attester) loop
/// drives it; it is unit-tested in isolation.
///
/// Leaves survive failed posts. They are cleared only by
/// [`mark_attested`](Self::mark_attested), which is what carries a
/// challenged or failed range's withdrawals forward into the next
/// successful output.
#[derive(Debug)]
pub(crate) struct AttestState {
    /// Maps a block number to that block's withdrawal leaves (nonce-ordered).
    pending: BTreeMap<u64, Vec<B256>>,
    /// Highest L2 block covered by a successfully posted, non-deleted output.
    last_attested: u64,
    /// Post cadence in blocks.
    interval: NonZeroU64,
    /// Lowest block this process attested. Set on its first successful post.
    ///
    /// This is a diagnostic value only. It tells an expected
    /// re-collection (a replayed receipt stream re-delivering blocks an
    /// earlier run already attested, everything below this floor) apart
    /// from an impossible one (at or above it, where the completeness gate
    /// should have held). See [`on_leaves`](Self::on_leaves).
    own_attest_floor: Option<u64>,
    /// Highest block whose receipts are known complete: whose
    /// `BlockBoundary` this process has seen on the `tx_receipts` stream. A
    /// block's state root cannot be attested until this reaches it, or the
    /// output could go out before the block's withdrawals were known.
    receipts_through: u64,
    /// State roots waiting for `receipts_through` to reach their block.
    roots: BTreeMap<u64, B256>,
}

impl AttestState {
    #[must_use]
    pub fn new(last_attested: u64, interval: NonZeroU64) -> Self {
        Self {
            pending: BTreeMap::new(),
            last_attested,
            interval,
            own_attest_floor: None,
            receipts_through: 0,
            roots: BTreeMap::new(),
        }
    }

    /// Buffer a committed block's state root. It becomes attestable only
    /// once the receipt stream confirms that block's receipts are
    /// complete. See [`next_attestable`](Self::next_attestable).
    pub fn on_root(&mut self, block: u64, state_root: B256) {
        if block > self.last_attested {
            self.roots.insert(block, state_root);
        }
    }

    /// The block to attest now, if any: the highest buffered root whose
    /// receipts are complete and whose block meets the post cadence.
    /// Attesting the highest block rather than each in turn is safe and
    /// cheaper. An output at block B commits to every withdrawal through
    /// B, and this keeps one L1 transaction per catch-up burst.
    #[must_use]
    pub fn next_attestable(&self) -> Option<(u64, B256)> {
        self.roots
            .range(..=self.receipts_through)
            .next_back()
            .filter(|(block, _)| self.due(**block))
            .map(|(block, root)| (*block, *root))
    }

    /// Record a committed block's withdrawal leaves. Every submission,
    /// even an empty one, marks that block's receipts complete, which is
    /// what releases its state root for attestation.
    ///
    /// Leaves for a block already attested fall into one of two cases,
    /// separated by [`own_attest_floor`](Self::own_attest_floor):
    ///
    /// - Below our floor: expected. After a restart, the attester resumes
    ///   at the oracle's attested height, while the validator replays the
    ///   receipt stream from its own, possibly older, cursor. So it
    ///   re-collects leaves for blocks an earlier run already covered.
    ///   Those are redundant and are dropped; re-adding them would
    ///   double-count them into a later output's tree.
    /// - At or above our floor: a bug. A block is attested only after its
    ///   receipts are complete, so leaves for it cannot still be in
    ///   flight. Reaching here means that gate failed. Report it clearly,
    ///   and still keep the leaves, because the alternative is silently
    ///   stranding a user's withdrawal on top of the defect.
    pub fn on_leaves(&mut self, block: u64, leaves: Vec<B256>) {
        self.receipts_through = self.receipts_through.max(block);
        if leaves.is_empty() {
            return;
        }
        if block > self.last_attested {
            self.pending.insert(block, leaves);
            return;
        }
        if self.own_attest_floor.is_none_or(|floor| block < floor) {
            return;
        }
        let carry = self.last_attested.saturating_add(1);
        tracing::error!(
            block,
            last_attested = self.last_attested,
            receipts_through = self.receipts_through,
            carried_to = carry,
            count = leaves.len(),
            "BUG: withdrawal leaves arrived for a block this process already attested — a block \
             must not be attested until its receipts are complete. Carrying them into the next \
             output so the withdrawal is not stranded, but the completeness gate is broken"
        );
        self.pending.entry(carry).or_default().extend(leaves);
    }

    /// True once `block`'s state root warrants a new output.
    #[must_use]
    pub fn due(&self, block: u64) -> bool {
        block >= self.last_attested.saturating_add(self.interval.get())
    }

    /// All pending leaves for blocks `<= block`, joined in block order,
    /// which is the same as the global withdrawal-nonce order, the tree's
    /// canonical leaf order. This does not clear the leaves; only
    /// [`mark_attested`] does that, after a successful post, so a failed
    /// post retries with the same accumulated set.
    #[must_use]
    pub fn leaves_through(&self, block: u64) -> Vec<B256> {
        self.pending
            .range(..=block)
            .flat_map(|(_, l)| l.iter().copied())
            .collect()
    }

    /// A successful post covered everything up to `block`.
    pub fn mark_attested(&mut self, block: u64) {
        // Record where this process's own coverage begins, before the
        // floor moves. See `own_attest_floor`.
        self.own_attest_floor
            .get_or_insert(self.last_attested.saturating_add(1));
        self.last_attested = block;
        // A wrap here (`block == u64::MAX`) would flush nothing instead of
        // clearing the just-attested range, leaking the pending map.
        let next = block.saturating_add(1);
        self.pending = self.pending.split_off(&next);
        self.roots = self.roots.split_off(&next);
    }

    #[must_use]
    pub fn last_attested(&self) -> u64 {
        self.last_attested
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::U256;

    fn nz(n: u64) -> NonZeroU64 {
        NonZeroU64::new(n).expect("fixture interval")
    }

    #[test]
    fn attest_state_accumulates_and_clears_only_on_success() {
        let l = |n: u64| B256::from(U256::from(n));
        let mut st = AttestState::new(0, nz(4));
        st.on_leaves(1, vec![l(1)]);
        st.on_leaves(2, vec![]); // Empty blocks are not stored.
        st.on_leaves(3, vec![l(2), l(3)]);
        assert!(!st.due(3));
        assert!(st.due(4));

        // Post attempt at block 4: joined in block order, which is nonce order.
        assert_eq!(st.leaves_through(4), vec![l(1), l(2), l(3)]);
        // Simulate a post failure: nothing is cleared, so the same set is
        // retried. A failed or challenged range's withdrawals carry into
        // the next attempt instead of being dropped.
        assert_eq!(st.leaves_through(4), vec![l(1), l(2), l(3)]);

        st.on_leaves(5, vec![l(4)]);
        // Success at block 8 covers everything through 8.
        assert_eq!(st.leaves_through(8), vec![l(1), l(2), l(3), l(4)]);
        st.mark_attested(8);
        assert_eq!(st.last_attested(), 8);
        assert!(st.leaves_through(8).is_empty());
        assert!(!st.due(11));
        assert!(st.due(12));

        // Leaves for a block this process attested cannot happen once the
        // completeness gate holds. If it ever happens, they are kept
        // rather than silently stranding the withdrawal.
        st.on_leaves(7, vec![l(9)]);
        assert_eq!(st.leaves_through(12), vec![l(9)]);
    }

    /// This is a safeguard for a state the completeness gate makes
    /// unreachable. If leaves for an already-attested block ever did
    /// arrive, stranding a user's withdrawal would be the worst response,
    /// so the code keeps them and reports the problem loudly; see
    /// `on_leaves`.
    #[test]
    fn leaves_for_an_already_attested_block_are_kept_not_dropped() {
        let l = |n: u64| B256::from(U256::from(n));
        let mut st = AttestState::new(0, nz(1));

        // Root for block 4 arrives first: posted with nothing.
        assert!(st.leaves_through(4).is_empty());
        st.mark_attested(4);

        // Block 4's leaves show up late. They belong to an output already
        // posted without them, so the next output must cover them.
        st.on_leaves(4, vec![l(1)]);
        assert_eq!(st.leaves_through(5), vec![l(1)]);
        st.mark_attested(5);
        assert!(st.leaves_through(9).is_empty());
    }

    /// This is the invariant the whole design rests on: a block's root is
    /// not attestable until the receipt stream confirms that block's
    /// receipts are complete. Without this rule, the output could go out
    /// before the block's withdrawals are known, leaving them with no
    /// output to prove against.
    #[test]
    fn a_root_is_not_attestable_until_its_receipts_are_complete() {
        let l = |n: u64| B256::from(U256::from(n));
        let root = B256::repeat_byte(7);
        let mut st = AttestState::new(0, nz(1));

        // The root arrives first: nothing to attest yet, receipts are unknown.
        st.on_root(4, root);
        assert_eq!(st.next_attestable(), None, "root must wait for receipts");

        // Receipts for an earlier block do not release it.
        st.on_leaves(3, vec![]);
        assert_eq!(st.next_attestable(), None);

        // Block 4's own boundary, carrying its withdrawal, releases it,
        // and the output that goes out now covers that withdrawal.
        st.on_leaves(4, vec![l(1)]);
        assert_eq!(st.next_attestable(), Some((4, root)));
        assert_eq!(st.leaves_through(4), vec![l(1)]);
    }

    /// A quiet block must still release its root. "No withdrawals here" is
    /// a fact the attester needs, so an empty submission must count as
    /// completeness, not be ignored.
    #[test]
    fn an_empty_boundary_still_releases_its_root() {
        let root = B256::repeat_byte(9);
        let mut st = AttestState::new(0, nz(1));
        st.on_root(6, root);
        st.on_leaves(6, vec![]);
        assert_eq!(st.next_attestable(), Some((6, root)));
    }

    /// Catch-up: several roots buffered behind a lagging receipt stream
    /// collapse into one output at the highest complete block, which still
    /// covers every withdrawal below it.
    #[test]
    fn buffered_roots_collapse_into_one_output_on_catch_up() {
        let l = |n: u64| B256::from(U256::from(n));
        let mut st = AttestState::new(0, nz(1));
        st.on_root(1, B256::repeat_byte(1));
        st.on_root(2, B256::repeat_byte(2));
        st.on_root(3, B256::repeat_byte(3));
        assert_eq!(st.next_attestable(), None);

        st.on_leaves(1, vec![l(1)]);
        st.on_leaves(2, vec![]);
        assert_eq!(st.next_attestable(), Some((2, B256::repeat_byte(2))));
        assert_eq!(st.leaves_through(2), vec![l(1)]);

        st.mark_attested(2);
        // Block 3's root stays buffered and becomes attestable when its
        // own receipts land.
        assert_eq!(st.next_attestable(), None);
        st.on_leaves(3, vec![l(2)]);
        assert_eq!(st.next_attestable(), Some((3, B256::repeat_byte(3))));
    }

    /// This is the case the drop exists for, and it must still drop:
    /// leaves re-collected for blocks covered before this process started
    /// (the oracle resume point, or a replayed receipt stream). Carrying
    /// those forward would double-count them into a later output's
    /// withdrawals tree.
    #[test]
    fn re_collected_leaves_below_our_own_floor_stay_dropped() {
        let l = |n: u64| B256::from(U256::from(n));
        // Resuming: the oracle says blocks through 10 are already attested.
        let mut st = AttestState::new(10, nz(1));
        st.on_leaves(6, vec![l(1)]);
        assert!(st.leaves_through(20).is_empty(), "no own floor yet ⇒ drop");

        // Attest block 12; our own coverage starts at 11.
        st.on_leaves(12, vec![l(2)]);
        st.mark_attested(12);
        // Still below our floor, so still dropped.
        st.on_leaves(9, vec![l(3)]);
        assert!(st.leaves_through(20).is_empty());
        // At or above our floor, so carried.
        st.on_leaves(11, vec![l(4)]);
        assert_eq!(st.leaves_through(20), vec![l(4)]);
    }

    #[test]
    fn leaves_through_excludes_blocks_past_the_output_boundary() {
        let l = |n: u64| B256::from(U256::from(n));
        let mut st = AttestState::new(0, nz(1));
        st.on_leaves(1, vec![l(1)]);
        st.on_leaves(2, vec![l(2)]);
        // The output for block 1 must not swallow block 2's leaves.
        assert_eq!(st.leaves_through(1), vec![l(1)]);
        st.mark_attested(1);
        assert_eq!(st.leaves_through(2), vec![l(2)]);
    }

    #[test]
    fn build_output_matches_types_helpers() {
        let sr = B256::from([0xab; 32]);
        let s = alloy_primitives::Address::from([0x11; 20]);
        let t = alloy_primitives::Address::from([0x22; 20]);
        let leaves = vec![withdrawals::withdrawal_leaf(
            U256::ZERO,
            s,
            t,
            U256::from(5u64),
        )];
        let out = build_output(sr, &leaves);
        assert_eq!(out.withdrawals_root, withdrawals::withdrawals_root(&leaves));
        assert_eq!(
            out.output_root,
            withdrawals::output_root(sr, out.withdrawals_root)
        );
    }
}
