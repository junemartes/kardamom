//! Property test for PartitionState::process.
//! Published nonces for each sender form a strictly ascending, dense run
//! that starts at 0. This is true for any shuffle of `(sender, nonce)` pairs.

use std::collections::HashMap;

use alloy_primitives::Address;
use proptest::prelude::*;

use kardamom_sequencer::state::{PartitionState, ProcessAction};

fn addr(i: u8) -> Address {
    Address::repeat_byte(i)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn published_nonces_per_sender_are_ascending_and_dense(
        seq in proptest::collection::vec((0u8..4u8, 0u64..16u64), 0..200),
    ) {
        let mut st: PartitionState<u64> = PartitionState::new(16);
        let mut per_sender_published: HashMap<Address, Vec<u64>> = HashMap::new();
        for (sidx, nonce) in seq {
            let r = st.process(addr(sidx), nonce, nonce);
            for action in r.actions {
                if let ProcessAction::Publish { nonce: n, .. } = action {
                    per_sender_published.entry(addr(sidx)).or_default().push(n);
                }
            }
        }
        for (s, ns) in per_sender_published {
            // Strictly ascending.
            for w in ns.windows(2) {
                prop_assert!(w[1] > w[0], "sender {}: nonces {:?} not ascending", s, ns);
            }
            // Dense starting at 0.
            if !ns.is_empty() {
                prop_assert_eq!(ns[0], 0, "sender {}: must start at 0", s);
                for (i, n) in ns.iter().enumerate() {
                    prop_assert_eq!(*n, i as u64, "sender {}: gap at idx {} ({:?})", s, i, ns);
                }
            }
        }
    }
}
