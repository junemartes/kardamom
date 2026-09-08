//! Property test for `PartitionState::process`.
//! Published nonces for each sender form a strictly ascending, dense run
//! that starts at 0. This is true for any shuffle of `(sender, nonce)`
//! pairs.

use std::collections::HashMap;

use alloy_primitives::Address;
use proptest::prelude::*;

use super::{PartitionState, ProcessAction};

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
            per_sender_published.entry(addr(sidx)).or_default().extend(
                r.actions.into_iter().filter_map(|action| match action {
                    ProcessAction::Publish { nonce: n, .. } => Some(n),
                    ProcessAction::ReportDuplicate { .. } => None,
                }),
            );
        }
        for (s, ns) in per_sender_published {
            // Strictly ascending.
            prop_assert!(
                ns.windows(2).all(|w| w[1] > w[0]),
                "sender {}: nonces {:?} not ascending",
                s,
                ns
            );
            // Dense starting at 0.
            if !ns.is_empty() {
                prop_assert_eq!(ns[0], 0, "sender {}: must start at 0", s);
                prop_assert!(
                    ns.iter().enumerate().all(|(i, n)| *n == i as u64),
                    "sender {}: not dense from 0: {:?}",
                    s,
                    ns
                );
            }
        }
    }
}
