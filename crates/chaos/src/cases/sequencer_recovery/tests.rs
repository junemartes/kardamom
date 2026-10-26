use super::Replica;

#[test]
fn every_lane_and_node_has_an_isolated_recovery_proof() {
    let covered: std::collections::BTreeSet<_> = Replica::ALL
        .iter()
        .map(|replica| (replica.lane, replica.node))
        .collect();
    assert_eq!(covered, [(0, 0), (0, 1), (1, 0), (1, 1)].into());
}

#[test]
fn recovery_senders_are_reserved_and_belong_to_the_faulted_lane() {
    for replica in Replica::ALL {
        replica.assert_route();
    }
}

impl Replica {
    fn assert_route(self) {
        assert!((1..=6).contains(&self.account));
        assert_eq!(
            crate::accounts::ACCT_SHARD[self.account as usize],
            self.lane
        );
        assert_ne!(self.node, self.twin);
        assert_eq!(
            [self.node, self.twin]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            [0, 1].into()
        );
    }
}
