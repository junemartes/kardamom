use super::*;

/// Only a cluster run whose cursor the sealer refused reads the egress
/// next, and only with an egress.
#[test]
fn only_a_refused_cluster_cursor_reads_the_egress_next() {
    let refused = ExecutorError::ClusterReplayUnavailable {
        from_index: 10,
        oldest_index: 500,
        oldest_block: 7,
    };
    let restarted = ExecutorError::SourceRestarted {
        next_index: 3,
        next_block: 2,
    };
    assert!(Source::Cluster.egress_next(Some(&refused), true));
    assert!(
        !Source::Cluster.egress_next(Some(&refused), false),
        "no egress: a peer checkpoint"
    );
    assert!(
        !Source::Egress.egress_next(Some(&refused), true),
        "the egress refused the cursor too: a peer checkpoint"
    );
    assert!(!Source::Cluster.egress_next(Some(&restarted), true));
    assert!(!Source::Cluster.egress_next(None, true));
}

/// A run of the egress and a carry run read the bytes, so they open no
/// `tx_data` lane.
#[test]
fn a_run_that_reads_the_bytes_opens_no_lane() {
    assert!(!Source::Cluster.carries(false));
    assert!(Source::Cluster.carries(true));
    assert!(Source::Egress.carries(false));
}
