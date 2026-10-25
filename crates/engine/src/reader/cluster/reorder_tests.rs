//! The head gauge of [`Reorder`]: the boundaries that it delivers and the
//! head of each `REPLAY_DONE`.

use kardamom_types::{BPosition, BlockBoundaryStart};

use super::{BoundaryMetrics, Reorder};
use crate::meter::tests::{render, value};
use crate::metrics::{SEALER_BLOCK_NUMBER, SEALER_BOUNDARIES_TOTAL, VALIDATOR_SEALER_BLOCK_NUMBER};
use crate::reader::cluster::ReplayCursor;

/// A reorder in catch-up mode at the cursor `(next_index, next_block)`,
/// with the gauges of `metrics`.
fn reorder(metrics: BoundaryMetrics, next_index: u64, next_block: u64) -> Reorder {
    let mut reorder = Reorder::new(ReplayCursor::new(next_index, next_block));
    reorder.metrics = metrics;
    reorder.restart();
    reorder
}

/// An empty block `block` that closes at the canonical index `end`.
fn boundary(block: u64, end: u64) -> BlockBoundaryStart {
    BlockBoundaryStart {
        block_number: block,
        end_tx_idx: BPosition::from_index(end),
        l2_timestamp: 0,
        l1_origin: 0,
    }
}

/// Deliver every item that `reorder` can deliver now.
fn drain(reorder: &mut Reorder) -> usize {
    std::iter::from_fn(|| reorder.try_deliver().unwrap()).count()
}

/// Does `text` hold a series of `name`?
fn exports(text: &str, name: &str) -> bool {
    text.lines().any(|line| line.starts_with(name))
}

#[test]
fn a_reader_at_the_head_exports_the_head_with_no_boundary() {
    let text = render(|| {
        let mut reorder = reorder(BoundaryMetrics::Validator, 5, 3);
        reorder.end_replay(5, 3).unwrap();
        assert_eq!(drain(&mut reorder), 0, "nothing to deliver");
    });
    assert_eq!(value(&text, VALIDATOR_SEALER_BLOCK_NUMBER), 2.0);
    assert!(
        !exports(&text, SEALER_BLOCK_NUMBER),
        "the validator role exports no sealer series"
    );
}

#[test]
fn a_reader_below_the_head_shows_the_head_of_the_replay_done() {
    let text = render(|| {
        let mut reorder = reorder(BoundaryMetrics::Validator, 0, 1);
        reorder.ingest_boundary(boundary(1, 0)).unwrap();
        reorder.ingest_boundary(boundary(2, 0)).unwrap();
        reorder.end_replay(0, 9).unwrap();
        assert_eq!(drain(&mut reorder), 2, "b1 and b2");
    });
    assert_eq!(
        value(&text, VALIDATOR_SEALER_BLOCK_NUMBER),
        8.0,
        "a delivered boundary below the head does not move the gauge down"
    );
}

#[test]
fn a_delivered_boundary_above_the_head_moves_the_gauge_up() {
    let text = render(|| {
        let mut reorder = reorder(BoundaryMetrics::Validator, 0, 1);
        reorder.end_replay(0, 1).unwrap();
        reorder.ingest_boundary(boundary(1, 0)).unwrap();
        assert_eq!(drain(&mut reorder), 1);
    });
    assert_eq!(value(&text, VALIDATOR_SEALER_BLOCK_NUMBER), 1.0);
}

#[test]
fn a_replay_done_with_no_sealed_block_exports_no_head() {
    let text = render(|| {
        let mut reorder = reorder(BoundaryMetrics::Validator, 0, 0);
        reorder.end_replay(0, 0).unwrap();
    });
    assert!(!exports(&text, VALIDATOR_SEALER_BLOCK_NUMBER));
}

#[test]
fn the_sealer_role_counts_the_boundaries_and_not_the_replay_end() {
    let text = render(|| {
        let mut reorder = reorder(BoundaryMetrics::Sealer, 0, 1);
        reorder.end_replay(0, 1).unwrap();
        reorder.ingest_boundary(boundary(1, 0)).unwrap();
        assert_eq!(drain(&mut reorder), 1);
    });
    assert_eq!(value(&text, SEALER_BLOCK_NUMBER), 1.0);
    assert_eq!(value(&text, SEALER_BOUNDARIES_TOTAL), 1.0);
    assert!(!exports(&text, VALIDATOR_SEALER_BLOCK_NUMBER));
}

#[test]
fn a_reorder_with_no_role_exports_nothing() {
    let text = render(|| {
        let mut reorder = reorder(BoundaryMetrics::Off, 0, 1);
        reorder.ingest_boundary(boundary(1, 0)).unwrap();
        reorder.end_replay(0, 4).unwrap();
        assert_eq!(drain(&mut reorder), 1);
    });
    assert!(!exports(&text, SEALER_BLOCK_NUMBER));
    assert!(!exports(&text, VALIDATOR_SEALER_BLOCK_NUMBER));
}
