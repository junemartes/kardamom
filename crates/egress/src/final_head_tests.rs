use std::ops::ControlFlow;

use super::{Output, Walk};

/// The walk over `outputs` from index 0, as one poll takes them.
fn walk(outputs: &[Output]) -> Walk {
    let mut walk = Walk::default();
    let _ = outputs.iter().try_for_each(|&output| walk.take(output));
    walk
}

#[test]
fn the_walk_stops_at_the_first_open_output() {
    let w = walk(&[
        Output::Final { block: 10 },
        Output::Final { block: 20 },
        Output::Open,
        Output::Final { block: 40 },
    ]);
    assert_eq!((w.next, w.head), (2, Some(20)));
}

#[test]
fn a_deleted_output_neither_stops_the_walk_nor_moves_the_head() {
    let w = walk(&[
        Output::Final { block: 10 },
        Output::Deleted,
        Output::Final { block: 20 },
        Output::Deleted,
    ]);
    assert_eq!((w.next, w.head), (4, Some(20)));
}

#[test]
fn with_no_final_output_the_walk_has_no_head() {
    let w = walk(&[Output::Deleted, Output::Open]);
    assert_eq!((w.next, w.head), (1, None));
}

#[test]
fn the_next_poll_resumes_at_the_open_output() {
    let mut w = walk(&[Output::Final { block: 10 }, Output::Open]);
    assert_eq!(
        w.take(Output::Final { block: 20 }),
        ControlFlow::Continue(())
    );
    assert_eq!((w.next, w.head), (2, Some(20)));
}
