use std::num::NonZeroU64;

use super::L1Heads;

const KEEP: NonZeroU64 = NonZeroU64::new(10).unwrap();

#[test]
fn with_no_head_read_the_store_keeps_every_block() {
    assert_eq!(L1Heads::posted_only().keep_from(1_000, KEEP), 1);
    let mut heads = L1Heads::with_oracle();
    heads.post(500);
    assert_eq!(heads.keep_from(1_000, KEEP), 1, "no output is final yet");
}

#[test]
fn with_no_oracle_the_floor_is_the_posted_head() {
    let mut heads = L1Heads::posted_only();
    heads.post(500);
    heads.finalize(900);
    assert_eq!(heads.floor(), 500);
    assert_eq!(heads.keep_from(1_000, KEEP), 501);
}

#[test]
fn the_floor_is_the_lower_head() {
    let mut heads = L1Heads::with_oracle();
    heads.post(500);
    heads.finalize(300);
    assert_eq!(heads.floor(), 300);
    heads.finalize(700);
    assert_eq!(heads.floor(), 500);
}

#[test]
fn an_older_head_changes_nothing() {
    let mut heads = L1Heads::with_oracle();
    heads.post(500);
    heads.finalize(400);
    heads.post(200);
    heads.finalize(100);
    assert_eq!((heads.posted, heads.final_head), (500, Some(400)));
}

#[test]
fn the_window_below_the_head_stays_when_the_floor_is_above_it() {
    let mut heads = L1Heads::posted_only();
    heads.post(995);
    assert_eq!(heads.keep_from(1_000, KEEP), 990);
    heads.post(u64::MAX);
    assert_eq!(heads.keep_from(1_000, KEEP), 990);
    assert_eq!(
        heads.keep_from(5, KEEP),
        0,
        "the window is longer than the chain"
    );
}
