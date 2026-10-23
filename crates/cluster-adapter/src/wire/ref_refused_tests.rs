//! The refusal of a transaction reference (egress kind 18) on the golden
//! frame that the sealer tests also read.

use super::entry_tests::{Fixture, golden_frame};
use super::*;

#[test]
fn the_golden_refusal_decodes() {
    let frame = golden_frame("ref_refused");
    assert_eq!(frame.len(), REF_REFUSED_LEN);
    assert_eq!(REF_REFUSED_LEN, 62);
    assert_eq!(
        EgressItem::decode(&frame).unwrap(),
        EgressItem::RefRefused(Fixture::ref_refused())
    );
}

#[test]
fn a_longer_refusal_reads_the_same() {
    let mut frame = golden_frame("ref_refused");
    frame.push(0x42);
    assert_eq!(
        EgressItem::decode(&frame).unwrap(),
        EgressItem::RefRefused(Fixture::ref_refused())
    );
}

#[test]
fn a_short_refusal_is_refused() {
    let frame = golden_frame("ref_refused");
    assert!(matches!(
        EgressItem::decode(&frame[..REF_REFUSED_LEN - 1]),
        Err(WireError::TooShort { .. })
    ));
}
