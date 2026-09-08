use super::{ExecutorError, TxIndex, U256, fee_delta_from_sink};

#[test]
fn observed_at_or_above_start_credits_the_difference() {
    let start = U256::from(1_000u64);
    let observed = U256::from(1_021u64);
    assert_eq!(
        fee_delta_from_sink(observed, start, 1, TxIndex(0)).unwrap(),
        U256::from(21u64)
    );
    // Untouched this block: observed equals the block-start value.
    assert_eq!(
        fee_delta_from_sink(start, start, 1, TxIndex(0)).unwrap(),
        U256::ZERO
    );
}

#[test]
fn observed_below_start_errors_not_wraps() {
    let start = U256::from(1_000u64);
    let observed = U256::from(999u64);
    let Err(ExecutorError::State(msg)) = fee_delta_from_sink(observed, start, 7, TxIndex(3)) else {
        panic!("a sink balance below the block-start value must error, not wrap");
    };
    assert!(msg.contains("block-start=1000"), "{msg}");
    assert!(msg.contains("observed=999"), "{msg}");
}
