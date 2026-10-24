use super::DropReason;
use crate::tx_error::TxErrorReason;

/// Each rejection maps to the drop reason with the geth error text.
#[test]
fn every_rejection_has_the_geth_text() {
    let cases = [
        (
            TxErrorReason::DuplicatedTx { expected_nonce: 3 },
            "nonce too low",
        ),
        (
            TxErrorReason::SealerNonceAhead {
                expected_nonce: 9,
                chain_nonce: 4,
            },
            "nonce too low",
        ),
        (
            TxErrorReason::InsufficientFunds { have: 1, want: 2 },
            "insufficient funds for gas * price + value",
        ),
        (
            TxErrorReason::AlreadyKnown { expected_nonce: 3 },
            "replacement transaction underpriced",
        ),
        (
            TxErrorReason::NonceInFlight { chain_nonce: 3 },
            "replacement transaction underpriced",
        ),
        (
            TxErrorReason::Evicted { expected_nonce: 3 },
            "txpool is full",
        ),
        (
            TxErrorReason::DaLag {
                sealed_head: 9,
                posted_head: 1,
                budget_blocks: 4,
            },
            "txpool is full",
        ),
        (
            TxErrorReason::RecordLag {
                sealed_index: 9,
                recorded_index: 1,
                budget: 4,
            },
            "txpool is full",
        ),
        (
            TxErrorReason::FeeTooLow {
                max_fee_per_gas: 1,
                base_fee: 2,
            },
            "max fee per gas less than block base fee",
        ),
        (
            TxErrorReason::Expired { expected_nonce: 3 },
            "transaction expired",
        ),
        (
            TxErrorReason::PastDeadline {
                max_inclusion_block: 4,
                at_block: 5,
            },
            "transaction expired",
        ),
        (
            TxErrorReason::FeeInvalid {
                max_fee_per_gas: 1,
                max_priority_fee_per_gas: 2,
            },
            "max priority fee per gas higher than max fee per gas",
        ),
    ];
    let texts: Vec<_> = cases
        .iter()
        .map(|(reason, _)| reason.drop_reason().text())
        .collect();
    let want: Vec<_> = cases.iter().map(|(_, text)| *text).collect();
    assert_eq!(texts, want);
}

#[test]
fn the_reason_bytes_keep_their_order() {
    let reasons = [
        DropReason::NonceTooLow,
        DropReason::InsufficientFunds,
        DropReason::ReplacementUnderpriced,
        DropReason::TxpoolFull,
        DropReason::FeeCapBelowBaseFee,
        DropReason::Expired,
        DropReason::TipAboveFeeCap,
    ];
    let bytes: Vec<u8> = reasons
        .iter()
        .map(|r| rkyv::to_bytes::<rkyv::rancor::Error>(r).unwrap()[0])
        .collect();
    assert_eq!(bytes, [0, 1, 2, 3, 4, 5, 6]);
}
