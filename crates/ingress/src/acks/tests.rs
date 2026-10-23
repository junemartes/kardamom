use std::num::NonZeroU8;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use kardamom_types::{TxError, TxErrorReason, TxStatus};

use super::AckWaiters;
use crate::config::AdmissionAck;
use crate::error::IngressError;

const HASH: B256 = B256::repeat_byte(7);
const SENDER: Address = Address::repeat_byte(1);

fn waiters() -> std::sync::Arc<AckWaiters> {
    AckWaiters::new(AdmissionAck {
        timeout: Duration::from_secs(2),
        reject_grace: Duration::from_millis(250),
        twins: NonZeroU8::new(2).unwrap(),
    })
}

fn accepted() -> TxStatus {
    TxStatus::accepted(HASH, SENDER, 5)
}

fn rejected(reason: TxErrorReason) -> TxStatus {
    TxStatus::rejected(
        HASH,
        &TxError {
            sender: SENDER,
            nonce: 5,
            reason,
        },
    )
}

#[tokio::test(start_paused = true)]
async fn an_accept_answers_with_the_hash() {
    let w = waiters();
    let wait = w.register(HASH).unwrap();
    w.on_verdict(accepted());
    assert_eq!(wait.answer().await.unwrap(), HASH);
    assert_eq!(w.len(), 0, "the wait removes its slot");
}

#[tokio::test(start_paused = true)]
async fn an_accept_of_the_other_twin_wins_over_a_rejection() {
    let w = waiters();
    let wait = w.register(HASH).unwrap();
    w.on_verdict(rejected(TxErrorReason::DuplicatedTx { expected_nonce: 6 }));
    w.on_verdict(accepted());
    assert_eq!(wait.answer().await.unwrap(), HASH);
}

#[tokio::test(start_paused = true)]
async fn one_rejection_answers_after_the_grace() {
    let w = waiters();
    let wait = w.register(HASH).unwrap();
    w.on_verdict(rejected(TxErrorReason::DuplicatedTx { expected_nonce: 6 }));
    let start = tokio::time::Instant::now();
    let err = wait.answer().await.unwrap_err();
    assert_eq!(start.elapsed(), Duration::from_millis(250));
    assert_eq!(
        err.to_string(),
        format!("nonce too low: address {SENDER}, tx: 5 state: 6"),
        "the text of geth"
    );
}

#[tokio::test(start_paused = true)]
async fn a_rejection_of_every_twin_answers_at_once() {
    let w = waiters();
    let wait = w.register(HASH).unwrap();
    w.on_verdict(rejected(TxErrorReason::Evicted { expected_nonce: 5 }));
    w.on_verdict(rejected(TxErrorReason::Evicted { expected_nonce: 5 }));
    let start = tokio::time::Instant::now();
    let err = wait.answer().await.unwrap_err();
    assert_eq!(start.elapsed(), Duration::ZERO);
    assert!(matches!(err, IngressError::PoolFull), "{err}");
}

#[tokio::test(start_paused = true)]
async fn no_verdict_answers_not_acknowledged_after_the_timeout() {
    let w = waiters();
    let err = w.register(HASH).unwrap().answer().await.unwrap_err();
    assert!(matches!(err, IngressError::NotAcknowledged(_)), "{err}");
}

#[tokio::test(start_paused = true)]
async fn a_second_submission_of_a_waiting_hash_is_already_known() {
    let w = waiters();
    let first = w.register(HASH).unwrap();
    assert!(matches!(
        w.register(HASH).map(|_| ()),
        Err(IngressError::AlreadyKnown)
    ));
    drop(first);
    assert!(
        w.register(HASH).is_ok(),
        "the slot is free after the first wait"
    );
}

#[test]
fn the_ack_waiters_gauge_follows_each_insert_and_each_remove() {
    // The first wait lives past the render; the second ends in the scope.
    let mut first = None;
    let text = crate::rendered_metrics_tests::Rendered::of(|| {
        let w = waiters();
        first = Some(w.register(HASH).unwrap());
        let _second = w.register(B256::repeat_byte(8)).unwrap();
    });
    assert!(first.is_some());
    assert_eq!(
        text.value(r#"kardamom_queue_depth{queue="ingress.ack_waiters"}"#),
        1.0
    );
}
