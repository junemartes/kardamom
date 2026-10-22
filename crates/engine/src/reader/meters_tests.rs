//! Tests for the join metrics.

use std::time::Duration;

use kardamom_types::{TxEnvelope, TxRef};

use super::join::{JoinBuffer, JoinContext, JoinOutcome, JoinWait, ReaderConfig, TxDataKey};
use super::meters::JoinMeters;
use crate::meter::tests::{render, value};

const MISSES: &str = "kardamom_executor_join_misses_total";
const WAITS: &str = r#"kardamom_step_seconds_count{step="executor.join_wait"}"#;

/// Run one join of the default `TxRef` against `buffer`, with no
/// recovery, and return whether it joined.
fn join(buffer: &JoinBuffer) -> bool {
    let cfg = ReaderConfig {
        join_timeout: Duration::from_millis(20),
        ..ReaderConfig::default()
    };
    let meters = JoinMeters::new();
    let mut recovery = None;
    let ctx = JoinContext {
        buffer,
        recovery: &mut recovery,
        cfg: &cfg,
        meters: &meters,
    };
    let outcome = JoinWait::new(ctx, &TxRef::default()).unwrap().run();
    matches!(outcome, JoinOutcome::Joined(_))
}

fn default_key() -> TxDataKey {
    let tx_ref = TxRef::default();
    TxDataKey::new(
        tx_ref.shard_id,
        tx_ref.tx_data_session_id,
        tx_ref.tx_data_position,
    )
}

fn envelope() -> TxEnvelope {
    let signer = alloy_signer_local::PrivateKeySigner::random();
    crate::actor::test_support::legacy(&signer, alloy_primitives::Address::ZERO, 0, 1)
}

#[test]
fn a_join_that_finds_its_envelope_at_once_records_nothing() {
    let buffer = JoinBuffer::new();
    buffer.insert(default_key(), envelope());
    let text = render(|| assert!(join(&buffer)));
    assert_eq!(value(&text, WAITS), 0.0, "{text}");
    assert_eq!(value(&text, MISSES), 0.0, "{text}");
}

#[test]
fn a_join_that_waits_records_its_wait_and_no_miss() {
    let buffer = JoinBuffer::new();
    let late = buffer.clone();
    let text = render(|| {
        let insert = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(2));
            late.insert(default_key(), envelope());
        });
        assert!(join(&buffer));
        insert.join().unwrap();
    });
    assert_eq!(value(&text, WAITS), 1.0, "{text}");
    assert_eq!(value(&text, MISSES), 0.0, "{text}");
}

#[test]
fn a_join_whose_wait_ends_without_the_envelope_counts_a_miss() {
    let buffer = JoinBuffer::new();
    let text = render(|| assert!(!join(&buffer)));
    assert_eq!(value(&text, WAITS), 1.0, "{text}");
    assert_eq!(value(&text, MISSES), 1.0, "{text}");
}
