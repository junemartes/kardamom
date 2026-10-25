use alloy_provider::mock::Asserter;
use alloy_provider::{ProviderBuilder, RootProvider};

use super::*;

/// The nonce of the account at the latest block, in the tests.
const NONCE: u64 = 5;

/// A provider whose answers come from `asserter`, in order.
fn mocked(asserter: &Asserter) -> RootProvider {
    ProviderBuilder::new()
        .disable_recommended_fillers()
        .connect_mocked_client(asserter.clone())
}

/// A refusal of a call with `code` and `message`.
fn refuse(asserter: &Asserter, code: i64, message: &str) {
    asserter.push_failure(
        serde_json::from_value(serde_json::json!({"code": code, "message": message})).unwrap(),
    );
}

/// The nonce read of an account through `provider`.
fn nonce_read(provider: &RootProvider) -> NonceRead<'_, RootProvider> {
    NonceRead {
        provider,
        from: SINK,
    }
}

#[tokio::test]
async fn a_nonce_read_repeats_through_account_state_unavailable() {
    let asserter = Asserter::new();
    for _ in 0..3 {
        refuse(
            &asserter,
            STATE_UNAVAILABLE_CODE,
            "account state unavailable: no executor answered",
        );
    }
    asserter.push_success(&format!("{NONCE:#x}"));
    let provider = mocked(&asserter);
    assert_eq!(nonce_read(&provider).nonce().await.unwrap(), NONCE);
    assert!(asserter.read_q().is_empty(), "the read took every answer");
}

#[tokio::test]
async fn a_nonce_read_fails_at_once_on_another_refusal() {
    let asserter = Asserter::new();
    // The same code with another message is another refusal.
    refuse(&asserter, STATE_UNAVAILABLE_CODE, "rate limited");
    asserter.push_success(&format!("{NONCE:#x}"));
    let provider = mocked(&asserter);
    let error = nonce_read(&provider).nonce().await.unwrap_err().to_string();
    assert!(error.contains("rate limited"), "{error}");
    assert_eq!(asserter.read_q().len(), 1, "the read did not repeat");
}
