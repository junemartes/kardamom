//! Protocol-limit rejection at submission (W1b,
//! docs/agents/l1-client-suite-port-spec.md). A tx that can never execute,
//! a gas limit above the EIP-7825 cap, or a blob (type-3) envelope, gets a
//! clear `-32602`-class error. It does not turn into a `status=false` skip
//! receipt downstream.

mod common;

use alloy_signer_local::PrivateKeySigner;
use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::error::IngressError;
use kardamom_ingress::{IngressProxy, MockChannels};
use kardamom_types::limits::TX_GAS_LIMIT_CAP;

fn proxy() -> IngressProxy<MockChannels, MockChannels> {
    let (mock, _rx) = MockChannels::new(8);
    IngressProxy::new(IngressConfig::default(), mock.clone(), mock)
}

#[tokio::test]
async fn gas_limit_above_the_eip7825_cap_is_rejected() {
    let signer = PrivateKeySigner::random();
    let p = proxy();
    let ip = "10.1.0.1".parse().unwrap();

    // 30M gas is a valid amount for a block, but it is over the per-tx cap.
    let raw = common::sign_legacy_with_gas(&signer, 0, 30_000_000);
    let err = p.submit_raw(ip, raw).await.unwrap_err();
    assert!(
        matches!(err, IngressError::GasLimitExceedsCap(30_000_000)),
        "{err:?}"
    );

    // A gas limit exactly at the cap passes validation. The call then waits
    // for a receipt that the mock pipeline never sends, so it times out.
    // This is not a validation error.
    let raw = common::sign_legacy_with_gas(&signer, 0, TX_GAS_LIMIT_CAP);
    let err = p.submit_raw(ip, raw).await.unwrap_err();
    assert!(
        !matches!(
            err,
            IngressError::GasLimitExceedsCap(_) | IngressError::UnsupportedTxType(_)
        ),
        "at-cap tx must not be limit-rejected, got {err:?}"
    );
}

#[tokio::test]
async fn blob_transactions_are_rejected() {
    let signer = PrivateKeySigner::random();
    let p = proxy();
    let ip = "10.1.0.2".parse().unwrap();

    let raw = common::sign_eip4844(&signer, 0);
    let err = p.submit_raw(ip, raw).await.unwrap_err();
    assert!(
        matches!(err, IngressError::UnsupportedTxType(0x03)),
        "{err:?}"
    );
}
