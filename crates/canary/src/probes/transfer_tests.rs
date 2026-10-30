//! The `transfer` probe while the ingress refuses submits on a DA-lag
//! halt, and after the halt ends.

use alloy_primitives::U256;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

use super::*;
use crate::test_support::{FakeIngress, HALT_CODE};

/// The transfer runs on the metrics page whose labels hold each of
/// `fragments`.
fn runs(handle: &PrometheusHandle, fragments: &[&str]) -> u64 {
    handle
        .render()
        .lines()
        .filter(|line| line.starts_with("kardamom_canary_probe_total"))
        .filter(|line| line.contains("probe=\"transfer\""))
        .filter(|line| fragments.iter().all(|fragment| line.contains(fragment)))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<u64>().ok())
        .sum()
}

#[tokio::test(flavor = "current_thread")]
async fn a_da_lag_refusal_counts_its_code_and_the_transfer_lands_after_the_halt() {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    let _local = ::metrics::set_default_local_recorder(&recorder);
    let ingress = FakeIngress::start().await;
    ingress.chain.lock().unwrap().balance = U256::from(10u64).pow(U256::from(18));
    let ring = ingress.ring().await;
    ring.set_funded(0, true);
    let mut transfer = Transfer::new(ingress.context(ring));
    let code = format!("code=\"{HALT_CODE}\"");
    let refused = ["outcome=\"rpc_error\"", code.as_str()];

    ingress.chain.lock().unwrap().halted = true;
    transfer.run().await;
    transfer.run().await;
    assert_eq!(runs(&handle, &refused), 2, "each run counts the halt code");
    assert_eq!(runs(&handle, &["outcome=\"success\""]), 0);
    assert_eq!(
        ingress.chain.lock().unwrap().sends,
        0,
        "the halted ingress takes no transaction"
    );

    ingress.chain.lock().unwrap().halted = false;
    transfer.run().await;
    assert_eq!(
        runs(&handle, &["outcome=\"success\""]),
        1,
        "the transfer lands after the halt"
    );
    assert_eq!(runs(&handle, &refused), 2);
    let chain = ingress.chain.lock().unwrap();
    assert_eq!(
        (chain.nonce, chain.sends),
        (1, 1),
        "the refused runs use no nonce: the next transfer takes nonce 0"
    );
}
