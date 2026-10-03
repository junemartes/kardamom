//! The DA-lag guard at the ingress: the `safe` and `finalized` tags
//! follow the posted head of the cluster's status. A halted status halts
//! the sealer (as the ingress observes it) on `da_lag`, pauses the
//! ingress on that root, and refuses a submit with the typed error; the
//! chain status shows the root and the pause. A status that says the
//! sealer accepts again clears the halt and resumes the ingress.

use std::time::Duration;

use alloy_primitives::{Address, U256};
use alloy_signer_local::PrivateKeySigner;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::rpc_params;
use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::error::CHAIN_HALTED_CODE;
use kardamom_ingress::test_support::{http_client, sign_legacy, start_test_server};
use kardamom_obs::halt::{HaltCause, PauseReason};
use kardamom_obs::lifecycle::process;
use kardamom_types::ClusterStatus;

/// Wait until the chain watch paused (or resumed) the ingress.
async fn wait_paused(paused: bool) {
    kardamom_obs::testkit::poll_until(
        "the ingress pause follows the cluster status",
        Duration::from_secs(5),
        Duration::from_millis(20),
        async || Ok((process().slots().pause.is_some() == paused).then_some(())),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn the_tags_follow_the_posted_head_and_a_halt_refuses_submits() {
    let server = start_test_server(IngressConfig::default()).await;
    let client = http_client(server.addr);

    // Before any status: nothing posted, the head is block 0, nothing halted.
    let safe: U256 = client
        .request("kardamom_blockNumberByTag", rpc_params!["safe"])
        .await
        .unwrap();
    assert_eq!(safe, U256::ZERO);

    let posted = ClusterStatus {
        posted_head: 5,
        sealed_head: 9,
        budget_blocks: 10,
        halted: false,
        ..Default::default()
    };
    server.mock.cluster_status_bus.send_replace(posted);
    for tag in ["safe", "finalized"] {
        let n: U256 = client
            .request("kardamom_blockNumberByTag", rpc_params![tag])
            .await
            .unwrap();
        assert_eq!(n, U256::from(5u64), "{tag} names the posted head");
    }
    let latest: U256 = client
        .request("kardamom_blockNumberByTag", rpc_params!["latest"])
        .await
        .unwrap();
    assert_eq!(latest, U256::ZERO, "the head is what the boundaries said");
    // The account state at `safe` is history the ingress does not hold
    // while the posted head is behind the head.
    let err = client
        .request::<U256, _>(
            "eth_getBalance",
            rpc_params![Address::repeat_byte(0x42), "safe"],
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not served"), "{err}");

    // The sealer refuses new transactions: the sealer halts on `da_lag`,
    // the ingress pauses on it, and the submit is refused with the typed
    // error that names the root.
    server.mock.cluster_status_bus.send_replace(ClusterStatus {
        sealed_head: 20,
        halted: true,
        ..posted
    });
    wait_paused(true).await;
    let pause = process().slots().pause.unwrap();
    let PauseReason::Upstream(root) = pause.reason else {
        panic!("an upstream pause, not {pause:?}");
    };
    assert_eq!(
        (root.service.as_str(), root.cause),
        ("sealer", HaltCause::DaLag)
    );
    assert!(
        process().slots().halt.is_none(),
        "the ingress is paused, not halted"
    );
    let status: serde_json::Value = client
        .request("kardamom_chainStatus", rpc_params![])
        .await
        .unwrap();
    assert_eq!(status["sealer"]["state"], "halted");
    assert_eq!(status["sealer"]["cause"], "da_lag");
    assert!(
        status["sealer"]["detail"]
            .as_str()
            .unwrap()
            .contains("sealed head 20"),
        "{status}"
    );
    assert_eq!(status["roots"][0]["service"], "sealer");
    assert_eq!(status["ingress"]["state"], "paused");
    assert_eq!(status["ingress"]["pause"]["root"]["cause"], "da_lag");
    let signer = PrivateKeySigner::random();
    let err = client
        .request::<alloy_primitives::B256, _>(
            "eth_sendRawTransaction",
            rpc_params![sign_legacy(&signer, 0)],
        )
        .await
        .unwrap_err();
    let text = err.to_string();
    assert!(text.contains("chain halted: da_lag at sealer"), "{text}");
    assert!(text.contains("docs/runbooks/da_lag.md"), "{text}");
    assert!(text.contains("/halt"), "{text}");
    assert!(text.contains(&CHAIN_HALTED_CODE.to_string()), "{text}");

    // The batcher posted: the sealer accepts again, the halt clears, the
    // ingress resumes, and a submit is published.
    server.mock.cluster_status_bus.send_replace(ClusterStatus {
        posted_head: 19,
        sealed_head: 20,
        halted: false,
        ..posted
    });
    wait_paused(false).await;
    let status: serde_json::Value = client
        .request("kardamom_chainStatus", rpc_params![])
        .await
        .unwrap();
    assert_eq!(status["sealer"]["state"], "running");
    assert_eq!(status["roots"].as_array().unwrap().len(), 0);
    let hash: alloy_primitives::B256 = client
        .request(
            "kardamom_sendRawTransactionAsync",
            rpc_params![sign_legacy(&signer, 0)],
        )
        .await
        .expect("the submit is published once the chain accepts again");
    assert_ne!(hash, alloy_primitives::B256::ZERO);
    server.handle.stop().unwrap();
}
