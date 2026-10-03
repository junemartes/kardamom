//! The DA-lag guard at the ingress: the `safe` and `finalized` tags
//! follow the posted head of the cluster's status, a halted status refuses
//! a submit with the typed error and raises the `da_lag` halt, and a
//! status that says the sealer accepts again clears it.

use std::time::Duration;

use alloy_primitives::{Address, U256};
use alloy_signer_local::PrivateKeySigner;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::rpc_params;
use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::error::CHAIN_HALTED_CODE;
use kardamom_ingress::test_support::{http_client, sign_legacy, start_test_server};
use kardamom_obs::halt::{self, HaltCause};
use kardamom_types::ClusterStatus;

/// Wait until the halt watcher mirrored the last status.
async fn wait_halted(halted: bool) {
    kardamom_obs::testkit::poll_until(
        "the ingress halt mirrors the cluster status",
        Duration::from_secs(5),
        Duration::from_millis(20),
        async || Ok((halt::current().is_some() == halted).then_some(())),
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

    // The sealer refuses new transactions: the submit is refused here
    // with the typed error, and the ingress halts on `da_lag`.
    server.mock.cluster_status_bus.send_replace(ClusterStatus {
        sealed_head: 20,
        halted: true,
        ..posted
    });
    wait_halted(true).await;
    let standing = halt::current().unwrap();
    assert_eq!(standing.cause, HaltCause::DaLag);
    assert!(
        standing.detail.contains("sealed head 20"),
        "{}",
        standing.detail
    );
    let signer = PrivateKeySigner::random();
    let err = client
        .request::<alloy_primitives::B256, _>(
            "eth_sendRawTransaction",
            rpc_params![sign_legacy(&signer, 0)],
        )
        .await
        .unwrap_err();
    let text = err.to_string();
    assert!(text.contains("chain halted: DA lag"), "{text}");
    assert!(text.contains("/halt"), "{text}");
    assert!(text.contains(&CHAIN_HALTED_CODE.to_string()), "{text}");

    // The batcher posted: the sealer accepts again, the halt clears, and
    // a submit is published.
    server.mock.cluster_status_bus.send_replace(ClusterStatus {
        posted_head: 19,
        sealed_head: 20,
        halted: false,
        ..posted
    });
    wait_halted(false).await;
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
