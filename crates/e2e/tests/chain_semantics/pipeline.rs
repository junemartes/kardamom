// S3/S4/S5 — the transaction-pipeline scenarios: nonce ordering, nonce gaps,
// and RPC liveness. Included from main.rs (see the header there); shared
// helpers (`client_timeout`, `target`, …) live in main.rs.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s3_nonces_unordered_all_land() {
    let stack = LocalStack::launch(StackConfig::default())
        .await
        .expect("stack");
    let t = target(&stack);
    nonce_unordered::run(&t, nonce_unordered::Params::default())
        .await
        .expect("S3");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s4_nonce_gap_is_never_processed() {
    let park = Duration::from_secs(4);
    let stack = LocalStack::launch(StackConfig {
        ingress: IngressOptions {
            pending_receipt_timeout: park,
            ..IngressOptions::default()
        },
        ..StackConfig::default()
    })
    .await
    .expect("stack");
    let t = stack.target(client_timeout(park)).expect("target");
    nonce_gap::run(&t, nonce_gap::Params::default())
        .await
        .expect("S4");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s5_rpc_endpoints_never_hang() {
    let park = Duration::from_secs(5);
    let stack = LocalStack::launch(StackConfig {
        ingress: IngressOptions {
            pending_receipt_timeout: park,
            ..IngressOptions::default()
        },
        ..StackConfig::default()
    })
    .await
    .expect("stack");
    let t = stack.target(client_timeout(park)).expect("target");
    rpc_liveness::run(&t, rpc_liveness::Params::default())
        .await
        .expect("S5");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s5_connection_cap_refusal_is_prompt() {
    let park = Duration::from_secs(5);
    let cap = 4usize;
    let stack = LocalStack::launch(StackConfig {
        ingress: IngressOptions {
            pending_receipt_timeout: park,
            rpc_max_connections: cap as u32,
        },
        ..StackConfig::default()
    })
    .await
    .expect("stack");
    let url = stack.target(client_timeout(park)).expect("target").rpc.url;
    rpc_liveness::connection_cap_refusal(&url, e2e::harness::DEV_CHAIN_ID, cap, park, 1)
        .await
        .expect("S5 cap");
}

/// Regression test for the #81 pending-registry leak (fixed in #91, the
/// Weak-indexed registry): client-aborted parked submits must leave no
/// registry entries behind — queue depth returns to baseline once the park
/// bound passes. Shipped as an env-gated known-failure canary until the fix
/// landed; runs unconditionally now.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s5_queue_depth_recovers_after_client_aborts() {
    let park = Duration::from_secs(4);
    let stack = LocalStack::launch(StackConfig {
        ingress: IngressOptions {
            pending_receipt_timeout: park,
            ..IngressOptions::default()
        },
        ..StackConfig::default()
    })
    .await
    .expect("stack");
    let t = stack.target(client_timeout(park)).expect("target");
    rpc_liveness::queue_depth_canary(&t, 1, 3)
        .await
        .expect("S5 canary");
}
