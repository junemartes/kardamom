// The account layer scenarios (`s17`). Included by `main.rs`; see the
// note there on `include!`.

/// The ingress serves an established sender's count from its local
/// layer, the executor agrees once the block commits, a retry answers
/// its hash, and a different transaction at a landed nonce is invalid
/// params.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s17a_ingress_count_matches_the_executor_and_a_retry_answers() {
    let stack = LocalStack::launch(StackConfig::default())
        .await
        .expect("stack");
    account_layer::nonce_matches_executor(&target(&stack), &account_layer::Params::default())
        .await
        .expect("S17a");
}

/// A restarted ingress holds no account layer. It admits the sender's
/// next nonce on a local miss, and counts the miss.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s17c_cold_ingress_admits_on_a_local_miss() {
    let mut stack = LocalStack::launch(StackConfig::default())
        .await
        .expect("stack");
    let params = account_layer::Params::default();
    account_layer::establish(&target(&stack), &params)
        .await
        .expect("S17c establish");
    stack.restart_ingress(None).expect("restart ingress");
    // The restarted ingress has a new metrics port. Rebuild the target.
    account_layer::cold_ingress_admits(&target(&stack), &params)
        .await
        .expect("S17c after restart");
}

/// The validator checks the batch rows against its own state: its
/// verified-rows counter rises under traffic.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s17e_validator_verifies_the_batch_rows() {
    let stack = LocalStack::launch(StackConfig {
        validator: true,
        ..StackConfig::default()
    })
    .await
    .expect("stack");
    account_layer::rows_verified(&target(&stack), &account_layer::Params::default())
        .await
        .expect("S17e");
}

/// A pays a fresh B. On A's receipt the ingress serves B's balance from
/// the layer, and B spends it at once, before any block boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s17g_recipient_spends_within_one_batch() {
    let stack = LocalStack::launch(StackConfig::default())
        .await
        .expect("stack");
    account_layer::recipient_spends_within_batch(
        &target(&stack),
        &account_layer::Params::default(),
    )
    .await
    .expect("S17g");
}
