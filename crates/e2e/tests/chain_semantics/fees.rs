// The priority fee scenario (`s18`). Included by `main.rs`; see the
// note there on `include!`.

/// With priority fees on, a standard transfer pays the base fee and the
/// full tip, the fee RPCs serve, and a transfer under the base fee is
/// rejected on the feed without a nonce slot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s18a_under_base_fee_is_rejected_on_the_feed() {
    let stack = LocalStack::launch(StackConfig {
        priority_fees: true,
        ..StackConfig::default()
    })
    .await
    .expect("stack");
    fees::under_base_fee_is_rejected(&target(&stack), &fees::Params::default())
        .await
        .expect("S18a");
}
