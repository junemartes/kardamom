// S12 — cross-chain delivery through the REAL destination stack, with a
// MockInteropFeed as the simulated origin chain. Included from main.rs (see
// the header there); shared helpers live in main.rs.

/// S12: the full destination pipeline — the kardamom-da-watcher BINARY in
/// interop mode against a scripted origin feed, sequencer relay, sealer
/// per-peer origin advance, 0x7D execution through the genesis-seeded Inbox —
/// then the adversarial arm on the same stack: a feed seq gap fail-stops the
/// watcher process (nonzero exit, nothing skipped) while the chain keeps
/// sealing. No anvil needed: the origin chain is the mock feed, and the
/// stack's L1 path is simply absent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s12_xchain_delivery() {
    use e2e::scenarios::xchain;
    use kardamom_da_watcher::interop::mock::MockInteropFeed;

    let stack = LocalStack::launch(StackConfig {
        genesis: e2e::harness::Genesis::DevInterop,
        ..StackConfig::default()
    })
    .await
    .expect("stack");
    let t = target(&stack);
    let exec_dir = stack.executor_state_dir().expect("executor state dir");

    // The simulated origin chain: a real jsonrpsee WS server speaking the
    // real outbox-feed protocol, scripted by the test.
    let feed = MockInteropFeed::new(xchain::ORIGIN_CHAIN_ID).await;

    // The real watcher binary, first boot: no cursor file yet, seeded at 0.
    let cursor_file = stack.root().join("interop-pair.cursor");
    let mut watcher = stack
        .spawn_interop_watcher(xchain::ORIGIN_CHAIN_ID, &feed.url(), &cursor_file)
        .expect("spawn interop watcher");

    let outcome = xchain::delivery(&t, &feed, &exec_dir, &cursor_file, watcher.metrics_addr)
        .await
        .expect("S12 delivery");

    xchain::gap_halts_pair_not_chain(&t, &feed, &mut watcher, outcome)
        .await
        .expect("S12 gap arm");
}
