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

/// S13: the interop chain rebuilt from its OWN DA (spec §16 Q8). The S12
/// delivery flow runs unchanged on a DevInterop stack that also carries the
/// anvil L1 (the S8 DA-posting idiom); the canonical blocks — remote-epoch
/// records attached to the block each one led — are recovered from the
/// pipeline's own receipts, posted to L1 as real EIP-4844 blobs, and
/// `kardamom-reconstruct --expect-root` (plus S8's non-vacuity control) must
/// rebuild the executor's exact state from L1 data alone: root match,
/// `Inbox.delivered`/`nextSeq` equal, 0x7D receipts reproduced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s13_xchain_da_parity() {
    use e2e::scenarios::{da_parity, xchain, xchain_da_parity};
    use kardamom_da_watcher::interop::mock::MockInteropFeed;

    let mut stack = launch_l1_or_skip!(StackConfig {
        l1: true,
        genesis: e2e::harness::Genesis::DevInterop,
        ..StackConfig::default()
    });
    let t = target(&stack);
    let exec_dir = stack.executor_state_dir().expect("executor state dir");

    let feed = MockInteropFeed::new(xchain::ORIGIN_CHAIN_ID).await;
    let cursor_file = stack.root().join("interop-pair.cursor");
    let mut watcher = stack
        .spawn_interop_watcher(xchain::ORIGIN_CHAIN_ID, &feed.url(), &cursor_file)
        .expect("spawn interop watcher");

    // 1. The S12 delivery flow, unchanged — every layer's evidence asserted.
    let outcome = xchain::delivery(&t, &feed, &exec_dir, &cursor_file, watcher.metrics_addr)
        .await
        .expect("S13 delivery");

    // 2. Fence the workload and recover the canonical blocks, records
    //    attached to the block each one led.
    let canonical = xchain_da_parity::collect_canonical_blocks(&t, &outcome)
        .await
        .expect("S13 canonical blocks");

    // 3. Quiesce: stop the interop watcher, then the stack — the executor
    //    closes its DB cleanly with every executed block durable.
    watcher.proc.kill();
    stack
        .shutdown_graceful()
        .await
        .expect("S13 graceful shutdown");

    // 4. The parity target: the executor's own final state, rooted offline.
    let expected_root =
        xchain_da_parity::executor_state_root(&exec_dir).expect("root the executor state");

    // 5. Post to L1 as real blob txs, then rebuild from L1 alone — the
    //    `--expect-root` gate plus its non-vacuity control (S8's machinery,
    //    reused verbatim).
    let l1 = stack.l1().expect("l1");
    let da_dir = tempfile::tempdir().expect("da dir");
    let da_store = kardamom_batcher::da_store::FsBlobStore::open(da_dir.path()).expect("da store");
    da_parity::post_to_l1(l1, l1.settlement, &canonical.blocks, &da_store)
        .await
        .expect("S13 post to L1");
    da_parity::assert_batches_on_l1(l1, l1.settlement, canonical.blocks.len(), &da_store)
        .await
        .expect("S13 L1 batch log");
    let recon_dir = tempfile::tempdir().expect("recon dir");
    let genesis = e2e::harness::services::repo_root().join("chains/dev-interop.toml");
    da_parity::reconstruct_and_compare(
        &l1.rpc_url(),
        l1.settlement,
        da_dir.path(),
        &genesis,
        recon_dir.path(),
        expected_root,
    )
    .expect("S13 DA parity");

    // 6. The rebuilt DB reproduces the interop substance: lane state equal to
    //    the live executor's, receiver calldata intact, 0x7D receipts
    //    matching what the live RPC served.
    xchain_da_parity::assert_reconstructed_interop_state(
        recon_dir.path(),
        &exec_dir,
        &outcome,
        &canonical,
    )
    .expect("S13 interop state");
}
