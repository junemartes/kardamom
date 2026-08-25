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
///
/// The stack ALSO runs a validator in the DEPLOYED configuration
/// (`--parallel-validation`), and its verdict is load-bearing: before this
/// slice, the whole-block path had no `BufferedRecord::XChain` arm and a
/// validator here would have fail-stopped on the first delivery — which S12
/// never noticed, because its stack ran no validator at all. Now every
/// interop block must clear the seeded-parallel claim checks and the
/// write-set/receipt cross-checks, divergence-free.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s12_xchain_delivery() {
    use e2e::scenarios::xchain;
    use kardamom_da_watcher::interop::mock::MockInteropFeed;

    let stack = LocalStack::launch(StackConfig {
        genesis: e2e::harness::Genesis::DevInterop,
        validator: true,
        validator_parallel: true,
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

    // The validator's verdict on the interop blocks — MUST come before the
    // gap arm so a whole-block fail-stop is attributed to the deliveries.
    t.assert_validator_verdict("S12 after delivery")
        .await
        .expect("S12 validator verdict");

    xchain::gap_halts_pair_not_chain(&t, &feed, &mut watcher, outcome)
        .await
        .expect("S12 gap arm");

    t.assert_validator_verdict("S12 after the gap arm")
        .await
        .expect("S12 final validator verdict");
}

/// S14: TWO real LocalStacks, cross-chain both ways, no mock anywhere — the
/// egress-E1 acceptance. Chain A (412346) and chain B (412347, patched
/// genesis) each run the full stack plus a `--parallel-validation
/// --serve-feed` validator; B's interop watcher subscribes to A's VALIDATOR
/// feed over real WS (and vice versa for the callback), so the serving
/// surfaces, the outbox extraction with its BAL cross-check, and the
/// two-chain round trip A→B→A are all on the line at once.
///
/// The harness already namespaces everything two stacks could collide on
/// (per-test temp roots, OS-assigned ports everywhere, per-stack aeron
/// dirs); the one genuinely new knob is chain B's id, which materialises a
/// patched-genesis copy so the predeploy blobs stay single-sourced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "TWO full local stacks; the heaviest scenario — run via `just test-e2e-local` or with --ignored"]
async fn s14_xchain_two_stacks() {
    use e2e::scenarios::xchain_two_stacks::{self, CHAIN_B_ID};

    let started = std::time::Instant::now();
    let interop_stack = |chain_id: u64| StackConfig {
        genesis: e2e::harness::Genesis::DevInterop,
        chain_id,
        validator: true,
        validator_parallel: true,
        validator_serve_feed: true,
        ..StackConfig::default()
    };
    // Brought up one after the other: two stacks are 4 JVMs + 10 service
    // processes, and racing both bring-ups doubles the peak load for no
    // scenario value.
    let stack_a = LocalStack::launch(interop_stack(e2e::harness::DEV_CHAIN_ID))
        .await
        .expect("stack A");
    let stack_b = LocalStack::launch(interop_stack(CHAIN_B_ID))
        .await
        .expect("stack B");
    eprintln!("S14: both stacks up in {:?}", started.elapsed());

    let a = target(&stack_a);
    let b = target(&stack_b);
    let a_exec_dir = stack_a.executor_state_dir().expect("A executor state dir");
    let b_exec_dir = stack_b.executor_state_dir().expect("B executor state dir");

    // B's watcher consumes A's VALIDATOR feed — the real serving surface.
    let a_feed_url = stack_a.validator_feed_url().await.expect("A feed url");
    let b_cursor = stack_b.root().join("lane-from-a.cursor");
    let _watcher_on_b = stack_b
        .spawn_interop_watcher(e2e::harness::DEV_CHAIN_ID, &a_feed_url, &b_cursor)
        .expect("spawn B's watcher of A");

    // Leg 1: user tx on A -> Outbox -> A validator extract/serve -> B
    // watcher -> 0x7D delivery on B (receiver storage, Inbox slots, cursor).
    let outcome = xchain_two_stacks::forward_leg(
        &a,
        &b,
        e2e::harness::DEV_CHAIN_ID,
        &b_exec_dir,
        &b_cursor,
    )
    .await
    .expect("S14 forward leg");
    eprintln!("S14: forward leg done at {:?}", started.elapsed());

    // Leg 2: the callback comes home through B's validator feed.
    let b_feed_url = stack_b.validator_feed_url().await.expect("B feed url");
    let a_cursor = stack_a.root().join("lane-from-b.cursor");
    let _watcher_on_a = stack_a
        .spawn_interop_watcher(CHAIN_B_ID, &b_feed_url, &a_cursor)
        .expect("spawn A's watcher of B");
    xchain_two_stacks::callback_leg(&a, &b, e2e::harness::DEV_CHAIN_ID, &a_exec_dir, &a_cursor, outcome)
        .await
        .expect("S14 callback leg");
    eprintln!("S14: round trip done at {:?}", started.elapsed());

    // Both validators' verdicts: every interop block cleared the whole-block
    // claim checks and the cross-checks, and the extraction served only
    // BAL-tied messages — divergence-free on both chains.
    a.assert_validator_verdict("S14 chain A")
        .await
        .expect("S14 A validator verdict");
    b.assert_validator_verdict("S14 chain B")
        .await
        .expect("S14 B validator verdict");
    eprintln!("S14: total {:?}", started.elapsed());
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
    if let Err(e) = da_parity::reconstruct_and_compare(
        &l1.rpc_url(),
        l1.settlement,
        da_dir.path(),
        &genesis,
        recon_dir.path(),
        expected_root,
    ) {
        // FORENSICS before the panic: a root mismatch from two opaque hashes
        // is undiagnosable once CI drops the temp dirs (this fired ONCE on a
        // CI runner and never reproduced locally across contention, repeated
        // runs, and the same block composition). Name the collected set and
        // the first differing rows table-by-table so the next occurrence is
        // attributable from the log alone. (headers/receipts rows differ
        // benignly: replay synthesizes timestamps and carries l1_origin 0 —
        // the accounts/storage lines are the signal.)
        eprintln!("S13 collected canonical set:");
        for b in &canonical.blocks {
            eprintln!(
                "  block {}: {} record(s), {} tx(s)",
                b.block_number,
                b.remote_epochs.len(),
                b.txs.len()
            );
        }
        match xchain_da_parity::state_diff(recon_dir.path(), &exec_dir) {
            Ok(diffs) if diffs.is_empty() => {
                eprintln!("S13 deep_compare: no table diffs (?)")
            }
            Ok(diffs) => {
                eprintln!("S13 deep_compare (reconstructed vs live executor):");
                for d in diffs {
                    eprintln!("  {d}");
                }
            }
            Err(de) => eprintln!("S13 deep_compare failed: {de:#}"),
        }
        panic!("S13 DA parity: {e:#}");
    }

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
