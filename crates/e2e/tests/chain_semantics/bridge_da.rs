// The L1-backed bridge round trips and DA parity. This file is
// included from main.rs (see the header there). Shared helpers live in
// main.rs.

/// S1: bridge a deposit in. `depositETH` on L1 appears on L2 as a receipt
/// keyed by the OP-style source_hash. The minted account can then spend
/// the funds (the ingress serves no `eth_getBalance`, so behavior is the
/// proof).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s1_bridge_deposit_round_trip() {
    let stack = launch_l1_or_skip!(StackConfig {
        l1: true,
        ..StackConfig::default()
    });
    let t = target(&stack);
    let l1 = stack.l1().expect("l1");
    bridge::deposit_round_trip(&t, l1, bridge::DepositParams::default())
        .await
        .expect("S1");
}

/// S2: bridge a withdrawal out. `initiateWithdrawal` runs on the L2
/// predeploy, the validator's attester posts the output root, and then,
/// after the finalization window, a test-built Merkle proof finalizes it
/// on L1 and pays the recipient. Replaying the same withdrawal must
/// revert.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s2_bridge_withdrawal_round_trip() {
    let mut stack = launch_l1_or_skip!(StackConfig {
        l1: true,
        validator: true,
        genesis: e2e::harness::Genesis::DevWithdrawals,
        ..StackConfig::default()
    });
    let t = target(&stack);
    let val_dir = stack.validator_state_dir().expect("validator state dir");

    // Do the L2 half first. The chain must be live for the withdrawal to
    // be sealed.
    let ticket = bridge::initiate_withdrawal(
        &t,
        stack.l1().expect("l1"),
        bridge::WithdrawalParams::default(),
    )
    .await
    .expect("S2 initiate");

    // This test deliberately does not freeze the chain here. The
    // withdrawal's receipt appears when the transaction executes, but its
    // block commits only at the next sealer boundary. Freezing on receipt
    // would strand it in an uncommitted block. The chain quiesces on its
    // own (empty boundaries do not commit), so the validator's head root
    // settles on the withdrawal's block.
    let finalize = {
        let l1 = stack.l1().expect("l1");
        bridge::finalize_withdrawal(l1, ticket, &val_dir).await
    };
    // Whether S2 fails often depends on whether the validator was still
    // alive when the read-only state open ran. A dead validator, with
    // unsteady mdbx metadata, points to an infrastructure cascade. An
    // alive validator points to a product bug. Record that distinction in
    // the panic message.
    if let Err(e) = finalize {
        panic!(
            "S2 finalize failed (validator alive: {:?}): {e:?}",
            stack.validator_alive()
        );
    }
}

/// S8: what the batcher posts to L1, re-executed from L1 alone, must
/// equal the state root the validator computed on its own. This is the
/// "batcher's state matches the validator's" guarantee. This test posts
/// real EIP-4844 blobs to anvil and runs the real
/// `kardamom-reconstruct --expect-root` binary (no caller used its gate
/// anywhere before this scenario).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s8_da_parity_batcher_matches_validator() {
    let stack = launch_l1_or_skip!(StackConfig {
        l1: true,
        validator: true,
        ..StackConfig::default()
    });
    let t = target(&stack);
    let l1 = stack.l1().expect("l1");
    let params = da_parity::Params::default();

    // 1. Run a deposit-free workload, then recover the canonical blocks it
    //    produced from the pipeline's own receipts.
    let blocks = da_parity::run_workload(&t, &params)
        .await
        .expect("S8 workload");

    // 2. Post them to L1 as real blob transactions.
    let da_dir = tempfile::tempdir().expect("da dir");
    let da_store = kardamom_batcher::da_store::FsBlobStore::open(da_dir.path()).expect("da store");
    da_parity::post_to_l1(l1, l1.settlement, &blocks, &da_store)
        .await
        .expect("S8 post to L1");
    da_parity::assert_batches_on_l1(l1, l1.settlement, blocks.len(), &da_store)
        .await
        .expect("S8 L1 batch log");

    // 3. The parity target: the validator's own committed root, read from
    //    its live database once the chain has settled on it.
    let val_dir = stack.validator_state_dir().expect("validator state dir");
    let expected_root = e2e::harness::metrics::poll_until(
        "validator root covering the workload",
        Duration::from_secs(60),
        Duration::from_millis(500),
        || async {
            let committed = t
                .validator_metric(e2e::scenarios::VALIDATOR_COMMITTED_BLOCK)
                .await
                .unwrap_or(0.0) as u64;
            let head = blocks.last().map(|b| b.block_number).unwrap_or(0);
            if committed < head {
                return Ok(None);
            }
            e2e::scenarios::read_validator_state_root(&val_dir)
        },
    )
    .await
    .expect("validator root");

    // 4. Rebuild from L1 alone. The roots must match.
    let recon_dir = tempfile::tempdir().expect("recon dir");
    let genesis =
        e2e::harness::services::repo_root().join("deploy/cluster/config/genesis/dev.toml");
    da_parity::reconstruct_and_compare(
        &l1.rpc_url(),
        l1.settlement,
        da_dir.path(),
        &genesis,
        recon_dir.path(),
        expected_root,
    )
    .expect("S8 DA parity");
}
