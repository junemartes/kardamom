/// A chain with carry enabled at genesis, a member feed, an egress and a batcher.
fn carry_stack_config(members: std::num::NonZeroUsize, park: ParkTimeout) -> StackConfig {
    StackConfig {
        l1: true,
        validator: true,
        genesis: e2e::harness::Genesis::ClusterDev,
        sealer_members: members,
        carry: Some(e2e::harness::CarryOptions {
            retention: std::num::NonZeroU32::new(512).unwrap(),
            genesis: true,
        }),
        ingress: IngressOptions {
            pending_receipt_timeout: park,
            ..IngressOptions::default()
        },
        ..StackConfig::default()
    }
}

/// S13g: the carry mode from genesis on a new chain. Every member starts at
/// decision version 5 with the carry window, the executor reads the member
/// feed from its first block, and the readers read the egress. A follower
/// restarts before the first snapshot and from a snapshot, under load.
/// The L2 genesis holds the feature words. The persisted state of the executor and
/// the validator matches, and a rebuild from L1 and the DA proxy alone
/// gives the root of the executor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s13g_carry_from_genesis() {
    let three = std::num::NonZeroUsize::new(3).unwrap();
    let park = IngressOptions::default().pending_receipt_timeout;
    let mut stack = launch_l1_or_skip!(carry_stack_config(three, park));
    e2e::scenarios::carry_window::from_genesis(
        &mut stack,
        e2e::scenarios::carry_window::Params::default(),
    )
    .await
    .expect("S13g");
    let exec_dir = stack.executor_state_dir().expect("executor state dir");
    let val_dir = stack.validator_state_dir().expect("validator state dir");
    stack.shutdown_graceful().await.expect("graceful shutdown");
    consistency::verify_state_dirs(&exec_dir, &val_dir).expect("S13g persisted state");

    let root = e2e::scenarios::xchain_da_parity::executor_state_root(&exec_dir)
        .expect("root the executor state");
    let l1 = stack.l1().expect("l1");
    let seams = stack.carry_seams().expect("carry seams");
    let da_proxy = seams.da_proxy.expect("the batcher's DA proxy");
    let l1_rpc = l1.rpc_url();
    let recon_dir = tempfile::tempdir().expect("recon dir");
    da_parity::Reconstruct {
        l1_rpc: &l1_rpc,
        settlement: l1.settlement,
        lockbox: l1.lockbox,
        da_proxy: &da_proxy,
        genesis: stack.genesis_path(),
    }
    .compare(&recon_dir.path().join("state"), root)
    .expect("S13g rebuild from L1");
}
