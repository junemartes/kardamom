// L1-governed upgrades. This file is included from main.rs (see the
// header there). Shared helpers live in main.rs.
//
// Every case runs the whole chain: a factory-owner transaction on L1, the
// da-watcher deriving a system deposit from its finalized log, the sealer
// ordering it, the executor applying it to the chain-state predeploy, and
// the protocol changing behavior from a specific block onward. The
// validator agrees, on its own, about which block that was.

/// The stack these scenarios need: an L1 to send upgrade transactions
/// to, a validator to prove activation parity, and `genesis`.
/// `Genesis::DevWithdrawals` carries the `KardamomChainState` predeploy;
/// `Genesis::ClusterDev`, the staging genesis, does not.
fn upgrade_stack_config(genesis: e2e::harness::Genesis) -> StackConfig {
    StackConfig {
        l1: true,
        validator: true,
        genesis,
        ..StackConfig::default()
    }
}

/// [`upgrade_stack_config`] for S13c and S13d: they send transactions
/// that cannot pay, so the sequencers keep the v1 loop.
fn unpayable_stack_config(genesis: e2e::harness::Genesis) -> StackConfig {
    StackConfig {
        unpayable_spends: true,
        ..upgrade_stack_config(genesis)
    }
}

/// The tests below differ only in the stack config and the scenario they
/// drive. They share the same L1-backed, validator-carrying stack and the
/// same `(target, l1, executor state dir, validator state dir)` hand-off.
/// This skips the test with an early return when anvil is absent.
async fn upgrade_case<F>(cfg: StackConfig, scenario: F, what: &str)
where
    F: AsyncFn(
        &e2e::scenarios::Target,
        &e2e::harness::l1::L1,
        &std::path::Path,
        &std::path::Path,
    ) -> anyhow::Result<()>,
{
    let stack = launch_l1_or_skip!(cfg);
    let t = target(&stack);
    let l1 = stack.l1().expect("l1");
    let exec_dir = stack.executor_state_dir().expect("executor state dir");
    let val_dir = stack.validator_state_dir().expect("validator state dir");
    scenario(&t, l1, &exec_dir, &val_dir).await.expect(what);
}

/// S13d on `genesis`, then the canonical chain-state code on the executor
/// and the validator.
async fn s13d_on(genesis: e2e::harness::Genesis) {
    upgrade_case(
        unpayable_stack_config(genesis),
        async |t, l1, exec_dir, val_dir| {
            let upgrade_block = sealer_upgrade::skip_uses_its_nonce_only_after_the_upgrade(
                t,
                l1,
                sealer_upgrade::Params::default(),
            )
            .await?;
            upgrade::assert_chain_state_code(exec_dir, val_dir, upgrade_block).await
        },
        "S13d",
    )
    .await;
}

/// S13d: before the upgrade transaction of the sealer rules, a skipped
/// sender stays stuck at its nonce. After it, a skip uses its nonce, and
/// the next nonce lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s13d_a_skip_uses_its_nonce_only_after_the_upgrade() {
    s13d_on(e2e::harness::Genesis::DevWithdrawals).await;
}

/// S13d on the staging genesis, which has no chain-state predeploy. The
/// upgrade deposit installs the code, writes the flag word, and the chain
/// switches as on a genesis with the predeploy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s13d_the_first_upgrade_installs_the_chain_state() {
    s13d_on(e2e::harness::Genesis::ClusterDev).await;
}

/// S13a: an upgrade transaction with no activation timestamp turns the
/// feature on immediately, starting with the block after the one that
/// carried it. It keeps firing in every later block, on both the executor
/// and the validator.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s13a_health_check_activates_immediately() {
    upgrade_case(
        upgrade_stack_config(e2e::harness::Genesis::DevWithdrawals),
        async |t, l1, exec_dir, val_dir| {
            upgrade::activates_immediately(t, l1, exec_dir, val_dir).await
        },
        "S13a",
    )
    .await;
}

/// S13b: an upgrade transaction carrying a future activation timestamp
/// is scheduled, not applied. The feature stays dormant until the
/// chain's own clock reaches that time, then fires from the first block
/// that executes at or after it.
///
/// This also guards a units trap: `l2_timestamp` is epoch milliseconds.
/// So the scenario anchors its schedule to the chain's own head
/// timestamp, not to wall-clock seconds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s13b_health_check_activates_at_timestamp() {
    upgrade_case(
        upgrade_stack_config(e2e::harness::Genesis::DevWithdrawals),
        async |t, l1, exec_dir, val_dir| {
            upgrade::activates_at_timestamp(t, l1, exec_dir, val_dir).await
        },
        "S13b",
    )
    .await;
}

/// S13c: both authority gates hold. An L1 account that is not the
/// factory owner cannot emit an upgrade transaction. An ordinary L2
/// transaction cannot write the flag store directly either: the
/// predeploy rejects any sender except the derivation pipeline's system
/// address.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s13c_upgrade_authority_is_enforced() {
    upgrade_case(
        unpayable_stack_config(e2e::harness::Genesis::DevWithdrawals),
        async |t, l1, exec_dir, _val_dir| {
            upgrade::authority_is_enforced(t, l1, exec_dir, upgrade::Params::default()).await
        },
        "S13c",
    )
    .await;
}

/// The stack of the carry window: `members` sealer members with the
/// member feed, the egress and a live batcher, on the staging genesis. The
/// chain starts on the reference rules: these scenarios test the live
/// upgrade path.
fn carry_stack_config(members: std::num::NonZeroUsize, park: ParkTimeout) -> StackConfig {
    StackConfig {
        sealer_members: members,
        carry: Some(e2e::harness::CarryOptions {
            retention: std::num::NonZeroU32::new(512).unwrap(),
            genesis: false,
        }),
        ingress: IngressOptions {
            pending_receipt_timeout: park,
            ..IngressOptions::default()
        },
        ..upgrade_stack_config(e2e::harness::Genesis::ClusterDev)
    }
}

/// S13e: the carry window rollout on a live chain. Features 3 and 4,
/// the sequencer floor, the readers on the egress, feature 5 under a DA
/// lag, the executor in carry mode, a member restart, a refused
/// reference without bytes, and feature 5 off and on again. Then the
/// persisted state of the executor and the validator matches.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s13e_carry_window_rollout() {
    let three = std::num::NonZeroUsize::new(3).unwrap();
    let park = IngressOptions::default().pending_receipt_timeout;
    let mut stack = launch_l1_or_skip!(carry_stack_config(three, park));
    e2e::scenarios::carry_window::run(&mut stack, e2e::scenarios::carry_window::Params::default())
        .await
        .expect("S13e");
    let exec_dir = stack.executor_state_dir().expect("executor state dir");
    let val_dir = stack.validator_state_dir().expect("validator state dir");
    stack.shutdown_graceful().await.expect("graceful shutdown");
    consistency::verify_state_dirs(&exec_dir, &val_dir).expect("S13e persisted state");
}

/// S13f: a sequencer floor above the version of the chain. The floor 5
/// on a chain at version 4 costs no stall. A transaction parked at version
/// 5 leaves in version-4 format after a downgrade, and every reader agrees.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s13f_a_wrong_floor_costs_no_stall() {
    let park = ParkTimeout::from_secs(std::num::NonZeroU64::new(90).unwrap());
    let mut stack = launch_l1_or_skip!(carry_stack_config(std::num::NonZeroUsize::MIN, park));
    e2e::scenarios::carry_window::wrong_floor(
        &mut stack,
        e2e::scenarios::carry_window::Params::default(),
    )
    .await
    .expect("S13f");
    let exec_dir = stack.executor_state_dir().expect("executor state dir");
    let val_dir = stack.validator_state_dir().expect("validator state dir");
    stack.shutdown_graceful().await.expect("graceful shutdown");
    consistency::verify_state_dirs(&exec_dir, &val_dir).expect("S13f persisted state");
}

/// S13g: the carry mode from genesis on a new chain. Every member starts at
/// decision version 5 with the carry window, the executor reads the member
/// feed from its first block, and the readers read the egress. A follower
/// restarts before the first snapshot and from a snapshot, under load.
/// The L2 genesis holds the feature words, and feature 5 turns off and on
/// by upgrade transaction. Then the persisted state of the executor and
/// the validator matches, and a rebuild from L1 and the DA proxy alone
/// gives the root of the executor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s13g_carry_from_genesis() {
    let three = std::num::NonZeroUsize::new(3).unwrap();
    let park = IngressOptions::default().pending_receipt_timeout;
    let base = carry_stack_config(three, park);
    let mut stack = launch_l1_or_skip!(StackConfig {
        carry: base.carry.map(|c| e2e::harness::CarryOptions {
            genesis: true,
            ..c
        }),
        ..base
    });
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
