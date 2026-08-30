// S13 — L1-governed upgrades. Included from main.rs (see the header there);
// shared helpers live in main.rs.
//
// Every case runs the whole chain: a factory-owner transaction on L1, the
// da-watcher deriving a system deposit from its finalized log, the sealer
// ordering it, the executor applying it to the chain-state predeploy, and the
// protocol changing behaviour from a specific block onward — with the
// validator independently agreeing about which block that was.

/// The stack these scenarios need: an L1 to send upgrade transactions to, a
/// validator to prove activation parity, and the genesis carrying the
/// `KardamomChainState` predeploy.
fn upgrade_stack_config() -> StackConfig {
    StackConfig {
        l1: true,
        validator: true,
        genesis: e2e::harness::Genesis::DevWithdrawals,
        ..StackConfig::default()
    }
}

/// S13a: an upgrade transaction with no activation timestamp turns the feature
/// on immediately — from the very block that carried it — and it keeps firing
/// in every block after that, on the executor and the validator alike.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s13a_health_check_activates_immediately() {
    let stack = launch_l1_or_skip!(upgrade_stack_config());
    let t = target(&stack);
    let l1 = stack.l1().expect("l1");
    let exec_dir = stack.executor_state_dir().expect("executor state dir");
    let val_dir = stack.validator_state_dir().expect("validator state dir");

    upgrade::activates_immediately(&t, l1, &exec_dir, &val_dir)
        .await
        .expect("S13a");
}

/// S13b: an upgrade transaction carrying a future activation timestamp is
/// SCHEDULED, not applied — the feature stays dormant until the chain's own
/// clock reaches it, then fires from the first block at or after it.
///
/// Guards the unit trap as much as the semantics: `l2_timestamp` is
/// epoch-MILLISECONDS, so the scenario anchors its schedule to the chain's own
/// head timestamp rather than to wall-clock seconds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s13b_health_check_activates_at_timestamp() {
    let stack = launch_l1_or_skip!(upgrade_stack_config());
    let t = target(&stack);
    let l1 = stack.l1().expect("l1");
    let exec_dir = stack.executor_state_dir().expect("executor state dir");
    let val_dir = stack.validator_state_dir().expect("validator state dir");

    upgrade::activates_at_timestamp(&t, l1, &exec_dir, &val_dir)
        .await
        .expect("S13b");
}

/// S13c: the two authority gates hold. An L1 account that is not the factory
/// owner cannot emit an upgrade transaction, and an ordinary L2 transaction
/// cannot write the flag store directly — the predeploy rejects any sender but
/// the derivation pipeline's system address.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s13c_upgrade_authority_is_enforced() {
    let stack = launch_l1_or_skip!(upgrade_stack_config());
    let t = target(&stack);
    let l1 = stack.l1().expect("l1");
    let exec_dir = stack.executor_state_dir().expect("executor state dir");

    upgrade::authority_is_enforced(&t, l1, &exec_dir, upgrade::Params::default())
        .await
        .expect("S13c");
}
