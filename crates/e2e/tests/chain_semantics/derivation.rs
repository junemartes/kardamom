// S10a-e + S11 — L1-origin deposit derivation. Included from main.rs (see
// the header there); shared helpers live in main.rs.
//
// The rules these assert live in
// docs/agents/l1-origin-deposit-derivation-spec.md. They are asserted against
// persisted headers and executed receipts, not logs: a component can log the
// right thing and still build the wrong chain.

/// S10b/c/d differ only in the derivation scenario they drive: same L1-backed
/// stack, same `(target, l1, executor state dir)` hand-off. Skips (early
/// return) when anvil is absent.
async fn s10_deposit_case<F>(scenario: F, what: &str)
where
    F: AsyncFn(
        &e2e::scenarios::Target,
        &e2e::harness::l1::L1,
        &std::path::Path,
    ) -> anyhow::Result<()>,
{
    let stack = launch_l1_or_skip!(StackConfig {
        l1: true,
        ..StackConfig::default()
    });
    let t = target(&stack);
    let l1 = stack.l1().expect("l1");
    let state_dir = stack.executor_state_dir().expect("executor state dir");
    scenario(&t, l1, &state_dir).await.expect(what);
}

/// S10a: the L1 origin advances, monotonically and without skipping, over a
/// stretch of L1 that carries NO deposits. An idle L1 is exactly where it is
/// tempting to emit nothing, and the no-skipping rule is what that breaks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s10a_origin_advances_over_an_idle_l1() {
    let stack = launch_l1_or_skip!(StackConfig {
        l1: true,
        ..StackConfig::default()
    });
    let l1 = stack.l1().expect("l1");
    let state_dir = stack.executor_state_dir().expect("executor state dir");
    derivation::origin_advances_over_an_idle_l1(l1, &state_dir)
        .await
        .expect("S10a");
}

/// S10b: an epoch's deposits LEAD the block they open, even while L2 traffic
/// is flowing. Under interleaving, "deposits come first" can only hold
/// because the epoch is atomic and forces a boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s10b_deposits_lead_their_block_under_load() {
    s10_deposit_case(derivation::deposits_lead_their_block_under_load, "S10b").await;
}

/// S10c: several deposits in ONE L1 block all land in ONE L2 block, in L1
/// log order — the grouping the atomic epoch record exists to guarantee.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s10c_multi_deposit_epoch_lands_in_log_order() {
    s10_deposit_case(derivation::multi_deposit_epoch_lands_in_log_order, "S10c").await;
}

/// S10d: a stalled L1 delays deposits but must not stall L2. This is the
/// difference between the bounded-lag rule being a liveness ALARM and a
/// liveness FAILURE.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s10d_stalled_l1_does_not_stall_l2() {
    s10_deposit_case(derivation::stalled_l1_does_not_stall_l2, "S10d").await;
}

/// S10e: every deposit L1 recorded appears on L2 exactly once. Derives the
/// expected set from L1 itself — the property a verifier will enforce in
/// phase D, asserted here against the producer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s10e_every_l1_deposit_appears_exactly_once() {
    let stack = launch_l1_or_skip!(StackConfig {
        l1: true,
        ..StackConfig::default()
    });
    let t = target(&stack);
    let l1 = stack.l1().expect("l1");
    let state_dir = stack.executor_state_dir().expect("executor state dir");

    // Give the chain something to find: two deposits in separate L1 blocks.
    let signers = e2e::harness::l2::dev_signers(4).expect("signers");
    for signer in &signers[2..4] {
        l1.deposit_eth(
            signer.address,
            alloy_primitives::U256::from(2_000_000_000_000_000u64),
        )
        .await
        .expect("depositETH");
        l1.mine(3).await.expect("mine");
    }
    l1.mine(6).await.expect("mine past finality");

    derivation::every_l1_deposit_appears_exactly_once(&t, l1, &state_dir)
        .await
        .expect("S10e");
}

/// S11: a forged epoch must halt the validator. S10a-e prove an honest
/// producer builds a derivable chain; this proves the guarantee has teeth —
/// an epoch L1 never produced is rejected rather than committed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s11_forged_epoch_halts_validator() {
    let stack = launch_l1_or_skip!(StackConfig {
        l1: true,
        validator: true,
        ..StackConfig::default()
    });
    let t = target(&stack);
    let l1 = stack.l1().expect("l1");
    divergence::forged_epoch_halts_validator(&stack, &t, l1)
        .await
        .expect("S11");
}

// ---------------------------------------------------------------------------
// S12 — the validator's L1 view comes through an interposed endpoint.
//
// Production points --l1-rpc-url at a light client rather than a raw RPC
// (issue #163). The real client needs a beacon chain and our L1 is anvil, so
// it cannot run here; what these cover is the contract between the validator
// and whatever serves it L1 data — including that a lying server is REJECTED.
//
// They do NOT cover helios's cryptography. A green run here says nothing
// about sync-committee verification or Merkle proofs.
// ---------------------------------------------------------------------------

/// S12a: a FAITHFUL interposed endpoint must not disturb verification. The
/// baseline — without it the lying cases below could pass for the wrong reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s12a_verified_l1_passthrough_still_verifies() {
    run_verified_l1_case(e2e::harness::l1_verified::Fault::None).await;
}

/// S12b: an endpoint serving a WRONG BLOCK HASH must halt the validator.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s12b_verified_l1_wrong_block_hash_halts_validator() {
    run_verified_l1_case(e2e::harness::l1_verified::Fault::WrongBlockHash { from_block: 0 }).await;
}

/// S12c: an endpoint whose blocks each look right but do NOT chain must halt
/// the validator. This is the case only parent-hash chaining catches — every
/// block's own hash is correct, so per-block verification passes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s12c_verified_l1_broken_parent_chain_halts_validator() {
    run_verified_l1_case(e2e::harness::l1_verified::Fault::BrokenParentChain { from_block: 0 })
        .await;
}

/// S12d: an endpoint that SWALLOWS deposit logs must halt the validator —
/// censorship seen from the L1 side rather than the sequencer's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s12d_verified_l1_swallowed_logs_halt_validator() {
    run_verified_l1_case(e2e::harness::l1_verified::Fault::SwallowLogs).await;
}

async fn run_verified_l1_case(fault: e2e::harness::l1_verified::Fault) {
    let Some(mut stack) = LocalStack::launch_opt(StackConfig {
        l1: true,
        validator: true,
        verified_l1: true,
        ..StackConfig::default()
    })
    .await
    .expect("stack") else {
        eprintln!("SKIP: anvil not available");
        return;
    };
    let t = stack
        .target(client_timeout(Duration::from_secs(30)))
        .expect("target");
    divergence::verified_l1_endpoint(&mut stack, &t, fault)
        .await
        .expect("S12");
}
