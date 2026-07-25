//! Chain-semantics suite, Target L: the scenario drivers bound to the local
//! stack (`docs/agents/chain-semantics-e2e-suite-spec.md`).
//!
//! Gated on the `full-pipeline-e2e` feature AND `#[ignore]`. Prerequisites
//! (`just test-e2e-local` handles all of them):
//!
//! - service binaries built: `cargo build -p kardamom-ingress -p
//!   kardamom-sequencer -p kardamom-executor --bins`
//! - the sealer jar: `just cluster-jar`
//! - the aeron-all jar cached: `just aeron-driver-up` once (any state; the
//!   harness spawns its own drivers)
//!
//! Each test brings up its own stack on OS-assigned ports, so the file runs
//! under the default parallel test runner; keep `--test-threads=2` on
//! constrained CI runners (each stack is 2 JVMs + 4 service processes).

#![cfg(feature = "full-pipeline-e2e")]

use std::time::Duration;

use e2e::harness::services::IngressOptions;
use e2e::harness::{LocalStack, StackConfig};
use e2e::scenarios::{
    bridge, consistency, crash_recovery, divergence, nonce_gap, nonce_unordered, rpc_liveness,
};

/// Client request bound, kept above every server-side park bound used here.
fn client_timeout(park: Duration) -> Duration {
    park * 3 + Duration::from_secs(5)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s3_nonces_unordered_all_land() {
    let stack = LocalStack::launch(StackConfig::default())
        .await
        .expect("stack");
    let t = stack
        .target(client_timeout(Duration::from_secs(30)))
        .expect("target");
    nonce_unordered::run(&t, nonce_unordered::Params::default())
        .await
        .expect("S3");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s4_nonce_gap_is_never_processed() {
    let park = Duration::from_secs(4);
    let stack = LocalStack::launch(StackConfig {
        ingress: IngressOptions {
            pending_receipt_timeout: park,
            ..IngressOptions::default()
        },
        ..StackConfig::default()
    })
    .await
    .expect("stack");
    let t = stack.target(client_timeout(park)).expect("target");
    nonce_gap::run(&t, nonce_gap::Params::default())
        .await
        .expect("S4");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s5_rpc_endpoints_never_hang() {
    let park = Duration::from_secs(5);
    let stack = LocalStack::launch(StackConfig {
        ingress: IngressOptions {
            pending_receipt_timeout: park,
            ..IngressOptions::default()
        },
        ..StackConfig::default()
    })
    .await
    .expect("stack");
    let t = stack.target(client_timeout(park)).expect("target");
    rpc_liveness::run(&t, rpc_liveness::Params::default())
        .await
        .expect("S5");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s5_connection_cap_refusal_is_prompt() {
    let park = Duration::from_secs(5);
    let cap = 4usize;
    let stack = LocalStack::launch(StackConfig {
        ingress: IngressOptions {
            pending_receipt_timeout: park,
            rpc_max_connections: cap as u32,
        },
        ..StackConfig::default()
    })
    .await
    .expect("stack");
    let url = stack.target(client_timeout(park)).expect("target").rpc.url;
    rpc_liveness::connection_cap_refusal(&url, e2e::harness::DEV_CHAIN_ID, cap, park, 1)
        .await
        .expect("S5 cap");
}

/// Regression test for the #81 pending-registry leak (fixed in #91, the
/// Weak-indexed registry): client-aborted parked submits must leave no
/// registry entries behind — queue depth returns to baseline once the park
/// bound passes. Shipped as an env-gated known-failure canary until the fix
/// landed; runs unconditionally now.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s5_queue_depth_recovers_after_client_aborts() {
    let park = Duration::from_secs(4);
    let stack = LocalStack::launch(StackConfig {
        ingress: IngressOptions {
            pending_receipt_timeout: park,
            ..IngressOptions::default()
        },
        ..StackConfig::default()
    })
    .await
    .expect("stack");
    let t = stack.target(client_timeout(park)).expect("target");
    rpc_liveness::queue_depth_canary(&t, 1, 3)
        .await
        .expect("S5 canary");
}

/// S6: the validator independently re-executes everything the executor
/// executes, verifies it against the executor's streams — and after a
/// graceful shutdown, the two libmdbx databases hold byte-identical chain
/// state (plus both pass the `kardamom_state::integrity` sweep, run both as
/// a library and through the `kardamom-statecheck` binary).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s6_validator_matches_executor() {
    let mut stack = LocalStack::launch(StackConfig {
        validator: true,
        ..StackConfig::default()
    })
    .await
    .expect("stack");
    let t = stack
        .target(client_timeout(Duration::from_secs(30)))
        .expect("target");
    consistency::run(&t, consistency::Params::default())
        .await
        .expect("S6 live phase");

    stack.shutdown_graceful().await.expect("graceful shutdown");
    let exec_dir = stack.executor_state_dir().expect("executor state dir");
    let val_dir = stack.validator_state_dir().expect("validator state dir");
    consistency::verify_state_dirs(&exec_dir, &val_dir).expect("S6 offline phase");

    // Same verdict through the operational CLI.
    let statecheck = e2e::harness::services::bin("kardamom-statecheck").expect("statecheck bin");
    let out = std::process::Command::new(statecheck)
        .arg(&exec_dir)
        .arg("--compare")
        .arg(&val_dir)
        .output()
        .expect("run statecheck");
    assert!(
        out.status.success(),
        "kardamom-statecheck failed:\n{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// S7: prove the divergence tripwire actually fires — a corrupt BAL on the
/// real tx_bal channel must halt the validator with the documented exit 2.
/// Closes the docs/failure-modes.md "divergence injection" gap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s7_corrupt_bal_halts_validator() {
    let mut stack = LocalStack::launch(StackConfig {
        validator: true,
        ..StackConfig::default()
    })
    .await
    .expect("stack");
    let t = stack
        .target(client_timeout(Duration::from_secs(30)))
        .expect("target");
    divergence::corrupt_bal_halts_validator(&mut stack, &t)
        .await
        .expect("S7");
}

/// S1: bridge a deposit in — `depositETH` on L1 surfaces on L2 as a receipt
/// keyed by the OP-style source_hash, and the minted account can SPEND the
/// funds (the ingress serves no `eth_getBalance`, so behaviour is the proof).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s1_bridge_deposit_round_trip() {
    let Some(stack) = LocalStack::launch_opt(StackConfig {
        l1: true,
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
    let l1 = stack.l1().expect("l1");
    bridge::deposit_round_trip(&t, l1, bridge::DepositParams::default())
        .await
        .expect("S1");
}

/// S2: bridge a withdrawal out — `initiateWithdrawal` on the L2 predeploy,
/// the validator's attester posts the output root, then (after the
/// finalization window) a test-built Merkle proof finalizes it on L1 and the
/// recipient is paid. Replaying the same withdrawal must revert.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack + anvil; run via `just test-e2e-local` or with --ignored"]
async fn s2_bridge_withdrawal_round_trip() {
    let Some(stack) = LocalStack::launch_opt(StackConfig {
        l1: true,
        validator: true,
        genesis: e2e::harness::Genesis::DevWithdrawals,
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
    let val_dir = stack.validator_state_dir().expect("validator state dir");

    // L2 half first — the chain must be LIVE for the withdrawal to be sealed.
    let ticket = bridge::initiate_withdrawal(
        &t,
        stack.l1().expect("l1"),
        bridge::WithdrawalParams::default(),
    )
    .await
    .expect("S2 initiate");

    // NOTE: deliberately no freeze here. The withdrawal's receipt appears
    // when the tx executes, but its block commits only at the next sealer
    // boundary — freezing on receipt strands it in an uncommitted block. The
    // chain quiesces by itself (empty boundaries do not commit), so the
    // validator's head root settles on the withdrawal's block.
    let l1 = stack.l1().expect("l1");
    bridge::finalize_withdrawal(l1, ticket, &val_dir)
        .await
        .expect("S2 finalize");
}

/// S9b: an unclean executor death (SIGKILL) must not corrupt or lose state —
/// the restarted process resumes from its persisted cursor instead of
/// re-syncing from genesis, the chain keeps working, and its DB still matches
/// the validator's (which never restarted) byte for byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s9_executor_crash_recovery_is_consistent() {
    let mut stack = LocalStack::launch(StackConfig {
        validator: true,
        // Crash recovery REQUIRES the durability archive: the restarted
        // executor replays canonical records from its persisted cursor, and
        // the envelopes for those records were published while it was down.
        archive_durability: true,
        ..StackConfig::default()
    })
    .await
    .expect("stack");
    let t = stack
        .target(client_timeout(Duration::from_secs(30)))
        .expect("target");
    let params = crash_recovery::Params::default();

    let pre_crash_block = crash_recovery::phase_before_crash(&t, &params)
        .await
        .expect("S9b pre-crash phase");

    stack.crash_executor();
    stack.restart_executor().expect("restart executor");

    crash_recovery::phase_after_restart(&t, &params, pre_crash_block)
        .await
        .expect("S9b post-restart phase");

    // Resume, not genesis re-sync: the restarted process says so explicitly.
    let log = stack.restarted_executor_log().unwrap_or_default();
    assert!(
        log.contains("resuming from persisted state cursor"),
        "restarted executor did not resume from its cursor; log tail:\n{}",
        log.lines().rev().take(15).collect::<Vec<_>>().join("\n")
    );

    // And the state survived the crash intact.
    stack.shutdown_graceful().await.expect("graceful shutdown");
    let exec_dir = stack.executor_state_dir().expect("executor state dir");
    let val_dir = stack.validator_state_dir().expect("validator state dir");
    consistency::verify_state_dirs(&exec_dir, &val_dir).expect("post-crash state comparison");
}
