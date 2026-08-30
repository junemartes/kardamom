// Executor and validator consistency, the divergence tripwire, crash
// recovery, and the Target-C runner pre-flight. This file is included
// from main.rs (see the header there). Shared helpers live in main.rs.

/// S6: the validator re-executes everything the executor executes, on
/// its own, and verifies it against the executor's streams. After a
/// graceful shutdown, the two libmdbx databases hold byte-identical
/// chain state. Both also pass the `kardamom_state::integrity` sweep, run
/// both as a library and through the `kardamom-statecheck` binary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s6_validator_matches_executor() {
    let mut stack = LocalStack::launch(StackConfig {
        validator: true,
        ..StackConfig::default()
    })
    .await
    .expect("stack");
    let t = target(&stack);
    consistency::run(&t, consistency::Params::default())
        .await
        .expect("S6 live phase");

    stack.shutdown_graceful().await.expect("graceful shutdown");
    let exec_dir = stack.executor_state_dir().expect("executor state dir");
    let val_dir = stack.validator_state_dir().expect("validator state dir");
    consistency::verify_state_dirs(&exec_dir, &val_dir).expect("S6 offline phase");

    // Same verdict through the operational CLI.
    let statecheck = e2e::harness::services::bin("kardamom-statecheck").expect("statecheck bin");
    run_bin_ok(
        std::process::Command::new(statecheck)
            .arg(&exec_dir)
            .arg("--compare")
            .arg(&val_dir),
    );
}

/// S7: prove the divergence tripwire actually fires. A corrupt BAL on
/// the real tx_bal channel must halt the validator with the documented
/// exit code 2. This closes the docs/failure-modes.md "divergence
/// injection" gap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s7_corrupt_bal_halts_validator() {
    let mut stack = LocalStack::launch(StackConfig {
        validator: true,
        ..StackConfig::default()
    })
    .await
    .expect("stack");
    let t = target(&stack);
    divergence::corrupt_bal_halts_validator(&mut stack, &t)
        .await
        .expect("S7");
}

/// An unclean executor death (SIGKILL) must not corrupt or lose
/// state. The restarted process resumes from its persisted cursor
/// instead of re-syncing from genesis, the chain keeps working, and its
/// database still matches the validator's (which never restarted) byte
/// for byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn s9_executor_crash_recovery_is_consistent() {
    let mut stack = LocalStack::launch(StackConfig {
        validator: true,
        // Crash recovery requires the durability archive. The restarted
        // executor replays canonical records from its persisted cursor, and
        // those records were published as envelopes while it was down.
        archive_durability: true,
        ..StackConfig::default()
    })
    .await
    .expect("stack");
    let t = target(&stack);
    let params = crash_recovery::Params::default();

    let pre_crash_block = crash_recovery::phase_before_crash(&t, &params)
        .await
        .expect("S9b pre-crash phase");

    stack.crash_executor();
    stack.restart_executor().expect("restart executor");

    crash_recovery::phase_after_restart(&t, &params, pre_crash_block)
        .await
        .expect("S9b post-restart phase");

    // The restarted process resumed, instead of re-syncing from genesis.
    // It states this explicitly in its log.
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

/// The Target-C runner, exercised at Target L: launch a local stack and
/// drive it through the real `kardamom-semantics` binary. This is the
/// same binary the cluster-e2e `semantics` shard invokes, with the same
/// arguments.
///
/// This is the pre-flight check for Target C. The cluster shard costs
/// about 40 minutes per attempt and only reports a log dump. Proving the
/// CLI's wiring here (argument parsing, `Target` construction, case
/// dispatch, exit code) means a shard failure points at the cluster
/// environment, not at the runner.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run via `just test-e2e-local` or with --ignored"]
async fn target_c_runner_drives_the_stack() {
    let park = Duration::from_secs(4);
    let stack = LocalStack::launch(StackConfig {
        validator: true,
        ingress: IngressOptions {
            pending_receipt_timeout: park,
            ..IngressOptions::default()
        },
        ..StackConfig::default()
    })
    .await
    .expect("stack");
    let t = stack.target(client_timeout(park)).expect("target");

    let bin = e2e::harness::services::bin("kardamom-semantics").expect("semantics bin");
    let stdout = run_bin_ok(
        std::process::Command::new(bin)
            .args(["--rpc", &t.rpc.url])
            .args(["--chain-id", &e2e::harness::DEV_CHAIN_ID.to_string()])
            .args(["--executor-metrics", &t.executor_metrics.to_string()])
            .args([
                "--sequencer-metrics",
                &t.sequencer_metrics
                    .iter()
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ])
            .args([
                "--validator-metrics",
                &t.validator_metrics.expect("validator").to_string(),
            ])
            .args([
                "--pending-receipt-timeout-ms",
                &park.as_millis().to_string(),
            ])
            // A representative slice: one nonce case and the consistency case.
            // The cluster shard runs the full set. This just proves the
            // plumbing.
            .args(["--cases", "nonce-unordered,consistency"]),
    );
    assert!(
        stdout.contains("semantics verdict PASS"),
        "no verdict line in:\n{stdout}"
    );
}
