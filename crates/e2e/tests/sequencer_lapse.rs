//! `sequencer-lapse` on the local stack: the real sequencer process
//! freezes past the client liveness of the media driver, exits through
//! its fail-fast handler on the thaw, and returns cold. See
//! `e2e::scenarios::recovery::lapse`.
//!
//! Prerequisites, as for the pipeline-recovery suite: the service
//! binaries, the sealer jar, the cached aeron-all jar.

#![cfg(feature = "full-pipeline-e2e")]

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "full local stack; run the sequencer-lapse component target"]
async fn a_frozen_sequencer_exits_on_the_client_timeout_and_returns_cold_with_fresh_work() {
    e2e::scenarios::recovery::lapse::run_lapse().await;
}
