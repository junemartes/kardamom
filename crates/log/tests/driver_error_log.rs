//! The error log of a media driver, as a failed test prints it.
//!
//! A subscription holds a UDP port, and a publication then asks for the
//! same port as its control port. The add call returns, but the bind of
//! the driver fails: only the error log of the driver records it. A child
//! run of this binary panics while it holds a [`DriverErrorLog`], and its
//! stderr must carry that bind failure.
//!
//! Gated on the `docker-e2e` feature and on Docker availability.

#![cfg(feature = "docker-e2e")]

use std::process::Command;
use std::time::{Duration, Instant};

use kardamom_log::testing::{AeronTestCluster, DriverErrorLog, SingleNodeRig};
use kardamom_obs::testkit::PortBlock;

const STREAM: i32 = 4701;
/// The `aeron.dir` that the child run reads. The child runs only when
/// the parent sets it.
const CHILD_AERON_DIR: &str = "KARDAMOM_DRIVER_ERROR_LOG_CHILD_DIR";
const CHILD: &str = "a_test_that_panics_while_it_holds_the_error_log";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run with `cargo test -p kardamom-log --features docker-e2e --test driver_error_log -- --ignored`"]
async fn a_failed_test_prints_the_bind_failure_of_its_driver() {
    kardamom_log::testing::require_docker().await;
    let SingleNodeRig { cluster, rt, .. } = AeronTestCluster::single_node_runtime(4700).await;
    let aeron_dir = cluster.aeron_dir_host(0).to_path_buf();
    let endpoint = format!("127.0.0.1:{}", PortBlock::DriverErrorLog.port(0));
    let _subscription = rt
        .open_subscription_raw(&format!("aeron:udp?endpoint={endpoint}"), STREAM)
        .expect("the subscription binds the port");
    let _publication = rt.open_publication(
        &format!("aeron:udp?control={endpoint}|control-mode=dynamic"),
        STREAM + 1,
    );
    let log = DriverErrorLog::new(&aeron_dir);
    let deadline = Instant::now() + Duration::from_secs(10);
    while !log.report().contains(&endpoint) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let report = log.report();
    assert!(
        report.contains(&endpoint),
        "no error names {endpoint}: {report}"
    );
    println!("{report}");

    let child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", CHILD, "--ignored", "--nocapture"])
        .env(CHILD_AERON_DIR, &aeron_dir)
        .output()
        .expect("the child run starts");
    let stderr = String::from_utf8_lossy(&child.stderr);
    assert!(!child.status.success(), "the child run fails: {stderr}");
    assert!(
        stderr.contains("==> driver error log of")
            && stderr.contains(&endpoint)
            && stderr.contains("Address already in use"),
        "the failed child prints the error log of the driver: {stderr}"
    );
}

/// The child run of the test above: it fails while it holds the error
/// log of the driver of the parent.
#[test]
#[ignore = "the child run of a_failed_test_prints_the_bind_failure_of_its_driver"]
fn a_test_that_panics_while_it_holds_the_error_log() {
    let Ok(aeron_dir) = std::env::var(CHILD_AERON_DIR) else {
        return;
    };
    let _log = DriverErrorLog::new(aeron_dir);
    panic!("the child run fails while it holds the error log");
}
