//! Chain-semantics suite, Target L: the scenario drivers bound to the
//! local stack (`docs/agents/chain-semantics-e2e-suite-spec.md`).
//!
//! This suite is gated on the `full-pipeline-e2e` feature and `#[ignore]`.
//! Prerequisites (`just test-e2e-local` handles all of them):
//!
//! - built service binaries: `cargo build -p kardamom-ingress -p
//!   kardamom-sequencer -p kardamom-executor --bins`
//! - the sealer jar: `just cluster-jar`
//! - the cached aeron-all jar: run `just aeron-driver-up` once (any state
//!   works; the harness spawns its own drivers)
//!
//! Each test brings up its own stack on OS-assigned ports, so the suite
//! runs under the default parallel test runner. Use `--test-threads=2` on
//! constrained CI runners (each stack is 2 JVMs and 4 service processes).
//!
//! The suite is split across sibling files, and pulled in with `include!`
//! instead of `mod`. This way, every test keeps its exact pre-split name
//! (`s7_…`, not `consistency::s7_…`). CI filter strings and historical
//! test names stay valid, and `--test chain_semantics` still names this
//! one target.

#![cfg(feature = "full-pipeline-e2e")]

use std::time::Duration;

use e2e::harness::services::IngressOptions;
use e2e::harness::{LocalStack, StackConfig};
use e2e::scenarios::{
    bridge, consistency, crash_recovery, da_parity, derivation, divergence, nonce_gap,
    nonce_unordered, rpc_liveness, rpc_vectors, upgrade,
};

/// The client request bound. It stays above every server park bound
/// used here.
fn client_timeout(park: Duration) -> Duration {
    park * 3 + Duration::from_secs(5)
}

/// The scenario seam for a stack running the default 30 s ingress park
/// bound. Tests that tune the park bound derive their own
/// `client_timeout(park)`.
fn target(stack: &LocalStack) -> e2e::scenarios::Target {
    stack
        .target(client_timeout(Duration::from_secs(30)))
        .expect("target")
}

/// Launch a stack whose config sets `l1: true`. Skip the test with an
/// early return when the `anvil` binary is absent. Every L1-backed test
/// here follows this convention.
macro_rules! launch_l1_or_skip {
    ($cfg:expr) => {
        match LocalStack::launch_opt($cfg).await.expect("stack") {
            Some(stack) => stack,
            None => {
                eprintln!("SKIP: anvil not available");
                return;
            }
        }
    };
}

/// Run a service binary to completion, require success, and return stdout.
fn run_bin_ok(cmd: &mut std::process::Command) -> String {
    let out = cmd.output().expect("run binary");
    assert!(
        out.status.success(),
        "{cmd:?} failed:\n{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

include!("pipeline.rs");
include!("consistency.rs");
include!("bridge_da.rs");
include!("derivation.rs");
include!("upgrades.rs");
