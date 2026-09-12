//! Chain-semantics suite, `Target` L: the scenario drivers bound to the
//! local stack.
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

use e2e::harness::services::{IngressOptions, ParkTimeout};
use e2e::harness::{LocalStack, StackConfig};
use e2e::scenarios::{
    bridge, consistency, crash_recovery, da_parity, derivation, divergence, nonce_gap,
    nonce_unordered, resize, rpc_liveness, rpc_vectors, sequencer_restart, upgrade,
};

/// The two pending-receipt park bounds every tuned-park test in this
/// suite uses. All of `ParkTimeout`'s constructors are `const fn`, so
/// these are computed once at compile time instead of repeated at every
/// call site.
const PARK_4S: ParkTimeout = ParkTimeout::from_secs(std::num::NonZeroU64::new(4).unwrap());
const PARK_5S: ParkTimeout = ParkTimeout::from_secs(std::num::NonZeroU64::new(5).unwrap());

/// The client request bound. It stays above every server park bound
/// used here.
fn client_timeout(park: Duration) -> Duration {
    // `park` comes from each test's own StackConfig; `Mul`/`Add` panic on
    // overflow, so a patient bound saturates instead.
    park.saturating_mul(3)
        .saturating_add(Duration::from_secs(5))
}

/// The scenario seam for a stack running the default 30 s ingress park
/// bound. Tests that tune the park bound derive their own
/// `client_timeout(park)`.
fn target(stack: &LocalStack) -> e2e::scenarios::Target {
    stack
        .target(client_timeout(Duration::from_secs(30)))
        .expect("target")
}

/// Launch a stack with `park` as its ingress pending-receipt timeout, and
/// build the matching `Target` (client timeout above the server park).
/// `cfg`'s own `ingress.pending_receipt_timeout` is overwritten with
/// `park`, so callers set every other field they need and leave that one
/// out.
async fn launch_with_park(
    park: e2e::harness::services::ParkTimeout,
    mut cfg: StackConfig,
) -> (LocalStack, e2e::scenarios::Target) {
    cfg.ingress.pending_receipt_timeout = park;
    let stack = LocalStack::launch(cfg).await.expect("stack");
    let t = stack
        .target(client_timeout(park.as_duration()))
        .expect("target");
    (stack, t)
}

/// Make a temp DA dir, open an `FsBlobStore` in it, post `blocks` to L1 as
/// real blob transactions, then require the posted batch log to match.
/// Returns the temp dir (the caller must keep it alive so
/// `kardamom-reconstruct` can read the blobs back from disk later);
/// `FsBlobStore` is a thin `PathBuf` wrapper with no `Drop`, so nothing
/// needs to keep the store itself alive. `what` names the case, for the
/// panic messages.
async fn post_and_verify_da(
    l1: &e2e::harness::l1::L1,
    blocks: &[kardamom_batcher::batch::ClosedBlock],
    what: &str,
) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("da dir");
    let store = kardamom_batcher::da_store::FsBlobStore::open(dir.path()).expect("da store");
    da_parity::post_to_l1(l1, l1.settlement, blocks, &store)
        .await
        .unwrap_or_else(|e| panic!("{what} post to L1: {e:?}"));
    da_parity::assert_batches_on_l1(l1, l1.settlement, blocks.len(), &store)
        .await
        .unwrap_or_else(|e| panic!("{what} L1 batch log: {e:?}"));
    dir
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
include!("xchain.rs");
