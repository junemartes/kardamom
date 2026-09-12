//! The container cluster chaos suite.
//!
//! The suite brings the cluster up through the same commands as the
//! `container-up` Make target (`OpenTofu` for the node containers, Ansible
//! for the substrate, the images, and the workloads), then runs failure
//! cases under a steady load and checks that the pipeline recovers
//! within its SLOs and that every accepted transaction gets a receipt.
//!
//! The cluster runs Docker-in-Docker: each node is a privileged container
//! `kardamom-<class>-<i>` with its own dockerd, and the pipeline services
//! are inner Nomad docker-driver tasks. So the suite has three failure
//! surfaces:
//!
//! - graceful kill: `nomad alloc stop`, through the Nomad HTTP API;
//! - hard crash: `docker exec <node> docker kill <inner>`;
//! - node failure: `docker kill <node>`, the whole node.
//!
//! Every read-only probe and every assertion encodes one rule the bash
//! suite learned the hard way: a failed scrape is never a zero, a
//! recovery needs evidence of replacement and not only a count, and a
//! freeze or a throttle is verified before the case goes on.
//!
//! The shard tests under `tests/` are gated on the `cluster-e2e` feature
//! and marked `#[ignore]`.

pub mod accounts;
pub mod asserts;
pub mod cases;
pub mod contract;
pub mod evidence;
pub mod harness;
pub mod inject;
pub mod knobs;
pub mod lifecycle;
pub mod load;
pub mod metrics;
pub mod nodes;
pub mod nomad;
pub mod poll;
pub mod probes;
pub mod rpc;
pub mod shard;

pub use contract::{Node, NodeContract};
pub use harness::Harness;
pub use knobs::Knobs;
pub use lifecycle::{DeployVars, Lifecycle};
pub use shard::Shard;

/// Print one progress line, in the `==> ...` shape the CI logs grep for.
pub fn log(msg: impl std::fmt::Display) {
    println!("==> {msg}");
}

/// The prefix of every failure line. A case failure is an `anyhow` error
/// whose message starts with it, so the CI log keeps its contract.
pub const FAIL_PREFIX: &str = "CHAOS FAIL";

/// Build the failure error of a case.
#[macro_export]
macro_rules! chaos_fail {
    ($($arg:tt)*) => {
        ::anyhow::anyhow!("{}: {}", $crate::FAIL_PREFIX, format!($($arg)*))
    };
}
