//! The transaction canary: a small service that uses the chain as a user
//! does, without pause, and reports each success and each failure as a
//! metric.
//!
//! - [`config`]: the command line and the typed settings.
//! - [`rpc`]: one ingress endpoint's JSON-RPC client.
//! - [`ring`]: the canary's L2 accounts, each with one nonce owner and a
//!   journal of its in-flight transaction.
//! - [`feed`]: the notifier's status feed and the board of the stages
//!   it reports.
//! - [`probes`]: the probes, each on its own timer.
//! - [`service`]: the wiring of the parts.

pub mod config;
pub mod contracts;
pub mod feed;
pub mod funds;
pub mod market;
pub mod metrics;
pub mod outcome;
pub mod probes;
pub mod ring;
pub mod rpc;
pub mod service;
pub mod store;
pub mod wait;

#[cfg(test)]
mod format_tests;
