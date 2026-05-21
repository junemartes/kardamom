//! Kardamom node library: in-memory EVM execution and JSON-RPC server.

pub mod error;

pub use error::NodeError;

pub mod metrics;

pub mod genesis;

pub use genesis::{AllocEntry, Genesis};

pub mod node;

pub use node::Node;

pub mod executor;

pub mod simulate;

pub(crate) mod transfers;

pub mod rpc;

pub use rpc::start_server;
