//! Kardamom node library: in-memory EVM execution and JSON-RPC server.

pub mod error;

pub use error::NodeError;

pub mod metrics;

// Genesis lives in `kardamom-types` so the executor bin can load it
// without pulling in the node's heavier deps. Re-exported here under the
// same names + module path so the kardamom top-level binary's existing
// `kardamom_node::{Genesis, ...}` and `kardamom_node::genesis::*`
// imports keep working.
pub use kardamom_types::{AllocEntry, Genesis, GenesisError};

pub mod genesis {
    pub use kardamom_types::{AllocEntry, Genesis, GenesisError};
}

pub mod node;

pub use node::Node;

pub mod executor;

pub mod rpc;

pub use rpc::start_server;

pub mod deposit;
