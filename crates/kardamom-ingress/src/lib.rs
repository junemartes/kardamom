//! Ingress proxy (S1).
//!
//! Terminates client connections (JSON-RPC over HTTP+WS plus an optional
//! length-prefixed RLP TCP/UDS binary line protocol), batches secp256k1
//! sender recovery, partitions tx publication on `ingress[keccak(sender) % M]`,
//! parks responses until both the quorum fsync watermark advances past the
//! tx's B-position and a matching receipt arrives on the receipt-cache
//! channel, and answers retries from an in-memory receipt cache.
//!
//! The proxy is **the only place** that computes either `TxEnvelope.sender`
//! or `TxEnvelope.tx_hash` (per S0 D-Sh3 + D-Sh4). Both are produced together
//! at the sig-verify boundary (ECDSA recovery + a single `keccak256(raw_tx)`
//! pass) and published into the envelope before any downstream consumer
//! observes the tx. Downstream code may trust both fields unconditionally.
//!
//! Stateless w.r.t. canonical truth — adding or removing a proxy is safe at
//! any time.

pub mod binary;
pub mod channels;
pub mod config;
pub mod error;
pub mod json_rpc;
pub mod pending;
pub mod proxy;
pub mod rate_limit;
pub mod receipt_cache;
pub mod routing;
pub mod sig_verify;

pub use channels::{InMemoryStateDb, IngressPublication, IngressSubscription, MockChannels};
pub use config::IngressConfig;
pub use error::IngressError;
pub use proxy::{IngressHandle, IngressProxy};
