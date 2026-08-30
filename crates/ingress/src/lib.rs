//! Ingress proxy.
//!
//! The proxy ends client connections. It supports JSON-RPC over HTTP and WS.
//! It also supports an optional length-prefixed RLP TCP/UDS binary protocol.
//! The proxy batches secp256k1 sender recovery. It sends each tx to
//! `ingress[keccak(sender) % M]`. It holds each response until the quorum
//! fsync watermark passes the tx's B-position and a matching receipt
//! arrives on the receipt-cache channel. It answers retries from an
//! in-memory receipt cache.
//!
//! The proxy is the only place that computes `TxEnvelope.sender` and
//! `TxEnvelope.tx_hash`. The sig-verify step produces both values together:
//! one ECDSA recovery and one `keccak256(raw_tx)` pass. The proxy writes
//! both fields into the envelope before any other code sees the tx.
//! Downstream code can trust both fields without a check.
//!
//! The proxy holds no canonical state. You can add or remove a proxy at
//! any time.

pub mod aeron_adapters;
pub mod binary;
pub mod channels;
pub mod cluster;
pub mod config;
pub mod error;
pub mod json_rpc;
pub mod metrics;
pub mod pending;
pub mod proxy;
pub mod rate_limit;
pub mod receipt_cache;
pub mod routing;
pub mod seen_receipts;
pub mod sig_verify;
pub mod tx_error_dedup;

pub use channels::{IngressPublication, IngressSubscription, MockChannels};
pub use config::IngressConfig;
pub use error::IngressError;
pub use proxy::{IngressHandle, IngressProxy};
