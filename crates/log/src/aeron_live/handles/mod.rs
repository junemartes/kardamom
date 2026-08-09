//! Typed per-channel publisher/subscriber handle pairs over
//! [`AeronRuntime`](super::AeronRuntime). Every handle is `Send` (publishers
//! also `Sync`) and wraps either a [`PubHandle`](super::PubHandle) or a typed
//! subscription receiver; all are re-exported from `aeron_live`.

pub mod simple;
pub mod tx_data;
pub mod tx_receipts;
