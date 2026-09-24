//! Typed per-channel publisher/subscriber handle pairs over
//! [`AeronRuntime`](super::AeronRuntime). Every handle is `Send`
//! (publishers are also `Sync`) and wraps either a
//! [`PubHandle`](super::PubHandle) or a typed subscription receiver. All
//! are re-exported from `aeron_live`.

pub(super) mod simple;
pub(super) mod tx_data;
pub(super) mod tx_receipts;
