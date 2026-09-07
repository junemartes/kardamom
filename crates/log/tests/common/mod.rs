//! Shared fixture builders for this crate's integration tests. Not part
//! of the library: `alloy-primitives`/`bytes` are dev-dependencies only
//! (see `crates/log/Cargo.toml`), so these helpers cannot live in
//! `kardamom_log::testing`. `cargo test` builds each `tests/*.rs` file as
//! its own binary, so this file must live under `tests/common/` (not
//! `tests/`) to avoid becoming its own no-op test binary; each file that
//! uses it adds `mod common;`.

#![allow(dead_code)] // Not every test file in this crate uses every helper.

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_types::{BPosition, TxEnvelope, TxRef};

/// A `TxEnvelope` built from one correlation id and one fill byte:
/// `sender` and `tx_hash` repeat `fill`, and `raw_tx` is `raw_len` bytes
/// of `fill`. Callers that only care about correlation-id/position
/// bookkeeping (not the exact payload size) pass 32; the docker-e2e
/// tests each pin their own historical length (48 or 64), since an MTU-
/// or timing-sensitive test's payload size is part of what it pins.
pub fn tx_envelope(correlation_id: u64, fill: u8, raw_len: usize) -> TxEnvelope {
    TxEnvelope {
        correlation_id,
        raw_tx: Bytes::from(vec![fill; raw_len]),
        sender: Address::repeat_byte(fill),
        tx_hash: B256::repeat_byte(fill),
    }
}

/// A `TxRef` for `shard_id`'s `tx_data` entry at `pos`, with `tx_hash`
/// zeroed and `tx_data_session_id` 0 (single-publisher fixtures).
pub fn tx_ref(shard_id: u8, pos: BPosition) -> TxRef {
    TxRef {
        tx_hash: B256::ZERO,
        shard_id,
        tx_data_position: pos,
        tx_data_session_id: 0,
    }
}
