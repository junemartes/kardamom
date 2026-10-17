//! Egress-direction encoders that mirror the Java service's framing byte
//! for byte. The Java service is the real encoder; tests and the in-Rust
//! service mock use these.

use alloy_primitives::Address;
use kardamom_types::ClusterStatus;
use kardamom_types::cluster_status::RecordLagStatus;

use super::{
    EGRESS_KIND_BOUNDARY, EGRESS_KIND_CONTIGUITY_REJECT, EGRESS_KIND_DA_LAG_REJECT,
    EGRESS_KIND_PAST_DEADLINE, EGRESS_KIND_RELAYED, EGRESS_KIND_REPLAY_DONE,
    EGRESS_KIND_REPLAY_UNAVAILABLE, EGRESS_KIND_STATUS, EGRESS_KIND_WINDOW_FULL, SENDER_LEN,
    WireError, encode_kind_2u64,
};

/// Frame a relayed record exactly as the Java service does. `payload` is the
/// relayed payload (`[canonical_id:32][record_type][fields…]`).
///
/// # Errors
///
/// Returns [`WireError::EntryTooLarge`] if `payload.len()` does not fit
/// in a `u32`.
pub fn encode_egress_record(index: u64, payload: &[u8]) -> Result<Vec<u8>, WireError> {
    let len = u32::try_from(payload.len())
        .map_err(|_| WireError::EntryTooLarge { len: payload.len() })?;
    let mut b = Vec::with_capacity(1 + 8 + 4 + payload.len());
    b.push(EGRESS_KIND_RELAYED);
    b.extend_from_slice(&index.to_le_bytes());
    b.extend_from_slice(&len.to_le_bytes());
    b.extend_from_slice(payload);
    Ok(b)
}

/// Frame a block boundary exactly as the Java service does.
#[must_use]
pub fn encode_egress_boundary(
    block_number: u64,
    end_tx_idx: u64,
    l2_timestamp: u64,
    l1_origin: u64,
) -> Vec<u8> {
    let mut b = Vec::with_capacity(1 + 8 + 8 + 8 + 8);
    b.push(EGRESS_KIND_BOUNDARY);
    b.extend_from_slice(&block_number.to_le_bytes());
    b.extend_from_slice(&end_tx_idx.to_le_bytes());
    b.extend_from_slice(&l2_timestamp.to_le_bytes());
    b.extend_from_slice(&l1_origin.to_le_bytes());
    b
}

/// Frame a replay-unavailable notice exactly as the Java service does.
#[must_use]
pub fn encode_replay_unavailable(oldest_index: u64, oldest_block: u64) -> Vec<u8> {
    encode_kind_2u64(EGRESS_KIND_REPLAY_UNAVAILABLE, oldest_index, oldest_block)
}

/// Frame a replay-done marker exactly as the Java service does.
#[must_use]
pub fn encode_replay_done(up_to_index: u64, up_to_block: u64) -> Vec<u8> {
    encode_kind_2u64(EGRESS_KIND_REPLAY_DONE, up_to_index, up_to_block)
}

/// Frame the client-local session-start marker. Only the session thread
/// sends it, on the consumer channel.
#[must_use]
pub fn encode_session_start() -> Vec<u8> {
    vec![super::EGRESS_KIND_SESSION_START]
}

/// Frame a replay-ahead refusal exactly as the Java service does.
#[must_use]
pub fn encode_replay_ahead(head_index: u64, head_block: u64) -> Vec<u8> {
    encode_kind_2u64(super::EGRESS_KIND_REPLAY_AHEAD, head_index, head_block)
}

/// Frame an origin-gap reject exactly as the Java service does. The real
/// encoder is the Java service; this is a test and mock-server helper.
#[cfg(any(test, feature = "testing"))]
#[must_use]
pub fn encode_origin_gap(offered_origin: u64, expected_origin: u64) -> Vec<u8> {
    encode_kind_2u64(
        super::EGRESS_KIND_ORIGIN_GAP,
        offered_origin,
        expected_origin,
    )
}

/// Frame a contiguity reject exactly as the Java service does. The real
/// encoder is the Java service; this is a test and mock-server helper.
#[cfg(any(test, feature = "testing"))]
#[must_use]
pub fn encode_contiguity_reject(sender: Address, nonce: u64, expected: u64) -> Vec<u8> {
    let mut b = Vec::with_capacity(1 + SENDER_LEN + 8 + 8);
    b.push(EGRESS_KIND_CONTIGUITY_REJECT);
    b.extend_from_slice(sender.as_slice());
    b.extend_from_slice(&nonce.to_le_bytes());
    b.extend_from_slice(&expected.to_le_bytes());
    b
}

/// Frame a past-deadline reject exactly as the Java service does. The
/// real encoder is the Java service; this is a test and mock-server
/// helper.
#[cfg(any(test, feature = "testing"))]
#[must_use]
pub fn encode_past_deadline(
    sender: Address,
    nonce: u64,
    max_inclusion_block: u64,
    at_block: u64,
) -> Vec<u8> {
    let mut b = Vec::with_capacity(1 + SENDER_LEN + 8 + 8 + 8);
    b.push(EGRESS_KIND_PAST_DEADLINE);
    b.extend_from_slice(sender.as_slice());
    b.extend_from_slice(&nonce.to_le_bytes());
    b.extend_from_slice(&max_inclusion_block.to_le_bytes());
    b.extend_from_slice(&at_block.to_le_bytes());
    b
}

/// Frame a window-full reject exactly as the Java service does. The real
/// encoder is the Java service; this is a test and mock-server helper.
#[cfg(any(test, feature = "testing"))]
#[must_use]
pub fn encode_window_full(sender: Address, nonce: u64) -> Vec<u8> {
    let mut b = Vec::with_capacity(1 + SENDER_LEN + 8);
    b.push(EGRESS_KIND_WINDOW_FULL);
    b.extend_from_slice(sender.as_slice());
    b.extend_from_slice(&nonce.to_le_bytes());
    b
}

/// Frame the chain's status exactly as the Java service does. The real
/// encoder is the Java service; this is a test and mock-server helper. A
/// status with no retained bytes gets no flag byte and no byte tail. A
/// status with retained bytes gets the flag byte as 0.
#[cfg(any(test, feature = "testing"))]
#[must_use]
pub fn encode_status(status: &ClusterStatus) -> Vec<u8> {
    let mut b = Vec::with_capacity(super::STATUS_WITH_RETAINED_BYTES_LEN);
    b.push(EGRESS_KIND_STATUS);
    b.extend_from_slice(&status.posted_head.to_le_bytes());
    b.extend_from_slice(&status.sealed_head.to_le_bytes());
    b.extend_from_slice(&status.budget_blocks.to_le_bytes());
    b.push(u8::from(status.halted));
    b.extend_from_slice(&status.retained_frames.to_le_bytes());
    b.extend_from_slice(&status.floor_index.to_le_bytes());
    b.extend_from_slice(&status.floor_block.to_le_bytes());
    let lag = &status.record_lag;
    b.extend_from_slice(&lag.best_recorded.unwrap_or(u64::MAX).to_le_bytes());
    b.extend_from_slice(&lag.budget.to_le_bytes());
    b.push(u8::from(lag.halted));
    b.push(u8::from(status.snapshot_follows));
    if let Some(retained_bytes) = status.retained_bytes {
        b.extend_from_slice(&retained_bytes.to_le_bytes());
    }
    b
}

/// Frame a DA-lag reject exactly as the Java service does. The real
/// encoder is the Java service; this is a test and mock-server helper.
#[cfg(any(test, feature = "testing"))]
#[must_use]
pub fn encode_da_lag_reject(sender: Address, nonce: u64, status: &ClusterStatus) -> Vec<u8> {
    let mut b = Vec::with_capacity(1 + SENDER_LEN + 8 * 4);
    b.push(EGRESS_KIND_DA_LAG_REJECT);
    b.extend_from_slice(sender.as_slice());
    b.extend_from_slice(&nonce.to_le_bytes());
    b.extend_from_slice(&status.sealed_head.to_le_bytes());
    b.extend_from_slice(&status.posted_head.to_le_bytes());
    b.extend_from_slice(&status.budget_blocks.to_le_bytes());
    b
}

/// Frame a record-lag reject exactly as the Java service does. The real
/// encoder is the Java service; this is a test and mock-server helper.
#[cfg(any(test, feature = "testing"))]
#[must_use]
pub fn encode_record_lag_reject(
    sender: Address,
    nonce: u64,
    sealed_index: u64,
    lag: &RecordLagStatus,
) -> Vec<u8> {
    let mut b = Vec::with_capacity(1 + SENDER_LEN + 8 * 4);
    b.push(super::EGRESS_KIND_RECORD_LAG_REJECT);
    b.extend_from_slice(sender.as_slice());
    b.extend_from_slice(&nonce.to_le_bytes());
    b.extend_from_slice(&sealed_index.to_le_bytes());
    b.extend_from_slice(&lag.best_recorded.unwrap_or(u64::MAX).to_le_bytes());
    b.extend_from_slice(&lag.budget.to_le_bytes());
    b
}
