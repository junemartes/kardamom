//! KAR1 framing: the on-blob payload format.
//!
//! The format carries no `state_root` field. After framing, zstd can
//! compress the whole payload (flag bit 0 set). The result is then sliced
//! into 31-byte field-element chunks for blob packing.
//!
//! Version 2 adds the per-block **remote-epoch** section (interop:
//! `RemoteEpoch` records are posted into the destination's own DA batches,
//! so every chain is self-reconstructible with no dependency on a peer
//! being alive). The records that LEAD a block — the sealer closes the open
//! block on a remote-epoch origin advance, so a record's messages always
//! execute at the head of the next block — are carried before that block's
//! transactions, messages by VALUE including calldata, exactly as they
//! travel the canonical stream. Only version 2 is accepted.
//!
//! ```text
//! Header:
//!   magic       4 bytes  'K' 'A' 'R' '1'
//!   version     u8       currently 2
//!   flags       u8       bit 0 = zstd-compressed
//!   block_count u32 LE
//!   reserved    u16      zero
//!
//! For each block:
//!   block_number       u64 LE
//!   l2_timestamp       u64 LE
//!   remote_epoch_count u32 LE
//!   For each remote epoch (in canonical-stream order):
//!     origin_chain_id  u64 LE
//!     anchor_number    u64 LE
//!     anchor_hash      32 bytes
//!     first_seq        u64 LE
//!     msg_count        u32 LE   (non-zero by construction upstream)
//!     For each message (dense seq order from first_seq):
//!       source_hash    32 bytes
//!       seq            u64 LE
//!       origin_sender  20 bytes
//!       target         20 bytes
//!       value          u128 LE
//!       gas_limit      u64 LE
//!       input_len      u32 LE
//!       input          input_len bytes
//!       has_callback   u8 (0 | 1)
//!       [if has_callback: cb_target 20 bytes, cb_gas_limit u64 LE,
//!        cb_context 32 bytes]
//!   tx_count     u32 LE
//!   For each tx:
//!     correlation_id u64 LE
//!     sender         20 bytes
//!     tx_hash        32 bytes
//!     raw_tx_len     u32 LE
//!     raw_tx         raw_tx_len bytes
//! ```
//!
//! Size budget: a message's calldata is capped by the origin Outbox at
//! `MAX_DATA_BYTES` = 65 536 bytes (`contracts/src/L2/Outbox.sol`), well under
//! one blob's 126 976 usable bytes — but a multi-message record can exceed a
//! single blob's remaining space. That needs no special handling here: the
//! framed payload is one buffer that [`crate::blob::pack_to_blobs`] slices
//! across as many blobs as needed (the same mechanism an oversized tx batch
//! uses today), and [`crate::batcher::pack_blocks`] keeps the loud 6-blob
//! ceiling as the batch-size guard.

use std::num::NonZeroUsize;

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_types::xchain::{Callback, RemoteEpochRecord, XChainMessage};

use crate::error::BatcherError;

pub const MAGIC: [u8; 4] = *b"KAR1";
pub const VERSION: u8 = 2;
pub const FLAG_ZSTD: u8 = 0x01;

const HEADER_LEN: usize = 4 + 1 + 1 + 4 + 2;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TxFrame {
    pub correlation_id: u64,
    pub sender: Address,
    pub tx_hash: B256,
    pub raw_tx: Bytes,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BlockFrame {
    pub block_number: u64,
    pub l2_timestamp: u64,
    /// Remote-epoch records LEADING this block, in canonical-stream order.
    /// Their messages execute (as 0x7D txs) at the head of the block, before
    /// `txs` — the reconstruction replay preserves exactly that order.
    pub remote_epochs: Vec<RemoteEpochRecord>,
    pub txs: Vec<TxFrame>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Kar1Payload {
    pub blocks: Vec<BlockFrame>,
    /// Reflects bit 0 of the flags byte. `encode` and `decode` set this
    /// field to match the byte they read or wrote. Compression itself
    /// happens in [`crate::compress`].
    pub compressed: bool,
}

/// Encode a [`Kar1Payload`] to its KAR1 byte form.
///
/// # Errors
/// Returns an error when the block count overflows `u32`.
pub fn encode(payload: &Kar1Payload) -> Result<Vec<u8>, BatcherError> {
    let block_count: u32 = payload
        .blocks
        .len()
        .try_into()
        .map_err(|_| BatcherError::Frame("block_count overflows u32".into()))?;
    let mut buf = Vec::with_capacity(HEADER_LEN + payload.blocks.len() * 24);
    buf.extend_from_slice(&MAGIC);
    buf.push(VERSION);
    buf.push(if payload.compressed { FLAG_ZSTD } else { 0 });
    buf.extend_from_slice(&block_count.to_le_bytes());
    buf.extend_from_slice(&0u16.to_le_bytes());

    for block in &payload.blocks {
        let tx_count: u32 = block
            .txs
            .len()
            .try_into()
            .map_err(|_| BatcherError::Frame("tx_count overflows u32".into()))?;
        let remote_epoch_count: u32 = block
            .remote_epochs
            .len()
            .try_into()
            .map_err(|_| BatcherError::Frame("remote_epoch_count overflows u32".into()))?;
        buf.extend_from_slice(&block.block_number.to_le_bytes());
        buf.extend_from_slice(&block.l2_timestamp.to_le_bytes());
        buf.extend_from_slice(&remote_epoch_count.to_le_bytes());
        for rec in &block.remote_epochs {
            encode_remote_epoch(&mut buf, rec)?;
        }
        buf.extend_from_slice(&tx_count.to_le_bytes());

        for tx in &block.txs {
            let raw_len: u32 = tx
                .raw_tx
                .len()
                .try_into()
                .map_err(|_| BatcherError::Frame("raw_tx_len overflows u32".into()))?;
            buf.extend_from_slice(&tx.correlation_id.to_le_bytes());
            buf.extend_from_slice(tx.sender.as_slice());
            buf.extend_from_slice(tx.tx_hash.as_slice());
            buf.extend_from_slice(&raw_len.to_le_bytes());
            buf.extend_from_slice(tx.raw_tx.as_ref());
        }
    }
    Ok(buf)
}

/// Encode one [`RemoteEpochRecord`] — the exact record off the canonical
/// stream, messages by value including calldata. `source_hash`/`seq` are
/// carried verbatim (like a tx frame's `sender`/`tx_hash`): the codec
/// round-trips bytes; re-derivation and verification stay the validator's
/// job, never the DA layer's.
fn encode_remote_epoch(buf: &mut Vec<u8>, rec: &RemoteEpochRecord) -> Result<(), BatcherError> {
    let msg_count: u32 = rec
        .messages
        .len()
        .try_into()
        .map_err(|_| BatcherError::Frame("remote epoch msg_count overflows u32".into()))?;
    buf.extend_from_slice(&rec.origin_chain_id.to_le_bytes());
    buf.extend_from_slice(&rec.anchor_number.to_le_bytes());
    buf.extend_from_slice(rec.anchor_hash.as_slice());
    buf.extend_from_slice(&rec.first_seq.to_le_bytes());
    buf.extend_from_slice(&msg_count.to_le_bytes());
    for msg in &rec.messages {
        let input_len: u32 = msg
            .input
            .len()
            .try_into()
            .map_err(|_| BatcherError::Frame("xchain input_len overflows u32".into()))?;
        buf.extend_from_slice(msg.source_hash.as_slice());
        buf.extend_from_slice(&msg.seq.to_le_bytes());
        buf.extend_from_slice(msg.origin_sender.as_slice());
        buf.extend_from_slice(msg.target.as_slice());
        buf.extend_from_slice(&msg.value.to_le_bytes());
        buf.extend_from_slice(&msg.gas_limit.to_le_bytes());
        buf.extend_from_slice(&input_len.to_le_bytes());
        buf.extend_from_slice(msg.input.as_ref());
        match &msg.callback {
            None => buf.push(0),
            Some(cb) => {
                buf.push(1);
                buf.extend_from_slice(cb.target.as_slice());
                buf.extend_from_slice(&cb.gas_limit.to_le_bytes());
                buf.extend_from_slice(cb.context.as_slice());
            }
        }
    }
    Ok(())
}

/// Min `XChainMessage` size: `source_hash`(32) + `seq`(8) +
/// `origin_sender`(20) + `target`(20) + `value`(16) + `gas_limit`(8) +
/// `input_len`(4) + callback flag(1).
const MIN_XCHAIN_MSG_BYTES: NonZeroUsize = NonZeroUsize::new(109).unwrap();

fn decode_remote_epoch(r: &mut Reader<'_>) -> Result<RemoteEpochRecord, BatcherError> {
    let origin_chain_id = r.read_u64_le()?;
    let anchor_number = r.read_u64_le()?;
    let anchor_hash = B256::from_slice(r.read_bytes(32)?);
    let first_seq = r.read_u64_le()?;
    let msg_count = r.read_u32_le()?;
    let mut messages = Vec::with_capacity(r.capacity_hint(msg_count, MIN_XCHAIN_MSG_BYTES));
    for _ in 0..msg_count {
        let source_hash = B256::from_slice(r.read_bytes(32)?);
        let seq = r.read_u64_le()?;
        let origin_sender = Address::from_slice(r.read_bytes(20)?);
        let target = Address::from_slice(r.read_bytes(20)?);
        let value = r.read_u128_le()?;
        let gas_limit = r.read_u64_le()?;
        let input_len = r.read_u32_le()?;
        let input = Bytes::copy_from_slice(r.read_bytes(input_len as usize)?);
        let callback = match r.read_u8()? {
            0 => None,
            1 => Some(Callback {
                target: Address::from_slice(r.read_bytes(20)?),
                gas_limit: r.read_u64_le()?,
                context: B256::from_slice(r.read_bytes(32)?),
            }),
            other => {
                return Err(BatcherError::Frame(format!(
                    "invalid callback flag: {other}"
                )));
            }
        };
        messages.push(XChainMessage {
            source_hash,
            seq,
            origin_sender,
            target,
            value,
            gas_limit,
            input,
            callback,
        });
    }
    Ok(RemoteEpochRecord {
        origin_chain_id,
        anchor_number,
        anchor_hash,
        first_seq,
        messages,
    })
}

/// Min `BlockFrame` size: `block_number`(8) + `l2_timestamp`(8) +
/// `remote_epoch_count`(4) + `tx_count`(4).
const MIN_BLOCK_FRAME_BYTES: NonZeroUsize = NonZeroUsize::new(24).unwrap();
/// Min `RemoteEpochRecord` size: `origin_chain_id`(8) + `anchor_number`(8)
/// + `anchor_hash`(32) + `first_seq`(8) + `msg_count`(4).
const MIN_REMOTE_EPOCH_RECORD_BYTES: NonZeroUsize = NonZeroUsize::new(60).unwrap();
/// Min `TxFrame` size: `correlation_id`(8) + `sender`(20) + `tx_hash`(32)
/// + `raw_tx_len`(4).
const MIN_TX_FRAME_BYTES: NonZeroUsize = NonZeroUsize::new(64).unwrap();

/// Decode a KAR1 byte form back into a [`Kar1Payload`].
///
/// # Errors
/// Returns an error when the magic, version, or a field is malformed, or
/// when `bytes` ends before a declared field.
pub fn decode(bytes: &[u8]) -> Result<Kar1Payload, BatcherError> {
    let mut r = Reader::new(bytes);
    let magic = r.read_bytes(4)?;
    if magic != MAGIC {
        return Err(BatcherError::Frame(format!("bad magic: {magic:?}")));
    }
    let version = r.read_u8()?;
    if version != VERSION {
        return Err(BatcherError::Frame(format!(
            "unsupported version: {version}"
        )));
    }
    let flags = r.read_u8()?;
    let compressed = (flags & FLAG_ZSTD) != 0;
    let block_count = r.read_u32_le()?;
    let _reserved = r.read_u16_le()?;

    let blocks = (0..block_count).try_fold(
        Vec::with_capacity(r.capacity_hint(block_count, MIN_BLOCK_FRAME_BYTES)),
        |mut acc, _| -> Result<_, BatcherError> {
            acc.push(decode_block_frame(&mut r)?);
            Ok(acc)
        },
    )?;
    Ok(Kar1Payload { blocks, compressed })
}

/// Decode one [`BlockFrame`]: its header, then its remote-epoch and tx
/// sections. Its own function so [`decode`]'s block loop does not nest a
/// loop inside a loop.
fn decode_block_frame(r: &mut Reader<'_>) -> Result<BlockFrame, BatcherError> {
    let block_number = r.read_u64_le()?;
    let l2_timestamp = r.read_u64_le()?;
    let remote_epoch_count = r.read_u32_le()?;
    let remote_epochs = (0..remote_epoch_count).try_fold(
        Vec::with_capacity(r.capacity_hint(remote_epoch_count, MIN_REMOTE_EPOCH_RECORD_BYTES)),
        |mut acc, _| -> Result<_, BatcherError> {
            acc.push(decode_remote_epoch(r)?);
            Ok(acc)
        },
    )?;
    let tx_count = r.read_u32_le()?;
    let txs = (0..tx_count).try_fold(
        Vec::with_capacity(r.capacity_hint(tx_count, MIN_TX_FRAME_BYTES)),
        |mut acc, _| -> Result<_, BatcherError> {
            acc.push(decode_tx_frame(r)?);
            Ok(acc)
        },
    )?;
    Ok(BlockFrame {
        block_number,
        l2_timestamp,
        remote_epochs,
        txs,
    })
}

/// Decode one [`TxFrame`]. Its own function so [`decode_block_frame`]'s tx
/// loop does not nest a loop inside a loop.
fn decode_tx_frame(r: &mut Reader<'_>) -> Result<TxFrame, BatcherError> {
    let correlation_id = r.read_u64_le()?;
    let sender_bytes = r.read_bytes(20)?;
    let sender = Address::from_slice(sender_bytes);
    let hash_bytes = r.read_bytes(32)?;
    let tx_hash = B256::from_slice(hash_bytes);
    let raw_tx_len = r.read_u32_le()?;
    let raw_tx_bytes = r.read_bytes(raw_tx_len as usize)?;
    Ok(TxFrame {
        correlation_id,
        sender,
        tx_hash,
        raw_tx: Bytes::copy_from_slice(raw_tx_bytes),
    })
}

/// Holds only the unread remainder of the buffer, so no position field
/// tracks an index into a longer-lived slice — no arithmetic on positions
/// is possible.
struct Reader<'a> {
    buf: &'a [u8],
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }
    /// A safe `Vec::with_capacity` hint for a wire count `n`, whose
    /// decoded elements are at least `min_elem_bytes` each. A corrupt or
    /// adversarial wire count (up to `u32::MAX`) must not turn straight
    /// into an allocation request: capping it against the bytes actually
    /// left to read bounds the hint by what could possibly still decode,
    /// while `read_bytes` below still catches the real short-read case.
    fn capacity_hint(&self, n: u32, min_elem_bytes: NonZeroUsize) -> usize {
        (n as usize).min(self.buf.len() / min_elem_bytes.get())
    }
    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], BatcherError> {
        let (s, rest) = self.buf.split_at_checked(n).ok_or_else(|| {
            BatcherError::Frame(format!("short read: want {n}, have {}", self.buf.len()))
        })?;
        self.buf = rest;
        Ok(s)
    }
    fn read_u8(&mut self) -> Result<u8, BatcherError> {
        Ok(self.read_bytes(1)?[0])
    }
    fn read_u16_le(&mut self) -> Result<u16, BatcherError> {
        let b = self.read_bytes(2)?;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }
    fn read_u32_le(&mut self) -> Result<u32, BatcherError> {
        let b = self.read_bytes(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
    fn read_u64_le(&mut self) -> Result<u64, BatcherError> {
        let b = self.read_bytes(8)?;
        Ok(u64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }
    fn read_u128_le(&mut self) -> Result<u128, BatcherError> {
        let b = self.read_bytes(16)?;
        Ok(u128::from_le_bytes(b.try_into().expect("16 bytes")))
    }
}
