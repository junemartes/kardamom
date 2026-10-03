//! KAR1 framing: the on-blob payload format.
//!
//! The format carries no `state_root` field. After framing, zstd can
//! compress the whole payload (flag bit 0 set). The result is then sliced
//! into 31-byte field-element chunks for blob packing.
//!
//! The codec lives here, not in the batcher, because the executors and
//! the validator keep every block they execute in this exact form (the
//! `block_payloads` table of the state database), and the batcher reads
//! those rows back when the sealer no longer retains the block. One
//! encoder means the recovered bytes are the bytes the batcher posts.
//!
//! Version 2 adds the per-block **remote-epoch** section (interop:
//! `RemoteEpoch` records are posted into the destination's own DA batches,
//! so every chain is self-reconstructible with no dependency on a peer
//! being alive). The records that LEAD a block — the sealer closes the open
//! block on a remote-epoch origin advance, so a record's messages always
//! execute at the head of the next block — are carried before that block's
//! transactions, messages by VALUE including calldata, exactly as they
//! travel the canonical stream. Versions 4 and 5 are accepted: a version 5
//! block carries its cursor after the timestamp.
//!
//! ```text
//! Header:
//!   magic       4 bytes  'K' 'A' 'R' '1'
//!   version     u8       4 (no block cursor) or 5 (block cursor)
//!   flags       u8       bit 0 = zstd-compressed
//!   block_count u32 LE
//!   reserved    u16      zero
//!
//! For each block:
//!   block_number       u64 LE
//!   l2_timestamp       u64 LE
//!   [version 5: end_tx_idx u64 LE, l1_origin u64 LE]
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
//!       hops           u8
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
//! framed payload is one buffer that the batcher's blob packer slices
//! across as many blobs as needed (the same mechanism an oversized tx batch
//! uses today). Two guards bound the batch size. The derivation rule caps
//! one record's wire size at
//! `kardamom_types::xchain::MAX_REMOTE_EPOCH_WIRE_BYTES`, so a record fits
//! in 5 blobs on its own. And the batcher's `pack_block_groups` splits a
//! block group that overflows the 6-blob ceiling; a block that overflows on
//! its own is a loud `BlockTooLarge` error.
//!
//! The DA payload is bound by the L1 records commitment: every record in
//! a block (remote epochs first, then txs) has an arm in
//! [`crate::BlockRecordsDigest`].

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::num::NonZeroUsize;

use alloy_primitives::{Address, B256};
use bytes::Bytes;

use crate::envelope::TxEnvelope;
use crate::xchain::{Callback, NonEmptyVec, RemoteEpochRecord, XChainMessage};

/// A malformed frame, or a count that does not fit its wire width.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("KAR1 frame: {0}")]
pub struct Kar1Error(pub String);

pub const MAGIC: [u8; 4] = *b"KAR1";
/// The current version. A block carries its [`BlockCursor`], and every
/// cross-chain message carries its `hops` byte (audit H6, #264).
pub const VERSION: u8 = 5;
/// The layout without a block cursor, with the `hops` byte. A state rebuilt
/// through such a block has no resume cursor.
///
/// The `hops` byte changed every message, so both layouts got new numbers.
/// Versions 2 and 3 (no `hops` byte) are not accepted: the contract change
/// that added the hop budget also resets the chain.
pub const VERSION_NO_CURSOR: u8 = 4;
pub const FLAG_ZSTD: u8 = 0x01;

const HEADER_LEN: usize = 4 + 1 + 1 + 4 + 2;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TxFrame {
    pub correlation_id: u64,
    pub sender: Address,
    pub tx_hash: B256,
    pub raw_tx: Bytes,
}

impl From<&TxEnvelope> for TxFrame {
    /// The four envelope fields the payload carries. The inclusion
    /// deadline stays behind: the sealer has already applied it.
    fn from(envelope: &TxEnvelope) -> Self {
        Self {
            correlation_id: envelope.correlation_id,
            sender: envelope.sender,
            tx_hash: envelope.tx_hash,
            raw_tx: envelope.raw_tx.clone(),
        }
    }
}

/// The records of one block as the payload carries them: the remote-epoch
/// records that lead the block, then its transactions. Deposits, epoch
/// markers and vacant slots take canonical slots and never reach the
/// payload; a reconstructor re-derives the deposits from L1.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BlockRecords {
    pub remote_epochs: Vec<RemoteEpochRecord>,
    pub txs: Vec<TxFrame>,
}

/// The header of one block frame: the fields a block boundary gives.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BlockHead {
    pub block_number: u64,
    pub l2_timestamp: u64,
    pub cursor: BlockCursor,
}

/// Where a block ends on the canonical stream, and the L1 block it
/// derives from. Neither enters the state trie, and neither is
/// derivable from the rest of the payload: epoch markers and deposits
/// take canonical slots and never reach the blob. A consumer resumes
/// from the end index, so a state rebuilt from L1 needs the true one.
///
/// The field is not part of the records commitment, so L1 does not
/// authenticate it. The sealer checks the index against its own
/// boundary before it serves a resume, so a wrong value is a refusal
/// and not a silent skip.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BlockCursor {
    /// The count of canonical records through the end of the block: the
    /// boundary's exclusive end index.
    pub end_tx_idx: u64,
    /// The L1 block number of the newest epoch at or before the block.
    pub l1_origin: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BlockFrame {
    pub block_number: u64,
    pub l2_timestamp: u64,
    /// `None` only for a block decoded from a version 2 blob.
    pub cursor: Option<BlockCursor>,
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
    /// happens in the batcher, around the framed bytes.
    pub compressed: bool,
}

impl Kar1Payload {
    /// The version the blocks encode as: 3 when every block carries its
    /// cursor, 2 when none does. One payload is one version.
    fn version(&self) -> Result<u8, Kar1Error> {
        let with_cursor = self.blocks.iter().filter(|b| b.cursor.is_some()).count();
        if with_cursor == self.blocks.len() {
            return Ok(VERSION);
        }
        if with_cursor == 0 {
            return Ok(VERSION_NO_CURSOR);
        }
        Err(Kar1Error(format!(
            "{with_cursor} of {} blocks carry a cursor; a payload is one version",
            self.blocks.len()
        )))
    }
}

/// Encode a [`Kar1Payload`] to its KAR1 byte form.
///
/// # Errors
/// Returns an error when the block count overflows `u32`.
pub fn encode(payload: &Kar1Payload) -> Result<Vec<u8>, Kar1Error> {
    let mut enc = FrameWriter::header(
        payload.version()?,
        payload.compressed,
        payload.blocks.len(),
    )?;
    payload
        .blocks
        .iter()
        .try_for_each(|block| enc.block(block))?;
    Ok(enc.finish())
}

/// Encode one block as a payload of its own: the KAR1 header for one
/// uncompressed version 5 block, then the block. The state database keeps
/// every executed block in this form, so [`decode`] reads a stored row
/// back as a one-block [`Kar1Payload`].
///
/// # Errors
/// Returns an error when a count overflows its wire width.
pub fn encode_block(head: BlockHead, records: &BlockRecords) -> Result<Vec<u8>, Kar1Error> {
    let mut enc = FrameWriter::header(VERSION, false, 1)?;
    enc.block_parts(
        head.block_number,
        head.l2_timestamp,
        Some(head.cursor),
        &records.remote_epochs,
        &records.txs,
    )?;
    Ok(enc.finish())
}

/// A KAR1 byte buffer under construction. One method per frame kind, each
/// appending to the same buffer, so no frame-encoding function threads
/// `buf: &mut Vec<u8>` as a loose parameter.
struct FrameWriter(Vec<u8>);

impl FrameWriter {
    /// A buffer that starts with the payload header for `block_count`
    /// blocks of `version`.
    fn header(version: u8, compressed: bool, block_count: usize) -> Result<Self, Kar1Error> {
        let count: u32 = block_count
            .try_into()
            .map_err(|_| Kar1Error("block_count overflows u32".into()))?;
        let mut buf = Vec::with_capacity(HEADER_LEN + block_count * 40);
        buf.extend_from_slice(&MAGIC);
        buf.push(version);
        buf.push(if compressed { FLAG_ZSTD } else { 0 });
        buf.extend_from_slice(&count.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes());
        Ok(Self(buf))
    }

    /// Encode one [`BlockFrame`]: its header, then its leading
    /// remote-epoch records, then its txs.
    fn block(&mut self, block: &BlockFrame) -> Result<(), Kar1Error> {
        self.block_parts(
            block.block_number,
            block.l2_timestamp,
            block.cursor,
            &block.remote_epochs,
            &block.txs,
        )
    }

    /// [`Self::block`] on the block's parts, so a stored block encodes
    /// from its boundary and its records without an owned frame.
    fn block_parts(
        &mut self,
        block_number: u64,
        l2_timestamp: u64,
        cursor: Option<BlockCursor>,
        remote_epochs: &[RemoteEpochRecord],
        txs: &[TxFrame],
    ) -> Result<(), Kar1Error> {
        let tx_count: u32 = txs
            .len()
            .try_into()
            .map_err(|_| Kar1Error("tx_count overflows u32".into()))?;
        let remote_epoch_count: u32 = remote_epochs
            .len()
            .try_into()
            .map_err(|_| Kar1Error("remote_epoch_count overflows u32".into()))?;
        self.0.extend_from_slice(&block_number.to_le_bytes());
        self.0.extend_from_slice(&l2_timestamp.to_le_bytes());
        if let Some(cursor) = cursor {
            self.0.extend_from_slice(&cursor.end_tx_idx.to_le_bytes());
            self.0.extend_from_slice(&cursor.l1_origin.to_le_bytes());
        }
        self.0.extend_from_slice(&remote_epoch_count.to_le_bytes());
        remote_epochs
            .iter()
            .try_for_each(|rec| self.remote_epoch(rec))?;
        self.0.extend_from_slice(&tx_count.to_le_bytes());
        txs.iter().try_for_each(|tx| self.tx(tx))
    }

    /// Encode one [`TxFrame`]: correlation id, sender, hash, then the raw
    /// signed transaction bytes.
    fn tx(&mut self, tx: &TxFrame) -> Result<(), Kar1Error> {
        let raw_len: u32 = tx
            .raw_tx
            .len()
            .try_into()
            .map_err(|_| Kar1Error("raw_tx_len overflows u32".into()))?;
        self.0.extend_from_slice(&tx.correlation_id.to_le_bytes());
        self.0.extend_from_slice(tx.sender.as_slice());
        self.0.extend_from_slice(tx.tx_hash.as_slice());
        self.0.extend_from_slice(&raw_len.to_le_bytes());
        self.0.extend_from_slice(tx.raw_tx.as_ref());
        Ok(())
    }

    /// Encode one [`RemoteEpochRecord`] — the exact record off the
    /// canonical stream, messages by value including calldata.
    /// `source_hash`/`seq` are carried verbatim (like a tx frame's
    /// `sender`/`tx_hash`): the codec round-trips bytes; re-derivation and
    /// verification stay the validator's job, never the DA layer's.
    fn remote_epoch(&mut self, rec: &RemoteEpochRecord) -> Result<(), Kar1Error> {
        let msg_count: u32 = rec
            .messages
            .len()
            .get()
            .try_into()
            .map_err(|_| Kar1Error("remote epoch msg_count overflows u32".into()))?;
        self.0.extend_from_slice(&rec.origin_chain_id.to_le_bytes());
        self.0.extend_from_slice(&rec.anchor_number.to_le_bytes());
        self.0.extend_from_slice(rec.anchor_hash.as_slice());
        self.0.extend_from_slice(&rec.first_seq.to_le_bytes());
        self.0.extend_from_slice(&msg_count.to_le_bytes());
        rec.messages
            .iter()
            .try_for_each(|msg| self.xchain_message(msg))
    }

    /// Encode one [`XChainMessage`]: its fields, then a callback flag byte
    /// (0 = none, 1 = present) followed by the callback fields when
    /// present.
    fn xchain_message(&mut self, msg: &XChainMessage) -> Result<(), Kar1Error> {
        let input_len: u32 = msg
            .input
            .len()
            .try_into()
            .map_err(|_| Kar1Error("xchain input_len overflows u32".into()))?;
        self.0.extend_from_slice(msg.source_hash.as_slice());
        self.0.extend_from_slice(&msg.seq.to_le_bytes());
        self.0.extend_from_slice(msg.origin_sender.as_slice());
        self.0.extend_from_slice(msg.target.as_slice());
        self.0.extend_from_slice(&msg.value.to_le_bytes());
        self.0.extend_from_slice(&msg.gas_limit.to_le_bytes());
        self.0.push(msg.hops);
        self.0.extend_from_slice(&input_len.to_le_bytes());
        self.0.extend_from_slice(msg.input.as_ref());
        match &msg.callback {
            None => self.0.push(0),
            Some(cb) => {
                self.0.push(1);
                self.0.extend_from_slice(cb.target.as_slice());
                self.0.extend_from_slice(&cb.gas_limit.to_le_bytes());
                self.0.extend_from_slice(cb.context.as_slice());
            }
        }
        Ok(())
    }

    fn finish(self) -> Vec<u8> {
        self.0
    }
}

/// Min `XChainMessage` size: `source_hash`(32) + `seq`(8) +
/// `origin_sender`(20) + `target`(20) + `value`(16) + `gas_limit`(8) +
/// `hops`(1) + `input_len`(4) + callback flag(1).
const MIN_XCHAIN_MSG_BYTES: NonZeroUsize = NonZeroUsize::new(110).unwrap();

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
pub fn decode(bytes: &[u8]) -> Result<Kar1Payload, Kar1Error> {
    let mut r = Reader::new(bytes);
    let magic = r.read_bytes(4)?;
    if magic != MAGIC {
        return Err(Kar1Error(format!("bad magic: {magic:?}")));
    }
    let version = r.read_u8()?;
    let layout = BlockLayout::of(version)?;
    let flags = r.read_u8()?;
    let compressed = (flags & FLAG_ZSTD) != 0;
    let block_count = r.read_u32_le()?;
    let _reserved = r.read_u16_le()?;

    let blocks = (0..block_count).try_fold(
        Vec::with_capacity(r.capacity_hint(block_count, MIN_BLOCK_FRAME_BYTES)),
        |mut acc, _| -> Result<_, Kar1Error> {
            acc.push(r.decode_block_frame(layout)?);
            Ok(acc)
        },
    )?;
    Ok(Kar1Payload { blocks, compressed })
}

/// What a block header holds, by payload version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BlockLayout {
    /// Version 4: block number and timestamp.
    NoCursor,
    /// Version 5: the cursor follows the timestamp.
    WithCursor,
}

impl BlockLayout {
    fn of(version: u8) -> Result<Self, Kar1Error> {
        match version {
            VERSION => Ok(Self::WithCursor),
            VERSION_NO_CURSOR => Ok(Self::NoCursor),
            other => Err(Kar1Error(format!("unsupported version: {other}"))),
        }
    }
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
    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], Kar1Error> {
        let (s, rest) = self.buf.split_at_checked(n).ok_or_else(|| {
            Kar1Error(format!("short read: want {n}, have {}", self.buf.len()))
        })?;
        self.buf = rest;
        Ok(s)
    }
    /// Read exactly `N` bytes as an array. `split_first_chunk` proves the
    /// length at the type level, so there is no fallible conversion after
    /// the short-read check.
    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], Kar1Error> {
        let (chunk, rest) = self.buf.split_first_chunk::<N>().ok_or_else(|| {
            Kar1Error(format!("short read: want {N}, have {}", self.buf.len()))
        })?;
        self.buf = rest;
        Ok(*chunk)
    }
    fn read_u8(&mut self) -> Result<u8, Kar1Error> {
        Ok(self.read_array::<1>()?[0])
    }
    fn read_u16_le(&mut self) -> Result<u16, Kar1Error> {
        Ok(u16::from_le_bytes(self.read_array()?))
    }
    fn read_u32_le(&mut self) -> Result<u32, Kar1Error> {
        Ok(u32::from_le_bytes(self.read_array()?))
    }
    fn read_u64_le(&mut self) -> Result<u64, Kar1Error> {
        Ok(u64::from_le_bytes(self.read_array()?))
    }
    fn read_u128_le(&mut self) -> Result<u128, Kar1Error> {
        Ok(u128::from_le_bytes(self.read_array()?))
    }

    /// Decode one [`BlockFrame`]: its header, then its remote-epoch and
    /// tx sections. Its own method so [`decode`]'s block loop does not
    /// nest a loop inside a loop.
    fn decode_block_frame(&mut self, layout: BlockLayout) -> Result<BlockFrame, Kar1Error> {
        let block_number = self.read_u64_le()?;
        let l2_timestamp = self.read_u64_le()?;
        let cursor = self.decode_cursor(layout)?;
        let remote_epoch_count = self.read_u32_le()?;
        let remote_epochs = (0..remote_epoch_count).try_fold(
            Vec::with_capacity(
                self.capacity_hint(remote_epoch_count, MIN_REMOTE_EPOCH_RECORD_BYTES),
            ),
            |mut acc, _| -> Result<_, Kar1Error> {
                acc.push(self.decode_remote_epoch()?);
                Ok(acc)
            },
        )?;
        let tx_count = self.read_u32_le()?;
        let txs = (0..tx_count).try_fold(
            Vec::with_capacity(self.capacity_hint(tx_count, MIN_TX_FRAME_BYTES)),
            |mut acc, _| -> Result<_, Kar1Error> {
                acc.push(self.decode_tx_frame()?);
                Ok(acc)
            },
        )?;
        Ok(BlockFrame {
            block_number,
            l2_timestamp,
            cursor,
            remote_epochs,
            txs,
        })
    }

    /// The block's cursor, when the version carries one.
    fn decode_cursor(&mut self, layout: BlockLayout) -> Result<Option<BlockCursor>, Kar1Error> {
        if layout == BlockLayout::NoCursor {
            return Ok(None);
        }
        Ok(Some(BlockCursor {
            end_tx_idx: self.read_u64_le()?,
            l1_origin: self.read_u64_le()?,
        }))
    }

    /// Decode one [`TxFrame`]. Its own method so [`Self::decode_block_
    /// frame`]'s tx loop does not nest a loop inside a loop.
    fn decode_tx_frame(&mut self) -> Result<TxFrame, Kar1Error> {
        let correlation_id = self.read_u64_le()?;
        let sender = Address::from_slice(self.read_bytes(20)?);
        let tx_hash = B256::from_slice(self.read_bytes(32)?);
        let raw_tx_len = self.read_u32_le()?;
        let raw_tx_bytes = self.read_bytes(raw_tx_len as usize)?;
        Ok(TxFrame {
            correlation_id,
            sender,
            tx_hash,
            raw_tx: Bytes::copy_from_slice(raw_tx_bytes),
        })
    }

    /// Decode one [`RemoteEpochRecord`]: its header, then its (non-empty)
    /// messages.
    fn decode_remote_epoch(&mut self) -> Result<RemoteEpochRecord, Kar1Error> {
        let origin_chain_id = self.read_u64_le()?;
        let anchor_number = self.read_u64_le()?;
        let anchor_hash = B256::from_slice(self.read_bytes(32)?);
        let first_seq = self.read_u64_le()?;
        let msg_count = self.read_u32_le()?;
        let mut messages = Vec::with_capacity(self.capacity_hint(msg_count, MIN_XCHAIN_MSG_BYTES));
        for _ in 0..msg_count {
            messages.push(self.decode_xchain_message()?);
        }
        let mut messages = messages.into_iter();
        let first = messages
            .next()
            .ok_or_else(|| Kar1Error("remote epoch record carries no messages".into()))?;
        Ok(RemoteEpochRecord {
            origin_chain_id,
            anchor_number,
            anchor_hash,
            first_seq,
            messages: NonEmptyVec::new(first, messages.collect()),
        })
    }

    /// Decode one `XChainMessage`, including its optional callback tail.
    fn decode_xchain_message(&mut self) -> Result<XChainMessage, Kar1Error> {
        let source_hash = B256::from_slice(self.read_bytes(32)?);
        let seq = self.read_u64_le()?;
        let origin_sender = Address::from_slice(self.read_bytes(20)?);
        let target = Address::from_slice(self.read_bytes(20)?);
        let value = self.read_u128_le()?;
        let gas_limit = self.read_u64_le()?;
        let hops = self.read_u8()?;
        let input_len = self.read_u32_le()?;
        let input = Bytes::copy_from_slice(self.read_bytes(input_len as usize)?);
        let callback = match self.read_u8()? {
            0 => None,
            1 => Some(Callback {
                target: Address::from_slice(self.read_bytes(20)?),
                gas_limit: self.read_u64_le()?,
                context: B256::from_slice(self.read_bytes(32)?),
            }),
            other => {
                return Err(Kar1Error(format!(
                    "invalid callback flag: {other}"
                )));
            }
        };
        Ok(XChainMessage {
            source_hash,
            seq,
            origin_sender,
            target,
            value,
            gas_limit,
            hops,
            input,
            callback,
        })
    }
}
