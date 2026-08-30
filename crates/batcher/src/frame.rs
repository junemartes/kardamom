//! KAR1 framing: the on-blob payload format.
//!
//! This format has no `state_root` field (by design, at this stage). After framing,
//! zstd can compress the whole payload (flag bit 0 set). The result is then
//! sliced into 31-byte field-element chunks for blob packing.
//!
//! ```text
//! Header:
//!   magic       4 bytes  'K' 'A' 'R' '1'
//!   version     u8       currently 1
//!   flags       u8       bit 0 = zstd-compressed
//!   block_count u32 LE
//!   reserved    u16      zero
//!
//! For each block:
//!   block_number u64 LE
//!   l2_timestamp u64 LE
//!   tx_count     u32 LE
//!   For each tx:
//!     correlation_id u64 LE
//!     sender         20 bytes
//!     tx_hash        32 bytes
//!     raw_tx_len     u32 LE
//!     raw_tx         raw_tx_len bytes
//! ```

use alloy_primitives::{Address, B256};
use bytes::Bytes;

use crate::error::BatcherError;

pub const MAGIC: [u8; 4] = *b"KAR1";
pub const VERSION: u8 = 1;
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
        buf.extend_from_slice(&block.block_number.to_le_bytes());
        buf.extend_from_slice(&block.l2_timestamp.to_le_bytes());
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

/// Decode a KAR1 byte form back into a [`Kar1Payload`].
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

    let mut blocks = Vec::with_capacity(block_count as usize);
    for _ in 0..block_count {
        let block_number = r.read_u64_le()?;
        let l2_timestamp = r.read_u64_le()?;
        let tx_count = r.read_u32_le()?;
        let mut txs = Vec::with_capacity(tx_count as usize);
        for _ in 0..tx_count {
            let correlation_id = r.read_u64_le()?;
            let sender_bytes = r.read_bytes(20)?;
            let sender = Address::from_slice(sender_bytes);
            let hash_bytes = r.read_bytes(32)?;
            let tx_hash = B256::from_slice(hash_bytes);
            let raw_tx_len = r.read_u32_le()?;
            let raw_tx_bytes = r.read_bytes(raw_tx_len as usize)?;
            txs.push(TxFrame {
                correlation_id,
                sender,
                tx_hash,
                raw_tx: Bytes::copy_from_slice(raw_tx_bytes),
            });
        }
        blocks.push(BlockFrame {
            block_number,
            l2_timestamp,
            txs,
        });
    }
    Ok(Kar1Payload { blocks, compressed })
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }
    fn read_bytes(&mut self, n: usize) -> Result<&'a [u8], BatcherError> {
        if self.pos + n > self.buf.len() {
            return Err(BatcherError::Frame(format!(
                "short read: want {n}, have {}",
                self.buf.len() - self.pos
            )));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
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
}
