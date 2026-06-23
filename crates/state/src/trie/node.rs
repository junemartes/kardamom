//! mdbx codec for `alloy_trie::BranchNodeCompact` — the stored intermediate
//! trie node. Fixed little-tooling layout (all big-endian):
//!
//! ```text
//! state_mask(u16) ++ tree_mask(u16) ++ hash_mask(u16)
//!   ++ has_root(u8) ++ [root_hash(32B) iff has_root]
//!   ++ hashes_len(u16) ++ hashes(32B each)
//! ```
//!
//! `hashes_len` always equals the number of set bits in `hash_mask` (alloy-trie
//! enforces this), but we store it explicitly so decode is self-describing.

use alloy_primitives::B256;
use alloy_trie::{BranchNodeCompact, TrieMask};

use crate::error::StateError;

const TRIE_NODE: &str = "trie_node";

pub fn encode_branch_node(n: &BranchNodeCompact) -> Vec<u8> {
    let mut out = Vec::with_capacity(6 + 1 + 32 + 2 + n.hashes.len() * 32);
    out.extend_from_slice(&n.state_mask.get().to_be_bytes());
    out.extend_from_slice(&n.tree_mask.get().to_be_bytes());
    out.extend_from_slice(&n.hash_mask.get().to_be_bytes());
    match n.root_hash {
        Some(r) => {
            out.push(1);
            out.extend_from_slice(r.as_slice());
        }
        None => out.push(0),
    }
    out.extend_from_slice(&(n.hashes.len() as u16).to_be_bytes());
    for h in n.hashes.iter() {
        out.extend_from_slice(h.as_slice());
    }
    out
}

pub fn decode_branch_node(bytes: &[u8]) -> Result<BranchNodeCompact, StateError> {
    let mut cur = Reader { b: bytes, pos: 0 };
    let state_mask = TrieMask::new(cur.u16()?);
    let tree_mask = TrieMask::new(cur.u16()?);
    let hash_mask = TrieMask::new(cur.u16()?);
    let root_hash = if cur.u8()? == 1 {
        Some(cur.b256()?)
    } else {
        None
    };
    let len = cur.u16()? as usize;
    let mut hashes = Vec::with_capacity(len);
    for _ in 0..len {
        hashes.push(cur.b256()?);
    }
    Ok(BranchNodeCompact::new(
        state_mask, tree_mask, hash_mask, hashes, root_hash,
    ))
}

struct Reader<'a> {
    b: &'a [u8],
    pos: usize,
}

impl Reader<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8], StateError> {
        if self.pos + n > self.b.len() {
            return Err(StateError::BadEncoding {
                table: TRIE_NODE,
                expected: self.pos + n,
                got: self.b.len(),
            });
        }
        let s = &self.b[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8, StateError> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16, StateError> {
        let s = self.take(2)?;
        Ok(u16::from_be_bytes([s[0], s[1]]))
    }
    fn b256(&mut self) -> Result<B256, StateError> {
        Ok(B256::from_slice(self.take(32)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branch_node_roundtrips() {
        let n = BranchNodeCompact::new(
            TrieMask::new(0b1011),
            TrieMask::new(0b0010),
            TrieMask::new(0b1001),
            vec![B256::repeat_byte(0x11), B256::repeat_byte(0x22)],
            Some(B256::repeat_byte(0x33)),
        );
        let bytes = encode_branch_node(&n);
        assert_eq!(decode_branch_node(&bytes).unwrap(), n);
    }

    #[test]
    fn branch_node_no_root_no_hashes() {
        let n = BranchNodeCompact::new(
            TrieMask::new(0b0001),
            TrieMask::new(0b0000),
            TrieMask::new(0b0000),
            vec![],
            None,
        );
        let bytes = encode_branch_node(&n);
        assert_eq!(decode_branch_node(&bytes).unwrap(), n);
    }

    #[test]
    fn truncated_bytes_error() {
        assert!(decode_branch_node(&[0u8; 3]).is_err());
    }
}
