//! The set of changed key-prefixes for one block. The walker uses this
//! to decide which subtries to descend into, and which to skip.

use alloy_primitives::B256;
use alloy_trie::Nibbles;

/// A sorted, deduplicated set of changed keys, as full nibble paths.
/// `contains_prefix` answers: does any changed key live under this
/// subtrie path?
#[derive(Debug, Default, Clone)]
pub struct PrefixSet {
    keys: Vec<Nibbles>,
}

impl PrefixSet {
    /// Build from the changed hashed keys (`keccak(addr)` / `keccak(slot)`).
    pub fn from_b256s(it: impl IntoIterator<Item = B256>) -> Self {
        let mut keys: Vec<Nibbles> = it
            .into_iter()
            .map(|h| Nibbles::unpack(h.as_slice()))
            .collect();
        keys.sort_unstable();
        keys.dedup();
        Self { keys }
    }

    /// Build from nibble paths directly. Proof-generation targets may be
    /// partial paths, a node position from the capture fixed point, which
    /// force the walker to descend exactly that far.
    pub fn from_nibbles(it: impl IntoIterator<Item = Nibbles>) -> Self {
        let mut keys: Vec<Nibbles> = it.into_iter().collect();
        keys.sort_unstable();
        keys.dedup();
        Self { keys }
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Returns true if some changed key starts with `prefix`. This means
    /// the subtrie rooted at `prefix` contains a change. Keys are sorted,
    /// so the changed keys under `prefix` form a contiguous range. This
    /// checks the first key that is `>= prefix`.
    pub fn contains_prefix(&self, prefix: &Nibbles) -> bool {
        let i = self.keys.partition_point(|k| k < prefix);
        self.keys.get(i).is_some_and(|k| k.starts_with(prefix))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(byte: u8) -> B256 {
        B256::repeat_byte(byte)
    }

    #[test]
    fn contains_prefix_matches_changed_keys() {
        let ps = PrefixSet::from_b256s([h(0xab), h(0xcd)]);
        // 0xab... has first nibble 0xa.
        assert!(ps.contains_prefix(&Nibbles::from_nibbles([0xa])));
        assert!(ps.contains_prefix(&Nibbles::from_nibbles([0xa, 0xb])));
        assert!(ps.contains_prefix(&Nibbles::from_nibbles([0xc])));
        // No changed key is under 0x0.
        assert!(!ps.contains_prefix(&Nibbles::from_nibbles([0x0])));
        // The empty prefix, the root, always matches when the set is not empty.
        assert!(ps.contains_prefix(&Nibbles::new()));
    }

    #[test]
    fn empty_set_contains_nothing() {
        let ps = PrefixSet::from_b256s([]);
        assert!(ps.is_empty());
        assert!(!ps.contains_prefix(&Nibbles::new()));
    }
}
