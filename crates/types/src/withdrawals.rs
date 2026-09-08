//! Withdrawal commitments shared by the attester (validator) and the on-chain
//! L1 bridge.
//!
//! These functions are the single source of truth for the byte layouts the
//! L1 contracts verify. They must stay identical to:
//! - `L2ToL1MessagePasser.hashWithdrawal` and `ETHLockbox.hashWithdrawal`
//!   (the withdrawal leaf),
//! - `ETHLockbox._merkleRoot` (the positional keccak Merkle tree), and
//! - `WithdrawalOutputOracle.OUTPUT_VERSION` and the output-root packing in
//!   `ETHLockbox.finalizeWithdrawal`.
//!
//! The anvil integration test enforces the cross-language tie: a proof
//! built here is verified by the real deployed `ETHLockbox`.

use alloc::vec::Vec;

use alloy_primitives::{Address, B256, U256, address, keccak256};

/// Output-root version byte. Must equal `WithdrawalOutputOracle.OUTPUT_VERSION`.
pub const OUTPUT_VERSION: u8 = 0;

/// Domain tag prefixed to a withdrawal hash before it enters the Merkle tree.
/// Must equal `ETHLockbox.LEAF_DOMAIN`. Leaves and internal nodes hash under
/// distinct domains, so an internal-node preimage can never be replayed as
/// a leaf. This is second-preimage hardening.
pub const LEAF_DOMAIN: u8 = 0x00;
/// Domain tag prefixed to each internal `(left, right)` pair. Must equal
/// `ETHLockbox.NODE_DOMAIN`.
pub const NODE_DOMAIN: u8 = 0x01;

/// Canonical L2 address of the `L2ToL1MessagePasser` predeploy. This is the
/// L2 withdrawal entry point. It is seeded into genesis; see
/// `chains/dev-withdrawals.toml`.
pub const MESSAGE_PASSER: Address = address!("0x4200000000000000000000000000000000000016");

/// `keccak256` of the `MessagePassed` event signature (topic0).
#[must_use]
pub fn message_passed_topic0() -> B256 {
    keccak256("MessagePassed(uint256,address,address,uint256,bytes32)")
}

/// Decode a `MessagePassed` log into `(nonce, leaf)`. `leaf` is the
/// withdrawal hash, recomputed from the decoded fields with
/// [`withdrawal_leaf`]. Returns `None` if the log is not a well-formed
/// `MessagePassed` event, or if the event-carried hash does not equal the
/// recomputed one. The recompute catches predeploy or bytecode drift,
/// because the predeploy runtime bytecode is duplicated by hand in
/// `chains/dev-withdrawals.toml`, instead of trusting the event-carried
/// data. The attester uses this to collect an output's withdrawals from
/// re-executed block receipts.
///
/// Event: `MessagePassed(uint256 indexed nonce, address indexed sender,
/// address indexed target, uint256 value, bytes32 withdrawalHash)`.
/// `topics = [topic0, nonce, sender, target]`.
/// `data = value(32) ++ hash(32)`.
#[must_use]
pub fn decode_message_passed(topics: &[B256], data: &[u8]) -> Option<(U256, B256)> {
    if topics.len() != 4 || topics[0] != message_passed_topic0() || data.len() != 64 {
        return None;
    }
    let nonce = U256::from_be_bytes::<32>(topics[1].0);
    let sender = Address::from_slice(&topics[2].0[12..]);
    let target = Address::from_slice(&topics[3].0[12..]);
    let value = U256::from_be_slice(&data[0..32]);
    let carried = B256::from_slice(&data[32..64]);
    let leaf = withdrawal_leaf(nonce, sender, target, value);
    if leaf != carried {
        return None;
    }
    Some((nonce, leaf))
}

/// Canonical withdrawal leaf: `keccak256(abi.encode(nonce, sender, target, value))`.
///
/// `abi.encode` lays out each static value as one 32-byte word: `nonce`
/// and `value` big-endian, `sender` and `target` right-aligned (12 zero
/// bytes, then the 20 address bytes).
#[must_use]
pub fn withdrawal_leaf(nonce: U256, sender: Address, target: Address, value: U256) -> B256 {
    let mut buf = [0u8; 128];
    buf[0..32].copy_from_slice(&crate::abi::word_u256(nonce));
    buf[32..64].copy_from_slice(&crate::abi::word_address(sender));
    buf[64..96].copy_from_slice(&crate::abi::word_address(target));
    buf[96..128].copy_from_slice(&crate::abi::word_u256(value));
    keccak256(buf)
}

/// `keccak256(tag ++ a ++ b)`: one domain-tag byte, then two 32-byte
/// words. [`hash_pair`] and [`output_root`] both mirror this exact
/// on-chain shape, under different tags.
fn tagged_hash2(tag: u8, a: B256, b: B256) -> B256 {
    let mut buf = [0u8; 65];
    buf[0] = tag;
    buf[1..33].copy_from_slice(a.as_slice());
    buf[33..65].copy_from_slice(b.as_slice());
    keccak256(buf)
}

/// `keccak256(NODE_DOMAIN ++ left ++ right)`. This is one internal node of
/// the withdrawals tree.
fn hash_pair(left: B256, right: B256) -> B256 {
    tagged_hash2(NODE_DOMAIN, left, right)
}

/// `keccak256(LEAF_DOMAIN ++ withdrawal_hash)`. This lifts a withdrawal
/// hash into the tree's leaf level.
fn hash_leaf(leaf: B256) -> B256 {
    let mut buf = [0u8; 33];
    buf[0] = LEAF_DOMAIN;
    buf[1..33].copy_from_slice(leaf.as_slice());
    keccak256(buf)
}

/// One level of the withdrawal tree during root or proof construction:
/// hashed nodes, padded to a power-of-two length so every node has a
/// sibling ([`Self::sibling`]) and the level halves cleanly into the one
/// above it ([`Self::up`]).
struct Level(Vec<B256>);

impl Level {
    /// Domain-hash `leaves` into the tree's leaf level, padded up to the
    /// next power-of-two size with the zero leaf. This is the fixed-shape
    /// convention that the on-chain positional verifier reconstructs.
    fn leaves(leaves: &[B256]) -> Self {
        let mut level = leaves.to_vec();
        // `0usize.next_power_of_two()` is already 1, so the empty case
        // needs no extra floor.
        let target = level.len().next_power_of_two();
        level.resize(target, B256::ZERO);
        Self(level.into_iter().map(hash_leaf).collect())
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    /// The single remaining node, once [`Self::up`] has reduced the level
    /// to one.
    fn root(&self) -> B256 {
        self.0[0]
    }

    /// The sibling of the node at `idx`. `idx ^ 1` toggles the low bit, so
    /// an even index reads its odd neighbor and an odd index reads its
    /// even one — the pair [`Self::up`] combines next. Always in range:
    /// `Self` is never built or reduced to an odd, non-one length.
    fn sibling(&self, idx: usize) -> B256 {
        self.0[idx ^ 1]
    }

    /// The level above: sibling pairs combined with [`hash_pair`].
    fn up(&self) -> Self {
        Self(self.0.chunks(2).map(|p| hash_pair(p[0], p[1])).collect())
    }
}

/// Merkle root over `leaves`: withdrawal hashes, in withdrawal-nonce order
/// within the output's block range. An empty input gives `B256::ZERO`. A
/// single leaf gives `hash_leaf(leaf)`.
#[must_use]
pub fn withdrawals_root(leaves: &[B256]) -> B256 {
    if leaves.is_empty() {
        return B256::ZERO;
    }
    let mut level = Level::leaves(leaves);
    while level.len() > 1 {
        level = level.up();
    }
    level.root()
}

/// A withdrawal-tree leaf index, checked once against the leaf count.
/// Nothing that holds one needs to re-check the bound.
#[derive(Clone, Copy)]
struct LeafIndex(usize);

impl LeafIndex {
    /// `None` if `index >= leaf_count`.
    fn new(index: usize, leaf_count: usize) -> Option<Self> {
        (index < leaf_count).then_some(Self(index))
    }
}

/// Sibling path for the leaf at `index`, from the bottom up. Its length
/// equals the tree depth (`0` for a single-leaf tree). It pairs with
/// [`withdrawals_root`], and `ETHLockbox._merkleRoot` verifies it.
///
/// # Panics
///
/// Panics if `leaves` is empty or `index >= leaves.len()`. The bound is
/// parsed once into a [`LeafIndex`], so the loop below never does its own
/// range check.
#[must_use]
pub fn withdrawal_proof(leaves: &[B256], index: usize) -> Vec<B256> {
    let Some(LeafIndex(mut idx)) = LeafIndex::new(index, leaves.len()) else {
        panic!(
            "withdrawal_proof: index {index} out of range for {} leaves",
            leaves.len()
        );
    };
    let mut level = Level::leaves(leaves);
    let mut proof = Vec::new();
    while level.len() > 1 {
        proof.push(level.sibling(idx));
        level = level.up();
        idx /= 2;
    }
    proof
}

/// Recompute a root from a leaf (a withdrawal hash), its index, and its
/// sibling proof. Mirrors the on-chain `ETHLockbox._merkleRoot`. The
/// reference verifier: a proof consumer runs this same rule against the
/// proof `crate::withdrawals` generated.
#[must_use]
pub fn recompute_root(leaf: B256, index: usize, proof: &[B256]) -> B256 {
    let mut node = hash_leaf(leaf);
    let mut idx = index;
    for sibling in proof {
        node = combine_with_sibling(node, *sibling, idx);
        idx >>= 1;
    }
    node
}

/// Combine `node` with `sibling` in proof order: `sibling` goes on the
/// left when `idx` is odd, on the right when `idx` is even.
fn combine_with_sibling(node: B256, sibling: B256, idx: usize) -> B256 {
    if idx & 1 == 0 {
        hash_pair(node, sibling)
    } else {
        hash_pair(sibling, node)
    }
}

/// Output root: `keccak256(abi.encodePacked(OUTPUT_VERSION, state_root, withdrawals_root))`.
#[must_use]
pub fn output_root(state_root: B256, withdrawals_root: B256) -> B256 {
    tagged_hash2(OUTPUT_VERSION, state_root, withdrawals_root)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;

    fn leaf(n: u64) -> B256 {
        withdrawal_leaf(
            U256::from(n),
            address!("0x00000000000000000000000000000000000000aa"),
            address!("0x00000000000000000000000000000000000000bb"),
            U256::from(1_000u64 + n),
        )
    }

    #[test]
    fn empty_and_single() {
        assert_eq!(withdrawals_root(&[]), B256::ZERO);
        let l = leaf(0);
        assert_eq!(withdrawals_root(&[l]), hash_leaf(l));
        assert!(withdrawal_proof(&[l], 0).is_empty());
    }

    #[test]
    fn two_leaf_root_matches_hash_pair() {
        let l0 = leaf(0);
        let l1 = leaf(1);
        assert_eq!(
            withdrawals_root(&[l0, l1]),
            hash_pair(hash_leaf(l0), hash_leaf(l1))
        );
    }

    #[test]
    fn leaves_and_nodes_are_domain_separated() {
        // A single-leaf root must not be the raw withdrawal hash. An
        // internal pair must not hash the same as an undomained concat.
        // This is the second-preimage hardening the domains exist for.
        let l = leaf(0);
        assert_ne!(withdrawals_root(&[l]), l);
        let (a, b) = (leaf(1), leaf(2));
        let mut concat = [0u8; 64];
        concat[..32].copy_from_slice(a.as_slice());
        concat[32..].copy_from_slice(b.as_slice());
        assert_ne!(hash_pair(a, b), keccak256(concat));
        assert_ne!(hash_leaf(a), hash_pair(a, b));
    }

    /// Every leaf's proof recomputes `root`, for one tree of size `n`.
    fn assert_proofs_recompute_root(leaves: &[B256], root: B256, n: usize) {
        for (i, &l) in leaves.iter().enumerate() {
            let proof = withdrawal_proof(leaves, i);
            assert_eq!(recompute_root(l, i, &proof), root, "n={n} i={i}");
        }
    }

    #[test]
    fn proofs_recompute_root_all_sizes() {
        for n in 1..=9usize {
            let leaves: Vec<B256> = (0..n as u64).map(leaf).collect();
            let root = withdrawals_root(&leaves);
            assert_proofs_recompute_root(&leaves, root, n);
        }
    }

    /// Build a well-formed `MessagePassed` (topics, data) pair.
    fn message_passed(
        nonce: u64,
        sender: Address,
        target: Address,
        value: u64,
    ) -> (Vec<B256>, Vec<u8>) {
        let leaf = withdrawal_leaf(U256::from(nonce), sender, target, U256::from(value));
        let mut sender_word = [0u8; 32];
        sender_word[12..].copy_from_slice(sender.as_slice());
        let mut target_word = [0u8; 32];
        target_word[12..].copy_from_slice(target.as_slice());
        let topics = vec![
            message_passed_topic0(),
            B256::from(U256::from(nonce)),
            B256::from(sender_word),
            B256::from(target_word),
        ];
        let mut data = U256::from(value).to_be_bytes::<32>().to_vec();
        data.extend_from_slice(leaf.as_slice());
        (topics, data)
    }

    #[test]
    fn decode_message_passed_recomputes_and_verifies_leaf() {
        let s = address!("0x00000000000000000000000000000000000000aa");
        let t = address!("0x00000000000000000000000000000000000000bb");
        let (topics, data) = message_passed(7, s, t, 1_000);
        let (nonce, leaf) = decode_message_passed(&topics, &data).expect("decodes");
        assert_eq!(nonce, U256::from(7u64));
        assert_eq!(leaf, withdrawal_leaf(nonce, s, t, U256::from(1_000u64)));
    }

    #[test]
    fn decode_message_passed_rejects_tampered_hash() {
        let s = address!("0x00000000000000000000000000000000000000aa");
        let t = address!("0x00000000000000000000000000000000000000bb");
        let (topics, mut data) = message_passed(7, s, t, 1_000);
        data[63] ^= 0x01; // corrupt the event-carried withdrawal hash
        assert!(decode_message_passed(&topics, &data).is_none());
        // A tampered value does not match the carried hash either.
        let (topics, mut data) = message_passed(7, s, t, 1_000);
        data[31] ^= 0x01;
        assert!(decode_message_passed(&topics, &data).is_none());
    }

    #[test]
    fn decode_message_passed_rejects_trailing_data() {
        let s = address!("0x00000000000000000000000000000000000000aa");
        let t = address!("0x00000000000000000000000000000000000000bb");
        let (topics, mut data) = message_passed(7, s, t, 1_000);
        data.push(0x00); // 65 bytes is malformed. It must be rejected (`!= 64`).
        assert!(decode_message_passed(&topics, &data).is_none());
    }

    #[test]
    fn output_root_is_deterministic_and_binds_inputs() {
        let sr = keccak256(b"state");
        let wr = keccak256(b"withdrawals");
        let a = output_root(sr, wr);
        assert_eq!(a, output_root(sr, wr));
        assert_ne!(a, output_root(wr, sr));
    }

    #[test]
    #[should_panic(expected = "index 3 out of range for 3 leaves")]
    fn withdrawal_proof_rejects_an_out_of_range_index() {
        // Defect: an unchecked `index` let `idx + 1` wrap past `usize::MAX`
        // back into range and return a wrong sibling with no panic at all.
        let leaves: Vec<B256> = (0..3u64).map(leaf).collect();
        let _ = withdrawal_proof(&leaves, 3);
    }
}
