//! Withdrawal commitments shared by the attester (validator) and the on-chain
//! L1 bridge.
//!
//! These functions are the **single source of truth** for the byte layouts the
//! L1 contracts verify. They must stay identical to:
//! - `L2ToL1MessagePasser.hashWithdrawal` / `ETHLockbox.hashWithdrawal`
//!   (the withdrawal leaf),
//! - `ETHLockbox._merkleRoot` (the positional keccak Merkle tree), and
//! - `WithdrawalOutputOracle.OUTPUT_VERSION` + the output-root packing in
//!   `ETHLockbox.finalizeWithdrawal`.
//!
//! The cross-language tie is enforced by the anvil integration test, where a
//! proof built here is verified by the real deployed `ETHLockbox`.

use alloy_primitives::{Address, B256, U256, address, keccak256};

/// Output-root version byte. Must equal `WithdrawalOutputOracle.OUTPUT_VERSION`.
pub const OUTPUT_VERSION: u8 = 0;

/// Canonical L2 address of the `L2ToL1MessagePasser` predeploy (the L2
/// withdrawal entry point). Seeded into genesis; see `chains/dev-withdrawals.toml`.
pub const MESSAGE_PASSER: Address = address!("0x4200000000000000000000000000000000000016");

/// `keccak256` of the `MessagePassed` event signature (topic0).
pub fn message_passed_topic0() -> B256 {
    keccak256("MessagePassed(uint256,address,address,uint256,bytes32)")
}

/// Decode a `MessagePassed` log into `(nonce, leaf)`, where `leaf` is the
/// withdrawal hash carried in the event (and equal to [`withdrawal_leaf`]).
/// Returns `None` if the log is not a `MessagePassed` event. The attester uses
/// this to collect an output's withdrawals from re-executed block receipts.
///
/// Event: `MessagePassed(uint256 indexed nonce, address indexed sender,
/// address indexed target, uint256 value, bytes32 withdrawalHash)`.
/// `topics = [topic0, nonce, sender, target]`; `data = value(32) ++ hash(32)`.
pub fn decode_message_passed(topics: &[B256], data: &[u8]) -> Option<(U256, B256)> {
    if topics.len() != 4 || topics[0] != message_passed_topic0() || data.len() < 64 {
        return None;
    }
    let nonce = U256::from_be_bytes::<32>(topics[1].0);
    let leaf = B256::from_slice(&data[32..64]);
    Some((nonce, leaf))
}

/// Canonical withdrawal leaf: `keccak256(abi.encode(nonce, sender, target, value))`.
///
/// `abi.encode` lays each static value out as one 32-byte word: `nonce` and
/// `value` big-endian, `sender`/`target` right-aligned (12 zero bytes then the
/// 20 address bytes).
pub fn withdrawal_leaf(nonce: U256, sender: Address, target: Address, value: U256) -> B256 {
    let mut buf = [0u8; 128];
    buf[0..32].copy_from_slice(&nonce.to_be_bytes::<32>());
    buf[44..64].copy_from_slice(sender.as_slice());
    buf[76..96].copy_from_slice(target.as_slice());
    buf[96..128].copy_from_slice(&value.to_be_bytes::<32>());
    keccak256(buf)
}

/// `keccak256(left ++ right)` — one internal node of the withdrawals tree.
fn hash_pair(left: B256, right: B256) -> B256 {
    let mut buf = [0u8; 64];
    buf[0..32].copy_from_slice(left.as_slice());
    buf[32..64].copy_from_slice(right.as_slice());
    keccak256(buf)
}

/// Pad a leaf set up to its next power-of-two size with the zero leaf, the
/// fixed-shape convention the on-chain positional verifier reconstructs.
fn padded(leaves: &[B256]) -> Vec<B256> {
    let mut level = leaves.to_vec();
    let target = level.len().next_power_of_two().max(1);
    level.resize(target, B256::ZERO);
    level
}

/// Merkle root over `leaves` (in withdrawal-nonce order within the output's
/// block range). Empty input yields `B256::ZERO`; a single leaf yields itself.
pub fn withdrawals_root(leaves: &[B256]) -> B256 {
    if leaves.is_empty() {
        return B256::ZERO;
    }
    let mut level = padded(leaves);
    while level.len() > 1 {
        level = level.chunks(2).map(|p| hash_pair(p[0], p[1])).collect();
    }
    level[0]
}

/// Sibling path for the leaf at `index`, bottom-up. Length equals the tree
/// depth (0 for a single-leaf tree). Pairs with [`withdrawals_root`] and is
/// verified by `ETHLockbox._merkleRoot`.
pub fn withdrawal_proof(leaves: &[B256], index: usize) -> Vec<B256> {
    let mut level = padded(leaves);
    let mut idx = index;
    let mut proof = Vec::new();
    while level.len() > 1 {
        let sibling = if idx % 2 == 0 { level[idx + 1] } else { level[idx - 1] };
        proof.push(sibling);
        level = level.chunks(2).map(|p| hash_pair(p[0], p[1])).collect();
        idx /= 2;
    }
    proof
}

/// Recompute a root from a leaf, its index, and its sibling proof. Mirrors the
/// on-chain `ETHLockbox._merkleRoot`; used to self-check generated proofs.
pub fn recompute_root(leaf: B256, index: usize, proof: &[B256]) -> B256 {
    let mut node = leaf;
    let mut idx = index;
    for sibling in proof {
        node = if idx & 1 == 0 { hash_pair(node, *sibling) } else { hash_pair(*sibling, node) };
        idx >>= 1;
    }
    node
}

/// Output root: `keccak256(abi.encodePacked(OUTPUT_VERSION, state_root, withdrawals_root))`.
pub fn output_root(state_root: B256, withdrawals_root: B256) -> B256 {
    let mut buf = [0u8; 65];
    buf[0] = OUTPUT_VERSION;
    buf[1..33].copy_from_slice(state_root.as_slice());
    buf[33..65].copy_from_slice(withdrawals_root.as_slice());
    keccak256(buf)
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
        assert_eq!(withdrawals_root(&[l]), l);
        assert!(withdrawal_proof(&[l], 0).is_empty());
    }

    #[test]
    fn two_leaf_root_matches_hash_pair() {
        let l0 = leaf(0);
        let l1 = leaf(1);
        assert_eq!(withdrawals_root(&[l0, l1]), hash_pair(l0, l1));
    }

    #[test]
    fn proofs_recompute_root_all_sizes() {
        for n in 1..=9usize {
            let leaves: Vec<B256> = (0..n as u64).map(leaf).collect();
            let root = withdrawals_root(&leaves);
            for (i, &l) in leaves.iter().enumerate() {
                let proof = withdrawal_proof(&leaves, i);
                assert_eq!(recompute_root(l, i, &proof), root, "n={n} i={i}");
            }
        }
    }

    #[test]
    fn output_root_is_deterministic_and_binds_inputs() {
        let sr = keccak256(b"state");
        let wr = keccak256(b"withdrawals");
        let a = output_root(sr, wr);
        assert_eq!(a, output_root(sr, wr));
        assert_ne!(a, output_root(wr, sr));
    }
}
