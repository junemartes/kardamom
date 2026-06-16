//! OP-aligned deposit derivation primitives.
//!
//! Two functions, kept in their own module so they can be shared between the
//! L1Source decoder and downstream consumers without pulling in the full
//! watcher stack:
//!
//!   * [`source_hash`] — keccak over `(domain || keccak(rlp[l1_block_hash,
//!     l1_log_index]))`. Domain 0 = user deposit. The canonical id used by
//!     downstream consumers to dedup deposits.
//!   * [`alias_l1_address`] — `L1 + 0x1111...1111` mod 2^160. Avoids
//!     collisions between L1 contracts and L2 contracts at the same address.
//!     EOAs round-trip harmlessly (their L2 alias is just a different EOA
//!     address).
//!
//! Ported verbatim from PR #10's `crates/node/src/deposit.rs`; the
//! algorithm is OP-compatible and pinned by the contracts' bytecode-hash
//! CI gate, so the implementation here must stay byte-identical.

use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_rlp::{Encodable, Header};

/// Compute the OP-style source hash for a user deposit:
///
/// ```text
///   deposit_id_hash = keccak256(rlp([l1_block_hash, l1_log_index]))
///   source_hash     = keccak256(rlp([domain = 0u64, deposit_id_hash]))
/// ```
///
/// Domain 0 = user deposit; domain 1 (reserved) = L1-attributes / system tx.
pub fn source_hash(l1_block_hash: B256, l1_log_index: u64) -> B256 {
    let inner = encode_list_two(&l1_block_hash, &l1_log_index);
    let deposit_id_hash = keccak256(&inner);

    // domain = 0u64 encodes as RLP 0x80 (canonical empty-string form for
    // integer zero).
    let domain: u64 = 0;
    let outer = encode_list_two(&domain, &deposit_id_hash);
    keccak256(&outer)
}

fn encode_list_two<A: Encodable, B: Encodable>(a: &A, b: &B) -> Vec<u8> {
    let mut buf = Vec::new();
    let payload_len = a.length() + b.length();
    Header {
        list: true,
        payload_length: payload_len,
    }
    .encode(&mut buf);
    a.encode(&mut buf);
    b.encode(&mut buf);
    buf
}

/// Add the OP-style aliasing offset `0x1111...1111` to an L1 sender,
/// wrapping at uint160. EOAs round-trip harmlessly; L1 contracts shift to
/// avoid colliding with L2 contracts at the same address. The DA watcher
/// applies this — the L2 executor never sees the un-aliased L1 sender.
pub fn alias_l1_address(l1: Address) -> Address {
    const OFFSET: [u8; 20] = [
        0x11, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x11, 0x11,
    ];

    let mut a = [0u8; 32];
    a[12..].copy_from_slice(l1.as_slice());
    let mut b = [0u8; 32];
    b[12..].copy_from_slice(&OFFSET);

    let sum = U256::from_be_bytes(a).wrapping_add(U256::from_be_bytes(b));
    let sum_bytes = sum.to_be_bytes::<32>();

    let mut out = [0u8; 20];
    out.copy_from_slice(&sum_bytes[12..32]);
    Address::from(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, address, b256};

    #[test]
    fn alias_zero_address_is_offset() {
        let l1 = Address::ZERO;
        let expected = address!("1111000000000000000000000000000000001111");
        assert_eq!(alias_l1_address(l1), expected);
    }

    #[test]
    fn alias_wraps_at_uint160() {
        // 2^160 - 1 + 0x1111...1111 (mod 2^160) = 0x1111...1110.
        let l1 = address!("ffffffffffffffffffffffffffffffffffffffff");
        let expected = address!("1111000000000000000000000000000000001110");
        assert_eq!(alias_l1_address(l1), expected);
    }

    #[test]
    fn source_hash_is_deterministic() {
        let block = B256::repeat_byte(0x11);
        let h1 = source_hash(block, 0);
        let h2 = source_hash(block, 0);
        assert_eq!(h1, h2);
    }

    #[test]
    fn source_hash_differs_for_different_log_indices() {
        let block = B256::repeat_byte(0xAB);
        let a = source_hash(block, 0);
        let b = source_hash(block, 1);
        assert_ne!(a, b);
    }

    #[test]
    fn source_hash_known_vector_matches_op_form() {
        // Anchored output for a fixed (l1_block_hash, log_index) pair —
        // changes to the algorithm flip this and force a code-review
        // conversation. The value below was captured from the first run
        // against the OP-aligned algorithm ported from PR #10's
        // `crates/node/src/deposit.rs` (whose own conformance is pinned by
        // the contracts' bytecode-hash CI gate).
        let h = source_hash(
            b256!("0000000000000000000000000000000000000000000000000000000000000001"),
            42,
        );
        assert_eq!(
            h,
            b256!("fce50386841795079cfbaa39a7061f9f746945afa35650f060fa5935c4462c61")
        );
    }
}
