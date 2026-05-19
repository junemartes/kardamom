//! L1→L2 deposit primitives: deposit tx type, address aliasing, source hash.

use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_rlp::{Encodable, Header};

/// EIP-2718 type byte for deposit transactions (OP-compatible).
pub const DEPOSIT_TX_TYPE: u8 = 0x7E;

/// Re-export the canonical OP deposit-tx type. Keep the dependency surface narrow:
/// only this type from op-alloy-consensus is used downstream.
pub use op_alloy_consensus::TxDeposit as DepositTx;

/// Compute the OP-style source hash for a user deposit:
///   deposit_id_hash = keccak256(rlp([l1_block_hash, l1_log_index]))
///   source_hash     = keccak256(rlp([domain = 0u64, deposit_id_hash]))
/// Domain 0 = user deposit; domain 1 (reserved) = L1-attributes / system tx.
pub fn source_hash(l1_block_hash: B256, l1_log_index: u64) -> B256 {
    let inner = encode_list_two(&l1_block_hash, &l1_log_index);
    let deposit_id_hash = keccak256(&inner);

    // domain = 0u64 encodes as RLP 0x80 (canonical empty-string form for integer zero).
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

/// Add the OP-style aliasing offset `0x1111...1111` to an L1 sender, wrapping at uint160.
/// EOAs round-trip harmlessly; L1 contracts shift to avoid colliding with L2 contracts at
/// the same address. The deriver (test fixture today, watcher tomorrow) applies this — the
/// L2 executor never sees the un-aliased L1 sender.
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
    use alloy_primitives::{Address, B256, address, b256};

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
    fn alias_is_pure_addition_for_typical_address() {
        // 0x0000...00FF + 0x1111...1111 = 0x1111...1210
        let l1 = address!("00000000000000000000000000000000000000ff");
        let expected = address!("1111000000000000000000000000000000001210");
        assert_eq!(alias_l1_address(l1), expected);
    }

    #[test]
    #[ignore = "one-shot; un-ignore, run once, copy hex into source_hash_known_vector"]
    fn print_source_hash_for_pinning() {
        let h = source_hash(B256::repeat_byte(0xAB), 7);
        println!("source_hash = {h:#x}");
    }

    #[test]
    fn source_hash_known_vector() {
        let h = source_hash(B256::repeat_byte(0xAB), 7);
        // Pinned: any drift in this value indicates a wire-format break.
        let expected = b256!("eb1e3ea87ea17a6143ad2897e0d85dd9b9d743c2fb1c59e16b98c6df6fc76c6a");
        assert_eq!(h, expected);
    }

    #[test]
    fn source_hash_changes_with_log_index() {
        let a = source_hash(B256::repeat_byte(0xAB), 0);
        let b = source_hash(B256::repeat_byte(0xAB), 1);
        assert_ne!(a, b);
    }

    #[test]
    fn source_hash_changes_with_block_hash() {
        let a = source_hash(B256::repeat_byte(0x01), 0);
        let b = source_hash(B256::repeat_byte(0x02), 0);
        assert_ne!(a, b);
    }

    #[test]
    fn deposit_tx_type_is_0x7e() {
        assert_eq!(DEPOSIT_TX_TYPE, 0x7E);
    }
}
