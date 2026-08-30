//! `WriteSet::hash` is the determinism witness every replica compares, so
//! its byte sequence is a consensus contract. The implementation has two
//! paths: a stack-buffered fast path, and a streaming fallback for write
//! sets carrying bytecode. The two must be indistinguishable.
//!
//! These tests recompute the documented sequence independently, and
//! compare it. So they pin the contract itself, not just whether the two
//! paths agree with each other.

use alloy_primitives::{Address, B256, Keccak256, U256, address};
use kardamom_exec_core::delta::WriteSet;

/// The v2 contract, spelled out independently of the implementation:
///
/// ```text
/// u8   0x02 (version)
/// var  n_accounts                       (LEB128)
///   per account: addr[20]
///                u8 flags: bits0-5 = balance byte length
///                          bits6-7 = 0 KECCAK_EMPTY, 1 ZERO, 2 explicit
///                var nonce
///                balance big-endian, leading zeros stripped
///                [32] code_hash            (only when tag == 2)
/// var  n_storage
///   per slot:    u8 flags: bits0-5 = value byte length
///                          bit6    = address equals the previous entry's
///                [20] address              (only when bit6 == 0)
///                [32] key
///                value big-endian, leading zeros stripped
/// var  n_code
///   per entry:   [32] hash, var len, [len] bytes
/// ```
fn varint(h: &mut Keccak256, mut v: u64) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            h.update([b]);
            return;
        }
        h.update([b | 0x80]);
    }
}

/// Minimal big-endian bytes (zero encodes as nothing).
fn minimal(v: &U256) -> Vec<u8> {
    let be = v.to_be_bytes::<32>();
    let lead = be.iter().take_while(|b| **b == 0).count();
    be[lead..].to_vec()
}

fn expected(ws: &WriteSet) -> B256 {
    let keccak_empty = alloy_primitives::keccak256([]);
    let mut h = Keccak256::new();
    h.update([0x02u8]);
    varint(&mut h, ws.accounts.len() as u64);
    for (addr, (nonce, balance, code_hash)) in &ws.accounts {
        let bal = minimal(balance);
        let tag: u8 = if *code_hash == keccak_empty {
            0
        } else if code_hash.is_zero() {
            1
        } else {
            2
        };
        h.update(addr.as_slice());
        h.update([bal.len() as u8 | (tag << 6)]);
        varint(&mut h, *nonce);
        h.update(&bal);
        if tag == 2 {
            h.update(code_hash.as_slice());
        }
    }
    varint(&mut h, ws.storage.len() as u64);
    let mut prev: Option<Address> = None;
    for ((addr, key), value) in &ws.storage {
        let val = minimal(value);
        let same = prev == Some(*addr);
        h.update([val.len() as u8 | (u8::from(same) << 6)]);
        if !same {
            h.update(addr.as_slice());
        }
        h.update(key.as_slice());
        h.update(&val);
        prev = Some(*addr);
    }
    varint(&mut h, ws.code.len() as u64);
    for (code_hash, bytes) in &ws.code {
        h.update(code_hash.as_slice());
        varint(&mut h, bytes.len() as u64);
        h.update(bytes.as_ref());
    }
    h.finalize()
}

fn addr(i: u8) -> Address {
    let mut b = [0u8; 20];
    b[19] = i;
    Address::from(b)
}

fn account(ws: &mut WriteSet, i: u8, nonce: u64, balance: u64) {
    ws.accounts.push((
        addr(i),
        (nonce, U256::from(balance), B256::with_last_byte(i)),
    ));
}

#[test]
fn empty_write_set_matches_the_contract() {
    let mut ws = WriteSet::default();
    ws.finish();
    assert_eq!(ws.hash(), expected(&ws));
}

/// The shape of a plain transfer: sender, recipient, fee sink. This is
/// the case the stack buffer exists for, and the one the STM commit
/// tail hashes once per transaction.
#[test]
fn transfer_shaped_write_set_matches_the_contract() {
    let mut ws = WriteSet::default();
    account(&mut ws, 0, 0, 0); // fee sink sorts first
    account(&mut ws, 7, 3, 1_000);
    account(&mut ws, 9, 0, 500);
    ws.finish();
    assert_eq!(ws.hash(), expected(&ws));
}

/// Accounts plus storage, still inside the inline buffer.
#[test]
fn defi_shaped_write_set_matches_the_contract() {
    let mut ws = WriteSet::default();
    for i in 0..3 {
        account(&mut ws, i, i as u64, 10_000 + i as u64);
    }
    for i in 0..8u8 {
        ws.storage.push((
            (addr(0xC0), B256::with_last_byte(i)),
            U256::from(i as u64 * 7 + 1),
        ));
    }
    ws.finish();
    assert_eq!(ws.hash(), expected(&ws));
}

/// A CREATE carries bytecode, which overflows the inline buffer and
/// takes the streaming fallback. The two paths must agree.
#[test]
fn code_carrying_write_set_takes_the_fallback_and_still_matches() {
    let mut ws = WriteSet::default();
    account(&mut ws, 1, 1, 1);
    let code: bytes::Bytes = vec![0x60u8; 4096].into();
    ws.code.push((B256::with_last_byte(0xAB), code));
    ws.finish();
    assert_eq!(ws.hash(), expected(&ws));
}

/// Right at the boundary: enough accounts to approach and then exceed the
/// inline capacity, so both paths are exercised across the seam.
#[test]
fn matches_across_the_inline_boundary() {
    for count in 0..12u8 {
        let mut ws = WriteSet::default();
        for i in 0..count {
            account(&mut ws, i, i as u64, i as u64 * 13);
        }
        ws.finish();
        assert_eq!(
            ws.hash(),
            expected(&ws),
            "write set with {count} accounts diverged"
        );
    }
}

/// Sanity check: the hash actually distinguishes different content. A
/// buffered implementation that silently truncated would still pass the
/// equality tests above, if `expected` shared the same bug. It does
/// not, but this test pins the property directly.
#[test]
fn distinct_write_sets_hash_differently() {
    let mut a = WriteSet::default();
    account(&mut a, 1, 0, 100);
    a.finish();
    let mut b = WriteSet::default();
    account(&mut b, 1, 0, 101);
    b.finish();
    assert_ne!(a.hash(), b.hash());
    assert_ne!(a.hash(), B256::ZERO);
    let _ = address!("0000000000000000000000000000000000000001");
}
