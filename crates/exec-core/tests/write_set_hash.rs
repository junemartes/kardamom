//! `WriteSet::hash` is the determinism witness every replica compares, so
//! its byte sequence is a consensus contract. The implementation has two
//! paths — a stack-buffered fast path and a streaming fallback for write
//! sets carrying bytecode — and they must be indistinguishable.
//!
//! These tests recompute the documented sequence independently and compare,
//! so they pin the CONTRACT rather than merely checking the two paths
//! agree with each other.

use alloy_primitives::{Address, B256, Keccak256, U256, address};
use kardamom_exec_core::delta::WriteSet;

/// The contract, spelled out: "ACC" ‖ u32be(count) ‖ per account
/// (addr ‖ u64le(nonce) ‖ u256be(balance) ‖ code_hash), then "STO" ‖
/// u32be(count) ‖ per slot (addr ‖ key ‖ u256be(value)), then "COD" ‖
/// u32be(count) ‖ per entry (hash ‖ u64le(len) ‖ bytes).
fn expected(ws: &WriteSet) -> B256 {
    let mut h = Keccak256::new();
    h.update(b"ACC");
    h.update((ws.accounts.len() as u32).to_be_bytes());
    for (addr, (nonce, balance, code_hash)) in &ws.accounts {
        h.update(addr.as_slice());
        h.update(nonce.to_le_bytes());
        h.update(balance.to_be_bytes::<32>());
        h.update(code_hash.as_slice());
    }
    h.update(b"STO");
    h.update((ws.storage.len() as u32).to_be_bytes());
    for ((addr, key), value) in &ws.storage {
        h.update(addr.as_slice());
        h.update(key.as_slice());
        h.update(value.to_be_bytes::<32>());
    }
    h.update(b"COD");
    h.update((ws.code.len() as u32).to_be_bytes());
    for (code_hash, bytes) in &ws.code {
        h.update(code_hash.as_slice());
        h.update((bytes.len() as u64).to_le_bytes());
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

/// The shape of a plain transfer: sender, recipient, fee sink — the case
/// the stack buffer exists for, and the one the STM commit tail hashes
/// once per transaction.
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

/// A CREATE carries bytecode, which overflows the inline buffer and takes
/// the streaming fallback — the two paths must agree.
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

/// Sanity: the hash actually distinguishes different content (a buffered
/// implementation that silently truncated would still pass the equality
/// tests above if `expected` shared the bug — it does not, but this pins
/// the property directly).
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
