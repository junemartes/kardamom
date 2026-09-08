use super::*;
use alloy_primitives::{Address, B256, Bytes as AlloyBytes, U256};
use alloy_sol_types::{SolCall, sol};
use bytes::Bytes;

sol! {
    struct SolCb {
        address target;
        uint64 gasLimit;
        bytes32 context;
    }

    function deliver(
        uint64 originChainId,
        uint64 seq,
        address originSender,
        address target,
        uint256 value,
        uint64 gasLimit,
        bytes data,
        SolCb cb
    );
}

const ORIGIN: u64 = 412_346;

fn wire_msg(data: &[u8], callback: Option<Callback>) -> XChainMessage {
    XChainMessage {
        source_hash: remote_source_hash(ORIGIN, 3),
        seq: 3,
        origin_sender: Address::repeat_byte(0xA7),
        target: Address::repeat_byte(0xB9),
        value: 0,
        gas_limit: 250_000,
        input: Bytes::copy_from_slice(data),
        callback,
    }
}

/// What `SolCall::abi_encode` produces for the same message — the
/// compiler-grade oracle the hand-encoding must match byte for byte.
fn reference(origin_chain_id: u64, m: &XChainMessage) -> alloc::vec::Vec<u8> {
    let cb = m.callback.unwrap_or_default();
    deliverCall {
        originChainId: origin_chain_id,
        seq: m.seq,
        originSender: m.origin_sender,
        target: m.target,
        value: U256::from(m.value),
        gasLimit: m.gas_limit,
        data: AlloyBytes::copy_from_slice(m.input.as_ref()),
        cb: SolCb {
            target: cb.target,
            gasLimit: cb.gas_limit,
            context: cb.context,
        },
    }
    .abi_encode()
}

#[test]
fn selector_matches_sol_resolution() {
    // `sol!` resolves the user-defined struct to its tuple type when
    // computing the selector — the same resolution Solidity applies to
    // `XChain.Callback calldata cb` — so this pins the signature string
    // against the actual Inbox dispatch.
    assert_eq!(inbox_deliver_selector(), deliverCall::SELECTOR);
}

/// For one payload, both callback shapes must match the `sol!` reference
/// encoding.
fn assert_hand_encoding_matches_sol_types(data: &[u8], cb: Callback) {
    for callback in [None, Some(cb)] {
        let m = wire_msg(data, callback);
        assert_eq!(
            deliver_calldata(ORIGIN, &m),
            reference(ORIGIN, &m),
            "data len {}, callback {}",
            data.len(),
            callback.is_some()
        );
    }
}

#[test]
fn hand_encoding_is_byte_identical_to_sol_types() {
    let cb = Callback {
        target: Address::repeat_byte(0x0C),
        gas_limit: 90_000,
        context: B256::repeat_byte(0x1D),
    };
    // Empty, sub-word, and word+1 payloads exercise the zero-length
    // tail, right-padding, and a two-word tail respectively.
    for data in [&b""[..], &[0x01][..], &[0xEE; 33][..]] {
        assert_hand_encoding_matches_sol_types(data, cb);
    }
}

#[test]
fn value_rides_the_uint256_word() {
    // v1 execution rejects nonzero value, but the encoding must already
    // carry it faithfully — the wire does not change when value ships.
    let mut m = wire_msg(&[0x02], None);
    m.value = u128::MAX;
    assert_eq!(deliver_calldata(ORIGIN, &m), reference(ORIGIN, &m));
}
