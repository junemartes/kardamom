//! Small RLP building blocks shared by more than one hashing scheme.

use alloc::vec::Vec;

use alloy_rlp::{Encodable, Header};

/// RLP-encode the two-element list `[a, b]`. [`crate::epoch::source_hash`]
/// and [`crate::xchain::remote_source_hash`] both feed a two-field
/// `[domain, id_hash]` pair through this, one inner and one outer call
/// each, into their consensus source hash.
pub(crate) fn encode_list_two<A: Encodable, B: Encodable>(a: &A, b: &B) -> Vec<u8> {
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
