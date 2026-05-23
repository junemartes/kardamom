use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_log::codec::{access, encode, materialize};
use kardamom_types::*;

#[test]
fn log_codec_access_and_materialize() {
    let v = TxEnvelope {
        correlation_id: 7,
        raw_tx: Bytes::from_static(b"raw"),
        sender: Address::repeat_byte(0xAA),
        tx_hash: B256::repeat_byte(0xBB),
    };
    let bytes = encode(&v).unwrap();

    // Zero-copy view.
    let view = access::<TxEnvelope>(&bytes).unwrap();
    assert_eq!(view.correlation_id, 7);

    // Owning view.
    let back: TxEnvelope = materialize(&bytes).unwrap();
    assert_eq!(back, v);
}
