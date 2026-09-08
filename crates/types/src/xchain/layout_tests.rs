//! Layout pins: `forge inspect Outbox storage-layout`, `forge inspect Inbox
//! storage-layout`, `forge inspect Outbox methodIdentifiers`, `forge inspect
//! Outbox events`, and `cast index`.

use super::*;
use alloy_primitives::{B256, b256};

#[test]
fn slot_indices_match_forge_inspect_storage_layout() {
    // Outbox: nonces at slot 0, sentMessages at slot 1.
    assert_eq!(OUTBOX_NONCES_SLOT_INDEX, 0);
    assert_eq!(SENT_MESSAGES_SLOT_INDEX, 1);
    // Inbox: delivered at slot 0, nextSeq at slot 1.
    assert_eq!(INBOX_DELIVERED_SLOT_INDEX, 0);
    assert_eq!(INBOX_NEXT_SEQ_SLOT_INDEX, 1);
}

#[test]
fn mapping_slots_match_cast_index() {
    // cast index uint64 412399 1
    assert_eq!(
        inbox_next_seq_slot(412_399),
        b256!("5ac03b415b91ae90f3b79893af8b4cdd0ad13a6ca17919c466ef18c217a044fd")
    );
    // inner = cast index uint64 412399 0; cast index uint64 3 <inner>
    assert_eq!(
        inbox_delivered_slot(412_399, 3),
        b256!("879e07c4bf04e0eabddfb4406f66267e8b610479d5da37e69d2025f905df870f")
    );
    // cast index uint64 412347 0
    assert_eq!(
        outbox_nonces_slot(412_347),
        b256!("8b93788024e4921562298d6c9986e253bebd7e9e30a69000736530e7c7ccb02c")
    );
    // cast index bytes32 0x11..11 1
    assert_eq!(
        sent_messages_slot(B256::repeat_byte(0x11)),
        b256!("7deb3b60ec0f1bf56dbdd0ffedbadafddeaa08947884ff0f215ce93ee1826102")
    );
    // The cross-language msg_leaf vector as the key:
    // cast index bytes32 0x0df14340..4d3c 1
    assert_eq!(
        sent_messages_slot(b256!(
            "0df14340efd8c8b32f4c333c3dca8470b0bae319a3dfe32adb213df2b8834d3c"
        )),
        b256!("d4b78be0c1de834d6a6db01a7ae3f433776afbc98b26f4e51412b94effd6d438")
    );
}

#[test]
fn send_message_selector_and_topic_match_forge_inspect() {
    // forge inspect Outbox methodIdentifiers
    assert_eq!(outbox_send_message_selector(), [0xbd, 0x1b, 0x0f, 0xd9]);
    // forge inspect Outbox events
    assert_eq!(
        message_sent_topic0(),
        b256!("a00ff5f6f9bf2c30c7cd578b6a82c98b08f2d33a5677222b9d8b925c62a48082")
    );
}
