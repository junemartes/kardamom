use super::*;

#[test]
fn header_roundtrips_little_endian() {
    let b = encode_session_keep_alive(7, 9);
    let h = MessageHeader::decode(&b).unwrap();
    assert_eq!(h.block_length, BLOCK_SESSION_KEEP_ALIVE);
    assert_eq!(h.template_id, TEMPLATE_SESSION_KEEP_ALIVE);
    assert_eq!(h.schema_id, SCHEMA_ID);
    assert_eq!(h.version, SCHEMA_VERSION);
    // First two bytes are the block length, little-endian.
    assert_eq!(&b[0..2], &16u16.to_le_bytes());
}

#[test]
#[allow(
    clippy::identity_op,
    reason = "(0<<16)|(3<<8) mirrors the major.minor version packing"
)]
fn session_connect_request_roundtrip() {
    let bytes = encode_session_connect_request(
        42,
        101,
        (0 << 16) | (3 << 8),
        "aeron:udp?endpoint=10.0.0.9:0",
        &[0xAA, 0xBB],
        "client-1",
    );
    let got = decode_session_connect_request(&bytes).unwrap();
    assert_eq!(got.correlation_id, 42);
    assert_eq!(got.response_stream_id, 101);
    assert_eq!(got.app_version, (0 << 16) | (3 << 8));
    assert_eq!(got.response_channel, "aeron:udp?endpoint=10.0.0.9:0");
    assert_eq!(got.encoded_credentials, vec![0xAA, 0xBB]);
    assert_eq!(got.client_info, "client-1");
}

#[test]
fn keep_alive_and_close_share_two_i64_body() {
    let ka = encode_session_keep_alive(3, 4);
    assert_eq!(
        decode_two_i64(&ka, TEMPLATE_SESSION_KEEP_ALIVE).unwrap(),
        (3, 4)
    );
    let cl = encode_session_close_request(5, 6);
    assert_eq!(
        decode_two_i64(&cl, TEMPLATE_SESSION_CLOSE_REQUEST).unwrap(),
        (5, 6)
    );
    // Wrong template id is rejected.
    assert_eq!(
        decode_two_i64(&ka, TEMPLATE_SESSION_CLOSE_REQUEST),
        Err(DecodeError::TemplateMismatch {
            want: TEMPLATE_SESSION_CLOSE_REQUEST,
            got: TEMPLATE_SESSION_KEEP_ALIVE
        })
    );
}

#[test]
fn session_message_wrap_unwrap_preserves_payload() {
    let payload = b"\x01canonical-record-bytes";
    let framed = wrap_session_message(11, 22, 1_700_000_000_000, payload);
    match decode_egress(&framed).unwrap() {
        Egress::SessionMessage(m) => {
            assert_eq!(m.leadership_term_id, 11);
            assert_eq!(m.cluster_session_id, 22);
            assert_eq!(m.timestamp, 1_700_000_000_000);
            assert_eq!(m.payload, payload);
        }
        other => panic!("expected SessionMessage, got {other:?}"),
    }
}

#[test]
fn session_event_roundtrip_via_egress() {
    let ev = SessionEvent {
        cluster_session_id: 99,
        correlation_id: 42,
        leadership_term_id: 3,
        leader_member_id: 1,
        code: EventCode::Ok,
        detail: String::new(),
    };
    let bytes = encode_session_event(&ev);
    assert!(matches!(decode_egress(&bytes), Ok(Egress::SessionEvent(g)) if g == ev));
}

#[test]
fn session_event_decodes_with_newer_server_block_length() {
    // Simulate a v16 server, with root block 44. This adds an optional
    // `version` field (i32) and `leaderHeartbeatTimeoutNs` (i64) after
    // `code`. The decoder must still find `detail`, using the sender's
    // blockLength.
    let mut b = Vec::new();
    put_header(&mut b, 44, TEMPLATE_SESSION_EVENT);
    b.extend_from_slice(&99i64.to_le_bytes()); // clusterSessionId
    b.extend_from_slice(&42i64.to_le_bytes()); // correlationId
    b.extend_from_slice(&3i64.to_le_bytes()); // leadershipTermId
    b.extend_from_slice(&2i32.to_le_bytes()); // leaderMemberId
    b.extend_from_slice(&i32::from(EventCode::Redirect).to_le_bytes()); // code
    b.extend_from_slice(&0i32.to_le_bytes()); // version (optional)
    b.extend_from_slice(&0i64.to_le_bytes()); // leaderHeartbeatTimeoutNs (optional)
    put_var(&mut b, b"0=host0:9000,1=host1:9000");
    match decode_egress(&b).unwrap() {
        Egress::SessionEvent(ev) => {
            assert_eq!(ev.code, EventCode::Redirect);
            assert_eq!(ev.leader_member_id, 2);
            assert_eq!(ev.detail, "0=host0:9000,1=host1:9000");
        }
        other => panic!("expected SessionEvent, got {other:?}"),
    }
}

#[test]
fn new_leader_event_roundtrip_via_egress() {
    let ev = NewLeaderEvent {
        leadership_term_id: 7,
        cluster_session_id: 22,
        leader_member_id: 2,
        ingress_endpoints: "0=h0:9,1=h1:9,2=h2:9".into(),
    };
    let bytes = encode_new_leader_event(&ev);
    assert!(matches!(decode_egress(&bytes), Ok(Egress::NewLeader(g)) if g == ev));
}

#[test]
fn unknown_template_is_preserved_not_an_error() {
    // TimerEvent (template 20) is not a client-facing message. The
    // egress decoder should return it as `Other`, not fail.
    let mut b = Vec::new();
    put_header(&mut b, 8, 20);
    b.extend_from_slice(&0i64.to_le_bytes());
    assert_eq!(
        decode_egress(&b).unwrap(),
        Egress::Other { template_id: 20 }
    );
}

#[test]
fn truncated_buffer_errors_cleanly() {
    let b = [0u8; 4]; // shorter than an 8-byte header
    assert!(matches!(
        MessageHeader::decode(&b),
        Err(DecodeError::TooShort { .. })
    ));
}

#[test]
fn wrong_schema_id_rejected() {
    let mut b = Vec::new();
    // schema id 222 instead of 111.
    b.extend_from_slice(&16u16.to_le_bytes());
    b.extend_from_slice(&TEMPLATE_SESSION_EVENT.to_le_bytes());
    b.extend_from_slice(&222u16.to_le_bytes());
    b.extend_from_slice(&SCHEMA_VERSION.to_le_bytes());
    b.extend_from_slice(&[0u8; 32]);
    assert_eq!(
        decode_egress(&b),
        Err(DecodeError::SchemaMismatch { got: 222 })
    );
}
