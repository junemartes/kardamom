//! SBE codec for the Aeron Cluster **client session protocol**.
//!
//! Hand-written to match the vendored `io.aeron.cluster.codecs` schema
//! (`aeron-cluster-codecs.xml`, `id="111" version="16"`, little-endian). Only
//! the client-facing subset is implemented — what a client sends to ingress and
//! receives on egress:
//!
//! | message               | template | dir     | root block |
//! |-----------------------|----------|---------|------------|
//! | SessionMessageHeader  | 1        | both    | 24         |
//! | SessionEvent          | 2        | egress  | ≥32        |
//! | SessionConnectRequest | 3        | ingress | 16         |
//! | SessionCloseRequest   | 4        | ingress | 16         |
//! | SessionKeepAlive      | 5        | ingress | 16         |
//! | NewLeaderEvent        | 6        | egress  | 20         |
//!
//! Every SBE message is `[messageHeader(8)][root block][var-length fields…]`.
//! `messageHeader` = `blockLength:u16, templateId:u16, schemaId:u16,
//! version:u16`. Var fields are `length:u32` + bytes. Decoders locate the
//! var section via the *sender's* `blockLength` (from the header), so newer
//! servers that append optional trailing fixed fields decode correctly.

use thiserror::Error;

/// SBE schema id for `io.aeron.cluster.codecs`.
pub const SCHEMA_ID: u16 = 111;
/// Schema version we encode at.
pub const SCHEMA_VERSION: u16 = 16;
/// SBE message header length in bytes.
pub const HEADER_LEN: usize = 8;

pub const TEMPLATE_SESSION_MESSAGE_HEADER: u16 = 1;
pub const TEMPLATE_SESSION_EVENT: u16 = 2;
pub const TEMPLATE_SESSION_CONNECT_REQUEST: u16 = 3;
pub const TEMPLATE_SESSION_CLOSE_REQUEST: u16 = 4;
pub const TEMPLATE_SESSION_KEEP_ALIVE: u16 = 5;
pub const TEMPLATE_NEW_LEADER_EVENT: u16 = 6;

const BLOCK_SESSION_MESSAGE_HEADER: u16 = 24;
const BLOCK_SESSION_EVENT_MIN: u16 = 32;
const BLOCK_SESSION_CONNECT_REQUEST: u16 = 16;
const BLOCK_SESSION_CLOSE_REQUEST: u16 = 16;
const BLOCK_SESSION_KEEP_ALIVE: u16 = 16;
const BLOCK_NEW_LEADER_EVENT: u16 = 20;

/// `EventCode` enum from the schema (a `SessionEvent.code`).
///
/// `num_enum` derives the two conversions. `catch_all` maps an unknown
/// code into `Unknown(i32)` and keeps the value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, num_enum::FromPrimitive, num_enum::IntoPrimitive)]
#[repr(i32)]
pub enum EventCode {
    Ok = 0,
    Error = 1,
    Redirect = 2,
    AuthenticationRejected = 3,
    Closed = 4,
    /// Unknown / future code, preserved verbatim.
    #[num_enum(catch_all)]
    Unknown(i32),
}

/// Errors decoding a cluster-protocol frame.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DecodeError {
    #[error("buffer too short: need {need} bytes at offset {at}, have {have}")]
    TooShort { at: usize, need: usize, have: usize },
    #[error("schema id mismatch: expected {SCHEMA_ID}, got {got}")]
    SchemaMismatch { got: u16 },
    #[error("unexpected template id {got} (wanted {want})")]
    TemplateMismatch { want: u16, got: u16 },
}

/// Decoded SBE message header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageHeader {
    pub block_length: u16,
    pub template_id: u16,
    pub schema_id: u16,
    pub version: u16,
}

impl MessageHeader {
    pub fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        Ok(Self {
            block_length: rd_u16(buf, 0)?,
            template_id: rd_u16(buf, 2)?,
            schema_id: rd_u16(buf, 4)?,
            version: rd_u16(buf, 6)?,
        })
    }
}

// ── byte helpers ────────────────────────────────────────────────────────────

fn need(buf: &[u8], at: usize, n: usize) -> Result<&[u8], DecodeError> {
    buf.get(at..at + n).ok_or(DecodeError::TooShort {
        at,
        need: n,
        have: buf.len().saturating_sub(at),
    })
}

// Exact-width LE reads come from [`crate::bytes`] (shared with the
// app-envelope codec in `kardamom-cluster-adapter`); a `None` maps to this
// codec's own [`DecodeError::TooShort`] with this codec's offsets.

fn too_short(buf: &[u8], at: usize, need: usize) -> DecodeError {
    DecodeError::TooShort {
        at,
        need,
        have: buf.len().saturating_sub(at),
    }
}

fn rd_u16(buf: &[u8], at: usize) -> Result<u16, DecodeError> {
    crate::bytes::u16_le(buf, at).ok_or_else(|| too_short(buf, at, 2))
}
fn rd_i32(buf: &[u8], at: usize) -> Result<i32, DecodeError> {
    crate::bytes::i32_le(buf, at).ok_or_else(|| too_short(buf, at, 4))
}
fn rd_u32(buf: &[u8], at: usize) -> Result<u32, DecodeError> {
    crate::bytes::u32_le(buf, at).ok_or_else(|| too_short(buf, at, 4))
}
fn rd_i64(buf: &[u8], at: usize) -> Result<i64, DecodeError> {
    crate::bytes::i64_le(buf, at).ok_or_else(|| too_short(buf, at, 8))
}

/// Read a var-length field (`length:u32` + bytes) starting at `at` in `body`.
/// Returns the bytes and the offset just past them.
fn rd_var(body: &[u8], at: usize) -> Result<(&[u8], usize), DecodeError> {
    let len = rd_u32(body, at)? as usize;
    let start = at + 4;
    let bytes = need(body, start, len)?;
    Ok((bytes, start + len))
}

fn put_header(buf: &mut Vec<u8>, block_length: u16, template_id: u16) {
    buf.extend_from_slice(&block_length.to_le_bytes());
    buf.extend_from_slice(&template_id.to_le_bytes());
    buf.extend_from_slice(&SCHEMA_ID.to_le_bytes());
    buf.extend_from_slice(&SCHEMA_VERSION.to_le_bytes());
}

fn put_var(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(bytes);
}

// ── SessionConnectRequest (ingress) ─────────────────────────────────────────

/// Encode a `SessionConnectRequest`. `app_version` is the client's semantic
/// protocol version; `response_channel` is the client's egress channel URI.
pub fn encode_session_connect_request(
    correlation_id: i64,
    response_stream_id: i32,
    app_version: i32,
    response_channel: &str,
    encoded_credentials: &[u8],
    client_info: &str,
) -> Vec<u8> {
    let mut b = Vec::new();
    put_header(
        &mut b,
        BLOCK_SESSION_CONNECT_REQUEST,
        TEMPLATE_SESSION_CONNECT_REQUEST,
    );
    b.extend_from_slice(&correlation_id.to_le_bytes());
    b.extend_from_slice(&response_stream_id.to_le_bytes());
    b.extend_from_slice(&app_version.to_le_bytes());
    put_var(&mut b, response_channel.as_bytes());
    put_var(&mut b, encoded_credentials);
    put_var(&mut b, client_info.as_bytes());
    b
}

/// Decoded `SessionConnectRequest` (owned).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionConnectRequest {
    pub correlation_id: i64,
    pub response_stream_id: i32,
    pub app_version: i32,
    pub response_channel: String,
    pub encoded_credentials: Vec<u8>,
    pub client_info: String,
}

pub fn decode_session_connect_request(buf: &[u8]) -> Result<SessionConnectRequest, DecodeError> {
    let h = MessageHeader::decode(buf)?;
    expect(&h, SCHEMA_ID, TEMPLATE_SESSION_CONNECT_REQUEST)?;
    let body = &buf[HEADER_LEN..];
    let correlation_id = rd_i64(body, 0)?;
    let response_stream_id = rd_i32(body, 8)?;
    let app_version = rd_i32(body, 12)?;
    let (chan, p1) = rd_var(body, h.block_length as usize)?;
    let (creds, p2) = rd_var(body, p1)?;
    let (info, _p3) = rd_var(body, p2)?;
    Ok(SessionConnectRequest {
        correlation_id,
        response_stream_id,
        app_version,
        response_channel: String::from_utf8_lossy(chan).into_owned(),
        encoded_credentials: creds.to_vec(),
        client_info: String::from_utf8_lossy(info).into_owned(),
    })
}

// ── SessionKeepAlive / SessionCloseRequest (ingress) ────────────────────────

/// Encode the two-i64 body shared by KeepAlive / CloseRequest (the encode-side
/// mirror of [`decode_two_i64`]).
fn encode_two_i64(block_length: u16, template_id: u16, a: i64, b: i64) -> Vec<u8> {
    let mut buf = Vec::new();
    put_header(&mut buf, block_length, template_id);
    buf.extend_from_slice(&a.to_le_bytes());
    buf.extend_from_slice(&b.to_le_bytes());
    buf
}

pub fn encode_session_keep_alive(leadership_term_id: i64, cluster_session_id: i64) -> Vec<u8> {
    encode_two_i64(
        BLOCK_SESSION_KEEP_ALIVE,
        TEMPLATE_SESSION_KEEP_ALIVE,
        leadership_term_id,
        cluster_session_id,
    )
}

pub fn encode_session_close_request(leadership_term_id: i64, cluster_session_id: i64) -> Vec<u8> {
    encode_two_i64(
        BLOCK_SESSION_CLOSE_REQUEST,
        TEMPLATE_SESSION_CLOSE_REQUEST,
        leadership_term_id,
        cluster_session_id,
    )
}

/// Decode the two-i64 body shared by KeepAlive / CloseRequest. Returns
/// `(leadership_term_id, cluster_session_id)`.
pub fn decode_two_i64(buf: &[u8], want_template: u16) -> Result<(i64, i64), DecodeError> {
    let h = MessageHeader::decode(buf)?;
    expect(&h, SCHEMA_ID, want_template)?;
    let body = &buf[HEADER_LEN..];
    Ok((rd_i64(body, 0)?, rd_i64(body, 8)?))
}

// ── SessionMessageHeader (both directions: wraps an app payload) ────────────

/// Wrap an application `payload` in a `SessionMessageHeader` for ingress.
pub fn wrap_session_message(
    leadership_term_id: i64,
    cluster_session_id: i64,
    timestamp: i64,
    payload: &[u8],
) -> Vec<u8> {
    let mut b =
        Vec::with_capacity(HEADER_LEN + BLOCK_SESSION_MESSAGE_HEADER as usize + payload.len());
    put_header(
        &mut b,
        BLOCK_SESSION_MESSAGE_HEADER,
        TEMPLATE_SESSION_MESSAGE_HEADER,
    );
    b.extend_from_slice(&leadership_term_id.to_le_bytes());
    b.extend_from_slice(&cluster_session_id.to_le_bytes());
    b.extend_from_slice(&timestamp.to_le_bytes());
    b.extend_from_slice(payload);
    b
}

/// A decoded egress `SessionMessageHeader` plus its application payload slice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionMessage<'a> {
    pub leadership_term_id: i64,
    pub cluster_session_id: i64,
    pub timestamp: i64,
    pub payload: &'a [u8],
}

// ── SessionEvent (egress) ───────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionEvent {
    pub cluster_session_id: i64,
    pub correlation_id: i64,
    pub leadership_term_id: i64,
    pub leader_member_id: i32,
    pub code: EventCode,
    /// Detail string — error text, or the ingress-endpoints list on REDIRECT.
    pub detail: String,
}

/// Encode a minimal `SessionEvent` (root block = 32; optional trailing fields
/// omitted). Primarily for tests and a future in-Rust cluster mock.
pub fn encode_session_event(ev: &SessionEvent) -> Vec<u8> {
    let mut b = Vec::new();
    put_header(&mut b, BLOCK_SESSION_EVENT_MIN, TEMPLATE_SESSION_EVENT);
    b.extend_from_slice(&ev.cluster_session_id.to_le_bytes());
    b.extend_from_slice(&ev.correlation_id.to_le_bytes());
    b.extend_from_slice(&ev.leadership_term_id.to_le_bytes());
    b.extend_from_slice(&ev.leader_member_id.to_le_bytes());
    b.extend_from_slice(&i32::from(ev.code).to_le_bytes());
    put_var(&mut b, ev.detail.as_bytes());
    b
}

fn decode_session_event(h: &MessageHeader, body: &[u8]) -> Result<SessionEvent, DecodeError> {
    let cluster_session_id = rd_i64(body, 0)?;
    let correlation_id = rd_i64(body, 8)?;
    let leadership_term_id = rd_i64(body, 16)?;
    let leader_member_id = rd_i32(body, 24)?;
    let code = EventCode::from(rd_i32(body, 28)?);
    // Optional trailing fixed fields (version, leaderHeartbeatTimeoutNs) are
    // skipped via the sender's blockLength.
    let (detail, _) = rd_var(body, h.block_length as usize)?;
    Ok(SessionEvent {
        cluster_session_id,
        correlation_id,
        leadership_term_id,
        leader_member_id,
        code,
        detail: String::from_utf8_lossy(detail).into_owned(),
    })
}

// ── NewLeaderEvent (egress) ─────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewLeaderEvent {
    pub leadership_term_id: i64,
    pub cluster_session_id: i64,
    pub leader_member_id: i32,
    /// Comma-separated `memberId=endpoint` ingress-endpoint list.
    pub ingress_endpoints: String,
}

pub fn encode_new_leader_event(ev: &NewLeaderEvent) -> Vec<u8> {
    let mut b = Vec::new();
    put_header(&mut b, BLOCK_NEW_LEADER_EVENT, TEMPLATE_NEW_LEADER_EVENT);
    b.extend_from_slice(&ev.leadership_term_id.to_le_bytes());
    b.extend_from_slice(&ev.cluster_session_id.to_le_bytes());
    b.extend_from_slice(&ev.leader_member_id.to_le_bytes());
    put_var(&mut b, ev.ingress_endpoints.as_bytes());
    b
}

fn decode_new_leader_event(h: &MessageHeader, body: &[u8]) -> Result<NewLeaderEvent, DecodeError> {
    let leadership_term_id = rd_i64(body, 0)?;
    let cluster_session_id = rd_i64(body, 8)?;
    let leader_member_id = rd_i32(body, 16)?;
    let (eps, _) = rd_var(body, h.block_length as usize)?;
    Ok(NewLeaderEvent {
        leadership_term_id,
        cluster_session_id,
        leader_member_id,
        ingress_endpoints: String::from_utf8_lossy(eps).into_owned(),
    })
}

// ── Egress dispatch ─────────────────────────────────────────────────────────

/// A decoded egress frame from the cluster. `Other` preserves the template id
/// for any message the client does not act on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Egress<'a> {
    SessionEvent(SessionEvent),
    NewLeader(NewLeaderEvent),
    SessionMessage(SessionMessage<'a>),
    Other { template_id: u16 },
}

/// Decode one egress frame. Used by the live egress poll loop.
pub fn decode_egress(buf: &[u8]) -> Result<Egress<'_>, DecodeError> {
    let h = MessageHeader::decode(buf)?;
    if h.schema_id != SCHEMA_ID {
        return Err(DecodeError::SchemaMismatch { got: h.schema_id });
    }
    let body = &buf[HEADER_LEN..];
    match h.template_id {
        TEMPLATE_SESSION_EVENT => Ok(Egress::SessionEvent(decode_session_event(&h, body)?)),
        TEMPLATE_NEW_LEADER_EVENT => Ok(Egress::NewLeader(decode_new_leader_event(&h, body)?)),
        TEMPLATE_SESSION_MESSAGE_HEADER => {
            let leadership_term_id = rd_i64(body, 0)?;
            let cluster_session_id = rd_i64(body, 8)?;
            let timestamp = rd_i64(body, 16)?;
            let payload = body
                .get(h.block_length as usize..)
                .ok_or(DecodeError::TooShort {
                    at: h.block_length as usize,
                    need: 0,
                    have: body.len(),
                })?;
            Ok(Egress::SessionMessage(SessionMessage {
                leadership_term_id,
                cluster_session_id,
                timestamp,
                payload,
            }))
        }
        other => Ok(Egress::Other { template_id: other }),
    }
}

fn expect(h: &MessageHeader, schema: u16, template: u16) -> Result<(), DecodeError> {
    if h.schema_id != schema {
        return Err(DecodeError::SchemaMismatch { got: h.schema_id });
    }
    if h.template_id != template {
        return Err(DecodeError::TemplateMismatch {
            want: template,
            got: h.template_id,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
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
    #[allow(clippy::identity_op)] // (0<<16)|(3<<8) mirrors the major.minor version packing
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
        // Simulate a v16 server: root block = 44 (adds optional `version` i32
        // and `leaderHeartbeatTimeoutNs` i64 after `code`). Our decoder must
        // still find `detail` via the sender's blockLength.
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
        // TimerEvent (template 20) is not a client-facing message; the egress
        // decoder should surface it as `Other`, not fail.
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
}
