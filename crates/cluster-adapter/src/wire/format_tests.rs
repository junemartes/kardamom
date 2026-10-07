//! The kinds and record types against the format registry
//! (`formats.toml` at the workspace root). The reader tests decode a frame
//! for every byte value, so a new kind in the decoder fails them until the
//! registry lists it.

use kardamom_formats::Registry;

use super::*;

/// The byte values for which `known` holds.
fn known_bytes(known: impl Fn(u8) -> bool) -> Vec<u32> {
    (0..=u8::MAX)
        .filter(|byte| known(*byte))
        .map(u32::from)
        .collect()
}

fn is_known_egress_kind(kind: u8) -> bool {
    !matches!(
        EgressItem::decode(&[kind]),
        Err(WireError::BadEgressKind(_))
    )
}

fn is_known_record_type(record_type: u8) -> bool {
    let payload = [&[0_u8; CANONICAL_ID_LEN][..], &[record_type]].concat();
    let frame = encode_egress_record(0, &payload).unwrap();
    !matches!(EgressItem::decode(&frame), Err(WireError::BadRecordType(_)))
}

#[test]
fn the_ingress_kinds_stay_within_the_sealer_reader() {
    let reads_max = Registry::workspace_versions("sealer-ingress-kinds").reads_max;
    let kinds = [
        KIND_INGRESS_RECORD,
        KIND_REPLAY_REQUEST,
        KIND_SUBSCRIBE,
        KIND_BATCH,
        KIND_ORIGIN_RECORD,
        KIND_REMOTE_ORIGIN_RECORD,
        KIND_VOID_REQUEST,
        KIND_POSTED_CURSOR,
        KIND_RECORDED_CURSOR,
    ];
    assert!(kinds.into_iter().all(|kind| u32::from(kind) <= reads_max));
}

#[test]
fn the_egress_reader_knows_exactly_the_registry_range() {
    let egress = Registry::workspace_versions("sealer-egress-kinds");
    let expected: Vec<u32> = (egress.reads_min..=egress.reads_max).collect();
    assert_eq!(known_bytes(is_known_egress_kind), expected);
}

#[test]
fn the_record_type_reader_knows_exactly_the_registry_range() {
    let record_types = Registry::workspace_versions("sealer-record-types");
    let expected: Vec<u32> = (record_types.reads_min..=record_types.reads_max).collect();
    assert_eq!(known_bytes(is_known_record_type), expected);
    assert_eq!(record_types.writes, u32::from(RT_VOID));
}
