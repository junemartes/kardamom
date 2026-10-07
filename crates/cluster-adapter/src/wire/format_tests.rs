//! The kinds and record types against the format registry
//! (`formats.toml` at the workspace root).

use kardamom_formats::{Registry, Versions};

use super::*;

fn versions(id: &str) -> Versions {
    Registry::workspace().unwrap().versions(id).unwrap()
}

fn is_known_egress_kind(kind: u32) -> bool {
    let frame = [u8::try_from(kind).unwrap()];
    !matches!(EgressItem::decode(&frame), Err(WireError::BadEgressKind(_)))
}

#[test]
fn the_ingress_kinds_stay_within_the_registry() {
    let writes = versions("sealer-ingress-kinds").writes;
    let kinds = [
        KIND_INGRESS_RECORD,
        KIND_REPLAY_REQUEST,
        KIND_SUBSCRIBE,
        KIND_BATCH,
        KIND_ORIGIN_RECORD,
        KIND_REMOTE_ORIGIN_RECORD,
        KIND_VOID_REQUEST,
        KIND_POSTED_CURSOR,
    ];
    assert!(kinds.into_iter().all(|kind| u32::from(kind) <= writes));
}

#[test]
fn the_egress_reader_knows_exactly_the_registry_range() {
    let egress = versions("sealer-egress-kinds");
    assert!((egress.reads_min..=egress.reads_max).all(is_known_egress_kind));
    assert!(!is_known_egress_kind(egress.reads_min - 1));
    assert!(!is_known_egress_kind(egress.reads_max + 1));
}

#[test]
fn the_record_types_match_the_registry() {
    let types = [RT_TXREF, RT_DEPOSITREF, RT_EPOCH, RT_REMOTE_EPOCH, RT_VOID].map(u32::from);
    let record_types = versions("sealer-record-types");
    assert_eq!(types.iter().min(), Some(&record_types.reads_min));
    assert_eq!(types.iter().max(), Some(&record_types.reads_max));
    assert_eq!(record_types.writes, u32::from(RT_VOID));
}
