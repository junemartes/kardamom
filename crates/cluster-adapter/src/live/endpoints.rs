//! Ingress endpoint helpers for the session thread. They parse the
//! `memberId=host:port,…` member list, open (and round-robin) ingress
//! publications, and provide the small byte and clock utilities the loop
//! shares.

use std::time::{SystemTime, UNIX_EPOCH};

use kardamom_log::aeron_live::{AeronRuntime, PubHandle};
use rkyv::util::AlignedVec;

use super::with_control_term_length;

/// Parse a `memberId=host:port,…` list and open an ingress publication to
/// the given member. Use small terms through [`with_control_term_length`]
/// (see its doc for the reason).
pub(super) fn open_leader_pub(
    rt: &AeronRuntime,
    endpoints: &str,
    member_id: i32,
    stream_id: i32,
) -> Option<PubHandle> {
    let endpoint = endpoint_for_member(endpoints, member_id)?;
    let uri = with_control_term_length(&format!("aeron:udp?endpoint={endpoint}"));
    rt.open_publication(&uri, stream_id).ok()
}

/// Extract `host:port` for `member_id` from a `memberId=host:port,…` list.
pub(super) fn endpoint_for_member(endpoints: &str, member_id: i32) -> Option<String> {
    let want = member_id.to_string();
    endpoints.split(',').find_map(|entry| {
        let (id, ep) = entry.split_once('=')?;
        (id.trim() == want).then(|| ep.trim().to_string())
    })
}

/// All member ids present in a `memberId=host:port,…` list, sorted.
pub(super) fn member_ids(endpoints: &str) -> Vec<i32> {
    let mut ids: Vec<i32> = endpoints
        .split(',')
        .filter_map(|entry| entry.split_once('=')?.0.trim().parse().ok())
        .collect();
    ids.sort_unstable();
    ids
}

/// Open a publication to the member after `current` in the list, and wrap
/// around at the end. This is the round-robin step for self-heal
/// reconnects. It skips dead IDs and returns the first member whose
/// publication opens.
pub(super) fn open_next_member_pub(
    rt: &AeronRuntime,
    endpoints: &str,
    current: i32,
    stream_id: i32,
) -> Option<(i32, PubHandle)> {
    let ids = member_ids(endpoints);
    if ids.is_empty() {
        return None;
    }
    let start = ids.iter().position(|&id| id == current).unwrap_or(0);
    // Try every member once, starting from the one after `current`.
    for step in 1..=ids.len() {
        let id = ids[(start + step) % ids.len()];
        if let Some(p) = open_leader_pub(rt, endpoints, id, stream_id) {
            return Some((id, p));
        }
    }
    None
}

pub(super) fn to_aligned(bytes: &[u8]) -> AlignedVec {
    let mut av = AlignedVec::new();
    av.extend_from_slice(bytes);
    av
}

pub(super) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_for_member_parses_list() {
        let eps = "0=h0:9000,1=h1:9001,2=h2:9002";
        assert_eq!(endpoint_for_member(eps, 0).as_deref(), Some("h0:9000"));
        assert_eq!(endpoint_for_member(eps, 2).as_deref(), Some("h2:9002"));
        assert_eq!(endpoint_for_member(eps, 5), None);
    }

    #[test]
    fn endpoint_for_member_tolerates_spaces() {
        assert_eq!(
            endpoint_for_member("0 = h0:9000 , 1 = h1:9001", 1).as_deref(),
            Some("h1:9001")
        );
    }
}
