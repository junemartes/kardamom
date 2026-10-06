//! Metric name constants for the live cluster client. Every service that
//! drives a cluster session exports them under its own `service` and
//! `host_id` labels, so the sealer cluster is observed from each client.

/// 1 while the session is open, 0 after it failed. A client that stays at 0
/// finds no member that answers.
pub const CONNECTED: &str = "kardamom_cluster_client_connected";
/// The member id of the leader the ingress publication points at.
pub const LEADER_MEMBER_ID: &str = "kardamom_cluster_client_leader_member_id";
/// Leader changes the session followed: a redirect at connect, or a new
/// leader event on an open session. A rate above zero in steady state is
/// an election.
pub const LEADER_CHANGES_TOTAL: &str = "kardamom_cluster_client_leader_changes_total";
/// Sessions opened. One per process life in steady state; more is a
/// reconnect.
pub const SESSIONS_TOTAL: &str = "kardamom_cluster_client_sessions_total";

/// The session opened against `leader_member_id`.
pub(crate) fn record_connected(leader_member_id: i32) {
    metrics::gauge!(CONNECTED).set(1.0);
    metrics::gauge!(LEADER_MEMBER_ID).set(f64::from(leader_member_id));
    metrics::counter!(SESSIONS_TOTAL).increment(1);
}

/// The session follows a new leader.
pub(crate) fn record_leader(leader_member_id: i32) {
    metrics::gauge!(LEADER_MEMBER_ID).set(f64::from(leader_member_id));
    metrics::counter!(LEADER_CHANGES_TOTAL).increment(1);
}

/// The session failed.
pub(crate) fn record_failed() {
    metrics::gauge!(CONNECTED).set(0.0);
}

pub fn describe() {
    metrics::describe_gauge!(
        CONNECTED,
        "1 while the cluster session is open, 0 after it failed"
    );
    metrics::describe_gauge!(
        LEADER_MEMBER_ID,
        "member id of the cluster leader the ingress publication points at"
    );
    metrics::describe_counter!(
        LEADER_CHANGES_TOTAL,
        "leader changes the session followed (a redirect or a new leader event)"
    );
    metrics::describe_counter!(SESSIONS_TOTAL, "cluster sessions opened");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn describe_and_record_with_the_no_op_recorder() {
        describe();
        record_connected(1);
        record_leader(2);
        record_failed();
    }

    #[test]
    fn constants_have_expected_prefix() {
        for name in [
            CONNECTED,
            LEADER_MEMBER_ID,
            LEADER_CHANGES_TOTAL,
            SESSIONS_TOTAL,
        ] {
            assert!(
                name.starts_with("kardamom_cluster_client_"),
                "expected kardamom_cluster_client_ prefix, got: {name}"
            );
        }
    }
}
