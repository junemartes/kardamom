//! The real election observer with a recovering Nomad HTTP peer.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::evidence::Evidence;

#[derive(Clone, Copy)]
enum Scenario {
    Replaced,
    Missing,
    NoMajority,
}

pub(super) struct Peer {
    scenario: Scenario,
    listings: Arc<AtomicUsize>,
}

struct Fixture {
    evidence: Evidence,
    listings: Arc<AtomicUsize>,
}

impl Fixture {
    async fn new(scenario: Scenario) -> Self {
        // The HTTP task owns updates; the test observes completed allocation reads.
        let listings = Arc::new(AtomicUsize::new(0));
        let peer = Peer {
            scenario,
            listings: Arc::clone(&listings),
        };
        let address = FakeAgent {
            election: Some(peer),
            ..FakeAgent::default()
        }
        .spawn()
        .await;
        Self {
            evidence: Evidence::new(nomad(&address)),
            listings,
        }
    }
}

impl Peer {
    pub(super) fn reply(&self, path: &str) -> (u16, String) {
        if path == "/v1/nodes" {
            return (
                200,
                r#"[{"ID":"node-0","Name":"sealer-0","Status":"ready"}]"#.into(),
            );
        }
        if path.starts_with("/v1/job/cluster/allocations") {
            return (200, self.allocations());
        }
        if let Some(id) = path.strip_prefix("/v1/client/fs/logs/new-") {
            return (
                200,
                self.logs(id.split('?').next().unwrap().parse().unwrap()),
            );
        }
        (404, "task log not found".into())
    }

    fn allocations(&self) -> String {
        let read = self.listings.fetch_add(1, Ordering::SeqCst);
        let missing = matches!(self.scenario, Scenario::Missing)
            || (matches!(self.scenario, Scenario::Replaced) && read == 0);
        let prefix = if missing { "old" } else { "new" };
        let allocations: Vec<_> = (0..3)
            .map(|id| {
                serde_json::json!({
                    "ID": format!("{prefix}-{id}"), "TaskGroup": "cluster",
                    "ClientStatus": "running", "DesiredStatus": "run", "JobVersion": 1,
                    "NodeName": format!("sealer-{id}"), "NodeID": "node-0",
                    "TaskStates": {"cluster": {"State": "running"}}
                })
            })
            .collect();
        serde_json::to_string(&allocations).unwrap()
    }

    fn logs(&self, id: u32) -> String {
        let role = if id == 0 { "LEADER" } else { "FOLLOWER" };
        let closed = !matches!(self.scenario, Scenario::NoMajority) || id == 0;
        let state = if closed { "CLOSED" } else { "FOLLOWER_CATCHUP" };
        format!(
            "cluster START mode=RESUME memberId={id}\ncluster role={role} memberId={id}\ncluster ELECTION memberId={id} state={state} logLeadershipTermId=9\n"
        )
    }
}

#[tokio::test]
async fn election_refreshes_allocations_after_a_log_retry_budget_expires() {
    let fixture = Fixture::new(Scenario::Replaced).await;
    let elected = fixture
        .evidence
        .cluster_elected(Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(elected.leader, 0);
    assert_eq!(elected.term, 9);
    assert!(fixture.listings.load(Ordering::SeqCst) >= 2);
}

#[tokio::test]
async fn missing_logs_exhaust_the_election_deadline_with_the_read_cause() {
    let fixture = Fixture::new(Scenario::Missing).await;
    let started = tokio::time::Instant::now();
    let error = fixture
        .evidence
        .cluster_elected(Duration::from_secs(1))
        .await
        .unwrap_err()
        .to_string();
    assert!(started.elapsed() >= Duration::from_secs(1));
    assert!(
        error.contains("no election closed on a majority within 1s"),
        "{error}"
    );
    assert!(error.contains("404"), "{error}");
}

#[tokio::test]
async fn readable_logs_without_a_closed_majority_do_not_certify_recovery() {
    let fixture = Fixture::new(Scenario::NoMajority).await;
    let error = fixture
        .evidence
        .cluster_elected(Duration::from_millis(200))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("no election closed on a majority"),
        "{error}"
    );
    assert!(fixture.listings.load(Ordering::SeqCst) > 0);
}
