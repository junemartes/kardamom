//! The chain status as the cases read it: `kardamom_chainStatus` on the
//! first ingress. The cases assert the roots and the pauses of the
//! service events, not only the gauges.

use std::time::Duration;

use serde_json::Value;

use crate::harness::Harness;
use crate::poll::{self, Budget};
use crate::rpc::Rpc;

/// How often a case reads the chain status while it waits.
const POLL: Duration = Duration::from_secs(2);

/// One reading of the chain status.
#[derive(Debug, Clone)]
pub(crate) struct ChainView(Value);

impl ChainView {
    /// Read the chain status from the first ingress.
    pub(crate) async fn read(h: &Harness) -> anyhow::Result<Self> {
        let rpc = Rpc::new(&h.rpc_url, h.knobs.chain_id)?;
        Ok(Self(rpc.chain_status().await?))
    }

    /// Read the chain status until `holds` accepts it, within `budget`.
    /// A failed read counts as a reading that does not hold.
    pub(crate) async fn await_until(
        h: &Harness,
        what: &str,
        budget: Duration,
        holds: impl Fn(&Self) -> bool,
    ) -> anyhow::Result<Self> {
        let holds = &holds;
        let (view, _) = poll::until(Budget::new(budget, POLL), |_| async move {
            Ok::<_, anyhow::Error>(Self::read(h).await.ok().filter(|v| holds(v)))
        })
        .await?
        .or_fail(|elapsed| {
            crate::chaos_fail!(
                "{what}: the chain status did not hold within {}s",
                elapsed.as_secs()
            )
        })?;
        Ok(view)
    }

    /// The roots: `(service, cause)` of every live halt a pause waits on.
    pub(crate) fn roots(&self) -> Vec<(String, String)> {
        self.0["roots"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|root| (Self::text(&root["service"]), Self::text(&root["cause"])))
            .collect()
    }

    /// The states of every process of `service` on the board.
    pub(crate) fn states_of(&self, service: &str) -> Vec<String> {
        self.services()
            .filter(|s| s["service"] == service)
            .map(|s| Self::text(&s["state"]))
            .collect()
    }

    /// The root cause each paused process of `service` waits on.
    pub(crate) fn paused_on(&self, service: &str) -> Vec<String> {
        self.services()
            .filter(|s| s["service"] == service && s["state"] == "paused")
            .map(|s| Self::text(&s["pause"]["root"]["cause"]))
            .collect()
    }

    /// No root stands and no live process is halted or paused.
    pub(crate) fn settled(&self) -> bool {
        self.roots().is_empty()
            && self
                .services()
                .all(|s| s["state"] != "halted" && s["state"] != "paused")
    }

    /// Every process of `service` on the board runs, and one at least.
    pub(crate) fn all_running(&self, service: &str) -> bool {
        let states = self.states_of(service);
        !states.is_empty() && states.iter().all(|s| s == "running")
    }

    fn services(&self) -> impl Iterator<Item = &Value> {
        self.0["services"].as_array().into_iter().flatten()
    }

    fn text(value: &Value) -> String {
        value.as_str().unwrap_or_default().to_string()
    }
}

impl std::fmt::Display for ChainView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::ChainView;

    #[test]
    fn a_reading_names_the_root_and_its_dependents() {
        let view = ChainView(serde_json::json!({
            "roots": [{"service": "sealer", "cause": "da_lag"}],
            "services": [
                {"service": "ingress", "state": "paused", "pause": {"root": {"cause": "da_lag"}}},
                {"service": "ingress", "state": "paused", "pause": {"root": {"cause": "da_lag"}}},
                {"service": "batcher", "state": "gone"},
                {"service": "executor", "state": "running"},
            ],
        }));
        assert_eq!(view.roots(), vec![("sealer".into(), "da_lag".into())]);
        assert_eq!(view.paused_on("ingress"), vec!["da_lag", "da_lag"]);
        assert_eq!(view.states_of("batcher"), vec!["gone"]);
        assert!(view.all_running("executor"));
        assert!(!view.all_running("ingress"));
        assert!(!view.settled());

        let calm = ChainView(serde_json::json!({
            "roots": [],
            "services": [{"service": "batcher", "state": "gone"}],
        }));
        assert!(calm.settled(), "a gone record is not a pause");
    }
}
