//! The non-chaos stages of a shard, as methods on the [`Harness`]: the
//! smoke gate, the sustained load, the chain-semantics suite, the
//! ingress-churn re-smoke and the validator verdict. A shard test
//! composes them; `run-tests.sh` used to.

mod churn;
mod semantics;
mod soak;
mod state;
mod verdict;

use std::time::Duration;

use crate::harness::Harness;
use crate::rpc::Rpc;

pub use churn::CHURN_ACCOUNT;

/// The funded account of the smoke gate.
pub const GATE_ACCOUNT: u32 = 0;

/// The receipt budget of one smoke transfer.
const SMOKE_BUDGET: Duration = Duration::from_secs(60);

impl Harness {
    /// The smoke gate: one transfer from `account` through ingress-0 must
    /// get a receipt before any other stage runs.
    ///
    /// # Errors
    ///
    /// Returns an error if the transfer gets no receipt with status 1.
    pub async fn smoke_gate(&self, account: u32) -> anyhow::Result<()> {
        crate::log(format!("smoke gate against {}", self.rpc_url));
        Rpc::new(&self.rpc_url, self.knobs.chain_id)?
            .transfer_smoke(account, SMOKE_BUDGET)
            .await
    }
}

/// The last `n` lines of `text`.
#[must_use]
pub fn tail_lines(text: &str, n: usize) -> String {
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(n)..].join("\n")
}

/// The last `n` lines of `text` that contain any of `needles`.
#[must_use]
pub fn matching_lines(text: &str, needles: &[&str], n: usize) -> String {
    let lines: Vec<&str> = text
        .lines()
        .filter(|l| needles.iter().any(|needle| l.contains(needle)))
        .collect();
    lines[lines.len().saturating_sub(n)..].join("\n")
}

/// The first `n` lines of `text`.
#[must_use]
pub fn head_lines(text: &str, n: usize) -> String {
    text.lines().take(n).collect::<Vec<_>>().join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn head_and_tail_cut_lines() {
        let text = "a\nb\nc\nd";
        assert_eq!(head_lines(text, 2), "a\nb");
        assert_eq!(tail_lines(text, 3), "b\nc\nd");
        assert_eq!(tail_lines(text, 9), text);
    }

    #[test]
    fn matching_lines_keep_the_last_matches_in_order() {
        let text = "x RESYNC 1\nnoise\ncluster session opened 2\nnoise\nx RESYNC 3";
        assert_eq!(
            matching_lines(text, &["RESYNC", "cluster session"], 2),
            "cluster session opened 2\nx RESYNC 3"
        );
        assert_eq!(matching_lines(text, &["absent"], 5), "");
    }
}
