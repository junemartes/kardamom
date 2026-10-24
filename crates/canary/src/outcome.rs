//! The result of one probe run, and its metric labels.

use crate::rpc::RpcError;

/// The stages a probe can time out in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    Submit,
    Receipt,
    Read,
    Safe,
    L1Inclusion,
    L1Finality,
    L2Credit,
}

impl Stage {
    /// Every stage, for the counters that start at zero.
    const ALL: [Self; 7] = [
        Self::Submit,
        Self::Receipt,
        Self::Read,
        Self::Safe,
        Self::L1Inclusion,
        Self::L1Finality,
        Self::L2Credit,
    ];

    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Submit => "submit",
            Self::Receipt => "receipt",
            Self::Read => "read",
            Self::Safe => "safe",
            Self::L1Inclusion => "l1_inclusion",
            Self::L1Finality => "l1_finality",
            Self::L2Credit => "l2_credit",
        }
    }
}

/// The `code` labels that start at zero: a transport failure, and each
/// code the ingress answers with.
const RPC_CODES: [&str; 6] = [
    "transport",
    "-32000",
    "-32005",
    "-32010",
    "-32602",
    "-32603",
];

/// The `reason` labels that start at zero: each reason word of the status
/// feed, and `unknown` for a rejection without a reason.
const REJECT_REASONS: [&str; 13] = [
    "duplicated-tx",
    "evicted",
    "expired",
    "past-deadline",
    "fee-invalid",
    "fee-too-low",
    "insufficient-funds",
    "da-lag",
    "record-lag",
    "sealer-nonce-ahead",
    "nonce-in-flight",
    "already-known",
    "unknown",
];

/// The `field` labels that start at zero: each check of the `fees` probe.
const FEE_FIELDS: [&str; 5] = [
    "max_priority_fee",
    "fee_history",
    "priority_fee_paid",
    "base_fee",
    "priority_fee_per_gas",
];

/// The outcomes without a detail label.
const PLAIN: [Outcome; 16] = [
    Outcome::Success,
    Outcome::ReceiptStatus0,
    Outcome::Skipped,
    Outcome::NonceGap,
    Outcome::Unfunded,
    Outcome::AccountStalled,
    Outcome::StateMismatch,
    Outcome::HeadStalled,
    Outcome::ReceiptLost,
    Outcome::L1Error,
    Outcome::SupplyMismatch,
    Outcome::AllowlistBreach,
    Outcome::SwapMismatch,
    Outcome::InvariantBroken,
    Outcome::ReserveMismatch,
    Outcome::LiquidityMismatch,
];

/// One probe run's result. Each failure names what a user would see.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Success,
    /// The ingress refused a call with this JSON-RPC code, or the call's
    /// transport failed (`code="transport"`).
    RpcError(String),
    /// The status feed reported a rejection with this reason.
    Rejected(String),
    /// The transaction executed and reverted: its nonce is used.
    ReceiptStatus0,
    /// The chain skipped the transaction: the receipt has status 0, and
    /// the committed nonce did not pass the transaction's nonce. The
    /// executors skip an invalid transaction, for example one without
    /// funds for its gas or one with a nonce gap.
    Skipped,
    Timeout(Stage),
    /// The chain's committed nonce of a ring account is past the
    /// canary's next nonce: another party used the key, or the journal
    /// was lost. The canary takes the chain's nonce.
    NonceGap,
    /// Every ring account is under the funding floor.
    Unfunded,
    /// Every funded ring account is in use or holds an unresolved
    /// transaction.
    AccountStalled,
    /// A state read does not show the value the write's receipt reports.
    StateMismatch,
    /// The endpoint's head did not move since the last run.
    HeadStalled,
    /// The receipt of an old canary transaction does not answer.
    ReceiptLost,
    /// A receipt breaks a fee rule, or a fee method does not answer. The
    /// field names the check.
    FeeMismatch(&'static str),
    /// An L1 call failed or the L1 deposit reverted.
    L1Error,
    /// The token's total supply is not the canary's mints less its burns.
    SupplyMismatch,
    /// A transfer to an address off the allowlist succeeded.
    AllowlistBreach,
    /// A swap's output is not the constant-product formula's.
    SwapMismatch,
    /// The product of the pool's reserves fell in a swap.
    InvariantBroken,
    /// The pool's ETH reserve is above its balance.
    ReserveMismatch,
    /// A liquidity change's shares or amounts are not the formula's.
    LiquidityMismatch,
}

impl From<RpcError> for Outcome {
    fn from(e: RpcError) -> Self {
        match e {
            RpcError::Refused { code, .. } => Self::RpcError(code.to_string()),
            RpcError::Unknown(_) => Self::RpcError("transport".to_string()),
        }
    }
}

impl Outcome {
    /// The `outcome` label.
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::RpcError(_) => "rpc_error",
            Self::Rejected(_) => "rejected",
            Self::ReceiptStatus0 => "receipt_status_0",
            Self::Skipped => "skipped",
            Self::Timeout(_) => "timeout",
            Self::NonceGap => "nonce_gap",
            Self::Unfunded => "unfunded",
            Self::AccountStalled => "account_stalled",
            Self::StateMismatch => "state_mismatch",
            Self::HeadStalled => "head_stalled",
            Self::ReceiptLost => "receipt_lost",
            Self::FeeMismatch(_) => "fee_mismatch",
            Self::L1Error => "l1_error",
            Self::SupplyMismatch => "supply_mismatch",
            Self::AllowlistBreach => "allowlist_breach",
            Self::SwapMismatch => "swap_mismatch",
            Self::InvariantBroken => "invariant_broken",
            Self::ReserveMismatch => "reserve_mismatch",
            Self::LiquidityMismatch => "liquidity_mismatch",
        }
    }

    /// The detail label of the outcome, if it has one.
    #[must_use]
    pub fn detail(&self) -> Option<(&'static str, String)> {
        match self {
            Self::RpcError(code) => Some(("code", code.clone())),
            Self::Rejected(reason) => Some(("reason", reason.clone())),
            Self::Timeout(stage) => Some(("stage", stage.label().to_string())),
            Self::FeeMismatch(field) => Some(("field", (*field).to_string())),
            _ => None,
        }
    }

    /// Every outcome that has a known label set: the plain outcomes, a
    /// timeout in each stage, a fee mismatch of each field, and the known
    /// RPC codes and rejection reasons.
    fn known() -> impl Iterator<Item = Self> {
        PLAIN
            .into_iter()
            .chain(Stage::ALL.into_iter().map(Self::Timeout))
            .chain(FEE_FIELDS.into_iter().map(Self::FeeMismatch))
            .chain(RPC_CODES.into_iter().map(|c| Self::RpcError(c.to_string())))
            .chain(
                REJECT_REASONS
                    .into_iter()
                    .map(|r| Self::Rejected(r.to_string())),
            )
    }

    /// The run counter of this outcome of `probe` on `endpoint`.
    fn counter(&self, probe: &'static str, endpoint: &str) -> metrics::Counter {
        let mut labels = vec![
            ("probe", probe.to_string()),
            ("endpoint", endpoint.to_string()),
            ("outcome", self.label().to_string()),
        ];
        labels.extend(self.detail());
        metrics::counter!(crate::metrics::PROBE_TOTAL, &labels)
    }

    /// Start the run counter of each known outcome of `probe` on
    /// `endpoint` at zero. Prometheus `increase()` takes the first sample
    /// of a series as its base, so a series that starts at 1 loses its
    /// first count. An RPC code or a rejection reason outside the known
    /// lists still starts at 1, and its first count is lost.
    pub fn start_at_zero(probe: &'static str, endpoint: &str) {
        Self::known().for_each(|outcome| outcome.counter(probe, endpoint).increment(0));
    }

    /// Count the run, and mark the time of a success.
    pub fn record(&self, probe: &'static str, endpoint: &str) {
        self.counter(probe, endpoint).increment(1);
        if *self == Self::Success {
            crate::metrics::success_now(probe);
        } else {
            tracing::warn!(probe, endpoint, outcome = ?self, "probe failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_refusal_keeps_its_code_and_a_transport_failure_is_named() {
        let refused = Outcome::from(RpcError::Refused {
            code: -32010,
            message: "chain halted".into(),
        });
        assert_eq!(refused.detail(), Some(("code", "-32010".to_string())));
        let unknown = Outcome::from(RpcError::Unknown("reset".into()));
        assert_eq!(unknown.detail(), Some(("code", "transport".to_string())));
        assert_eq!(Outcome::Success.detail(), None);
    }

    /// The run counter series that `record` gives on a fresh local
    /// recorder, as their rendered lines.
    fn series(record: impl FnOnce()) -> Vec<String> {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        metrics::with_local_recorder(&recorder, record);
        recorder
            .handle()
            .render()
            .lines()
            .filter(|line| line.starts_with(crate::metrics::PROBE_TOTAL))
            .map(str::to_string)
            .collect()
    }

    #[test]
    fn every_known_outcome_starts_at_zero_and_its_first_run_counts_on_that_series() {
        let started = series(|| Outcome::start_at_zero("transfer", "a"));
        assert_eq!(started.len(), Outcome::known().count());
        assert!(started.iter().all(|line| line.ends_with(" 0")));
        assert!(
            started
                .iter()
                .any(|line| line.contains(r#"outcome="timeout",stage="receipt""#))
        );
        let ran = series(|| {
            Outcome::start_at_zero("transfer", "a");
            Outcome::known().for_each(|outcome| outcome.record("transfer", "a"));
        });
        let names = |lines: &[String]| -> std::collections::BTreeSet<String> {
            lines
                .iter()
                .map(|line| line.rsplit_once(' ').unwrap().0.to_string())
                .collect()
        };
        assert_eq!(names(&ran), names(&started));
        assert!(ran.iter().all(|line| line.ends_with(" 1")));
    }
}
