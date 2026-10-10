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

/// One probe run's result. Each failure names what a user would see.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Success,
    /// The ingress refused a call with this JSON-RPC code, or the call's
    /// transport failed (`code="transport"`).
    RpcError(String),
    /// The status feed reported a rejection with this reason.
    Rejected(String),
    /// The transaction executed and reverted.
    ReceiptStatus0,
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

    /// Count the run, and mark the time of a success.
    pub fn record(&self, probe: &'static str, endpoint: &str) {
        let mut labels = vec![
            ("probe", probe.to_string()),
            ("endpoint", endpoint.to_string()),
            ("outcome", self.label().to_string()),
        ];
        labels.extend(self.detail());
        metrics::counter!(crate::metrics::PROBE_TOTAL, &labels).increment(1);
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
}
