//! Recovery of a chain born with the final carry configuration.

mod deposits;
mod egress;
mod lockbox;
mod restart;
#[cfg(test)]
mod tests;

use std::time::Duration;

use crate::harness::Harness;
use crate::knobs::Knobs;

/// One recovery fault under sustained transaction load.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    DepositsProviderFault,
    EgressRestart,
}

/// Every recovery case, in run order.
pub(crate) const STEPS: &[Step] = &[Step::DepositsProviderFault, Step::EgressRestart];

struct Run<'a> {
    h: &'a mut Harness,
    ctx: &'static str,
}

impl Step {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::DepositsProviderFault => "carry-deposits-provider-fault",
            Self::EgressRestart => "carry-egress-restart",
        }
    }

    /// Load covers service configuration, the fault and recovery observations.
    pub(crate) fn floor(self, k: &Knobs) -> Duration {
        match self {
            Self::DepositsProviderFault => k
                .inject_delay
                .saturating_add(super::window::FULL_DEPLOY_LOAD)
                .saturating_add(Duration::from_mins(20))
                .saturating_add(k.l1_fault),
            Self::EgressRestart => k.inject_delay + k.restart_slo + Duration::from_mins(2),
        }
    }

    pub(crate) async fn run(self, h: &mut Harness) -> anyhow::Result<()> {
        let mut run = Run {
            h,
            ctx: self.name(),
        };
        match self {
            Self::DepositsProviderFault => {
                run.configure_lockbox().await?;
                deposits::across_provider_fault(run.h).await
            }
            Self::EgressRestart => run.egress_restart().await,
        }
    }
}
