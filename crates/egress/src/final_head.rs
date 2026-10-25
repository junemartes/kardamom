//! [`FinalHead`]: the final head, read from the output oracle on L1.
//!
//! The final head is the newest L2 block of an output whose challenge
//! window ended: `WithdrawalOutputOracle.isFinalizable` is true for it.
//! Outputs settle in index order, and the oracle never deletes a settled
//! output. So the final outputs are a prefix of the list, with the deleted
//! outputs left out. The watch walks up the list from the first output
//! that it did not find final, and sends the block of the last final
//! output to the owner thread.
//!
//! Each poll reads at the finalized L1 block, so an L1 reorg never takes
//! back a final head that the watch sent. While finalization is paused,
//! no output is finalizable: the walk stops, and the final head stays.

use std::ops::ControlFlow;
use std::time::Duration;

use alloy_primitives::{Address, U256};
use alloy_provider::{Provider, ProviderBuilder, RootProvider};
use alloy_rpc_types_eth::{BlockId, BlockNumberOrTag};
use alloy_sol_types::sol;
use anyhow::{Context, Result};
use crossbeam_channel::{Sender, TrySendError};
use tokio::time::Interval;

use crate::owner::Input;

sol!(
    #[sol(rpc)]
    IWithdrawalOutputOracle,
    concat!(
        env!("CARGO_WORKSPACE_DIR"),
        "/contracts/abi/WithdrawalOutputOracle.json"
    )
);

/// The period of the poll. An output stays open for the whole challenge
/// window, so the final head moves slowly.
const EVERY: Duration = Duration::from_secs(30);
/// The outputs that one poll reads at most. After a start, the walk
/// climbs to the final head over some polls, and the store keeps more
/// blocks meanwhile, never fewer.
const OUTPUTS_PER_POLL: u64 = 256;

/// One output, as the walk sees it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Output {
    /// A challenge or a rollback deleted it.
    Deleted,
    /// Its window ended. `block` is the last L2 block that it covers.
    Final { block: u64 },
    /// It is inside its window, or finalization is paused.
    Open,
}

/// The walk up the list of outputs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Walk {
    /// The index of the next output to read.
    next: u64,
    /// The block of the last final output.
    head: Option<u64>,
}

impl Walk {
    /// Take the output at `next`. Stop at the first open output: every
    /// output above it is open too.
    pub(crate) fn take(&mut self, output: Output) -> ControlFlow<()> {
        match output {
            Output::Open => return ControlFlow::Break(()),
            Output::Final { block } => self.head = Some(block),
            Output::Deleted => {}
        }
        // The index stays below the output count, a u64, so the sum never
        // saturates.
        self.next = self.next.saturating_add(1);
        ControlFlow::Continue(())
    }
}

/// The watch of the final head.
pub struct FinalHead {
    oracle: IWithdrawalOutputOracle::IWithdrawalOutputOracleInstance<RootProvider>,
    walk: Walk,
    inputs: Sender<Input>,
}

impl FinalHead {
    /// A watch of the oracle at `oracle`, read through `l1_rpc`, that
    /// sends the final head on `inputs`.
    #[must_use]
    pub fn new(l1_rpc: reqwest::Url, oracle: Address, inputs: Sender<Input>) -> Self {
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .connect_http(l1_rpc);
        Self {
            oracle: IWithdrawalOutputOracle::new(oracle, provider),
            walk: Walk::default(),
            inputs,
        }
    }

    /// Poll until the owner thread is gone.
    pub async fn run(mut self) {
        let mut every = tokio::time::interval(EVERY);
        while self.poll(&mut every).await.is_continue() {}
    }

    /// Wait for the period, walk up the outputs, and send the final head.
    /// A full queue skips this send: the next poll sends again.
    async fn poll(&mut self, every: &mut Interval) -> ControlFlow<()> {
        every.tick().await;
        if let Err(error) = self.walk_up().await {
            tracing::warn!(
                error = format!("{error:#}"),
                next = self.walk.next,
                "egress: the output oracle did not answer; the final head stays"
            );
        }
        let Some(head) = self.walk.head else {
            return ControlFlow::Continue(());
        };
        match self.inputs.try_send(Input::Final(head)) {
            Ok(()) | Err(TrySendError::Full(_)) => ControlFlow::Continue(()),
            Err(TrySendError::Disconnected(_)) => ControlFlow::Break(()),
        }
    }

    /// Read at most [`OUTPUTS_PER_POLL`] outputs from `next`, at the
    /// finalized L1 block, up to the first open output.
    async fn walk_up(&mut self) -> Result<()> {
        let at = self.finalized().await?;
        let count = self
            .oracle
            .outputCount()
            .block(at)
            .call()
            .await
            .context("outputCount")?;
        // A count past u64::MAX reads as u64::MAX: a poll reads only a
        // few outputs.
        let count = u64::try_from(count).unwrap_or(u64::MAX);
        let end = count.min(self.walk.next.saturating_add(OUTPUTS_PER_POLL));
        while self.walk.next < end {
            if self.step(at).await?.is_break() {
                break;
            }
        }
        Ok(())
    }

    /// Read the output at `next` and take it.
    async fn step(&mut self, at: BlockId) -> Result<ControlFlow<()>> {
        let output = self.read(self.walk.next, at).await?;
        Ok(self.walk.take(output))
    }

    async fn read(&self, index: u64, at: BlockId) -> Result<Output> {
        let index = U256::from(index);
        let output = self
            .oracle
            .getOutput(index)
            .block(at)
            .call()
            .await
            .with_context(|| format!("getOutput({index})"))?;
        if output.deleted {
            return Ok(Output::Deleted);
        }
        let finalizable = self
            .oracle
            .isFinalizable(index)
            .block(at)
            .call()
            .await
            .with_context(|| format!("isFinalizable({index})"))?;
        Ok(if finalizable {
            Output::Final {
                block: output.l2BlockNumber,
            }
        } else {
            Output::Open
        })
    }

    /// The finalized L1 block, so every read of one poll sees one state.
    async fn finalized(&self) -> Result<BlockId> {
        let block = self
            .oracle
            .provider()
            .get_block_by_number(BlockNumberOrTag::Finalized)
            .await
            .context("the finalized L1 block")?
            .context("L1 has no finalized block")?;
        Ok(BlockId::number(block.header.number))
    }
}

#[cfg(test)]
#[path = "final_head_tests.rs"]
mod tests;
