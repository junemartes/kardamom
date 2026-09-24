//! `MixedWorkflow` interleaves signed transfers and `eth_call`s, at a
//! configured ratio. It stresses the read and write paths together.

use std::ops::ControlFlow;

use alloy_primitives::{Address, Bytes, U256};
use alloy_rpc_types_eth::BlockNumberOrTag;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::HttpClient;
use jsonrpsee::rpc_params;
use kardamom_types::AllocEntry;

use crate::benchmark::Prepared;
use crate::signers::{DerivedSigner, SignerSet, presign_transfers};
use crate::workflow::{BenchWorkflow, DispatchOutcome, default_signer_balance};
use crate::workflows::transfers::{allocs_with_contract, preflight_chain_id};
use crate::workflows::{
    ANVIL_MNEMONIC, DEFAULT_CALL_CONTRACT, TRANSFER_SINK, WARMUP_PER_TASK,
    assert_contract_deployed, call_req, default_call_bytecode,
};

const SEND_RAW: &str = "eth_sendRawTransaction";
const CALL: &str = "eth_call";
const METHODS: &[&str] = &[SEND_RAW, CALL];

/// The transfers-to-calls ratio for [`MixedWorkflow`]. The two counts
/// can never both be zero: [`MixRatio::new`] rejects that at
/// construction, so [`MixedWorkflow::prepare`] cannot fail this way.
#[derive(Debug, Clone, Copy)]
pub struct MixRatio {
    transfers: u32,
    calls: u32,
}

impl MixRatio {
    /// Build a ratio. Returns `None` if `transfers` and `calls` are both
    /// zero, since a cycle needs at least one item.
    #[must_use]
    pub const fn new(transfers: u32, calls: u32) -> Option<Self> {
        if transfers == 0 && calls == 0 {
            None
        } else {
            Some(Self { transfers, calls })
        }
    }

    /// The number of transfers in one cycle.
    #[must_use]
    pub fn transfers(&self) -> u32 {
        self.transfers
    }

    /// The number of `eth_call`s in one cycle.
    #[must_use]
    pub fn calls(&self) -> u32 {
        self.calls
    }

    /// The total items in one cycle. This is always at least 1.
    #[must_use]
    pub fn total(&self) -> u32 {
        self.transfers.saturating_add(self.calls)
    }
}

/// [`MixedWorkflow::default`]'s ratio: one transfer for every four calls.
const DEFAULT_MIX: MixRatio = match MixRatio::new(1, 4) {
    Some(m) => m,
    None => unreachable!(),
};

/// A built-in workflow that interleaves signed transfers and `eth_call`s,
/// at the configured ratio. It stresses the read and write paths together.
#[derive(Debug, Clone)]
pub struct MixedWorkflow {
    /// The BIP-39 phrase the signers are derived from.
    pub mnemonic: String,
    /// The balance each prefunded signer EOA gets in genesis.
    pub signer_balance: U256,
    /// The address of the contract that is the `to` value of every
    /// `eth_call`.
    pub contract: Address,
    /// The bytecode deployed at `contract` in the in-process genesis.
    pub contract_code: Bytes,
    /// The transfers-to-calls ratio for each cycle.
    pub ratio: MixRatio,
}

impl Default for MixedWorkflow {
    fn default() -> Self {
        Self {
            mnemonic: ANVIL_MNEMONIC.to_string(),
            signer_balance: default_signer_balance(),
            contract: DEFAULT_CALL_CONTRACT,
            contract_code: default_call_bytecode(),
            ratio: DEFAULT_MIX,
        }
    }
}

/// A per-item work unit for [`MixedWorkflow`].
pub enum MixedItem {
    /// A pre-signed value transfer, EIP-2718 encoded.
    Transfer(Bytes),
    /// An `eth_call` to the workflow's `contract`. This variant carries
    /// no per-item state.
    Call,
}

impl BenchWorkflow for MixedWorkflow {
    type Item = MixedItem;

    fn name(&self) -> &'static str {
        "mixed"
    }

    fn methods(&self) -> &'static [&'static str] {
        METHODS
    }

    fn genesis_alloc(&self, n_tasks: u32) -> anyhow::Result<Vec<AllocEntry>> {
        allocs_with_contract(
            &self.mnemonic,
            n_tasks,
            self.signer_balance,
            self.contract,
            self.contract_code.clone(),
        )
    }

    async fn prepare(
        &self,
        client: &HttpClient,
        n_tasks: u32,
        txs_per_task: u32,
    ) -> anyhow::Result<Prepared<Self::Item>> {
        let cycle_total = self.ratio.total();

        let chain_id = preflight_chain_id(client).await?;
        assert_contract_deployed(client, self.contract).await?;

        let signers = SignerSet::derive(&self.mnemonic, n_tasks)?;

        // Apply the same transfers-per-cap ratio to both warmup and main.
        let transfers_per_task = (txs_per_task as usize)
            .saturating_mul(self.ratio.transfers() as usize)
            / cycle_total as usize;
        let warmup_transfers_per_task =
            WARMUP_PER_TASK.saturating_mul(self.ratio.transfers() as usize) / cycle_total as usize;

        // Warmup transfers: presign in rotation across all signers, at
        // nonces 0 to warmup_transfers_per_task. Main transfers for each
        // signer resume at that nonce. Interleave each phase into its
        // own queue.
        let warmup_presigned = if warmup_transfers_per_task > 0 {
            presign_transfers(
                &signers,
                chain_id,
                TRANSFER_SINK,
                U256::from(1u64),
                warmup_transfers_per_task * signers.len(),
                0,
            )?
        } else {
            Vec::new()
        };
        let warmup = interleave(
            warmup_presigned,
            WARMUP_PER_TASK.saturating_mul(signers.len()),
            self.ratio.transfers() as usize,
            self.ratio.calls() as usize,
        );

        let main: Vec<Vec<MixedItem>> = signers
            .iter()
            .map(|s| {
                self.prepare_signer_queue(
                    s,
                    chain_id,
                    transfers_per_task,
                    warmup_transfers_per_task,
                    txs_per_task,
                )
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Prepared { warmup, main })
    }

    async fn dispatch(&self, client: &HttpClient, item: MixedItem) -> DispatchOutcome {
        match item {
            MixedItem::Transfer(bytes) => {
                let r: Result<alloy_primitives::B256, _> =
                    client.request(SEND_RAW, rpc_params![bytes]).await;
                DispatchOutcome {
                    method: SEND_RAW,
                    success: r.is_ok(),
                }
            }
            MixedItem::Call => {
                let r: Result<Bytes, _> = client
                    .request(
                        CALL,
                        rpc_params![call_req(self.contract), BlockNumberOrTag::Latest],
                    )
                    .await;
                DispatchOutcome {
                    method: CALL,
                    success: r.is_ok(),
                }
            }
        }
    }
}

impl MixedWorkflow {
    /// One signer's main-phase queue: its presigned transfers (if this
    /// mix has any), interleaved with `Call` markers up to
    /// `txs_per_task`.
    fn prepare_signer_queue(
        &self,
        s: &DerivedSigner,
        chain_id: u64,
        transfers_per_task: usize,
        warmup_transfers_per_task: usize,
        txs_per_task: u32,
    ) -> anyhow::Result<Vec<MixedItem>> {
        let presigned = if transfers_per_task > 0 {
            let one = SignerSet::new(vec![s.clone()])?;
            presign_transfers(
                &one,
                chain_id,
                TRANSFER_SINK,
                U256::from(1u64),
                transfers_per_task,
                warmup_transfers_per_task as u64,
            )?
        } else {
            Vec::new()
        };
        Ok(interleave(
            presigned,
            txs_per_task as usize,
            self.ratio.transfers() as usize,
            self.ratio.calls() as usize,
        ))
    }
}

/// Interleave presigned transfers with `Call` markers, at the mix
/// ratio, up to `cap` items in total.
fn interleave(
    presigned: Vec<Bytes>,
    cap: usize,
    transfers_per_cycle: usize,
    calls_per_cycle: usize,
) -> Vec<MixedItem> {
    Interleaver::new(presigned, cap, transfers_per_cycle, calls_per_cycle).run()
}

/// Builds one interleaved item list, cycling transfers and calls at
/// the mix ratio, up to `cap` items in total.
struct Interleaver {
    out: Vec<MixedItem>,
    iter: std::vec::IntoIter<Bytes>,
    cap: usize,
    transfers_per_cycle: usize,
    calls_per_cycle: usize,
}

impl Interleaver {
    fn new(
        presigned: Vec<Bytes>,
        cap: usize,
        transfers_per_cycle: usize,
        calls_per_cycle: usize,
    ) -> Self {
        Self {
            out: Vec::with_capacity(cap),
            iter: presigned.into_iter(),
            cap,
            transfers_per_cycle,
            calls_per_cycle,
        }
    }

    /// Run every cycle until [`Self::cycle`] says to stop.
    fn run(mut self) -> Vec<MixedItem> {
        while let ControlFlow::Continue(()) = self.cycle() {}
        self.out
    }

    /// One transfers-then-calls cycle. [`ControlFlow::Break`] means the
    /// caller should stop: the cap is reached, or transfers ran out.
    fn cycle(&mut self) -> ControlFlow<()> {
        let ControlFlow::Continue(()) = self.fill_transfers() else {
            return ControlFlow::Break(());
        };
        self.fill_calls()
    }

    /// Push up to `transfers_per_cycle` transfers from `iter` into
    /// `out`.
    fn fill_transfers(&mut self) -> ControlFlow<()> {
        for _ in 0..self.transfers_per_cycle {
            let ControlFlow::Continue(()) = self.push_one_transfer() else {
                return ControlFlow::Break(());
            };
        }
        ControlFlow::Continue(())
    }

    /// Push one transfer, if `out` has room and `iter` has one left.
    fn push_one_transfer(&mut self) -> ControlFlow<()> {
        if self.out.len() >= self.cap {
            return ControlFlow::Break(());
        }
        let Some(b) = self.iter.next() else {
            return ControlFlow::Break(());
        };
        self.out.push(MixedItem::Transfer(b));
        ControlFlow::Continue(())
    }

    /// Push up to `calls_per_cycle` call markers into `out`.
    fn fill_calls(&mut self) -> ControlFlow<()> {
        for _ in 0..self.calls_per_cycle {
            let ControlFlow::Continue(()) = self.push_one_call() else {
                return ControlFlow::Break(());
            };
        }
        ControlFlow::Continue(())
    }

    /// Push one call marker, if `out` has room.
    fn push_one_call(&mut self) -> ControlFlow<()> {
        if self.out.len() >= self.cap {
            return ControlFlow::Break(());
        }
        self.out.push(MixedItem::Call);
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count_transfers_and_calls(items: &[MixedItem]) -> (usize, usize) {
        items.iter().fold((0, 0), |(t, c), it| match it {
            MixedItem::Transfer(_) => (t + 1, c),
            MixedItem::Call => (t, c + 1),
        })
    }

    #[test]
    fn interleave_default_mix_respects_ratio_and_cap() {
        // Ratio 1:4, cap 10, 100 presigned items available.
        // Expect 2 transfers and 8 calls.
        let presigned: Vec<Bytes> = (0..100).map(|_| Bytes::from_static(&[0])).collect();
        let out = interleave(presigned, 10, 1, 4);
        assert_eq!(out.len(), 10);
        assert_eq!(count_transfers_and_calls(&out), (2, 8));
    }

    #[test]
    fn interleave_calls_only_when_no_transfers_per_cycle() {
        let out = interleave(Vec::new(), 5, 0, 3);
        assert_eq!(out.len(), 5);
        assert_eq!(count_transfers_and_calls(&out), (0, 5));
    }

    #[test]
    fn interleave_stops_when_presigned_exhausted() {
        // Ratio 3:0, cap 100, only 4 presigned items. Output is 4 transfers,
        // then it stops: a calls_per_cycle of 0 gives no items to refill with.
        let presigned: Vec<Bytes> = (0..4).map(|_| Bytes::from_static(&[0])).collect();
        let out = interleave(presigned, 100, 3, 0);
        assert_eq!(out.len(), 4);
        assert_eq!(count_transfers_and_calls(&out), (4, 0));
    }

    #[test]
    fn interleave_cap_mid_cycle_truncates_cleanly() {
        // cap=3, mid-cycle: 1 transfer, then 2 calls. The cycle is 1:4, but
        // the cap is hit before the 4-call run finishes.
        let presigned = vec![Bytes::from_static(&[0])];
        let out = interleave(presigned, 3, 1, 4);
        assert_eq!(out.len(), 3);
        assert_eq!(count_transfers_and_calls(&out), (1, 2));
    }
}
