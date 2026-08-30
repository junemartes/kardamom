//! `MixedWorkflow` interleaves signed transfers and `eth_call`s, at a
//! configured ratio. It stresses the read and write paths together.

use alloy_primitives::{Address, Bytes, TxKind, U256};
use alloy_rpc_types_eth::{BlockNumberOrTag, TransactionRequest};
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::HttpClient;
use jsonrpsee::rpc_params;
use kardamom_types::AllocEntry;

use crate::benchmark::Prepared;
use crate::mnemonic;
use crate::signers::presign_transfers;
use crate::workflow::{BenchWorkflow, default_signer_balance};
use crate::workflows::transfers::{preflight_chain_id, prefunded_signer_allocs};
use crate::workflows::{
    ANVIL_MNEMONIC, DEFAULT_CALL_CONTRACT, TRANSFER_SINK, WARMUP_PER_TASK, default_call_bytecode,
};

const SEND_RAW: &str = "eth_sendRawTransaction";
const CALL: &str = "eth_call";
const METHODS: &[&str] = &[SEND_RAW, CALL];

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
    /// The number of transfers in each transfers-plus-calls cycle.
    pub transfers_per_cycle: u32,
    /// The number of `eth_call`s in each transfers-plus-calls cycle.
    pub calls_per_cycle: u32,
}

impl Default for MixedWorkflow {
    fn default() -> Self {
        Self {
            mnemonic: ANVIL_MNEMONIC.to_string(),
            signer_balance: default_signer_balance(),
            contract: DEFAULT_CALL_CONTRACT,
            contract_code: default_call_bytecode(),
            transfers_per_cycle: 1,
            calls_per_cycle: 4,
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
        let mut alloc = prefunded_signer_allocs(&self.mnemonic, n_tasks, self.signer_balance)?;
        alloc.push(AllocEntry {
            address: self.contract,
            balance: U256::ZERO,
            code: Some(self.contract_code.clone()),
            nonce: Some(1),
        });
        Ok(alloc)
    }

    async fn prepare(
        &self,
        client: &HttpClient,
        n_tasks: u32,
        txs_per_task: u32,
    ) -> anyhow::Result<Prepared<Self::Item>> {
        let cycle_total = self
            .transfers_per_cycle
            .saturating_add(self.calls_per_cycle);
        if cycle_total == 0 {
            anyhow::bail!("transfers_per_cycle + calls_per_cycle must be > 0");
        }

        let chain_id = preflight_chain_id(client).await?;
        // Check that the call target is deployed.
        let req = TransactionRequest {
            to: Some(TxKind::Call(self.contract)),
            ..Default::default()
        };
        let probe: Result<Bytes, _> = client
            .request(CALL, rpc_params![req, BlockNumberOrTag::Latest])
            .await;
        match probe {
            Ok(b) if !b.is_empty() => {}
            _ => anyhow::bail!(
                "contract {addr} is not deployed (empty eth_call output)",
                addr = self.contract,
            ),
        }

        let signers = mnemonic::derive_signers(&self.mnemonic, n_tasks)?;

        // Apply the same transfers-per-cap ratio to both warmup and main.
        let transfers_per_task = (txs_per_task as usize)
            .saturating_mul(self.transfers_per_cycle as usize)
            / cycle_total as usize;
        let warmup_transfers_per_task = WARMUP_PER_TASK
            .saturating_mul(self.transfers_per_cycle as usize)
            / cycle_total as usize;

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
            WARMUP_PER_TASK * signers.len(),
            self.transfers_per_cycle as usize,
            self.calls_per_cycle as usize,
        );

        let mut main: Vec<Vec<MixedItem>> = Vec::with_capacity(signers.len());
        for s in &signers {
            let presigned = if transfers_per_task > 0 {
                presign_transfers(
                    std::slice::from_ref(s),
                    chain_id,
                    TRANSFER_SINK,
                    U256::from(1u64),
                    transfers_per_task,
                    warmup_transfers_per_task as u64,
                )?
            } else {
                Vec::new()
            };
            main.push(interleave(
                presigned,
                txs_per_task as usize,
                self.transfers_per_cycle as usize,
                self.calls_per_cycle as usize,
            ));
        }
        Ok(Prepared { warmup, main })
    }

    async fn dispatch(&self, client: &HttpClient, item: MixedItem) -> (&'static str, bool) {
        match item {
            MixedItem::Transfer(bytes) => {
                let r: Result<alloy_primitives::B256, _> =
                    client.request(SEND_RAW, rpc_params![bytes]).await;
                (SEND_RAW, r.is_ok())
            }
            MixedItem::Call => {
                let req = TransactionRequest {
                    to: Some(TxKind::Call(self.contract)),
                    ..Default::default()
                };
                let r: Result<Bytes, _> = client
                    .request(CALL, rpc_params![req, BlockNumberOrTag::Latest])
                    .await;
                (CALL, r.is_ok())
            }
        }
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
    let mut out: Vec<MixedItem> = Vec::with_capacity(cap);
    let mut iter = presigned.into_iter();
    'fill: loop {
        for _ in 0..transfers_per_cycle {
            if out.len() >= cap {
                break 'fill;
            }
            match iter.next() {
                Some(b) => out.push(MixedItem::Transfer(b)),
                None => break 'fill,
            }
        }
        for _ in 0..calls_per_cycle {
            if out.len() >= cap {
                break 'fill;
            }
            out.push(MixedItem::Call);
        }
    }
    out
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
