//! This is the sequential capture runner. It executes blocks through
//! the real engine, and records per-transaction footprints with a
//! fresh `Bal` for each transaction, so `storage_reads`, which is
//! block-scoped in EIP-7928, attributes to the single transaction
//! inside it.

use alloy_primitives::B256;
use kardamom_engine::delta::{PendingDelta, WriteSet};
use kardamom_types::{BPosition, Receipt, StateDatabase, TxEnvelope};

use super::{BlockAt, Cell, SeqExec, TxObs};
use kardamom_footprint::envelope_view;

/// Execute `blocks`, each a list of signed envelopes, in order against
/// `snap`. Returns per-transaction observations.
///
/// # Panics
///
/// Panics on an execution error, since capture inputs are
/// deterministic fixtures: an error here means the fixture is broken,
/// not a runtime condition to recover from.
pub fn run_capture<S: StateDatabase>(
    snap: &S,
    blocks: &[Vec<TxEnvelope>],
    chain_id: u64,
) -> Vec<TxObs> {
    let mut capture = Capture {
        snap,
        chain_id,
        global: 0,
        delta: PendingDelta::new(),
        out: Vec::new(),
    };
    capture.run(blocks);
    capture.out
}

/// The capture run's fixed inputs and the state that carries from one
/// transaction to the next: `delta` is every prior block's write set,
/// layered under each new block's scope, since `snap` itself never
/// advances across this run.
struct Capture<'a, S> {
    snap: &'a S,
    chain_id: u64,
    global: u64,
    delta: PendingDelta,
    out: Vec<TxObs>,
}

impl<S: StateDatabase> Capture<'_, S> {
    fn run(&mut self, blocks: &[Vec<TxEnvelope>]) {
        for (bi, block) in blocks.iter().enumerate() {
            self.capture_block(bi, block);
        }
    }

    /// Execute one block's transactions in order, against one scope
    /// layered on every prior block's delta, appending each
    /// observation to `self.out`.
    fn capture_block(&mut self, bi: usize, block: &[TxEnvelope]) {
        let env = BlockAt(bi).env(self.chain_id);
        let mut exec =
            SeqExec::new(self.snap, Some(&self.delta), env).expect("capture: open block scope");
        for (i, envelope) in block.iter().enumerate() {
            self.capture_tx(bi, i, envelope, &mut exec);
        }
    }

    /// Execute one transaction, fold its write set into `self.delta`
    /// for the next block, then record its observation.
    fn capture_tx(
        &mut self,
        bi: usize,
        i: usize,
        envelope: &TxEnvelope,
        exec: &mut SeqExec<'_, S>,
    ) {
        let mut bal = revm::state::bal::Bal::new();
        let out = exec
            .run(
                kardamom_engine::TxIndex(self.global),
                BPosition::from_index(self.global),
                envelope,
                i as u64,
                // This is a per-transaction Bal at index 1: this transaction's
                // touches are the whole list, so reads attribute exactly.
                Some((&mut bal, 1)),
            )
            .expect("capture execute");
        if !out.receipt.status {
            let (to, selector, args, _) = envelope_view(&envelope.raw_tx);
            panic!(
                "capture tx failed (block {bi} idx {i}): sender={} to={:?} selector={:02x?} args0={:?}",
                envelope.sender,
                to,
                selector,
                args.first()
            );
        }
        self.record_observation(bi, envelope, &out.receipt, bal, &out.write_set);
        self.delta.apply(out.write_set);
        self.global += 1;
    }

    /// Build this transaction's `TxObs` from its receipt, `Bal`, and
    /// write set, and append it to `self.out`.
    fn record_observation(
        &mut self,
        bi: usize,
        envelope: &TxEnvelope,
        receipt: &Receipt,
        bal: revm::state::bal::Bal,
        ws: &WriteSet,
    ) {
        let (to, selector, args, has_value) = envelope_view(&envelope.raw_tx);
        let alloy_bal = bal.into_alloy_bal();
        let mut reads: Vec<Cell> = alloy_bal
            .iter()
            .flat_map(|acct| {
                acct.storage_reads
                    .iter()
                    .map(move |s| Cell::Slot(acct.address, B256::from(s.to_be_bytes::<32>())))
            })
            .collect();
        let mut writes: Vec<Cell> = alloy_bal
            .iter()
            .flat_map(|acct| {
                acct.storage_changes.iter().map(move |sc| {
                    Cell::Slot(acct.address, B256::from(sc.slot.to_be_bytes::<32>()))
                })
            })
            .chain(alloy_bal.iter().filter_map(|acct| {
                (!acct.balance_changes.is_empty() || !acct.nonce_changes.is_empty())
                    .then_some(Cell::Account(acct.address))
            }))
            // WriteSet accounts catch anything the Bal's change classifier
            // collapsed. This is a second check; the dedup below removes
            // any duplicate.
            .chain(ws.accounts.iter().map(|(addr, _)| Cell::Account(*addr)))
            .collect();
        reads.sort_unstable();
        reads.dedup();
        writes.sort_unstable();
        writes.dedup();

        self.out.push(TxObs {
            index: self.global,
            block: BlockAt(bi).number(),
            sender: envelope.sender,
            to,
            selector,
            args,
            gas: receipt.gas_used,
            has_value,
            reads,
            writes,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::run_capture;
    use crate::signers::SignerSet;
    use crate::stm::workload;

    /// A capture run's later blocks must see every earlier block's
    /// writes: each block opens a fresh scope over the same base
    /// snapshot, so a flow block that reads setup-block state, such as
    /// a funded balance or a deployed contract, needs that state
    /// layered in through `Capture::delta`, not read from the base
    /// snapshot alone.
    #[test]
    fn run_capture_carries_state_across_block_boundaries() {
        const CHAIN_ID: u64 = 412_346;
        let signers = SignerSet::derive(crate::ANVIL_MNEMONIC, 3).unwrap();
        let snap = workload::funded_snapshot(&signers);
        let blocks = workload::defi_blocks(
            &signers,
            CHAIN_ID,
            2,
            4,
            std::num::NonZeroUsize::new(3).unwrap(),
        )
        .unwrap();
        let mut all = blocks.setup;
        all.extend(blocks.flows);
        let obs = run_capture(&snap, &all, CHAIN_ID);
        assert!(!obs.is_empty(), "capture produced no observations");
        assert!(
            obs.iter().all(|o| o.gas > 0),
            "every transaction must execute against the state the prior block left, not revert"
        );
    }
}
