//! Sequential capture runner: execute blocks through the real engine,
//! recording per-tx footprints with a FRESH `Bal` per tx so that
//! `storage_reads` (block-scoped in EIP-7928) attribute to the single tx
//! inside it.

use alloy_primitives::B256;
use kardamom_engine::executor::Executor;
use kardamom_engine::{ExecEnv, PendingDelta, TxIndex};
use kardamom_types::{BPosition, BlockBoundaryStart, StateDatabase, TxEnvelope};

use super::Cell;
// Re-exported so `capture::TxObs` keeps resolving for the stm-p0 bin; the
// scheduling-time view decoder is shared with the live shadow.
pub use super::TxObs;
use kardamom_footprint::envelope_view;

/// Execute `blocks` (each a list of signed envelopes) sequentially against
/// `snap`, returning per-tx observations. Panics on execution errors —
/// capture inputs are deterministic fixtures.
pub fn run_capture<S: StateDatabase>(
    snap: &S,
    blocks: &[Vec<TxEnvelope>],
    chain_id: u64,
) -> Vec<TxObs> {
    let mut out = Vec::new();
    let mut delta = PendingDelta::new();
    let mut global = 0u64;
    for (bi, block) in blocks.iter().enumerate() {
        let env = ExecEnv::new(
            chain_id,
            &BlockBoundaryStart {
                block_number: bi as u64 + 1,
                end_tx_idx: BPosition::from_index(0),
                l2_timestamp: 1_700_000_000 + bi as u64 * 2,
                l1_origin: 0,
            },
        );
        let mut cumulative = 0u64;
        for (i, envelope) in block.iter().enumerate() {
            let mut bal = revm::state::bal::Bal::new();
            let (receipt, ws) = Executor::execute_once(
                snap,
                None,
                &delta,
                env,
                TxIndex(global),
                BPosition::from_index(global),
                envelope,
                i as u64,
                cumulative,
                // Per-tx Bal at index 1: this tx's touches are the whole
                // list — reads attribute exactly.
                Some((&mut bal, 1)),
            )
            .expect("capture execute");
            if !receipt.status {
                let (to, selector, args, _) = envelope_view(&envelope.raw_tx);
                panic!(
                    "capture tx failed (block {bi} idx {i}): sender={} to={:?} selector={:02x?} args0={:?}",
                    envelope.sender,
                    to,
                    selector,
                    args.first()
                );
            }
            cumulative = receipt.cumulative_gas_used;

            let (to, selector, args, has_value) = envelope_view(&envelope.raw_tx);
            let mut reads = Vec::new();
            let mut writes = Vec::new();
            let alloy_bal = bal.into_alloy_bal();
            for acct in alloy_bal.iter() {
                for s in &acct.storage_reads {
                    reads.push(Cell::Slot(acct.address, B256::from(s.to_be_bytes::<32>())));
                }
                for sc in &acct.storage_changes {
                    writes.push(Cell::Slot(
                        acct.address,
                        B256::from(sc.slot.to_be_bytes::<32>()),
                    ));
                }
                if !acct.balance_changes.is_empty() || !acct.nonce_changes.is_empty() {
                    writes.push(Cell::Account(acct.address));
                }
            }
            // WriteSet accounts catch anything the Bal's change-classifier
            // collapsed (belt and braces; dedup below).
            for (addr, _) in ws.accounts.iter() {
                writes.push(Cell::Account(*addr));
            }
            reads.sort_unstable();
            reads.dedup();
            writes.sort_unstable();
            writes.dedup();

            out.push(TxObs {
                index: global,
                block: bi as u64 + 1,
                sender: envelope.sender,
                to,
                selector,
                args,
                gas: receipt.gas_used,
                has_value,
                reads,
                writes,
            });
            delta.apply(ws);
            global += 1;
        }
    }
    out
}
