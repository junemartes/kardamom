//! The validator's two engine seams: the block-close BAL write-set check
//! ([`ValidatorWriterQueue`]) and the per-tx receipt check with the
//! account-row check ([`ValidatorReceiptSink`]). Both use existing engine
//! trait seams ([`StateWriterQueue`], [`TxReceiptsPublication`]) and stop
//! the process via [`Divergence`] on a proven mismatch. Both compare the
//! result of every executor replica, and a mismatch names the replica.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::Address;
use kardamom_engine::{
    CMessage, ExecutorError, StateWriterQueue, TxReceiptsPublication, publish_each,
};
use kardamom_types::{
    AccountRow, BPosition, BlockBoundary, BlockDelta, Receipt, ReceiptRows, TxRef,
};

use crate::buffers::{BalBuffer, ReceiptBuffer};
use crate::replica::{Arrival, Attribution, Check, Checked, Compared, Mismatch, Taken};
use crate::{Divergence, metrics};

/// How long the exec thread waits for a block's BAL before it skips the check.
const BAL_WAIT: Duration = Duration::from_secs(5);
/// How long the commit thread waits for a tx's published receipt before it skips.
const RECEIPT_WAIT: Duration = Duration::from_secs(5);

impl Compared for BlockDelta {
    /// Returns `true` when two block deltas have the same write-set
    /// (accounts, storage, code). Receipts are checked separately via
    /// `tx_receipts`, so this check skips them on purpose. The validator
    /// and the executor both build these vectors from sorted maps, so
    /// equal content means equal order.
    fn agrees(&self, other: &Self) -> bool {
        self.accounts == other.accounts && self.storage == other.storage && self.code == other.code
    }
}

/// Return a short summary of the first write-set field that differs.
fn write_set_diff_summary(local: &BlockDelta, bal: &BlockDelta) -> String {
    if local.accounts != bal.accounts {
        format!(
            "accounts differ (local {} vs bal {})",
            local.accounts.len(),
            bal.accounts.len()
        )
    } else if local.storage != bal.storage {
        format!(
            "storage differs (local {} vs bal {})",
            local.storage.len(),
            bal.storage.len()
        )
    } else {
        format!(
            "code differs (local {} vs bal {})",
            local.code.len(),
            bal.code.len()
        )
    }
}

impl Compared for Receipt {
    /// Returns `true` when the two receipts agree on the execution-output
    /// fields: success status, gas used, the write-set hash (the per-tx
    /// determinism witness), and the emitted logs. The check includes logs
    /// because `write_set_hash` covers state writes but not events. Without
    /// the log check, a log-only divergence would pass silently.
    ///
    /// The check skips the RPC enrichment fields on purpose (`nonce`,
    /// `from`, `to`, `contract_address`, `effective_gas_price`,
    /// `block_number`, `transaction_index`, `cumulative_gas_used`). These
    /// fields derive deterministically from inputs the check already
    /// covers: the envelope, the canonical order, and the per-block
    /// `gas_used` sums. A divergence in one of them implies a divergence
    /// in a checked field, so comparing them would only re-verify
    /// arithmetic, not execution.
    fn agrees(&self, other: &Self) -> bool {
        self.status == other.status
            && self.gas_used == other.gas_used
            && self.write_set_hash == other.write_set_hash
            && self.logs == other.logs
            // The typed skip cause is part of the deterministic transition:
            // same input, same reason on every replica.
            && self.skip_reason == other.skip_reason
    }
}

// ---------------------------------------------------------------------------
// ValidatorWriterQueue: checks the BAL, then forwards to the trie-aware writer.
// ---------------------------------------------------------------------------

/// Wraps the trie-aware [`StateWriterQueue`]. It checks each block's
/// write-set against the BAL of every executor replica, then sends the
/// delta to the writer, which advances the MPT state root. It stops the
/// process on a mismatch.
pub struct ValidatorWriterQueue<Q: StateWriterQueue> {
    inner: Q,
    bals: Arc<BalBuffer>,
    divergence: Arc<Divergence>,
    wait: Duration,
    /// The checked write-sets of recent blocks, for the replica BALs that
    /// arrive after their block's check.
    checked: Checked<u64, BlockDelta>,
    /// Highest block already submitted in this process's lifetime. A
    /// cluster session replay (lapse and reconnect, no restart) re-delivers
    /// blocks the validator already ran. Re-execution against already-
    /// applied state gives empty deltas that cannot match the BAL. This
    /// field catches that case, the session form of the restart cascade
    /// below.
    high_water: u64,
    /// Blocks at or below this value were verified before a restart.
    /// Crash recovery replays them to rebuild state, but re-execution
    /// against already-applied state gives empty deltas (every tx skips as
    /// nonce-too-low). Comparing those against retained BAL frames would
    /// report a false divergence on every replay of these blocks.
    verify_floor: u64,
}

impl<Q: StateWriterQueue> ValidatorWriterQueue<Q> {
    pub fn new(inner: Q, bals: Arc<BalBuffer>, divergence: Arc<Divergence>) -> Self {
        Self {
            inner,
            bals,
            divergence,
            wait: BAL_WAIT,
            checked: Checked::new(Check::Bal, BalBuffer::CHECK_WINDOW),
            verify_floor: 0,
            high_water: 0,
        }
    }

    /// Skip BAL verification for blocks at or below `floor`, the recovery
    /// resume point. These blocks were verified before the restart. Their
    /// replay re-execution correctly produces deltas that do not match the
    /// BAL, because the state is already applied.
    #[must_use]
    pub fn with_verify_floor(mut self, floor: u64) -> Self {
        self.verify_floor = floor;
        self
    }

    #[cfg(test)]
    fn with_wait(mut self, wait: Duration) -> Self {
        self.wait = wait;
        self
    }

    /// Compare the write-set of `block` with the BAL of every replica:
    /// the BALs that arrived for the block, and the late BALs of lower
    /// blocks. A missing BAL leaves the block unverified.
    fn verify(&mut self, block: u64, delta: &BlockDelta) -> Result<(), ExecutorError> {
        let taken = self.bals.take(block, self.wait);
        let arrived = !taken.current.is_empty();
        self.checked
            .check(block, delta, taken)
            .map_err(|m| self.diverged(&m))?;
        if arrived {
            metrics::counter_block_verified();
        } else {
            // The BAL never arrived, so this block stays unverified. This
            // is not a proven divergence: log it and count it.
            tracing::warn!(
                block,
                "no BAL received within timeout; block left unverified"
            );
            metrics::counter_bal_missing();
        }
        Ok(())
    }

    /// Record the divergence of a replica BAL whose write-set differs
    /// from the local one. `Divergence` is fatal; the engine does not
    /// retry it.
    fn diverged(&self, m: &Mismatch<u64, BlockDelta>) -> ExecutorError {
        let summary = write_set_diff_summary(&m.local, &m.published.value);
        let detail = format!("block {} write-set != BAL: {summary}", m.key);
        let attribution = Attribution {
            replica: m.published.replica,
            check: Check::Bal,
        };
        ExecutorError::Divergence(self.divergence.halt_replica(attribution, &detail))
    }
}

impl<Q: StateWriterQueue> StateWriterQueue for ValidatorWriterQueue<Q> {
    fn submit(
        &mut self,
        block: BlockBoundary,
        delta: BlockDelta,
        refs: Vec<TxRef>,
    ) -> Result<(), ExecutorError> {
        if block.block_number <= self.verify_floor || block.block_number <= self.high_water {
            tracing::debug!(
                block = block.block_number,
                floor = self.verify_floor,
                high_water = self.high_water,
                "replay overlap; BAL verification skipped (already verified)"
            );
            return self.inner.submit(block, delta, refs);
        }
        self.high_water = block.block_number;
        self.verify(block.block_number, &delta)?;
        // Send the delta to the trie-aware writer; this advances the MPT state root.
        self.inner.submit(block, delta, refs)
    }
}

// ---------------------------------------------------------------------------
// ValidatorReceiptSink: checks tx_receipts; never publishes.
// ---------------------------------------------------------------------------

/// How far behind the newest local receipt a recorded account value stays
/// available for a row check, in canonical positions. A published batch
/// spans at most 64 receipts, so its rows reference writes within that
/// reach; the margin covers a validator that lags the executors.
const RECENT_LOOKBEHIND: u64 = 4096;

/// The latest local post-state of recently written accounts. The sink
/// records every local receipt's rows in canonical order, so at the moment
/// it processes the receipt at position P, an entry is the account's state
/// after P. That is what a published batch row tagged P claims.
#[derive(Default)]
struct RecentAccounts {
    latest: HashMap<Address, (u64, AccountRow)>,
    /// Write order, for eviction. An address repeats once per write.
    order: VecDeque<(u64, Address)>,
}

impl RecentAccounts {
    fn record(&mut self, at: BPosition, rows: &[AccountRow]) {
        let idx = at.as_index();
        for row in rows {
            self.latest.insert(row.address, (idx, row.clone()));
            self.order.push_back((idx, row.address));
        }
        self.evict_below(idx.saturating_sub(RECENT_LOOKBEHIND));
    }

    /// Drop entries last written before `below`. An address rewritten
    /// since keeps its newer value.
    fn evict_below(&mut self, below: u64) {
        while let Some(&(idx, address)) = self.order.front()
            && idx < below
        {
            self.order.pop_front();
            if self.latest.get(&address).is_some_and(|(at, _)| *at == idx) {
                self.latest.remove(&address);
            }
        }
    }

    /// The local value of `row`'s account, when one is recorded.
    fn local(&self, row: &AccountRow) -> Option<&AccountRow> {
        self.latest.get(&row.address).map(|(_, local)| local)
    }
}

/// Implements [`TxReceiptsPublication`], but checks receipts instead of
/// publishing them. It compares each recomputed receipt against the
/// receipt of every executor replica for the same `tx_idx`, and the
/// published account rows of a batch that ends there against the local
/// state, and stops the process on a mismatch.
pub struct ValidatorReceiptSink {
    receipts: Arc<ReceiptBuffer>,
    divergence: Arc<Divergence>,
    wait: Duration,
    /// The checked receipts of recent positions, for the replica receipts
    /// that arrive after their position's check.
    checked: Checked<BPosition, Receipt>,
    /// Recent-block input ring for the receipt-divergence dump. The mismatch
    /// fires after the block's records and claims are gone, so without this
    /// ring a mismatch leaves only a log line, with nothing to replay.
    flight: Option<Arc<crate::flight::FlightRing>>,
    /// The local account state at the current position, for the row check.
    recent: RecentAccounts,
}

impl ValidatorReceiptSink {
    pub fn new(receipts: Arc<ReceiptBuffer>, divergence: Arc<Divergence>) -> Self {
        Self {
            receipts,
            divergence,
            wait: RECEIPT_WAIT,
            checked: Checked::new(Check::Receipt, ReceiptBuffer::CHECK_WINDOW),
            flight: None,
            recent: RecentAccounts::default(),
        }
    }

    /// Attach the flight ring. It dumps block inputs on a receipt mismatch.
    #[must_use]
    pub fn with_flight(mut self, flight: Arc<crate::flight::FlightRing>) -> Self {
        self.flight = Some(flight);
        self
    }

    #[cfg(test)]
    fn with_wait(mut self, wait: Duration) -> Self {
        self.wait = wait;
        self
    }
}

impl TxReceiptsPublication for ValidatorReceiptSink {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
        match msg {
            CMessage::Receipt(local) => self.check_item(&ReceiptRows::bare(local)),
            // A block boundary carries no per-tx data to check.
            CMessage::BlockBoundary(_) => self.latched(),
        }
    }

    /// The engine's commit thread delivers receipts through this path,
    /// each with the rows the local execution wrote. Every item is checked
    /// in order, and the first failure stops the batch.
    fn publish_receipts(&mut self, items: &[ReceiptRows]) -> (usize, Option<ExecutorError>) {
        publish_each(items, |item| self.check_item(item))
    }
}

impl ValidatorReceiptSink {
    /// The divergence latch: once a divergence is proven, the sink keeps
    /// failing. The first failing publish took the published receipt out
    /// of the buffer. Without this latch, a retrying caller would find an
    /// empty buffer, land in the "unverified" arm, and commit past a
    /// proven mismatch.
    fn latched(&self) -> Result<(), ExecutorError> {
        match self
            .divergence
            .halt_reason("validator halted on divergence")
        {
            Some(reason) => Err(ExecutorError::Divergence(reason)),
            None => Ok(()),
        }
    }

    /// Check one local receipt, then the published account rows of every
    /// replica batch that ends at its position, when a frame ending there
    /// has arrived.
    fn check_item(&mut self, item: &ReceiptRows) -> Result<(), ExecutorError> {
        self.latched()?;
        self.check_receipt(&item.receipt)?;
        self.recent.record(item.receipt.tx_idx, &item.accounts);
        let Taken { current, late } = self.receipts.take_rows(item.receipt.tx_idx);
        // Rows that arrive after the sink passed their position meet a
        // later local state, so they stay unverified.
        metrics::counter_rows_unverified(late.len());
        // Under parallel validation only a block's last receipt carries
        // rows. A receipt with no rows is not at a position where the
        // local state is known per position, so the rows stay unverified.
        if item.accounts.is_empty() {
            metrics::counter_rows_unverified(current.len());
            return Ok(());
        }
        current
            .iter()
            .try_for_each(|published| self.check_rows(&item.receipt, published))
    }

    /// Compare one replica's published rows with the local state at the
    /// same position. A row for an account with no recorded local value
    /// is unverified, not a divergence: a cold start can begin inside a
    /// batch.
    fn check_rows(
        &self,
        local: &Receipt,
        published: &Arrival<Vec<AccountRow>>,
    ) -> Result<(), ExecutorError> {
        let mismatch = published.value.iter().find_map(|row| {
            self.recent
                .local(row)
                .filter(|l| *l != row)
                .map(|l| (row, l))
        });
        let Some((row, local_row)) = mismatch else {
            self.count_rows(&published.value);
            return Ok(());
        };
        let detail = format!(
            "account row mismatch at tx_idx {:?}: {} local(nonce={}, balance={}) vs \
             published(nonce={}, balance={}) [tx_hash={} block={}]",
            local.tx_idx,
            row.address,
            local_row.nonce,
            local_row.balance,
            row.nonce,
            row.balance,
            local.tx_hash,
            local.block_number,
        );
        let attribution = Attribution {
            replica: published.replica,
            check: Check::Rows,
        };
        Err(ExecutorError::Divergence(
            self.divergence.halt_replica(attribution, &detail),
        ))
    }

    /// Count a batch with no mismatch: verified when every row had a
    /// local value to compare with, unverified otherwise.
    fn count_rows(&self, published: &[AccountRow]) {
        if published.iter().all(|row| self.recent.local(row).is_some()) {
            metrics::counter_rows_verified();
        } else {
            metrics::counter_rows_unverified(1);
        }
    }
    /// Cross-checks one local receipt against the published receipt of
    /// every replica, and the late replica receipts of lower positions
    /// against their checked receipts. `Ok` when they all match, or when
    /// no published receipt turned up in time to compare (counted as
    /// `receipt_missing`). `Err` halts the validator on a proven
    /// mismatch, after a best-effort flight dump.
    fn check_receipt(&mut self, local: &Receipt) -> Result<(), ExecutorError> {
        let taken = self.receipts.take(local.tx_idx, self.wait);
        if taken.current.is_empty() {
            // No published receipt to compare against, so skip the check.
            metrics::counter_receipt_missing();
        }
        self.checked
            .check(local.tx_idx, local, taken)
            .map_err(|m| self.diverged(&m))
    }

    /// Record the divergence of a replica receipt that differs from the
    /// local one. `local` here is the checked receipt of the position.
    fn diverged(&self, m: &Mismatch<BPosition, Receipt>) -> ExecutorError {
        let (local, published) = (&m.local, &m.published.value);
        // Include the tx identity, not only the mismatch, so
        // responders can find the transaction without a separate
        // hash lookup.
        let detail = format!(
            "receipt mismatch at tx_idx {:?}: local(status={}, gas={}, wsh={}, \
             logs={}) vs published(status={}, gas={}, wsh={}, logs={}) \
             [tx_hash={} from={} to={:?} block={} tx_index={}]",
            local.tx_idx,
            local.status,
            local.gas_used,
            local.write_set_hash,
            local.logs.len(),
            published.status,
            published.gas_used,
            published.write_set_hash,
            published.logs.len(),
            local.tx_hash,
            local.from,
            local.to,
            local.block_number,
            local.transaction_index,
        );
        // Dump the flight ring first, on a best-effort basis. The
        // stop is permanent, so this is the only chance to capture
        // the block inputs behind the mismatch.
        if let Some(f) = self.flight.as_ref() {
            f.dump_receipt_divergence(local, published);
        }
        let attribution = Attribution {
            replica: m.published.replica,
            check: Check::Receipt,
        };
        ExecutorError::Divergence(self.divergence.halt_replica(attribution, &detail))
    }
}

#[cfg(test)]
#[path = "seams_tests.rs"]
mod tests;
