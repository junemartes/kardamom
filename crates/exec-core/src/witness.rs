//! Stateless execution over a captured pre-state witness (spec:
//! no-std-exec-core, phase 2).
//!
//! Two halves of one contract:
//!
//! - [`WitnessRecorder`] (std, validator-side collector) wraps a real
//!   `StateDatabase` snapshot and records every account/slot/code read with
//!   the value the snapshot returned. Execution reads reach the snapshot
//!   only on first touch (`CacheDB` memoizes), so the recording is exactly
//!   the block's pre-state slice — including PROVEN ABSENCE (an account read
//!   that returned `None`) and explicit zero slots.
//! - [`WitnessDb`] (`no_std`, the zk-guest shape) replays a
//!   [`kardamom_types::ExecutionWitness`] as a `StateDatabase`. It is
//!   FAIL-CLOSED: a read of any key the witness does not carry is an error
//!   ([`WitnessError`]), never an empty default — an incomplete witness must
//!   abort a stateless re-execution (and, in phase 3, a proof), not warp it.
//!
//! Round-trip guarantee (asserted by the validator's stateless re-execution
//! test): executing the same records over `WitnessDb::from_witness(capture)`
//! produces the identical receipts and `BlockDelta` the recorded execution
//! produced.

use alloc::collections::BTreeMap;

use alloy_primitives::{Address, B256, U256};
use bytes::Bytes;
use kardamom_types::delta::CodeEntry;
use kardamom_types::{
    BPosition, ExecutionWitness, Receipt, StateDatabase, StateError, WitnessAccount, WitnessSlot,
};
use revm::primitives::KECCAK_EMPTY;

/// Error surfaced by [`WitnessDb`] when the execution reads a key the
/// witness does not carry. Deterministic-fatal for a stateless re-execution:
/// the witness is incomplete for these records.
#[derive(Debug, thiserror::Error)]
pub enum WitnessError {
    #[error("witness missing account {0}")]
    MissingAccount(Address),
    #[error("witness missing storage slot ({0}, {1})")]
    MissingSlot(Address, B256),
    #[error("witness missing code for hash {0}")]
    MissingCode(B256),
}

impl StateError for WitnessError {}

/// `no_std` witness-backed [`StateDatabase`]. See the module docs for the
/// fail-closed contract.
#[derive(Debug, Default, Clone)]
pub struct WitnessDb {
    /// `None` = proven absent.
    accounts: BTreeMap<Address, Option<(u64, U256, B256)>>,
    storage: BTreeMap<(Address, B256), U256>,
    code: BTreeMap<B256, Bytes>,
}

impl WitnessDb {
    pub fn from_witness(w: &ExecutionWitness) -> Self {
        let mut db = Self::default();
        for a in &w.accounts {
            let v = a.exists.then_some((a.nonce, a.balance, a.code_hash));
            db.accounts.insert(a.address, v);
        }
        for s in &w.storage {
            db.storage.insert((s.address, s.key), s.value);
        }
        for c in &w.code {
            db.code.insert(c.code_hash, c.code.clone());
        }
        db
    }
}

impl StateDatabase for WitnessDb {
    type Error = WitnessError;

    fn basic(&self, address: Address) -> Result<Option<(u64, U256, B256)>, Self::Error> {
        self.accounts
            .get(&address)
            .copied()
            .ok_or(WitnessError::MissingAccount(address))
    }

    fn code_by_hash(&self, code_hash: B256) -> Result<Bytes, Self::Error> {
        // Both spellings of "no code" resolve to empty without a witness
        // entry — mirrors `SnapshotRef::code_by_hash_ref`'s handling plus
        // the #161 zero-hash normalization.
        if code_hash == KECCAK_EMPTY || code_hash == B256::ZERO {
            return Ok(Bytes::new());
        }
        self.code
            .get(&code_hash)
            .cloned()
            .ok_or(WitnessError::MissingCode(code_hash))
    }

    fn storage(&self, address: Address, key: B256) -> Result<U256, Self::Error> {
        self.storage
            .get(&(address, key))
            .copied()
            .ok_or(WitnessError::MissingSlot(address, key))
    }

    /// Receipts are not pre-state; a stateless re-execution never reads them.
    fn get_receipt(&self, _pos: BPosition) -> Result<Option<Receipt>, Self::Error> {
        Ok(None)
    }

    fn get_tx_position(&self, _tx_hash: B256) -> Result<Option<BPosition>, Self::Error> {
        Ok(None)
    }
}

/// Validator-side witness collector: a recording decorator over the real
/// snapshot. `std`-only (interior `Mutex` so it satisfies the `Sync` bound
/// `execute_block_parallel` places on the shared snapshot).
///
/// First-touch wins: the value recorded for a key is the one the FIRST read
/// observed; later reads are answered from the recording, so the witness and
/// the live execution can never disagree mid-block.
#[cfg(feature = "std")]
pub struct WitnessRecorder<S> {
    inner: S,
    rec: std::sync::Mutex<Recorded>,
}

#[cfg(feature = "std")]
#[derive(Default)]
struct Recorded {
    accounts: BTreeMap<Address, Option<(u64, U256, B256)>>,
    storage: BTreeMap<(Address, B256), U256>,
    code: BTreeMap<B256, Bytes>,
}

#[cfg(feature = "std")]
impl<S: StateDatabase> WitnessRecorder<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            rec: std::sync::Mutex::new(Recorded::default()),
        }
    }

    /// Drain the recording into a canonical [`ExecutionWitness`].
    pub fn into_witness(self, block_number: u64) -> ExecutionWitness {
        let rec = self.rec.into_inner().expect("witness recorder poisoned");
        let accounts = rec
            .accounts
            .into_iter()
            .map(|(address, v)| match v {
                Some((nonce, balance, code_hash)) => WitnessAccount {
                    address,
                    exists: true,
                    nonce,
                    balance,
                    code_hash,
                },
                None => WitnessAccount {
                    address,
                    exists: false,
                    nonce: 0,
                    balance: U256::ZERO,
                    code_hash: B256::ZERO,
                },
            })
            .collect();
        let storage = rec
            .storage
            .into_iter()
            .map(|((address, key), value)| WitnessSlot {
                address,
                key,
                value,
            })
            .collect();
        let code = rec
            .code
            .into_iter()
            .map(|(code_hash, code)| CodeEntry { code_hash, code })
            .collect();
        // BTreeMap iteration is already the canonical sorted order.
        ExecutionWitness {
            block_number,
            accounts,
            storage,
            code,
            pre_state_root: None,
        }
    }
}

#[cfg(feature = "std")]
impl<S: StateDatabase> StateDatabase for WitnessRecorder<S> {
    type Error = S::Error;

    fn basic(&self, address: Address) -> Result<Option<(u64, U256, B256)>, Self::Error> {
        let mut g = self.rec.lock().expect("witness recorder poisoned");
        if let Some(v) = g.accounts.get(&address) {
            return Ok(*v);
        }
        let v = self.inner.basic(address)?;
        g.accounts.insert(address, v);
        Ok(v)
    }

    fn code_by_hash(&self, code_hash: B256) -> Result<Bytes, Self::Error> {
        // Empty-code hashes never enter the witness (WitnessDb resolves them
        // structurally), keeping witnesses minimal.
        if code_hash == KECCAK_EMPTY || code_hash == B256::ZERO {
            return Ok(Bytes::new());
        }
        let mut g = self.rec.lock().expect("witness recorder poisoned");
        if let Some(c) = g.code.get(&code_hash) {
            return Ok(c.clone());
        }
        let c = self.inner.code_by_hash(code_hash)?;
        g.code.insert(code_hash, c.clone());
        Ok(c)
    }

    fn storage(&self, address: Address, key: B256) -> Result<U256, Self::Error> {
        let mut g = self.rec.lock().expect("witness recorder poisoned");
        if let Some(v) = g.storage.get(&(address, key)) {
            return Ok(*v);
        }
        let v = self.inner.storage(address, key)?;
        g.storage.insert((address, key), v);
        Ok(v)
    }

    /// Pass-through, unrecorded: receipts are not pre-state.
    fn get_receipt(&self, pos: BPosition) -> Result<Option<Receipt>, Self::Error> {
        self.inner.get_receipt(pos)
    }

    fn get_tx_position(&self, tx_hash: B256) -> Result<Option<BPosition>, Self::Error> {
        self.inner.get_tx_position(tx_hash)
    }
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;
    use crate::state::MockStateDatabase;

    fn addr(b: u8) -> Address {
        Address::from([b; 20])
    }

    #[test]
    fn recorder_first_touch_and_absence_round_trip() {
        let snap = MockStateDatabase::builder()
            .account(addr(1), U256::from(7u64), 3, B256::ZERO)
            .storage(addr(1), B256::repeat_byte(0xAA), U256::from(42u64))
            .build();
        let rec = WitnessRecorder::new(snap);

        assert_eq!(
            rec.basic(addr(1)).unwrap(),
            Some((3, U256::from(7u64), B256::ZERO))
        );
        // Absent account: recorded as proven-absent.
        assert_eq!(rec.basic(addr(2)).unwrap(), None);
        // Explicit zero slot: recorded, not defaulted.
        assert_eq!(rec.storage(addr(1), B256::ZERO).unwrap(), U256::ZERO);
        assert_eq!(
            rec.storage(addr(1), B256::repeat_byte(0xAA)).unwrap(),
            U256::from(42u64)
        );

        let w = rec.into_witness(9);
        assert_eq!(w.block_number, 9);
        assert_eq!(w.accounts.len(), 2);
        assert_eq!(w.storage.len(), 2);

        let db = WitnessDb::from_witness(&w);
        assert_eq!(
            db.basic(addr(1)).unwrap(),
            Some((3, U256::from(7u64), B256::ZERO))
        );
        assert_eq!(db.basic(addr(2)).unwrap(), None);
        assert_eq!(db.storage(addr(1), B256::ZERO).unwrap(), U256::ZERO);
        assert_eq!(
            db.storage(addr(1), B256::repeat_byte(0xAA)).unwrap(),
            U256::from(42u64)
        );
    }

    #[test]
    fn witness_db_is_fail_closed() {
        let db = WitnessDb::from_witness(&ExecutionWitness::default());
        assert!(matches!(
            db.basic(addr(9)),
            Err(WitnessError::MissingAccount(_))
        ));
        assert!(matches!(
            db.storage(addr(9), B256::ZERO),
            Err(WitnessError::MissingSlot(..))
        ));
        assert!(matches!(
            db.code_by_hash(B256::repeat_byte(0x11)),
            Err(WitnessError::MissingCode(_))
        ));
        // Structural empties never error.
        assert_eq!(db.code_by_hash(KECCAK_EMPTY).unwrap(), Bytes::new());
        assert_eq!(db.code_by_hash(B256::ZERO).unwrap(), Bytes::new());
    }

    #[test]
    fn digest_is_order_stable() {
        let snap = MockStateDatabase::builder()
            .account(addr(3), U256::from(1u64), 0, B256::ZERO)
            .account(addr(1), U256::from(1u64), 0, B256::ZERO)
            .build();
        let rec = WitnessRecorder::new(snap.clone());
        // Touch in one order…
        rec.basic(addr(3)).unwrap();
        rec.basic(addr(1)).unwrap();
        let w1 = rec.into_witness(1);
        // …and the reverse.
        let rec = WitnessRecorder::new(snap);
        rec.basic(addr(1)).unwrap();
        rec.basic(addr(3)).unwrap();
        let w2 = rec.into_witness(1);
        assert_eq!(w1.digest(), w2.digest());
    }
}
