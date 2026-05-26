//! In-memory test fixtures for the sequencer.
//!
//! Gated on `cfg(any(test, feature = "testing"))` so production builds
//! never pull this code in.

use std::collections::HashMap;
use std::sync::RwLock;

use alloy_primitives::{Address, B256, U256};
use bytes::Bytes;
use kardamom_types::{BPosition, Receipt, StateDatabase, StateError};

/// In-memory `StateDatabase` used by sequencer tests. By default every
/// `basic` lookup returns `Ok(None)` (sender hydrates as a new account at
/// nonce 0). Use [`Self::set_nonce`] to seed established senders so the
/// cache-miss hydration path picks up a non-zero starting nonce.
#[derive(Default)]
pub struct FakeStateDatabase {
    nonces: RwLock<HashMap<Address, u64>>,
}

impl FakeStateDatabase {
    pub fn new() -> Self {
        Self::default()
    }

    /// Seed `sender`'s committed nonce. Hydration will read this back.
    pub fn set_nonce(&self, sender: Address, nonce: u64) {
        self.nonces.write().unwrap().insert(sender, nonce);
    }
}

#[derive(Debug, thiserror::Error)]
#[error("fake state-database error")]
pub struct FakeStateError;

impl StateError for FakeStateError {}

impl StateDatabase for FakeStateDatabase {
    type Error = FakeStateError;

    fn basic(&self, address: Address) -> Result<Option<(u64, U256, B256)>, Self::Error> {
        Ok(self
            .nonces
            .read()
            .unwrap()
            .get(&address)
            .map(|&n| (n, U256::ZERO, B256::ZERO)))
    }

    fn storage(&self, _address: Address, _key: B256) -> Result<U256, Self::Error> {
        Ok(U256::ZERO)
    }

    fn code_by_hash(&self, _code_hash: B256) -> Result<Bytes, Self::Error> {
        Ok(Bytes::new())
    }

    fn get_receipt(&self, _pos: BPosition) -> Result<Option<Receipt>, Self::Error> {
        Ok(None)
    }

    fn get_tx_position(&self, _tx_hash: B256) -> Result<Option<BPosition>, Self::Error> {
        Ok(None)
    }
}
