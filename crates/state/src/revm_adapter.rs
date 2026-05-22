//! `MdbxBackend: revm::DatabaseRef` — read-only view of the persistent state
//! suitable for wrapping in revm's `CacheDB` as a per-block overlay.

use std::convert::Infallible;

use alloy_primitives::{Address, B256, U256};
use revm::database::DatabaseRef;
use revm::primitives::KECCAK_EMPTY;
use revm::state::{AccountInfo, Bytecode};

use crate::error::StateError;
use crate::state::State;

/// A `DatabaseRef` over an `Arc<State>`. Cheap to clone; reads go through
/// MDBX read txns.
#[derive(Clone)]
pub struct MdbxBackend {
    state: State,
}

impl MdbxBackend {
    pub fn new(state: State) -> Self {
        Self { state }
    }

    pub fn state(&self) -> &State {
        &self.state
    }
}

impl DatabaseRef for MdbxBackend {
    type Error = StateError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        self.state.account_info(address)
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        if code_hash == KECCAK_EMPTY {
            return Ok(Bytecode::new());
        }
        match self.state.code_by_hash(code_hash)? {
            Some(bytes) if !bytes.is_empty() => Ok(Bytecode::new_raw(bytes)),
            _ => Ok(Bytecode::new()),
        }
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let slot = B256::from(index.to_be_bytes());
        self.state.storage(address, slot)
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        // revm asks for B256::ZERO when the requested block is outside the
        // history window. We delegate to `State::block_hash`, which returns
        // `None` for unknown headers; we map that to ZERO so revm's
        // BLOCKHASH semantics are preserved.
        Ok(self.state.block_hash(number)?.unwrap_or(B256::ZERO))
    }
}

// `DatabaseRef::Error = StateError` is the natural choice. Provide a tiny
// helper that pins down the type for tests / consumers that don't care.
#[allow(dead_code)]
fn _assert_infallible() {
    fn _is_send<T: Send>() {}
    _is_send::<Infallible>();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codecs::AccountRow;
    use crate::state::{BlockCommit, Genesis, State};
    use alloy_consensus::Header;
    use alloy_primitives::{Bytes, address, hex};
    use tempfile::TempDir;

    fn fresh(chain_id: u64) -> (TempDir, State) {
        let dir = TempDir::new().unwrap();
        let state = State::open(dir.path(), chain_id).unwrap();
        state
            .initialize_genesis(&Genesis {
                chain_id,
                alloc: vec![],
            })
            .unwrap();
        (dir, state)
    }

    #[test]
    fn basic_ref_returns_account_info() {
        let (_dir, state) = fresh(1);
        let addr = address!("00000000000000000000000000000000000000aa");
        let row = AccountRow {
            balance: U256::from(99u64),
            nonce: 3,
            code_hash: KECCAK_EMPTY,
        };
        state
            .commit_block(BlockCommit {
                block_number: 1,
                header: Header {
                    number: 1,
                    ..Default::default()
                },
                accounts: vec![(addr, Some(row))],
                storage: vec![],
                code: vec![],
                receipts: vec![],
                applied_deposits: vec![],
            })
            .unwrap();
        let backend = MdbxBackend::new(state);
        let info = backend.basic_ref(addr).unwrap().expect("present");
        assert_eq!(info.balance, U256::from(99u64));
        assert_eq!(info.nonce, 3);
        assert_eq!(info.code_hash, KECCAK_EMPTY);
    }

    #[test]
    fn basic_ref_returns_none_for_unknown() {
        let (_dir, state) = fresh(1);
        let backend = MdbxBackend::new(state);
        assert!(
            backend
                .basic_ref(address!("00000000000000000000000000000000000000aa"))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn code_by_hash_returns_bytecode() {
        let (_dir, state) = fresh(1);
        let code = Bytes::from(hex!("604260005260206000f3").to_vec());
        let code_hash = revm::primitives::keccak256(&code);
        let addr = address!("00000000000000000000000000000000000000bb");

        state
            .commit_block(BlockCommit {
                block_number: 1,
                header: Header {
                    number: 1,
                    ..Default::default()
                },
                accounts: vec![(
                    addr,
                    Some(AccountRow {
                        balance: U256::ZERO,
                        nonce: 1,
                        code_hash,
                    }),
                )],
                storage: vec![],
                code: vec![(code_hash, code.clone())],
                receipts: vec![],
                applied_deposits: vec![],
            })
            .unwrap();

        let backend = MdbxBackend::new(state);
        let bc = backend.code_by_hash_ref(code_hash).unwrap();
        assert_eq!(bc.original_bytes().to_vec(), code.to_vec());
    }

    #[test]
    fn code_by_hash_empty_returns_empty_bytecode() {
        let (_dir, state) = fresh(1);
        let backend = MdbxBackend::new(state);
        let bc = backend.code_by_hash_ref(KECCAK_EMPTY).unwrap();
        assert!(bc.is_empty());
    }

    #[test]
    fn storage_ref_returns_zero_for_missing() {
        let (_dir, state) = fresh(1);
        let backend = MdbxBackend::new(state);
        assert_eq!(
            backend
                .storage_ref(
                    address!("00000000000000000000000000000000000000aa"),
                    U256::from(7u64)
                )
                .unwrap(),
            U256::ZERO
        );
    }

    #[test]
    fn block_hash_returns_known_header_hash() {
        let (_dir, state) = fresh(1);
        let header = Header {
            number: 1,
            timestamp: 1,
            ..Default::default()
        };
        let expected = header.hash_slow();
        state
            .commit_block(BlockCommit {
                block_number: 1,
                header,
                accounts: vec![],
                storage: vec![],
                code: vec![],
                receipts: vec![],
                applied_deposits: vec![],
            })
            .unwrap();
        let backend = MdbxBackend::new(state);
        assert_eq!(backend.block_hash_ref(1).unwrap(), expected);
        // Outside the chain: zero.
        assert_eq!(backend.block_hash_ref(9999).unwrap(), B256::ZERO);
    }
}
