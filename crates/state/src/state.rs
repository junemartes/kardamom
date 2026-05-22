//! Public state API: persistent reads, genesis init, and atomic block commit.

use std::path::Path;
use std::sync::Arc;

use alloy_consensus::Header;
use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_rpc_types_eth::TransactionReceipt;
use libmdbx::WriteFlags;
use revm::primitives::KECCAK_EMPTY;
use revm::state::AccountInfo;

use crate::codecs::{
    AccountRow, decode_header, decode_storage_value, encode_header, encode_storage_key,
    encode_storage_value,
};
use crate::error::StateError;
use crate::schema::{self, tables};
use crate::store::{Store, decode_u64};

/// All state durably committed in one block.
pub struct BlockCommit {
    pub block_number: u64,
    pub header: Header,
    pub accounts: Vec<(Address, Option<AccountRow>)>,
    pub storage: Vec<(Address, B256, U256)>,
    pub code: Vec<(B256, Bytes)>,
    pub receipts: Vec<(B256, TransactionReceipt)>,
    pub applied_deposits: Vec<B256>,
}

/// The persistent latest world. Cloneable handle around an `Arc<Store>`.
#[derive(Clone)]
pub struct State {
    store: Arc<Store>,
    chain_id: u64,
}

impl State {
    /// Open (creating if absent) the env at `path` and verify chain id +
    /// schema version against `chain_id`.
    pub fn open(path: &Path, chain_id: u64) -> Result<Self, StateError> {
        let store = Store::open(path, chain_id)?;
        Ok(Self {
            store: Arc::new(store),
            chain_id,
        })
    }

    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    #[allow(dead_code)]
    pub(crate) fn store(&self) -> &Store {
        &self.store
    }

    /// Latest sealed block number. `None` before genesis has been initialized.
    pub fn latest_block_number(&self) -> Result<Option<u64>, StateError> {
        let txn = self.store.begin_ro()?;
        let meta = txn.open_table(Some(tables::META))?;
        match txn.get::<Vec<u8>>(&meta, schema::meta::LATEST_BLOCK)? {
            Some(bytes) => Ok(Some(decode_u64(&bytes)?)),
            None => Ok(None),
        }
    }

    pub fn account(&self, addr: Address) -> Result<Option<AccountRow>, StateError> {
        let txn = self.store.begin_ro()?;
        let accounts = txn.open_table(Some(tables::ACCOUNTS))?;
        match txn.get::<Vec<u8>>(&accounts, addr.as_slice())? {
            Some(bytes) => Ok(Some(AccountRow::decode(&bytes)?)),
            None => Ok(None),
        }
    }

    pub fn balance(&self, addr: Address) -> Result<U256, StateError> {
        Ok(self.account(addr)?.map(|a| a.balance).unwrap_or(U256::ZERO))
    }

    pub fn nonce(&self, addr: Address) -> Result<u64, StateError> {
        Ok(self.account(addr)?.map(|a| a.nonce).unwrap_or(0))
    }

    pub fn code_by_hash(&self, code_hash: B256) -> Result<Option<Bytes>, StateError> {
        if code_hash == KECCAK_EMPTY {
            return Ok(Some(Bytes::new()));
        }
        let txn = self.store.begin_ro()?;
        let code = txn.open_table(Some(tables::CODE))?;
        match txn.get::<Vec<u8>>(&code, code_hash.as_slice())? {
            Some(bytes) => Ok(Some(Bytes::from(bytes))),
            None => Ok(None),
        }
    }

    pub fn code(&self, addr: Address) -> Result<Bytes, StateError> {
        let account = match self.account(addr)? {
            Some(a) => a,
            None => return Ok(Bytes::new()),
        };
        if account.code_hash == KECCAK_EMPTY {
            return Ok(Bytes::new());
        }
        Ok(self.code_by_hash(account.code_hash)?.unwrap_or_default())
    }

    pub fn storage(&self, addr: Address, slot: B256) -> Result<U256, StateError> {
        let txn = self.store.begin_ro()?;
        let storage = txn.open_table(Some(tables::STORAGE))?;
        let key = encode_storage_key(&addr, &slot);
        match txn.get::<Vec<u8>>(&storage, &key)? {
            Some(bytes) => decode_storage_value(&bytes),
            None => Ok(U256::ZERO),
        }
    }

    pub fn header(&self, number: u64) -> Result<Option<Header>, StateError> {
        let txn = self.store.begin_ro()?;
        let headers = txn.open_table(Some(tables::HEADERS))?;
        match txn.get::<Vec<u8>>(&headers, &number.to_be_bytes())? {
            Some(bytes) => Ok(Some(decode_header(&bytes)?)),
            None => Ok(None),
        }
    }

    pub fn block_hash(&self, number: u64) -> Result<Option<B256>, StateError> {
        Ok(self.header(number)?.map(|h| h.hash_slow()))
    }

    pub fn receipt(&self, tx_hash: B256) -> Result<Option<TransactionReceipt>, StateError> {
        let txn = self.store.begin_ro()?;
        let receipts = txn.open_table(Some(tables::RECEIPTS))?;
        match txn.get::<Vec<u8>>(&receipts, tx_hash.as_slice())? {
            Some(bytes) => {
                let r: TransactionReceipt = serde_json::from_slice(&bytes)
                    .map_err(|e| StateError::codec(format!("receipt json: {e}")))?;
                Ok(Some(r))
            }
            None => Ok(None),
        }
    }

    pub fn is_deposit_applied(&self, source_hash: B256) -> Result<bool, StateError> {
        let txn = self.store.begin_ro()?;
        let table = txn.open_table(Some(tables::APPLIED_DEPOSITS))?;
        Ok(txn
            .get::<Vec<u8>>(&table, source_hash.as_slice())?
            .is_some())
    }

    /// Idempotent: writes genesis alloc + sets `latest_block = 0` + records
    /// the genesis header hash. Re-calling on an env where `latest_block`
    /// is already set is a no-op.
    pub fn initialize_genesis(&self, genesis: &Genesis) -> Result<(), StateError> {
        if self.latest_block_number()?.is_some() {
            return Ok(());
        }

        let txn = self.store.begin_rw()?;
        let accounts = txn.open_table(Some(tables::ACCOUNTS))?;
        let code = txn.open_table(Some(tables::CODE))?;
        let headers = txn.open_table(Some(tables::HEADERS))?;
        let meta = txn.open_table(Some(tables::META))?;

        for entry in &genesis.alloc {
            let (code_hash, bytecode) = match &entry.code {
                Some(bytes) => {
                    let hash = revm::primitives::keccak256(bytes);
                    (hash, Some(bytes.clone()))
                }
                None => (KECCAK_EMPTY, None),
            };
            let nonce = entry
                .nonce
                .unwrap_or_else(|| if entry.code.is_some() { 1 } else { 0 });
            let row = AccountRow {
                balance: entry.balance,
                nonce,
                code_hash,
            };
            txn.put(
                &accounts,
                entry.address.as_slice(),
                row.encode(),
                WriteFlags::default(),
            )?;
            if let Some(b) = bytecode
                && code_hash != KECCAK_EMPTY
            {
                txn.put(&code, code_hash.as_slice(), &b[..], WriteFlags::default())?;
            }
        }

        // Block-zero header: minimal, deterministic.
        let genesis_header = Header {
            number: 0,
            ..Default::default()
        };
        let genesis_hash = genesis_header.hash_slow();
        txn.put(
            &headers,
            0u64.to_be_bytes(),
            encode_header(&genesis_header),
            WriteFlags::default(),
        )?;

        txn.put(
            &meta,
            schema::meta::LATEST_BLOCK,
            0u64.to_be_bytes(),
            WriteFlags::default(),
        )?;
        txn.put(
            &meta,
            schema::meta::GENESIS_HASH,
            genesis_hash.as_slice(),
            WriteFlags::default(),
        )?;
        txn.commit()?;
        Ok(())
    }

    /// Atomically commit one block's writes. fsync on success; any failure
    /// aborts the txn so partial state cannot appear.
    pub fn commit_block(&self, commit: BlockCommit) -> Result<(), StateError> {
        let txn = self.store.begin_rw()?;
        let accounts = txn.open_table(Some(tables::ACCOUNTS))?;
        let storage = txn.open_table(Some(tables::STORAGE))?;
        let code_t = txn.open_table(Some(tables::CODE))?;
        let headers = txn.open_table(Some(tables::HEADERS))?;
        let receipts = txn.open_table(Some(tables::RECEIPTS))?;
        let deposits = txn.open_table(Some(tables::APPLIED_DEPOSITS))?;
        let meta = txn.open_table(Some(tables::META))?;

        for (addr, row) in &commit.accounts {
            match row {
                Some(r) => {
                    txn.put(
                        &accounts,
                        addr.as_slice(),
                        r.encode(),
                        WriteFlags::default(),
                    )?;
                }
                None => {
                    txn.del(&accounts, addr.as_slice(), None)?;
                }
            }
        }
        for (addr, slot, value) in &commit.storage {
            let key = encode_storage_key(addr, slot);
            if value.is_zero() {
                txn.del(&storage, key, None)?;
            } else {
                txn.put(
                    &storage,
                    key,
                    encode_storage_value(*value),
                    WriteFlags::default(),
                )?;
            }
        }
        for (hash, bytecode) in &commit.code {
            if *hash == KECCAK_EMPTY {
                continue;
            }
            txn.put(
                &code_t,
                hash.as_slice(),
                &bytecode[..],
                WriteFlags::default(),
            )?;
        }
        txn.put(
            &headers,
            commit.block_number.to_be_bytes(),
            encode_header(&commit.header),
            WriteFlags::default(),
        )?;
        for (tx_hash, receipt) in &commit.receipts {
            let bytes = serde_json::to_vec(receipt)
                .map_err(|e| StateError::codec(format!("receipt json: {e}")))?;
            txn.put(
                &receipts,
                tx_hash.as_slice(),
                &bytes,
                WriteFlags::default(),
            )?;
        }
        for source_hash in &commit.applied_deposits {
            txn.put(
                &deposits,
                source_hash.as_slice(),
                commit.block_number.to_be_bytes(),
                WriteFlags::default(),
            )?;
        }
        txn.put(
            &meta,
            schema::meta::LATEST_BLOCK,
            commit.block_number.to_be_bytes(),
            WriteFlags::default(),
        )?;
        txn.commit()?;
        Ok(())
    }

    /// Convert an `AccountRow` to revm's `AccountInfo`, resolving code lazily.
    pub fn account_info(&self, addr: Address) -> Result<Option<AccountInfo>, StateError> {
        let row = match self.account(addr)? {
            Some(r) => r,
            None => return Ok(None),
        };
        let code = if row.code_hash == KECCAK_EMPTY {
            None
        } else {
            self.code_by_hash(row.code_hash)?
                .map(revm::state::Bytecode::new_raw)
        };
        Ok(Some(AccountInfo {
            balance: row.balance,
            nonce: row.nonce,
            code_hash: row.code_hash,
            account_id: None,
            code,
        }))
    }
}

/// Minimal genesis shape understood by the state crate. The `node` crate
/// converts its own `Genesis` to this form.
#[derive(Debug, Clone)]
pub struct Genesis {
    pub chain_id: u64,
    pub alloc: Vec<AllocEntry>,
}

#[derive(Debug, Clone)]
pub struct AllocEntry {
    pub address: Address,
    pub balance: U256,
    pub code: Option<Bytes>,
    pub nonce: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{address, b256, hex};
    use tempfile::TempDir;

    fn fresh_state(chain_id: u64) -> (TempDir, State) {
        let dir = TempDir::new().unwrap();
        let state = State::open(dir.path(), chain_id).unwrap();
        (dir, state)
    }

    fn empty_genesis(chain_id: u64) -> Genesis {
        Genesis {
            chain_id,
            alloc: vec![],
        }
    }

    #[test]
    fn latest_block_is_none_before_genesis() {
        let (_dir, state) = fresh_state(1);
        assert_eq!(state.latest_block_number().unwrap(), None);
    }

    #[test]
    fn balance_of_unknown_is_zero() {
        let (_dir, state) = fresh_state(1);
        assert_eq!(
            state.balance(Address::from([0xaau8; 20])).unwrap(),
            U256::ZERO
        );
    }

    #[test]
    fn nonce_of_unknown_is_zero() {
        let (_dir, state) = fresh_state(1);
        assert_eq!(state.nonce(Address::from([0xaau8; 20])).unwrap(), 0);
    }

    #[test]
    fn code_of_unknown_is_empty() {
        let (_dir, state) = fresh_state(1);
        assert!(state.code(Address::from([0xaau8; 20])).unwrap().is_empty());
    }

    #[test]
    fn storage_of_unknown_slot_is_zero() {
        let (_dir, state) = fresh_state(1);
        assert_eq!(
            state
                .storage(Address::from([0xaau8; 20]), B256::ZERO)
                .unwrap(),
            U256::ZERO
        );
    }

    #[test]
    fn receipt_for_unknown_hash_is_none() {
        let (_dir, state) = fresh_state(1);
        assert!(state.receipt(B256::ZERO).unwrap().is_none());
    }

    #[test]
    fn is_deposit_applied_returns_false_for_unknown() {
        let (_dir, state) = fresh_state(1);
        assert!(!state.is_deposit_applied(B256::ZERO).unwrap());
    }

    #[test]
    fn initialize_genesis_is_idempotent() {
        let (_dir, state) = fresh_state(1);
        let g = empty_genesis(1);
        state.initialize_genesis(&g).unwrap();
        assert_eq!(state.latest_block_number().unwrap(), Some(0));
        // Second call must not error or change state.
        state.initialize_genesis(&g).unwrap();
        assert_eq!(state.latest_block_number().unwrap(), Some(0));
    }

    #[test]
    fn initialize_genesis_writes_alloc_entries() {
        let (_dir, state) = fresh_state(1);
        let funded = address!("00000000000000000000000000000000000000aa");
        let contract = address!("00000000000000000000000000000000000000bb");
        let code = Bytes::from(hex!("604260005260206000f3").to_vec());
        let g = Genesis {
            chain_id: 1,
            alloc: vec![
                AllocEntry {
                    address: funded,
                    balance: U256::from(1_000u64),
                    code: None,
                    nonce: None,
                },
                AllocEntry {
                    address: contract,
                    balance: U256::ZERO,
                    code: Some(code.clone()),
                    nonce: None,
                },
            ],
        };
        state.initialize_genesis(&g).unwrap();
        assert_eq!(state.balance(funded).unwrap(), U256::from(1_000u64));
        assert_eq!(state.nonce(funded).unwrap(), 0);
        assert_eq!(state.code(contract).unwrap(), code);
        assert_eq!(state.nonce(contract).unwrap(), 1);
    }

    #[test]
    fn commit_block_persists_account_changes() {
        let dir = TempDir::new().unwrap();
        let addr = address!("00000000000000000000000000000000000000aa");
        {
            let state = State::open(dir.path(), 1).unwrap();
            state.initialize_genesis(&empty_genesis(1)).unwrap();
            let header = Header {
                number: 1,
                ..Default::default()
            };
            state
                .commit_block(BlockCommit {
                    block_number: 1,
                    header,
                    accounts: vec![(
                        addr,
                        Some(AccountRow {
                            balance: U256::from(7u64),
                            nonce: 1,
                            code_hash: KECCAK_EMPTY,
                        }),
                    )],
                    storage: vec![],
                    code: vec![],
                    receipts: vec![],
                    applied_deposits: vec![],
                })
                .unwrap();
        }
        // Reopen and verify.
        let state = State::open(dir.path(), 1).unwrap();
        assert_eq!(state.latest_block_number().unwrap(), Some(1));
        assert_eq!(state.balance(addr).unwrap(), U256::from(7u64));
        assert_eq!(state.nonce(addr).unwrap(), 1);
    }

    #[test]
    fn commit_block_zero_storage_deletes_row() {
        let (_dir, state) = fresh_state(1);
        state.initialize_genesis(&empty_genesis(1)).unwrap();
        let addr = address!("00000000000000000000000000000000000000aa");
        let slot = b256!("0000000000000000000000000000000000000000000000000000000000000001");

        // Write non-zero.
        state
            .commit_block(BlockCommit {
                block_number: 1,
                header: Header {
                    number: 1,
                    ..Default::default()
                },
                accounts: vec![],
                storage: vec![(addr, slot, U256::from(42u64))],
                code: vec![],
                receipts: vec![],
                applied_deposits: vec![],
            })
            .unwrap();
        assert_eq!(state.storage(addr, slot).unwrap(), U256::from(42u64));

        // Overwrite with zero: row should be deleted.
        state
            .commit_block(BlockCommit {
                block_number: 2,
                header: Header {
                    number: 2,
                    ..Default::default()
                },
                accounts: vec![],
                storage: vec![(addr, slot, U256::ZERO)],
                code: vec![],
                receipts: vec![],
                applied_deposits: vec![],
            })
            .unwrap();
        assert_eq!(state.storage(addr, slot).unwrap(), U256::ZERO);

        // Confirm absence at the raw layer.
        let txn = state.store().begin_ro().unwrap();
        let storage_t = txn.open_table(Some(tables::STORAGE)).unwrap();
        let key = encode_storage_key(&addr, &slot);
        assert!(txn.get::<Vec<u8>>(&storage_t, &key).unwrap().is_none());
    }

    #[test]
    fn commit_block_inserts_header_and_bumps_meta() {
        let (_dir, state) = fresh_state(1);
        state.initialize_genesis(&empty_genesis(1)).unwrap();
        let header = Header {
            number: 1,
            timestamp: 123,
            gas_used: 21_000,
            ..Default::default()
        };
        state
            .commit_block(BlockCommit {
                block_number: 1,
                header: header.clone(),
                accounts: vec![],
                storage: vec![],
                code: vec![],
                receipts: vec![],
                applied_deposits: vec![],
            })
            .unwrap();
        assert_eq!(state.latest_block_number().unwrap(), Some(1));
        let got = state.header(1).unwrap().expect("header present");
        assert_eq!(got.number, 1);
        assert_eq!(got.timestamp, 123);
        assert_eq!(got.gas_used, 21_000);
        assert_eq!(state.block_hash(1).unwrap(), Some(header.hash_slow()));
    }

    #[test]
    fn code_dedup_across_accounts() {
        let (_dir, state) = fresh_state(1);
        state.initialize_genesis(&empty_genesis(1)).unwrap();
        let code = Bytes::from(hex!("604260005260206000f3").to_vec());
        let code_hash = revm::primitives::keccak256(&code);

        let a = address!("00000000000000000000000000000000000000aa");
        let b = address!("00000000000000000000000000000000000000bb");
        state
            .commit_block(BlockCommit {
                block_number: 1,
                header: Header {
                    number: 1,
                    ..Default::default()
                },
                accounts: vec![
                    (
                        a,
                        Some(AccountRow {
                            balance: U256::ZERO,
                            nonce: 1,
                            code_hash,
                        }),
                    ),
                    (
                        b,
                        Some(AccountRow {
                            balance: U256::ZERO,
                            nonce: 1,
                            code_hash,
                        }),
                    ),
                ],
                storage: vec![],
                code: vec![(code_hash, code.clone())],
                receipts: vec![],
                applied_deposits: vec![],
            })
            .unwrap();

        // Both accounts resolve to the same code; the underlying code table
        // has exactly one row for this hash.
        assert_eq!(state.code(a).unwrap(), code);
        assert_eq!(state.code(b).unwrap(), code);

        let txn = state.store().begin_ro().unwrap();
        let code_t = txn.open_table(Some(tables::CODE)).unwrap();
        let bytes = txn
            .get::<Vec<u8>>(&code_t, code_hash.as_slice())
            .unwrap()
            .expect("present");
        assert_eq!(bytes, code.to_vec());
    }

    #[test]
    fn commit_block_records_applied_deposits_and_receipts() {
        let (_dir, state) = fresh_state(1);
        state.initialize_genesis(&empty_genesis(1)).unwrap();
        let source = b256!("0000000000000000000000000000000000000000000000000000000000000077");
        let tx_hash = b256!("0000000000000000000000000000000000000000000000000000000000000088");

        // A real receipt-with-bloom shape would be nice; for now we lean on
        // serde to round-trip a default. The full Node tests exercise the
        // real shape end-to-end.
        let receipt = TransactionReceipt {
            inner: alloy_consensus::ReceiptEnvelope::Eip1559(alloy_consensus::ReceiptWithBloom {
                receipt: alloy_consensus::Receipt {
                    status: alloy_consensus::Eip658Value::Eip658(true),
                    cumulative_gas_used: 21_000,
                    logs: vec![],
                },
                logs_bloom: Default::default(),
            }),
            transaction_hash: tx_hash,
            transaction_index: Some(0),
            block_hash: None,
            block_number: Some(1),
            gas_used: 21_000,
            effective_gas_price: 0,
            blob_gas_used: None,
            blob_gas_price: None,
            from: Address::ZERO,
            to: None,
            contract_address: None,
        };

        state
            .commit_block(BlockCommit {
                block_number: 1,
                header: Header {
                    number: 1,
                    ..Default::default()
                },
                accounts: vec![],
                storage: vec![],
                code: vec![],
                receipts: vec![(tx_hash, receipt.clone())],
                applied_deposits: vec![source],
            })
            .unwrap();

        assert!(state.is_deposit_applied(source).unwrap());
        let got = state.receipt(tx_hash).unwrap().expect("receipt present");
        assert_eq!(got.transaction_hash, tx_hash);
        assert_eq!(got.gas_used, 21_000);
    }
}
