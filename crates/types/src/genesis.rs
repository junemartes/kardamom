//! Genesis chain config: chain id plus initial account allocation.
//!
//! On disk this is kardamom-native TOML (see `chains/dev.toml`). The struct
//! derives `serde::Deserialize` directly. The only custom serde code is on
//! the string-parsed `balance` and `code` fields. `Genesis::validate` checks
//! chain id != 0 and duplicate alloc addresses. The TOML loader calls it
//! after parsing. The optional `[fees]` section holds the chain's fee
//! schedule (see [`crate::fees::FeeSchedule`]). The optional
//! `genesis_features` list holds the sealer features that the chain runs
//! from its genesis (see [`GenesisFeatures`]).

use alloc::{format, string::String, vec, vec::Vec};

use alloy_primitives::{Address, B256, Bytes, U256, hex};
use serde::Deserialize;

use crate::AccountChange;
use crate::delta::{CodeEntry, StorageChange};
use crate::upgrades::{
    CHAIN_STATE, CHAIN_STATE_CODE, CHAIN_STATE_CODE_HASH, GENESIS_ACTIVATION_MS, GenesisFeatures,
    activation_slot,
};

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Genesis {
    pub chain_id: u64,
    #[serde(default)]
    pub alloc: Vec<AllocEntry>,
    /// The fee schedule. Absent means no schedule: a zero base fee, and
    /// every tip burns. It is a chain value: every state consumer reads
    /// it from the same genesis, so every one of them charges the same
    /// fees and computes the same roots.
    #[serde(default)]
    pub fees: Option<crate::fees::FeeSchedule>,
    /// The sealer features that the chain runs from its genesis, as
    /// `genesis_features = [3, 4, 5]`. Absent means none. The sealer
    /// setting `kardamom.cluster.genesisFeatures` names the same set.
    #[serde(default)]
    pub genesis_features: crate::upgrades::GenesisFeatures,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllocEntry {
    pub address: Address,
    #[serde(default, deserialize_with = "deserialize_balance")]
    pub balance: U256,
    #[serde(default, deserialize_with = "deserialize_code")]
    pub code: Option<Bytes>,
    /// Starting nonce. Omitted means 0.
    pub nonce: Option<u64>,
}

impl AllocEntry {
    /// This entry's code hash: `B256::ZERO` for an empty-code account, the
    /// executor and validator convention. Kardamom does not use the
    /// Ethereum `KECCAK_EMPTY` sentinel.
    fn code_hash(&self) -> alloy_primitives::B256 {
        self.code
            .as_ref()
            .map_or(alloy_primitives::B256::ZERO, |c| {
                alloy_primitives::keccak256(c.as_ref())
            })
    }
}

impl Genesis {
    /// Build the genesis state ([`GenesisAlloc`]) for genesis seeding
    /// (`kardamom_state::seed_genesis`).
    ///
    /// Each [`AllocEntry`] becomes one [`AccountChange`] with its declared
    /// balance and nonce. An empty-code account uses `code_hash =
    /// B256::ZERO`. This is the executor and validator convention; kardamom
    /// does not use the Ethereum `KECCAK_EMPTY` sentinel. Code bytes become
    /// a [`CodeEntry`]. Then the genesis features add the chain-state code
    /// and their activation words. This shared builder
    /// keeps the rebuild-from-L1 reconstructor's genesis identical, byte for
    /// byte, to the live executor's genesis, so their state roots match.
    #[must_use]
    pub fn to_alloc(&self) -> GenesisAlloc {
        // One pass, one `code_hash()` per entry: the account and its code
        // entry (if any) both need the hash, so computing it here instead
        // of once per collection avoids hashing the same code twice.
        let (accounts, code): (Vec<_>, Vec<Option<_>>) = self
            .alloc
            .iter()
            .map(|entry| {
                let code_hash = entry.code_hash();
                let account = AccountChange {
                    address: entry.address,
                    nonce: entry.nonce.unwrap_or(0),
                    balance: entry.balance,
                    code_hash,
                };
                let code_entry = entry.code.as_ref().map(|c| CodeEntry {
                    code_hash,
                    code: c.0.clone(),
                });
                (account, code_entry)
            })
            .unzip();
        let mut alloc = GenesisAlloc {
            accounts,
            code: code.into_iter().flatten().collect(),
            storage: vec![],
        };
        alloc.add_features(self.genesis_features);
        alloc
    }

    /// Checks rules that the type and derive cannot express.
    ///
    /// # Errors
    ///
    /// Returns [`GenesisError::ZeroChainId`] if `chain_id` is 0,
    /// [`GenesisError::ChainStateCode`] if a chain with genesis features
    /// holds other code at [`CHAIN_STATE`], or
    /// [`GenesisError::DuplicateAlloc`] if two allocation entries share an
    /// address.
    pub fn validate(&self) -> Result<(), GenesisError> {
        if self.chain_id == 0 {
            return Err(GenesisError::ZeroChainId);
        }
        if self.genesis_features != GenesisFeatures::None && self.foreign_chain_state_code() {
            return Err(GenesisError::ChainStateCode);
        }
        let mut seen = alloc::collections::BTreeSet::new();
        self.alloc.iter().try_for_each(|entry| {
            if seen.insert(entry.address) {
                Ok(())
            } else {
                Err(GenesisError::DuplicateAlloc(entry.address))
            }
        })
    }

    /// `true` when the allocation puts code other than
    /// [`CHAIN_STATE_CODE`] at [`CHAIN_STATE`].
    fn foreign_chain_state_code(&self) -> bool {
        self.alloc
            .iter()
            .filter(|entry| entry.address == CHAIN_STATE)
            .any(|entry| !matches!(entry.code_hash(), B256::ZERO | CHAIN_STATE_CODE_HASH))
    }
}

/// The genesis state of a chain: the accounts, the code, and the storage
/// words. [`Genesis::to_alloc`] builds it, and every state consumer seeds
/// from it (`kardamom_state::seed_genesis`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GenesisAlloc {
    pub accounts: Vec<AccountChange>,
    pub code: Vec<CodeEntry>,
    pub storage: Vec<StorageChange>,
}

impl GenesisAlloc {
    /// An allocation of `accounts` only: no code and no storage word.
    #[must_use]
    pub fn of_accounts(accounts: Vec<AccountChange>) -> Self {
        Self {
            accounts,
            ..Self::default()
        }
    }

    /// Add the genesis `features`: the canonical code at [`CHAIN_STATE`]
    /// and the activation word of each feature. These are the writes of
    /// `setFeature(feature, GENESIS_ACTIVATION_MS)` system deposits on a
    /// chain without the features, the code install included. An
    /// allocation entry at [`CHAIN_STATE`] keeps its nonce and balance, as
    /// that install does. With no feature, the allocation stays unchanged.
    fn add_features(&mut self, features: GenesisFeatures) {
        if features == GenesisFeatures::None {
            return;
        }
        let account = self.accounts.iter_mut().find(|a| a.address == CHAIN_STATE);
        let has_code = account
            .as_ref()
            .is_some_and(|a| a.code_hash == CHAIN_STATE_CODE_HASH);
        match account {
            Some(a) => a.code_hash = CHAIN_STATE_CODE_HASH,
            None => self.accounts.push(AccountChange {
                address: CHAIN_STATE,
                nonce: 0,
                balance: U256::ZERO,
                code_hash: CHAIN_STATE_CODE_HASH,
            }),
        }
        if !has_code {
            self.code.push(CodeEntry {
                code_hash: CHAIN_STATE_CODE_HASH,
                code: bytes::Bytes::from_static(CHAIN_STATE_CODE),
            });
        }
        self.storage = features
            .ids()
            .iter()
            .map(|id| StorageChange {
                address: CHAIN_STATE,
                key: activation_slot(*id),
                value: U256::from(GENESIS_ACTIVATION_MS),
            })
            .collect();
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GenesisError {
    #[error("chain_id must be > 0")]
    ZeroChainId,
    #[error("duplicate alloc address: {0}")]
    DuplicateAlloc(Address),
    #[error("genesis_features needs the KardamomChainState code at the chain-state address")]
    ChainStateCode,
}

fn deserialize_balance<'de, D>(d: D) -> Result<U256, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(d)?;
    parse_u256(&s).map_err(serde::de::Error::custom)
}

fn deserialize_code<'de, D>(d: D) -> Result<Option<Bytes>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(d)?;
    if s.is_empty() || s.eq_ignore_ascii_case("0x") {
        Ok(None)
    } else {
        parse_hex_bytes(&s)
            .map(Some)
            .map_err(serde::de::Error::custom)
    }
}

fn parse_u256(s: &str) -> Result<U256, String> {
    if let Some(stripped) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        U256::from_str_radix(stripped, 16).map_err(|e| format!("invalid hex balance `{s}`: {e}"))
    } else {
        U256::from_str_radix(s, 10).map_err(|e| format!("invalid decimal balance `{s}`: {e}"))
    }
}

fn parse_hex_bytes(s: &str) -> Result<Bytes, String> {
    let trimmed = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    let bytes = hex::decode(trimmed).map_err(|e| format!("invalid hex `{s}`: {e}"))?;
    Ok(Bytes::from(bytes))
}

#[cfg(test)]
#[path = "genesis_tests.rs"]
mod tests;
