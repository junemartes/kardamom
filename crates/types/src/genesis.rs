//! Genesis chain config: chain id plus initial account allocation.
//!
//! On disk this is kardamom-native TOML (see `chains/dev.toml`). The struct
//! derives `serde::Deserialize` directly. The only custom serde code is on
//! the string-parsed `balance` and `code` fields. `Genesis::validate` checks
//! chain id != 0 and duplicate alloc addresses. The TOML loader calls it
//! after parsing.

use alloc::{format, string::String, vec::Vec};

use alloy_primitives::{Address, Bytes, U256, hex};
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Genesis {
    pub chain_id: u64,
    #[serde(default)]
    pub alloc: Vec<AllocEntry>,
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
    /// Build the `(accounts, code)` allocation set for
    /// [`crate::AccountChange`]-based genesis seeding (`kardamom_state::seed_genesis`).
    ///
    /// Each [`AllocEntry`] becomes one [`crate::AccountChange`] with its
    /// declared balance and nonce. An empty-code account uses `code_hash =
    /// B256::ZERO`. This is the executor and validator convention; kardamom
    /// does not use the Ethereum `KECCAK_EMPTY` sentinel. Code bytes become
    /// a [`crate::delta::CodeEntry`]. This shared builder keeps the
    /// rebuild-from-L1 reconstructor's genesis identical, byte for byte, to
    /// the live executor's genesis, so their state roots match.
    #[must_use]
    pub fn to_alloc(&self) -> (Vec<crate::AccountChange>, Vec<crate::delta::CodeEntry>) {
        // One pass, one `code_hash()` per entry: the account and its code
        // entry (if any) both need the hash, so computing it here instead
        // of once per collection avoids hashing the same code twice.
        let (accounts, code): (Vec<_>, Vec<Option<_>>) = self
            .alloc
            .iter()
            .map(|entry| {
                let code_hash = entry.code_hash();
                let account = crate::AccountChange {
                    address: entry.address,
                    nonce: entry.nonce.unwrap_or(0),
                    balance: entry.balance,
                    code_hash,
                };
                let code_entry = entry.code.as_ref().map(|c| crate::delta::CodeEntry {
                    code_hash,
                    code: c.0.clone(),
                });
                (account, code_entry)
            })
            .unzip();
        (accounts, code.into_iter().flatten().collect())
    }

    /// Checks rules that the type and derive cannot express.
    ///
    /// # Errors
    ///
    /// Returns [`GenesisError::ZeroChainId`] if `chain_id` is 0, or
    /// [`GenesisError::DuplicateAlloc`] if two allocation entries share an
    /// address.
    pub fn validate(&self) -> Result<(), GenesisError> {
        if self.chain_id == 0 {
            return Err(GenesisError::ZeroChainId);
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
}

#[derive(Debug, thiserror::Error)]
pub enum GenesisError {
    #[error("chain_id must be > 0")]
    ZeroChainId,
    #[error("duplicate alloc address: {0}")]
    DuplicateAlloc(Address),
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
