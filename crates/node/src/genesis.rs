//! Genesis chain config: chain id plus initial account allocation.
//!
//! On disk this is kardamom-native TOML (see `chains/dev.toml`). The struct
//! derives `serde::Deserialize` directly; the only custom serde code lives
//! on the `balance` and `code` string-parsed fields. Semantic validation
//! (chain id != 0, duplicate alloc addresses) happens in `Genesis::validate`,
//! which the TOML loader calls after parsing.

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
    pub nonce: Option<u64>,
}

impl Genesis {
    /// Semantic validation that can't be expressed in the type or derive.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.chain_id == 0 {
            anyhow::bail!("chain_id must be > 0");
        }
        let mut seen = std::collections::BTreeSet::new();
        for entry in &self.alloc {
            if !seen.insert(entry.address) {
                anyhow::bail!("duplicate alloc address: {}", entry.address);
            }
        }
        Ok(())
    }
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
