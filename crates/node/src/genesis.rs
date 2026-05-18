//! Genesis chain config: chain id plus initial account allocation.
//!
//! On disk this is kardamom-native TOML (see `chains/dev.toml`). In memory
//! it is a `BTreeMap<Address, AllocEntry>` for keyed lookup at `Node`
//! construction time. The conversion happens in this file's custom
//! `serde::Deserialize` impl.

use std::collections::BTreeMap;
use std::fmt;

use alloy_primitives::{Address, Bytes, U256, hex};
use serde::Deserialize;
use serde::de::{self, Deserializer, MapAccess, Visitor};

#[derive(Debug, Clone, PartialEq)]
pub struct Genesis {
    pub chain_id: u64,
    pub alloc: BTreeMap<Address, AllocEntry>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AllocEntry {
    pub balance: U256,
    pub code: Option<Bytes>,
    pub nonce: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAllocEntry {
    address: String,
    balance: Option<String>,
    code: Option<String>,
    nonce: Option<u64>,
}

impl<'de> Deserialize<'de> for Genesis {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct GenesisVisitor;

        impl<'de> Visitor<'de> for GenesisVisitor {
            type Value = Genesis;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a kardamom genesis table with `chain_id` and optional `alloc`")
            }

            fn visit_map<M>(self, mut map: M) -> Result<Genesis, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut chain_id: Option<u64> = None;
                let mut raw_alloc: Vec<RawAllocEntry> = Vec::new();
                let mut saw_alloc = false;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "chain_id" => {
                            if chain_id.is_some() {
                                return Err(de::Error::duplicate_field("chain_id"));
                            }
                            chain_id = Some(map.next_value()?);
                        }
                        "alloc" => {
                            if saw_alloc {
                                return Err(de::Error::duplicate_field("alloc"));
                            }
                            saw_alloc = true;
                            raw_alloc = map.next_value()?;
                        }
                        other => {
                            return Err(de::Error::unknown_field(other, &["chain_id", "alloc"]));
                        }
                    }
                }

                let chain_id = chain_id.ok_or_else(|| de::Error::missing_field("chain_id"))?;
                if chain_id == 0 {
                    return Err(de::Error::custom("chain_id must be > 0"));
                }

                let mut alloc: BTreeMap<Address, AllocEntry> = BTreeMap::new();
                for raw in raw_alloc {
                    let addr = parse_address(&raw.address).map_err(de::Error::custom)?;
                    let balance = match raw.balance.as_deref() {
                        None => U256::ZERO,
                        Some(s) => parse_u256(s).map_err(de::Error::custom)?,
                    };
                    let code = match raw.code.as_deref() {
                        None => None,
                        Some(s) if s.is_empty() || s.eq_ignore_ascii_case("0x") => None,
                        Some(s) => Some(parse_hex_bytes(s).map_err(de::Error::custom)?),
                    };
                    let nonce = raw.nonce.unwrap_or(if code.is_some() { 1 } else { 0 });
                    let entry = AllocEntry {
                        balance,
                        code,
                        nonce,
                    };
                    if alloc.insert(addr, entry).is_some() {
                        return Err(de::Error::custom(format!(
                            "duplicate alloc address: {addr}"
                        )));
                    }
                }

                Ok(Genesis { chain_id, alloc })
            }
        }

        deserializer.deserialize_map(GenesisVisitor)
    }
}

fn parse_address(s: &str) -> Result<Address, String> {
    s.parse::<Address>()
        .map_err(|e| format!("invalid address `{s}`: {e}"))
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
