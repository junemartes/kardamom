//! Build a `kardamom_node::Genesis` from a bench `FileConfig`.
//!
//! Walks the `[mnemonic]` to produce `count` prefunded EOAs, then walks
//! `[[contracts]]` for code entries. The chain id is passed in by the
//! caller (the chain config / genesis file is the source of truth for
//! chain id; the bench config is the source of truth for accounts and
//! contracts).

use std::collections::BTreeMap;

use alloy_primitives::{Address, Bytes, U256, hex};
use kardamom_node::{AllocEntry, Genesis};

use crate::config::FileConfig;
use crate::mnemonic;

pub fn from_config(cfg: &FileConfig, chain_id: u64) -> anyhow::Result<Genesis> {
    let mnem = cfg.mnemonic.as_ref().ok_or_else(|| {
        anyhow::anyhow!("bench config has no [mnemonic] section")
    })?;
    let count = mnem
        .count
        .or(cfg.concurrency)
        .ok_or_else(|| anyhow::anyhow!("[mnemonic].count or top-level concurrency must be set"))?;
    let balance = parse_balance(&mnem.balance)
        .map_err(|e| anyhow::anyhow!("[mnemonic].balance: {e}"))?;

    let signers = mnemonic::derive_signers(&mnem.phrase, count)?;

    let mut alloc: BTreeMap<Address, AllocEntry> = BTreeMap::new();
    for s in &signers {
        alloc.insert(
            s.address,
            AllocEntry {
                balance,
                code: None,
                nonce: 0,
            },
        );
    }

    let signer_addrs: std::collections::HashSet<Address> = alloc.keys().copied().collect();

    for entry in &cfg.contracts {
        let code = parse_code(&entry.code)
            .map_err(|e| anyhow::anyhow!("[[contracts]] {}: code: {e}", entry.address))?;
        let entry_balance = match entry.balance.as_deref() {
            None => U256::ZERO,
            Some(s) => parse_balance(s)
                .map_err(|e| anyhow::anyhow!("[[contracts]] {}: balance: {e}", entry.address))?,
        };
        let nonce = entry.nonce.unwrap_or(1);
        let alloc_entry = AllocEntry {
            balance: entry_balance,
            code: Some(code),
            nonce,
        };
        if alloc.insert(entry.address, alloc_entry).is_some() {
            if signer_addrs.contains(&entry.address) {
                anyhow::bail!(
                    "address {} appears in both [mnemonic]-derived signers and [[contracts]]",
                    entry.address
                );
            } else {
                anyhow::bail!(
                    "address {} appears more than once in [[contracts]]",
                    entry.address
                );
            }
        }
    }

    Ok(Genesis { chain_id, alloc })
}

fn parse_balance(s: &str) -> Result<U256, String> {
    if let Some(stripped) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        U256::from_str_radix(stripped, 16)
            .map_err(|e| format!("invalid hex wei `{s}`: {e}"))
    } else {
        U256::from_str_radix(s, 10)
            .map_err(|e| format!("invalid decimal wei `{s}`: {e}"))
    }
}

fn parse_code(s: &str) -> Result<Bytes, String> {
    let trimmed = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    if trimmed.is_empty() {
        return Err("contract code must not be empty".to_string());
    }
    let bytes = hex::decode(trimmed).map_err(|e| format!("invalid hex `{s}`: {e}"))?;
    Ok(Bytes::from(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ContractEntry, FileConfig, MnemonicCfg};
    use alloy_primitives::address;

    const ANVIL_PHRASE: &str =
        "test test test test test test test test test test test junk";

    fn minimal_cfg(mnemonic: Option<MnemonicCfg>, contracts: Vec<ContractEntry>) -> FileConfig {
        FileConfig {
            rpc: None,
            workload: None,
            rate: None,
            duration: None,
            concurrency: Some(4),
            warmup: None,
            seed: None,
            output: None,
            mix: None,
            calls: None,
            mnemonic,
            contracts,
        }
    }

    fn anvil_mnemonic() -> MnemonicCfg {
        MnemonicCfg {
            phrase: ANVIL_PHRASE.to_string(),
            balance: "1000000000000000000000".to_string(),
            count: None,
        }
    }

    #[test]
    fn from_config_produces_concurrency_prefunded_eoas() {
        let cfg = minimal_cfg(Some(anvil_mnemonic()), vec![]);
        let g = from_config(&cfg, 1).expect("from_config");
        assert_eq!(g.chain_id, 1);
        assert_eq!(g.alloc.len(), 4);
        let expected = U256::from(1_000u64) * U256::from(10u64).pow(U256::from(18u64));
        let a0 = address!("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266");
        assert_eq!(g.alloc[&a0].balance, expected);
        assert!(g.alloc[&a0].code.is_none());
        assert_eq!(g.alloc[&a0].nonce, 0);
    }

    #[test]
    fn explicit_count_overrides_concurrency() {
        let mut m = anvil_mnemonic();
        m.count = Some(2);
        let cfg = minimal_cfg(Some(m), vec![]);
        let g = from_config(&cfg, 1).expect("from_config");
        assert_eq!(g.alloc.len(), 2);
    }

    #[test]
    fn contracts_become_alloc_entries() {
        let contract_addr = address!("0000000000000000000000000000000000001234");
        let cfg = minimal_cfg(
            Some(anvil_mnemonic()),
            vec![ContractEntry {
                address: contract_addr,
                code: "0x604260005260206000f3".to_string(),
                nonce: None,
                balance: None,
            }],
        );
        let g = from_config(&cfg, 1).expect("from_config");
        assert_eq!(g.alloc.len(), 5);
        let entry = &g.alloc[&contract_addr];
        assert!(entry.code.is_some());
        assert_eq!(entry.nonce, 1);
        assert_eq!(entry.balance, U256::ZERO);
    }

    #[test]
    fn contract_with_explicit_nonce_and_balance() {
        let contract_addr = address!("0000000000000000000000000000000000001234");
        let cfg = minimal_cfg(
            Some(anvil_mnemonic()),
            vec![ContractEntry {
                address: contract_addr,
                code: "0x60".to_string(),
                nonce: Some(7),
                balance: Some("0x100".to_string()),
            }],
        );
        let g = from_config(&cfg, 1).expect("from_config");
        assert_eq!(g.alloc[&contract_addr].nonce, 7);
        assert_eq!(g.alloc[&contract_addr].balance, U256::from(0x100u64));
    }

    #[test]
    fn missing_mnemonic_errors() {
        let cfg = minimal_cfg(None, vec![]);
        let err = from_config(&cfg, 1).expect_err("should fail");
        assert!(format!("{err:#}").contains("[mnemonic]"));
    }

    #[test]
    fn duplicate_contract_addresses_collapse_and_error() {
        let contract_addr = address!("0000000000000000000000000000000000001234");
        let cfg = minimal_cfg(
            Some(anvil_mnemonic()),
            vec![
                ContractEntry {
                    address: contract_addr,
                    code: "0x60".to_string(),
                    nonce: None,
                    balance: None,
                },
                ContractEntry {
                    address: contract_addr,
                    code: "0x61".to_string(),
                    nonce: None,
                    balance: None,
                },
            ],
        );
        let err = from_config(&cfg, 1).expect_err("should fail");
        let msg = format!("{err:#}");
        assert!(msg.contains(&format!("{contract_addr}")), "msg = {msg}");
        assert!(msg.contains("more than once in [[contracts]]"), "msg = {msg}");
        assert!(!msg.contains("mnemonic"), "should not mention mnemonic; msg = {msg}");
    }

    #[test]
    fn address_collision_between_signers_and_contracts_errors() {
        let a0 = address!("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266");
        let cfg = minimal_cfg(
            Some(anvil_mnemonic()),
            vec![ContractEntry {
                address: a0,
                code: "0x60".to_string(),
                nonce: None,
                balance: None,
            }],
        );
        let err = from_config(&cfg, 1).expect_err("should fail");
        assert!(format!("{err:#}").contains(&format!("{a0}")));
        assert!(format!("{err:#}").contains("[mnemonic]"));
    }

    #[test]
    fn empty_code_errors() {
        let contract_addr = address!("0000000000000000000000000000000000001234");
        let cfg = minimal_cfg(
            Some(anvil_mnemonic()),
            vec![ContractEntry {
                address: contract_addr,
                code: "0x".to_string(),
                nonce: None,
                balance: None,
            }],
        );
        let err = from_config(&cfg, 1).expect_err("should fail");
        assert!(format!("{err:#}").contains("code must not be empty"));
    }
}
