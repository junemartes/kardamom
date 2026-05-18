//! Bench harness configuration. TOML file is the base, CLI flags override.

use std::path::Path;
use std::time::Duration;

use alloy_primitives::Address;
use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Workload {
    Transfers,
    Calls,
    Mixed,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MixCfg {
    pub transfers: u32,
    pub calls: u32,
}

impl Default for MixCfg {
    fn default() -> Self {
        Self {
            transfers: 1,
            calls: 4,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallsCfg {
    /// Address of the contract the bench will eth_call.
    pub contract: Address,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MnemonicCfg {
    pub phrase: String,
    pub balance: String,
    pub count: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContractEntry {
    pub address: Address,
    pub code: String,
    pub nonce: Option<u64>,
    pub balance: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileConfig {
    pub rpc: Option<String>,
    pub workload: Option<Workload>,
    pub rate: Option<u32>,
    #[serde(default, with = "humantime_serde::option")]
    pub duration: Option<Duration>,
    pub concurrency: Option<u32>,
    #[serde(default, with = "humantime_serde::option")]
    pub warmup: Option<Duration>,
    pub output: Option<String>,
    #[serde(default)]
    pub mix: Option<MixCfg>,
    #[serde(default)]
    pub calls: Option<CallsCfg>,
    #[serde(default)]
    pub mnemonic: Option<MnemonicCfg>,
    #[serde(default)]
    pub contracts: Vec<ContractEntry>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub rpc: String,
    pub workload: Workload,
    pub rate: u32,
    pub duration: Duration,
    pub concurrency: u32,
    pub warmup: Duration,
    pub output: Option<String>,
    pub mix: MixCfg,
    pub calls: Option<CallsCfg>,
    pub mnemonic: MnemonicCfg,
    pub contracts: Vec<ContractEntry>,
}

impl FileConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("reading {}: {e}", path.display()))?;
        toml::from_str(&text).map_err(|e| anyhow::anyhow!("parsing {}: {e}", path.display()))
    }
}

/// Merge a file config with CLI overrides into the final, fully-resolved
/// `Config`. Each CLI option wins if `Some`. Defaults fill in anything that
/// remains unset.
pub fn resolve(file: Option<FileConfig>, cli: FileConfig) -> anyhow::Result<Config> {
    let f = file.unwrap_or(FileConfig {
        rpc: None,
        workload: None,
        rate: None,
        duration: None,
        concurrency: None,
        warmup: None,
        output: None,
        mix: None,
        calls: None,
        mnemonic: None,
        contracts: vec![],
    });

    let rpc = cli
        .rpc
        .or(f.rpc)
        .ok_or_else(|| anyhow::anyhow!("`--rpc` is required (or set it in the config file)"))?;
    let workload = cli.workload.or(f.workload).unwrap_or(Workload::Mixed);
    let rate = cli.rate.or(f.rate).unwrap_or(100);
    let duration = cli
        .duration
        .or(f.duration)
        .unwrap_or(Duration::from_secs(10));
    let concurrency = cli.concurrency.or(f.concurrency).unwrap_or(16);
    let warmup = cli.warmup.or(f.warmup).unwrap_or(Duration::from_secs(2));
    let output = cli.output.or(f.output);
    let mix = cli.mix.or(f.mix).unwrap_or_default();
    let calls = cli.calls.or(f.calls);

    if workload == Workload::Calls && calls.is_none() {
        anyhow::bail!("workload=`calls` requires a `[calls]` section with a `contract` address");
    }
    if workload == Workload::Mixed && calls.is_none() {
        anyhow::bail!("workload=`mixed` requires a `[calls]` section with a `contract` address");
    }

    let mnemonic = cli
        .mnemonic
        .or(f.mnemonic)
        .ok_or_else(|| anyhow::anyhow!("[mnemonic] is required in the bench config"))?;

    let count = mnemonic.count.unwrap_or(concurrency);
    if count > 1000 {
        anyhow::bail!("[mnemonic].count = {count} exceeds cap of 1000 (likely a typo)");
    }

    let contracts = if !cli.contracts.is_empty() {
        cli.contracts
    } else {
        f.contracts
    };

    // No duplicate addresses across [[contracts]] entries.
    {
        let mut seen = std::collections::BTreeSet::new();
        for c in &contracts {
            if !seen.insert(c.address) {
                anyhow::bail!("[[contracts]] has duplicate address: {}", c.address);
            }
        }
    }

    // Cross-check: [calls.contract] must be in [[contracts]] for calls/mixed workloads.
    if matches!(workload, Workload::Calls | Workload::Mixed) {
        let calls_cfg = calls.as_ref().expect("validated above");
        let listed = contracts.iter().any(|c| c.address == calls_cfg.contract);
        if !listed {
            anyhow::bail!(
                "[calls.contract] = {} is not listed in [[contracts]]; \
                 add an entry with that address and its bytecode",
                calls_cfg.contract
            );
        }
    }

    Ok(Config {
        rpc,
        workload,
        rate,
        duration,
        concurrency,
        warmup,
        output,
        mix,
        calls,
        mnemonic,
        contracts,
    })
}

#[cfg(test)]
mod resolve_tests {
    use super::*;

    fn empty_file() -> FileConfig {
        FileConfig {
            rpc: Some("http://x".to_string()),
            workload: Some(Workload::Transfers),
            rate: None,
            duration: None,
            concurrency: None,
            warmup: None,
            output: None,
            mix: None,
            calls: None,
            mnemonic: None,
            contracts: vec![],
        }
    }

    fn anvil_mnemonic_cfg() -> MnemonicCfg {
        MnemonicCfg {
            phrase: "test test test test test test test test test test test junk".to_string(),
            balance: "1000000000000000000000".to_string(),
            count: None,
        }
    }

    fn empty_cli() -> FileConfig {
        FileConfig {
            rpc: None,
            workload: None,
            rate: None,
            duration: None,
            concurrency: None,
            warmup: None,
            output: None,
            mix: None,
            calls: None,
            mnemonic: None,
            contracts: vec![],
        }
    }

    #[test]
    fn resolve_errors_without_mnemonic() {
        let err = resolve(Some(empty_file()), empty_cli()).expect_err("should fail");
        assert!(format!("{err:#}").contains("[mnemonic]"));
    }

    #[test]
    fn resolve_caps_count_at_1000() {
        let mut f = empty_file();
        let mut m = anvil_mnemonic_cfg();
        m.count = Some(1001);
        f.mnemonic = Some(m);
        let err = resolve(Some(f), empty_cli()).expect_err("should fail");
        assert!(format!("{err:#}").contains("1000"));
    }

    #[test]
    fn resolve_errors_when_calls_contract_not_in_contracts_list() {
        let mut f = empty_file();
        f.workload = Some(Workload::Calls);
        f.mnemonic = Some(anvil_mnemonic_cfg());
        f.calls = Some(CallsCfg {
            contract: alloy_primitives::address!("0000000000000000000000000000000000001234"),
        });
        // contracts intentionally empty
        let err = resolve(Some(f), empty_cli()).expect_err("should fail");
        let msg = format!("{err:#}");
        assert!(msg.contains("not listed in [[contracts]]"), "msg = {msg}");
    }

    #[test]
    fn resolve_accepts_calls_workload_when_contract_listed() {
        let contract = alloy_primitives::address!("0000000000000000000000000000000000001234");
        let mut f = empty_file();
        f.workload = Some(Workload::Calls);
        f.mnemonic = Some(anvil_mnemonic_cfg());
        f.calls = Some(CallsCfg { contract });
        f.contracts = vec![ContractEntry {
            address: contract,
            code: "0x60".to_string(),
            nonce: None,
            balance: None,
        }];
        let cfg = resolve(Some(f), empty_cli()).expect("ok");
        assert_eq!(cfg.contracts.len(), 1);
    }

    #[test]
    fn resolve_rejects_duplicate_contracts() {
        let contract = alloy_primitives::address!("0000000000000000000000000000000000001234");
        let mut f = empty_file();
        f.mnemonic = Some(anvil_mnemonic_cfg());
        f.contracts = vec![
            ContractEntry {
                address: contract,
                code: "0x60".to_string(),
                nonce: None,
                balance: None,
            },
            ContractEntry {
                address: contract,
                code: "0x61".to_string(),
                nonce: None,
                balance: None,
            },
        ];
        let err = resolve(Some(f), empty_cli()).expect_err("should fail");
        let msg = format!("{err:#}");
        assert!(msg.contains("duplicate address"), "msg = {msg}");
        assert!(msg.contains(&format!("{contract}")), "msg = {msg}");
    }
}
