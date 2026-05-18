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
    pub seed: Option<u64>,
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
    pub seed: u64,
    pub output: Option<String>,
    pub mix: MixCfg,
    pub calls: Option<CallsCfg>,
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
        seed: None,
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
    let seed = cli.seed.or(f.seed).unwrap_or(0xC0FFEE);
    let output = cli.output.or(f.output);
    let mix = cli.mix.or(f.mix).unwrap_or_default();
    let calls = cli.calls.or(f.calls);

    if workload == Workload::Calls && calls.is_none() {
        anyhow::bail!("workload=`calls` requires a `[calls]` section with a `contract` address");
    }
    if workload == Workload::Mixed && calls.is_none() {
        anyhow::bail!("workload=`mixed` requires a `[calls]` section with a `contract` address");
    }

    Ok(Config {
        rpc,
        workload,
        rate,
        duration,
        concurrency,
        warmup,
        seed,
        output,
        mix,
        calls,
    })
}
