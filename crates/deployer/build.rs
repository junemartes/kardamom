//! Run `forge build` to fill `<workspace>/contracts/out/`. Then write a Rust
//! module that exposes the creation bytecode of each artifact through
//! `include_bytes!`. After this script runs:
//!   * `<workspace>/contracts/out/<Name>.sol/<Name>.json` exists.
//!   * `$OUT_DIR/embedded_artifacts.rs` defines `KARDAMOM_FACTORY_V1_CREATION`,
//!     `ERC1967_PROXY_CREATION`, `ETH_LOCKBOX_CREATION`.
//!
//! Forge is the source of truth for contract compilation. This script only
//! runs forge and embeds the result. It needs `forge` on the PATH.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, anyhow};

#[path = "build_support/sol_watch.rs"]
mod sol_watch;

/// Solidity dependencies under `contracts/lib/`. `(dir_name, forge_install_spec)`.
const LIB_DEPS: &[(&str, &str)] = &[
    ("forge-std", "foundry-rs/forge-std"),
    (
        "openzeppelin-contracts",
        "openzeppelin/openzeppelin-contracts@v5.0.2",
    ),
    (
        "openzeppelin-contracts-upgradeable",
        "openzeppelin/openzeppelin-contracts-upgradeable@v5.0.2",
    ),
];

/// Artifacts to embed. `(rust_const_name, contract_name)`.
const EMBEDDED_ARTIFACTS: &[(&str, &str)] = &[
    ("KARDAMOM_FACTORY_V1_CREATION", "KardamomFactoryV1"),
    ("ERC1967_PROXY_CREATION", "ERC1967Proxy"),
    ("ETH_LOCKBOX_CREATION", "ETHLockbox"),
    ("KARDAMOM_L2_SETTLEMENT_CREATION", "KardamomL2Settlement"),
    ("KARDAMOM_PROOF_ORACLE_CREATION", "KardamomProofOracle"),
    (
        "WITHDRAWAL_OUTPUT_ORACLE_CREATION",
        "WithdrawalOutputOracle",
    ),
];

fn main() -> Result<()> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| anyhow!("workspace root not found from {}", manifest_dir.display()))?
        .to_path_buf();
    let contracts_root = workspace_root.join("contracts");

    sol_watch::emit_sol_rerun_triggers(&contracts_root);
    println!(
        "cargo:rerun-if-changed={}",
        contracts_root.join("remappings.txt").display()
    );

    ensure_lib_deps(&contracts_root)?;
    forge_build(&contracts_root)?;
    emit_embedded_module(&contracts_root)?;
    Ok(())
}

/// `forge install` any missing entries in `LIB_DEPS`. No-op if all present.
fn ensure_lib_deps(contracts_root: &Path) -> Result<()> {
    let lib = contracts_root.join("lib");
    let missing: Vec<&str> = LIB_DEPS
        .iter()
        .filter(|(dir, _)| !lib.join(dir).exists())
        .map(|(_, spec)| *spec)
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    let status = Command::new("forge")
        .arg("install")
        .arg("--no-git")
        .arg("--shallow")
        .args(&missing)
        .current_dir(contracts_root)
        .status()
        .map_err(|e| {
            anyhow!(
                "failed to spawn `forge install`: {e}. Install Foundry from https://getfoundry.sh \
                 or pre-populate contracts/lib/ with: {missing:?}"
            )
        })?;
    if !status.success() {
        return Err(anyhow!("`forge install` failed with status {status}"));
    }
    Ok(())
}

fn forge_build(contracts_root: &Path) -> Result<()> {
    let status = Command::new("forge")
        .arg("build")
        .current_dir(contracts_root)
        .status()
        .map_err(|e| {
            anyhow!(
                "failed to spawn `forge build`: {e}. Install Foundry from https://getfoundry.sh"
            )
        })?;
    if !status.success() {
        return Err(anyhow!("`forge build` failed with status {status}"));
    }
    Ok(())
}

fn emit_embedded_module(contracts_root: &Path) -> Result<()> {
    let out_dir = std::env::var_os("OUT_DIR")
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("OUT_DIR not set (cargo always sets it for build scripts)"))?;
    let out_file = out_dir.join("embedded_artifacts.rs");

    let mut body = String::new();
    for (const_name, contract_name) in EMBEDDED_ARTIFACTS {
        let artifact_path = contracts_root
            .join("out")
            .join(format!("{contract_name}.sol"))
            .join(format!("{contract_name}.json"));
        let bin_path = write_creation_bin(&artifact_path, &out_dir, contract_name)?;
        #[allow(
            clippy::unnecessary_debug_formatting,
            reason = "needs a quoted, escaped Rust string literal for include_bytes!, not a \
                      plain path string from .display()"
        )]
        let _ = writeln!(
            body,
            "pub const {const_name}: &[u8] = include_bytes!({bin_path:?});"
        );
    }
    std::fs::write(&out_file, body).with_context(|| format!("write {}", out_file.display()))?;
    Ok(())
}

/// Read `bytecode.object` (hex) from `artifact_path`. Decode it to raw bytes.
/// Write the bytes to `<out_dir>/<contract>.bin` and return that path for
/// `include_bytes!`. The JSON artifact stays the source of truth; the consts
/// hold only raw bytes.
fn write_creation_bin(
    artifact_path: &Path,
    out_dir: &Path,
    contract_name: &str,
) -> Result<PathBuf> {
    let raw = std::fs::read_to_string(artifact_path)
        .with_context(|| format!("read {}", artifact_path.display()))?;
    let v: serde_json::Value =
        serde_json::from_str(&raw).with_context(|| format!("parse {}", artifact_path.display()))?;
    let hex = v["bytecode"]["object"]
        .as_str()
        .ok_or_else(|| anyhow!("{}: missing bytecode.object", artifact_path.display()))?;
    let hex = hex.strip_prefix("0x").unwrap_or(hex);
    let bytes = hex_decode(hex)
        .with_context(|| format!("{}: invalid hex bytecode", artifact_path.display()))?;
    let bin_path = out_dir.join(format!("{contract_name}.bin"));
    std::fs::write(&bin_path, &bytes).with_context(|| format!("write {}", bin_path.display()))?;
    Ok(bin_path)
}

fn hex_decode(s: &str) -> Result<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return Err(anyhow!("odd-length hex string"));
    }
    s.as_bytes()
        .chunks(2)
        .map(|pair| Ok((hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?))
        .collect::<Result<Vec<u8>>>()
}

fn hex_nibble(c: u8) -> Result<u8> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(anyhow!("invalid hex char: {}", c as char)),
    }
}
