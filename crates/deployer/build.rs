//! Compile the Solidity contracts under `<workspace>/contracts/` and emit a
//! Rust module that exposes the creation bytecode of each artifact the deployer
//! cares about via `include_bytes!`. After this script runs:
//!   * `<workspace>/contracts/out/<Name>.sol/<Name>.json` exists (forge-compatible).
//!   * `$OUT_DIR/embedded_artifacts.rs` exposes `KARDAMOM_FACTORY_V1_CREATION`,
//!     `ERC1967_PROXY_CREATION`, `ETH_LOCKBOX_CREATION`.
//!
//! Settings here mirror `contracts/foundry.toml`. The CI `bytecode-hash` job is
//! the canary for divergence between this build and `forge build`.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use foundry_compilers::artifacts::remappings::Remapping;
use foundry_compilers::artifacts::{BytecodeHash, Optimizer, Settings, SettingsMetadata};
use foundry_compilers::compilers::solc::{Solc, SolcCompiler, SolcLanguage, SolcSettings};
use foundry_compilers::{ConfigurableArtifacts, ProjectBuilder, ProjectPathsConfig};
use semver::Version;

/// Pin solc to match `contracts/foundry.toml`. Bumping here without bumping
/// there (or vice versa) breaks the `bytecode-hash` CI gate.
const SOLC_MAJOR: u64 = 0;
const SOLC_MINOR: u64 = 8;
const SOLC_PATCH: u64 = 26;
const OPTIMIZER_RUNS: usize = 200;

/// Artifacts to embed into the deployer binary as `pub const X: &[u8]`.
/// Each entry is `(rust_const_name, contract_name)`. The artifact file is found at
/// `contracts/out/<contract_name>.sol/<contract_name>.json`.
const EMBEDDED_ARTIFACTS: &[(&str, &str)] = &[
    ("KARDAMOM_FACTORY_V1_CREATION", "KardamomFactoryV1"),
    ("ERC1967_PROXY_CREATION", "ERC1967Proxy"),
    ("ETH_LOCKBOX_CREATION", "ETHLockbox"),
];

/// Solidity dependencies we need under `contracts/lib/`. `(dir_name, forge_install_spec)`.
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

fn main() -> Result<()> {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| anyhow!("workspace root not found from {}", manifest_dir.display()))?
        .to_path_buf();
    let contracts_root = workspace_root.join("contracts");

    ensure_lib_deps(&contracts_root)?;
    compile(&contracts_root)?;
    emit_embedded_module(&contracts_root)?;
    Ok(())
}

/// `forge install` any missing entries in `LIB_DEPS`. No-op if all present.
/// Requires `forge` on PATH only when at least one dep is missing.
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
    let status = std::process::Command::new("forge")
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

fn compile(contracts_root: &Path) -> Result<()> {
    let remappings_txt = contracts_root.join("remappings.txt");
    let remappings: Vec<Remapping> = std::fs::read_to_string(&remappings_txt)
        .with_context(|| format!("read {}", remappings_txt.display()))?
        .lines()
        .filter_map(|l| {
            let l = l.trim();
            (!l.is_empty() && !l.starts_with('#')).then(|| l.parse::<Remapping>())
        })
        .collect::<Result<_, _>>()
        .context("parse remappings.txt")?;

    let paths: ProjectPathsConfig<SolcLanguage> = ProjectPathsConfig::builder()
        .root(contracts_root)
        .sources(contracts_root.join("src"))
        .artifacts(contracts_root.join("out"))
        .lib(contracts_root.join("lib"))
        .remappings(remappings)
        .build()
        .context("build ProjectPathsConfig")?;

    let solc_version = Version::new(SOLC_MAJOR, SOLC_MINOR, SOLC_PATCH);
    let solc = Solc::find_or_install(&solc_version)
        .with_context(|| format!("find or install solc {solc_version}"))?;

    let settings = SolcSettings {
        settings: Settings {
            optimizer: Optimizer {
                enabled: Some(true),
                runs: Some(OPTIMIZER_RUNS),
                details: None,
            },
            metadata: Some(SettingsMetadata {
                use_literal_content: None,
                bytecode_hash: Some(BytecodeHash::None),
                cbor_metadata: None,
            }),
            ..Settings::default()
        },
        cli_settings: Default::default(),
    };

    let project = ProjectBuilder::<SolcCompiler, ConfigurableArtifacts>::new(
        ConfigurableArtifacts::default(),
    )
    .paths(paths)
    .settings(settings)
    .build(SolcCompiler::Specific(solc))
    .context("build Project")?;

    project.rerun_if_sources_changed();
    println!(
        "cargo:rerun-if-changed={}",
        contracts_root.join("remappings.txt").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        contracts_root.join("foundry.toml").display()
    );

    let output = project.compile().context("solc compile")?;
    if output.has_compiler_errors() {
        return Err(anyhow!("solc compilation errors:\n{output}"));
    }
    Ok(())
}

fn emit_embedded_module(contracts_root: &Path) -> Result<()> {
    let out_dir = PathBuf::from(std::env::var_os("OUT_DIR").expect("OUT_DIR set by cargo"));
    let out_file = out_dir.join("embedded_artifacts.rs");

    let mut body = String::new();
    for (const_name, contract_name) in EMBEDDED_ARTIFACTS {
        let artifact_path = contracts_root
            .join("out")
            .join(format!("{contract_name}.sol"))
            .join(format!("{contract_name}.json"));
        let bin_path = write_creation_bin(&artifact_path, &out_dir, contract_name)?;
        body.push_str(&format!(
            "pub const {const_name}: &[u8] = include_bytes!({:?});\n",
            bin_path
        ));
    }
    std::fs::write(&out_file, body).with_context(|| format!("write {}", out_file.display()))?;
    Ok(())
}

/// Reads `bytecode.object` (hex) from `artifact_path`, decodes to raw bytes, and
/// writes them to `<out_dir>/<contract>.bin`. Returns that path so build.rs can
/// `include_bytes!` it. Keeps the source-of-truth as the JSON artifact while
/// giving the include_bytes! a clean binary file (and avoids decoding hex at
/// runtime).
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
    let mut out = Vec::with_capacity(s.len() / 2);
    for pair in s.as_bytes().chunks(2) {
        let hi = hex_nibble(pair[0])?;
        let lo = hex_nibble(pair[1])?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn hex_nibble(c: u8) -> Result<u8> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(anyhow!("invalid hex char: {}", c as char)),
    }
}
