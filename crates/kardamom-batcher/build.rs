//! Ensure forge has built the contracts before the `sol!` macro tries to load
//! `KardamomL2Settlement.sol/KardamomL2Settlement.json` at compile time.
//!
//! kardamom-deployer already runs `forge build`; we re-run it here so this
//! crate is self-sufficient (no dependency on the deployer's build order). The
//! second run is a no-op when nothing changed.

use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let contracts_root = workspace_root.join("contracts");

    println!(
        "cargo:rerun-if-changed={}",
        contracts_root.join("src").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        contracts_root.join("foundry.toml").display()
    );

    // If the artifact already exists, skip. The deployer's build.rs is the
    // canonical builder; this is just a safety net for batcher-only builds.
    let artifact = contracts_root
        .join("out")
        .join("KardamomL2Settlement.sol")
        .join("KardamomL2Settlement.json");
    if artifact.exists() {
        return;
    }

    let status = Command::new("forge")
        .arg("build")
        .arg("--silent")
        .current_dir(&contracts_root)
        .status();

    match status {
        Ok(s) if s.success() => {}
        Ok(s) => println!("cargo:warning=forge build exited with {s:?}"),
        Err(e) => println!("cargo:warning=forge build failed to spawn: {e}"),
    }
}
