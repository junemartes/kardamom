//! Build script: invoke `forge build` so the e2e integration test can find compiled artifacts.
//! Gracefully skip if `forge` is not on PATH — the e2e test handles the missing-artifact case
//! itself.

use std::path::PathBuf;
use std::process::Command;

fn main() {
    // Track Solidity sources so cargo re-runs us on changes.
    let contracts_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("contracts");
    println!(
        "cargo:rerun-if-changed={}",
        contracts_root.join("src").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        contracts_root.join("foundry.toml").display()
    );

    // If forge isn't installed, just emit a warning and continue. The e2e test will skip.
    let forge_present = Command::new("forge")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);

    if !forge_present {
        println!(
            "cargo:warning=forge not found on PATH; skipping `forge build`. e2e deposit test will skip."
        );
        return;
    }

    let status = Command::new("forge")
        .arg("build")
        .current_dir(&contracts_root)
        .status();

    match status {
        Ok(s) if s.success() => {}
        Ok(s) => {
            println!("cargo:warning=forge build exited with status {s}. e2e deposit test may fail.")
        }
        Err(e) => println!("cargo:warning=could not run forge build: {e}"),
    }
}
