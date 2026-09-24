//! Shared `build.rs` helper: emit `cargo:rerun-if-changed` triggers for
//! every `.sol` file under `contracts/src`, plus `src` and
//! `foundry.toml`. Both `kardamom-deployer`'s and `kardamom-batcher`'s
//! build scripts use this (`#[path]`-included, not a crate dependency —
//! a `build.rs` has no stable place to depend on a sibling crate's
//! build-time code): `kardamom-batcher` depends on `kardamom-deployer`'s
//! `sol!` macro output, so both need to know when a contract changes.

use std::path::{Path, PathBuf};

/// Emit a `cargo:rerun-if-changed` line for every `.sol` file under
/// `contracts_root/src`, found recursively, plus `src` and
/// `foundry.toml` themselves. Cargo's `rerun-if-changed` on a directory
/// tracks only direct children, not subdirectories, so a new contract
/// under a subdirectory needs its own explicit trigger.
pub(crate) fn emit_sol_rerun_triggers(contracts_root: &Path) {
    for entry in walk_sol_files(&contracts_root.join("src")) {
        println!("cargo:rerun-if-changed={}", entry.display());
    }
    println!(
        "cargo:rerun-if-changed={}",
        contracts_root.join("src").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        contracts_root.join("foundry.toml").display()
    );
}

/// Every `*.sol` file under `dir`, found recursively.
fn walk_sol_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    walk_sol_files_into(dir, &mut out);
    out
}

fn walk_sol_files_into(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    entries
        .flatten()
        .for_each(|entry| visit_sol_entry(&entry.path(), out));
}

/// One `read_dir` entry: recurse into a directory, or record a `.sol`
/// file.
fn visit_sol_entry(path: &Path, out: &mut Vec<PathBuf>) {
    if path.is_dir() {
        walk_sol_files_into(path, out);
    } else if path.extension().and_then(|e| e.to_str()) == Some("sol") {
        out.push(path.to_path_buf());
    }
}
