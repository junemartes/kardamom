//! Sets `rerun-if-changed` triggers for the forge artifact used by this
//! crate's `sol!` macro.
//!
//! `kardamom-deployer`'s build.rs runs the actual `forge build`. This crate
//! depends on `kardamom-deployer`, so cargo builds it first and populates
//! `contracts/out/` before this crate compiles. Do not run forge here. It
//! would race with the parallel `forge install` step in the other build
//! script.

use std::path::{Path, PathBuf};

fn main() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let contracts_root = workspace_root.join("contracts");

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

fn walk_sol_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    walk_sol_files_into(dir, &mut out);
    out
}

fn walk_sol_files_into(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk_sol_files_into(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("sol") {
            out.push(path);
        }
    }
}
