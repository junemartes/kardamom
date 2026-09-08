//! Sets `rerun-if-changed` triggers for the forge artifact used by this
//! crate's `sol!` macro.
//!
//! `kardamom-deployer`'s build.rs runs the actual `forge build`. This crate
//! depends on `kardamom-deployer`, so cargo builds it first and populates
//! `contracts/out/` before this crate compiles. Do not run forge here. It
//! would race with the parallel `forge install` step in the other build
//! script.

use std::path::{Path, PathBuf};

#[path = "../deployer/build_support/sol_watch.rs"]
mod sol_watch;

fn main() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let contracts_root = workspace_root.join("contracts");

    sol_watch::emit_sol_rerun_triggers(&contracts_root);
}
