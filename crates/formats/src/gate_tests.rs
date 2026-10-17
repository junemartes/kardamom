use std::path::{Path, PathBuf};

use super::*;

fn registry(dir: &Path, name: &str, versions: [u32; 3], shared: bool) -> PathBuf {
    let [writes, reads_min, reads_max] = versions;
    let path = dir.join(name);
    std::fs::write(
        &path,
        format!(
            "[format.spool]\nwrites = {writes}\nreads_min = {reads_min}\nreads_max = {reads_max}\nshared = {shared}\ncode = [\"a.rs#A\"]\n"
        ),
    )
    .unwrap();
    path
}

/// The findings of a target with `head` versions against an accepted
/// release with `base` versions, as the JSON that the deploy role reads.
fn findings_json(base: [u32; 3], head: [u32; 3], shared: bool) -> serde_json::Value {
    let dir = tempfile::tempdir().unwrap();
    let gate = FormatGate {
        base: registry(dir.path(), "accepted.toml", base, shared),
        root: dir.path().to_path_buf(),
    };
    registry(dir.path(), "formats.toml", head, shared);
    serde_json::to_value(gate.findings().unwrap()).unwrap()
}

#[test]
fn a_one_way_release_has_a_rollback_finding() {
    assert_eq!(
        findings_json([1, 1, 1], [2, 1, 2], false),
        serde_json::json!([{"rule": "rollback", "id": "spool", "head": 2, "base": 1}])
    );
}

#[test]
fn a_shared_one_way_release_is_also_a_mixed_fleet_finding() {
    let findings = findings_json([1, 1, 1], [2, 1, 2], true);
    let rules: Vec<&str> = findings
        .as_array()
        .unwrap()
        .iter()
        .map(|finding| finding["rule"].as_str().unwrap())
        .collect();
    assert_eq!(rules, ["rollback", "mixed_fleet"]);
}

#[test]
fn a_compatible_release_has_no_finding() {
    assert_eq!(
        findings_json([1, 1, 2], [2, 1, 2], false),
        serde_json::json!([])
    );
}

#[test]
fn a_missing_registry_is_an_error_that_names_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let gate = FormatGate {
        base: dir.path().join("absent.toml"),
        root: dir.path().to_path_buf(),
    };
    let error = gate.findings().unwrap_err().to_string();
    assert!(error.contains("absent.toml"), "{error}");
}
