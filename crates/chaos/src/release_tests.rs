use super::*;

fn registry(dir: &std::path::Path, name: &str, versions: [u32; 3], shared: bool) -> PathBuf {
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

#[test]
fn a_one_way_release_prints_a_rollback_finding() {
    let dir = tempfile::tempdir().unwrap();
    let base = registry(dir.path(), "accepted.toml", [1, 1, 1], false);
    registry(dir.path(), "formats.toml", [2, 1, 2], false);
    let gate = FormatGate {
        base,
        root: dir.path().to_path_buf(),
    };
    let findings: serde_json::Value = serde_json::from_str(&gate.findings_json().unwrap()).unwrap();
    assert_eq!(
        findings,
        serde_json::json!([{"rule": "rollback", "id": "spool", "head": 2, "base": 1}])
    );
}

#[test]
fn a_shared_one_way_release_is_also_a_mixed_fleet_finding() {
    let dir = tempfile::tempdir().unwrap();
    let base = registry(dir.path(), "accepted.toml", [1, 1, 1], true);
    registry(dir.path(), "formats.toml", [2, 1, 2], true);
    let gate = FormatGate {
        base,
        root: dir.path().to_path_buf(),
    };
    let findings: serde_json::Value = serde_json::from_str(&gate.findings_json().unwrap()).unwrap();
    let rules: Vec<&str> = findings
        .as_array()
        .unwrap()
        .iter()
        .map(|finding| finding["rule"].as_str().unwrap())
        .collect();
    assert_eq!(rules, ["rollback", "mixed_fleet"]);
}

#[test]
fn a_compatible_release_prints_an_empty_array() {
    let dir = tempfile::tempdir().unwrap();
    let base = registry(dir.path(), "accepted.toml", [1, 1, 2], false);
    registry(dir.path(), "formats.toml", [2, 1, 2], false);
    let gate = FormatGate {
        base,
        root: dir.path().to_path_buf(),
    };
    assert_eq!(gate.findings_json().unwrap(), "[]");
}

#[test]
fn a_missing_registry_is_an_error_that_names_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let gate = FormatGate {
        base: dir.path().join("absent.toml"),
        root: dir.path().to_path_buf(),
    };
    let error = gate.findings_json().unwrap_err().to_string();
    assert!(error.contains("absent.toml"), "{error}");
}
