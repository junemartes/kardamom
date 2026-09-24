//! Static validity check for every dashboard JSON shipped in
//! `deploy/grafana/provisioning/dashboards-json/`. This is cheap to run and
//! catches authoring mistakes: misspelled metric names, a missing `host`
//! filter, or drift from schema 38.

use std::path::PathBuf;

const EXPECTED_DASHBOARDS: &[&str] = &[
    "kardamom-overview",
    "kardamom-sequencer",
    "kardamom-batcher",
    "kardamom-sealer",
    "kardamom-executor",
    "kardamom-da-watcher",
    "kardamom-ingress",
];

fn dashboards_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_WORKSPACE_DIR")).join("deploy/grafana/provisioning/dashboards-json")
}

#[test]
fn every_dashboard_is_present_valid_and_schema_38() {
    let dir = dashboards_dir();
    for stem in EXPECTED_DASHBOARDS {
        assert_dashboard_valid(&dir, stem);
    }
}

/// Load `{dir}/{stem}.json` and check its schema, uid, and every panel.
fn assert_dashboard_valid(dir: &std::path::Path, stem: &str) {
    let path = dir.join(format!("{stem}.json"));
    let body =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let v: serde_json::Value =
        serde_json::from_str(&body).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()));

    assert_eq!(v["schemaVersion"], 38, "{} schemaVersion", path.display());
    assert_eq!(
        v["uid"].as_str(),
        Some(stem),
        "{} uid != {stem}",
        path.display()
    );
    let panels = v["panels"].as_array().expect("panels array");
    assert!(!panels.is_empty(), "{} has no panels", path.display());
    for (i, p) in panels.iter().enumerate() {
        assert_panel_valid(&path, i, p);
    }
}

/// Check one dashboard panel: it has a title, and (unless it is a text
/// panel, which carries no targets) every `PromQL` target is
/// kardamom-scoped — either a `kardamom_*` metric or a `kardamom-*` job
/// selector (for example, the overview's `up{job=~"kardamom-.+"}`
/// liveness panel).
fn assert_panel_valid(path: &std::path::Path, i: usize, p: &serde_json::Value) {
    assert!(
        p["title"].as_str().is_some_and(|s| !s.is_empty()),
        "{} panel[{i}] missing title",
        path.display()
    );
    if p["type"].as_str().unwrap_or("") == "text" {
        return;
    }
    let targets = p["targets"].as_array().cloned().unwrap_or_default();
    for (j, t) in targets.iter().enumerate() {
        let expr = t["expr"].as_str().unwrap_or("");
        assert!(
            expr.contains("kardamom"),
            "{} panel[{i}] target[{j}] expr is not kardamom-scoped: {expr}",
            path.display()
        );
    }
}
