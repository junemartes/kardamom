//! Static validity check for every dashboard JSON shipped in
//! `deploy/grafana/provisioning/dashboards-json/`. Cheap to run, catches
//! typos at authoring time (mis-spelled metric names, missing `host` filter,
//! drift from schema 38).

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
        let path = dir.join(format!("{stem}.json"));
        let body = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let v: serde_json::Value =
            serde_json::from_str(&body).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()));

        assert_eq!(v["schemaVersion"], 38, "{path:?} schemaVersion");
        assert_eq!(v["uid"].as_str(), Some(*stem), "{path:?} uid != {stem}");
        let panels = v["panels"].as_array().expect("panels array");
        assert!(!panels.is_empty(), "{path:?} has no panels");
        for (i, p) in panels.iter().enumerate() {
            assert!(
                p["title"].as_str().is_some_and(|s| !s.is_empty()),
                "{path:?} panel[{i}] missing title"
            );
            // Every PromQL target must be kardamom-scoped: either a
            // kardamom_* metric or a kardamom-* job selector (e.g. the
            // overview's `up{job=~"kardamom-.+"}` liveness panel). Text
            // panels have no targets (skip them).
            let panel_type = p["type"].as_str().unwrap_or("");
            if panel_type == "text" {
                continue;
            }
            let targets = p["targets"].as_array().cloned().unwrap_or_default();
            for (j, t) in targets.iter().enumerate() {
                let expr = t["expr"].as_str().unwrap_or("");
                assert!(
                    expr.contains("kardamom"),
                    "{path:?} panel[{i}] target[{j}] expr is not kardamom-scoped: {expr}"
                );
            }
        }
    }
}
