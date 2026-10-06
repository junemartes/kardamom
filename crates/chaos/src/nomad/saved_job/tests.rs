use super::*;
use std::net::TcpListener;
use std::process::Stdio;

#[tokio::test]
#[ignore = "starts an isolated local Nomad dev agent; requires the nomad binary"]
async fn stopped_jobs_restore_their_definition_and_allocations() {
    let directory = tempfile::tempdir().unwrap();
    let port = || {
        TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    };
    let http = port();
    let config = directory.path().join("agent.hcl");
    std::fs::write(&config, format!(
        "ports {{ http = {http}\n rpc = {}\n serf = {} }}\nplugin \"raw_exec\" {{ config {{ enabled = true }} }}", port(), port()
    )).unwrap();
    let mut agent =
        tokio::process::Command::new(std::env::var("NOMAD_BIN").unwrap_or_else(|_| "nomad".into()))
            .args([
                "agent",
                "-dev",
                "-bind=127.0.0.1",
                "-config",
                config.to_str().unwrap(),
                "-data-dir",
                directory.path().join("data").to_str().unwrap(),
            ])
            .current_dir(directory.path())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
    let nomad = Nomad::new(&format!("http://127.0.0.1:{http}")).unwrap();
    let ready = poll::until(Budget::secs(30, 1), |_| async {
        Ok(nomad
            .read::<String>("/v1/status/leader")
            .await
            .ok()
            .filter(|s| !s.is_empty()))
    })
    .await
    .unwrap();
    ready
        .or_fail(|_| anyhow::anyhow!("dev agent unavailable"))
        .unwrap();
    let job = serde_json::json!({"ID":"audit-test","Name":"audit-test","Type":"service","Datacenters":["dc1"],"TaskGroups":[{
        "Name":"workers","Count":1,"Tasks":[{"Name":"sleep","Driver":"raw_exec","Config":{"command":"/bin/sleep","args":["300"]},"Resources":{"CPU":20,"MemoryMB":16}}]
    }]});
    nomad
        .http
        .post(nomad.url("/v1/jobs"))
        .json(&serde_json::json!({"Job":job}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let saved = SavedJob::capture(&nomad, "audit-test").await.unwrap();
    saved.restore().await.unwrap();
    saved.stop().await.unwrap();
    assert!(nomad.running("audit-test").await.unwrap().is_empty());
    saved.restore().await.unwrap();
    assert_eq!(nomad.running("audit-test").await.unwrap().len(), 1);
    saved.stop().await.unwrap();
    agent.kill().await.unwrap();
    agent.wait().await.unwrap();
}

#[test]
fn extra_args_reach_only_the_named_task() {
    let mut job = serde_json::json!({
        "TaskGroups": [{
            "Tasks": [
                {"Name": "da-watcher", "Config": {"args": ["--l1-rpc", "x"]}},
                {"Name": "sidecar", "Config": {"args": ["--a"]}},
            ],
        }],
    });
    let extra = ["--l1-resume-after".to_string(), "41".to_string()];

    assert_eq!(with_task_args(&mut job, "da-watcher", &extra), 1);
    assert_eq!(
        job["TaskGroups"][0]["Tasks"][0]["Config"]["args"],
        serde_json::json!(["--l1-rpc", "x", "--l1-resume-after", "41"])
    );
    assert_eq!(
        job["TaskGroups"][0]["Tasks"][1]["Config"]["args"],
        serde_json::json!(["--a"])
    );
    assert_eq!(with_task_args(&mut job, "absent", &extra), 0);
}
