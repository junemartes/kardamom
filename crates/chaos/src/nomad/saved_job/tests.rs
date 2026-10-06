use super::*;
use std::net::TcpListener;
use std::process::Stdio;

/// An isolated Nomad dev agent on free local ports, and a client of it.
/// The agent stops when the child drops.
async fn dev_agent() -> (tempfile::TempDir, tokio::process::Child, Nomad) {
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
    let agent =
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
    (directory, agent, nomad)
}

#[tokio::test]
#[ignore = "starts an isolated local Nomad dev agent; requires the nomad binary"]
async fn stopped_jobs_restore_their_definition_and_allocations() {
    let (_directory, mut agent, nomad) = dev_agent().await;
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
    let mut changed = saved.definition().clone();
    changed["TaskGroups"][0]["Tasks"][0]["Config"]["args"] = serde_json::json!(["301"]);
    saved.register(&changed).await.unwrap();
    let version = nomad.job("audit-test").await.unwrap().version;
    let running = nomad.running("audit-test").await.unwrap();
    assert_eq!(running.len(), 1);
    assert_eq!(running[0].job_version, version, "the changed version runs");
    saved.stop().await.unwrap();
    agent.kill().await.unwrap();
    agent.wait().await.unwrap();
}

#[tokio::test]
#[ignore = "starts an isolated local Nomad dev agent; requires the nomad binary"]
async fn a_job_variable_is_written_and_deleted() {
    let (_directory, mut agent, nomad) = dev_agent().await;
    let path = "nomad/jobs/cluster";
    // A new agent refuses a variable write until its keyring is ready.
    let written = poll::until(Budget::secs(30, 1), |_| async {
        Ok(nomad
            .put_variable(path, &serde_json::json!({ "bootstrap": "true" }))
            .await
            .ok())
    })
    .await
    .unwrap();
    written
        .or_fail(|_| anyhow::anyhow!("the variable write never succeeded"))
        .unwrap();
    let variable: serde_json::Value = nomad.read(&format!("/v1/var/{path}")).await.unwrap();
    assert_eq!(variable["Items"]["bootstrap"], "true");
    nomad.delete_variable(path).await.unwrap();
    assert!(
        nomad
            .read::<serde_json::Value>(&format!("/v1/var/{path}"))
            .await
            .is_err()
    );
    nomad.delete_variable(path).await.unwrap();
    agent.kill().await.unwrap();
    agent.wait().await.unwrap();
}
