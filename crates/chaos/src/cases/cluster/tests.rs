use super::*;

#[test]
fn the_catchup_block_counts_only_lines_after_the_last_fresh_start() {
    let logs = "sealer snapshot TAKEN memberId=1 block=90\n\
            sealer state FRESH at genesis memberId=1\n\
            sealer snapshot TAKEN memberId=1 block=30\n\
            sealer snapshot TAKEN memberId=2 block=120\n\
            sealer snapshot TAKEN memberId=1 block=60\n";
    assert_eq!(BlankStart::of(1).catchup_block(logs, 1), 60);
    assert_eq!(BlankStart::of(2).catchup_block(logs, 2), 0);
    let mut track = Catchup::default();
    track.observe(0, false);
    track.observe(30, true);
    assert!(!track.moved);
    assert!(track.blank);
    track.observe(60, true);
    assert!(track.moved);
}

#[test]
fn the_catchup_block_counts_only_lines_after_the_last_seed() {
    let logs = "sealer state FRESH at genesis memberId=1\n\
            sealer snapshot TAKEN memberId=1 block=90\n\
            cluster SEED from-peer memberId=1 snapshotPosition=4096 round=1\n\
            sealer snapshot RESTORED memberId=1 block=80 canonicalCount=7\n\
            sealer snapshot TAKEN memberId=1 block=95\n";
    assert_eq!(BlankStart::of(1).catchup_block(logs, 1), 95);
    let start = BlankStart::of(1);
    assert!(start.matches("cluster SEED from-peer memberId=1 snapshotPosition=-1 round=2"));
    assert!(!start.matches("cluster SEED from-peer memberId=10 snapshotPosition=-1 round=2"));
    assert!(!start.matches("cluster SEED waiting-for-peer memberId=1 round=1"));
}

#[test]
fn an_offline_copy_needs_a_new_resume_and_a_post_resume_snapshot() {
    let start = BlankStart::copied(1);
    let old =
        "cluster START mode=RESUME memberId=1 old\nsealer snapshot TAKEN memberId=1 block=800\n";
    let current = format!(
        "{old}cluster START mode=RESUME memberId=1 new\nsealer snapshot RESTORED memberId=1 block=750\n"
    );
    assert_eq!(start.catchup_block(&current, 1), 0);
    let caught_up = format!("{current}sealer snapshot TAKEN memberId=1 block=810\n");
    assert_eq!(start.catchup_block(&caught_up, 1), 810);
    assert!(!start.matches("cluster START mode=RESUME memberId=10 new"));
    assert!(!start.matches("cluster SEED from-peer memberId=1 snapshotPosition=99"));
    assert_eq!(BlankStart::of(1).catchup_block(&caught_up, 1), 0);
}
#[test]
fn only_a_new_start_in_the_same_allocation_certifies_a_snapshot() {
    let before = HashMap::from([("old".to_string(), 1)]);
    let observed = |old_count, new_count, new_block| {
        StartLogs(HashMap::from([
            (
                "old".to_string(),
                StartSample {
                    count: old_count,
                    block: 999,
                },
            ),
            (
                "new".to_string(),
                StartSample {
                    count: new_count,
                    block: new_block,
                },
            ),
        ]))
    };
    assert_eq!(observed(1, 0, 888).new_block(&before), None);
    assert_eq!(observed(1, 1, 0).new_block(&before), Some(0));
    assert_eq!(observed(1, 1, 810).new_block(&before), Some(810));
    assert_eq!(observed(2, 0, 888).new_block(&before), Some(999));
}

struct ReplacementAgent {
    old_status: &'static str,
    /// The status of the removed node: Nomad lists it as ready until its
    /// heartbeat grace ends.
    old_node: &'static str,
    live_status: u16,
    live_logs: &'static str,
}

impl ReplacementAgent {
    async fn read(self) -> anyhow::Result<StartLogs> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let nomad = Nomad::new(&format!("http://{}", listener.local_addr()?))?;
        let server = tokio::spawn(async move {
            loop {
                self.answer(&listener).await;
            }
        });
        let live = ["10.0.0.2".to_string()];
        let result = Blank::starts_by_alloc(&nomad, &live, 0, &BlankStart::of(0)).await;
        server.abort();
        result
    }

    async fn answer(&self, listener: &tokio::net::TcpListener) {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
        let (socket, _) = listener.accept().await.unwrap();
        let mut reader = tokio::io::BufReader::new(socket);
        let mut request = String::new();
        reader.read_line(&mut request).await.unwrap();
        let mut header = String::new();
        while reader.read_line(&mut header).await.unwrap() > 2 {
            header.clear();
        }
        let path = request.split_whitespace().nth(1).unwrap();
        let (status, body) = self.response(path.split('?').next().unwrap());
        reader.get_mut().write_all(format!(
            "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len(),
        ).as_bytes()).await.unwrap();
    }

    fn response(&self, path: &str) -> (u16, String) {
        match path {
            "/v1/nodes" => (
                200,
                serde_json::json!([
                    {"ID":"removed-node", "Name":"sealer-0", "Status":self.old_node, "Address":"10.0.0.1"},
                    {"ID":"replacement-node", "Name":"sealer-0", "Status":"ready", "Address":"10.0.0.2"}
                ])
                .to_string(),
            ),
            "/v1/job/cluster/allocations" => (
                200,
                serde_json::json!([
                    Self::allocation("old", "removed-node", self.old_status),
                    Self::allocation("new", "replacement-node", "running")
                ])
                .to_string(),
            ),
            "/v1/client/fs/logs/new" => (self.live_status, self.live_logs.to_string()),
            _ => (
                400,
                format!("unexpected request to unavailable evidence: {path}"),
            ),
        }
    }

    fn allocation(id: &str, node: &str, status: &str) -> serde_json::Value {
        serde_json::json!({
            "ID": id, "NodeID": node, "NodeName": "sealer-0", "TaskGroup": "cluster",
            "ClientStatus": status, "DesiredStatus": "run", "JobVersion": 1,
            "TaskStates": {"cluster": {"State": "running"}}
        })
    }
}

#[tokio::test]
async fn replacement_catchup_ignores_old_allocations_by_node_identity() {
    // The removed node is down, or still ready inside its heartbeat grace
    // but gone from the contract.
    for (old_status, old_node) in ["running", "complete", "failed", "lost"]
        .into_iter()
        .flat_map(|s| [(s, "down"), (s, "ready")])
    {
        let logs = ReplacementAgent {
            old_status,
            old_node,
            live_status: 200,
            live_logs: "sealer state FRESH at genesis memberId=0\nsealer snapshot TAKEN memberId=0 block=969\n",
        }.read().await.unwrap();
        assert_eq!(
            logs.new_block(&HashMap::from([("old".to_string(), 1)])),
            Some(969)
        );
        assert_eq!(logs.counts(), HashMap::from([("new".to_string(), 1)]));
    }
}

#[tokio::test]
async fn replacement_catchup_requires_live_start_and_snapshot_evidence() {
    for (live_logs, expected) in [
        ("", None),
        ("sealer snapshot TAKEN memberId=0 block=969\n", None),
        ("sealer state FRESH at genesis memberId=0\n", Some(0)),
    ] {
        let logs = ReplacementAgent {
            old_status: "complete",
            old_node: "down",
            live_status: 200,
            live_logs,
        }
        .read()
        .await
        .unwrap();
        assert_eq!(logs.new_block(&HashMap::new()), expected);
    }
}

#[tokio::test]
async fn replacement_catchup_keeps_live_log_failures_fatal() {
    let error = ReplacementAgent {
        old_status: "complete",
        old_node: "down",
        live_status: 400,
        live_logs: "live log unavailable",
    }
    .read()
    .await
    .err()
    .unwrap();
    let error = format!("{error:#}");
    assert!(error.contains("(400 "), "{error}");
    assert!(error.contains("/v1/client/fs/logs/new"), "{error}");
}
