use super::*;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

struct Peer {
    address: SocketAddr,
    task: tokio::task::JoinHandle<()>,
}

impl Peer {
    async fn start(reply: Option<String>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut stream = BufReader::new(socket);
            while let Some(command) = Self::command(&mut stream).await {
                if command[0] == "CLIENT" {
                    stream.get_mut().write_all(b"+OK\r\n").await.unwrap();
                } else if let Some(reply) = &reply {
                    stream.get_mut().write_all(reply.as_bytes()).await.unwrap();
                } else {
                    std::future::pending::<()>().await;
                }
            }
        });
        Self { address, task }
    }

    async fn command(stream: &mut BufReader<tokio::net::TcpStream>) -> Option<Vec<String>> {
        let mut line = String::new();
        if stream.read_line(&mut line).await.ok()? == 0 {
            return None;
        }
        let count: usize = line.trim().strip_prefix('*')?.parse().ok()?;
        let mut command = Vec::new();
        for _ in 0..count {
            line.clear();
            stream.read_line(&mut line).await.ok()?;
            let len: usize = line.trim().strip_prefix('$')?.parse().ok()?;
            let mut bytes = vec![0; len + 2];
            stream.read_exact(&mut bytes).await.ok()?;
            command.push(String::from_utf8(bytes[..len].to_vec()).unwrap());
        }
        Some(command)
    }

    async fn sentinel(host: &str, port: u16) -> Self {
        let port = port.to_string();
        Self::start(Some(format!(
            "*2\r\n${}\r\n{host}\r\n${}\r\n{port}\r\n",
            host.len(),
            port.len()
        )))
        .await
    }

    fn probe(&self) -> Probe {
        let contract = serde_json::from_value(serde_json::json!({
            "version": 1,
            "network": {"name":"test", "bridge":"test", "subnet":"127.0.0.0/8"},
            "nodes": {"primary": {
                "name":"primary", "container":"test-primary", "role":"aux",
                "tier":"worker", "index":0, "control_plane":false, "ip":"127.0.0.1"
            }}
        }))
        .unwrap();
        Probe {
            sentinel: self.address,
            contract,
        }
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn a_hanging_role_and_a_hanging_sentinel_do_not_hide_a_live_primary() {
    let frozen = Peer::start(None).await;
    let stale = Peer::sentinel("primary.node.dc1.consul", frozen.address.port()).await;
    let dark = Peer::start(None).await;
    let live = Peer::start(Some("*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n".into())).await;
    let current = Peer::sentinel("primary.node.dc1.consul", live.address.port()).await;
    let mut tasks = JoinSet::new();
    for peer in [&stale, &dark, &current] {
        tasks.spawn(peer.probe().bounded());
    }
    let found = tokio::time::timeout(Duration::from_secs(1), Discovery::first_live(tasks))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.as_deref(), Some("primary.node.dc1.consul"));
}

#[tokio::test]
async fn a_sentinel_cannot_certify_a_demoted_primary() {
    let replica = Peer::start(Some("*1\r\n$5\r\nslave\r\n".into())).await;
    let sentinel = Peer::sentinel("primary", replica.address.port()).await;
    assert_eq!(sentinel.probe().bounded().await, None);
}

#[tokio::test]
async fn an_unknown_node_is_not_a_primary() {
    let sentinel = Peer::sentinel("unknown.node.dc1.consul", 6379).await;
    assert_eq!(sentinel.probe().bounded().await, None);
}

#[tokio::test]
async fn malformed_discovery_and_role_replies_do_not_pass() {
    let malformed = Peer::start(Some("*0\r\n".into())).await;
    assert_eq!(malformed.probe().bounded().await, None);
    let empty = Peer::start(Some("*0\r\n".into())).await;
    let sentinel = Peer::sentinel("primary", empty.address.port()).await;
    assert_eq!(sentinel.probe().bounded().await, None);
}

#[tokio::test]
async fn all_dark_sentinels_share_one_probe_budget() {
    let peers = [
        Peer::start(None).await,
        Peer::start(None).await,
        Peer::start(None).await,
    ];
    let mut tasks = JoinSet::new();
    for peer in &peers {
        tasks.spawn(peer.probe().bounded());
    }
    let found = tokio::time::timeout(
        PROBE_BUDGET + Duration::from_secs(1),
        Discovery::first_live(tasks),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(found, None);
}
