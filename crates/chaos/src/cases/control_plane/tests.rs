use std::net::Ipv4Addr;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinSet;

use super::*;
use crate::contract::{Network, Node, NodeContract};
use crate::knobs::Knobs;
use crate::lifecycle::Lifecycle;
use crate::nomad::Nomad;

/// Real HTTP endpoints keep allocation state separate from RPC readiness.
struct Recovery {
    servers: JoinSet<()>,
    ip: Ipv4Addr,
}

impl Recovery {
    async fn new() -> (Self, Harness) {
        let mut fixture = Self {
            servers: JoinSet::new(),
            ip: Ipv4Addr::new(127, 91, 54, 1),
        };
        let nomad = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}", nomad.local_addr().unwrap());
        fixture.serve(nomad, Endpoint::Nomad);
        fixture.start(9006, Endpoint::Exporter).await;
        fixture.start(8545, Endpoint::Rpc).await;
        let mut h = Harness::new(
            fixture.contract(),
            Knobs::read(&[]).unwrap(),
            Lifecycle::in_workspace(),
        )
        .unwrap();
        h.nomad = Nomad::new(&address).unwrap();
        h.knobs.reschedule_slo = Duration::from_secs(10);
        fixture.ip = Ipv4Addr::new(127, 91, 54, 2);
        fixture.start(9006, Endpoint::Exporter).await;
        (fixture, h)
    }

    fn contract(&self) -> NodeContract {
        let nodes = [
            ("control", 0, self.ip),
            ("aux", 0, self.ip),
            ("ingress", 0, self.ip),
            ("ingress", 1, Ipv4Addr::new(127, 91, 54, 2)),
        ]
        .map(|(role, index, ip)| {
            let name = format!("{role}-{index}");
            let node = Node {
                name: name.clone(),
                container: format!("readiness-test-{name}"),
                role: role.to_string(),
                tier: "worker".to_string(),
                index,
                control_plane: role == "control",
                ip,
                generation: 0,
            };
            (name, node)
        });
        NodeContract {
            version: 1,
            network: Network {
                name: "loopback".to_string(),
                bridge: "lo".to_string(),
                subnet: "127.0.0.0/8".to_string(),
            },
            nodes: nodes.into_iter().collect(),
        }
    }

    async fn start(&mut self, port: u16, endpoint: Endpoint) {
        let listener = TcpListener::bind((self.ip, port)).await.unwrap();
        self.serve(listener, endpoint);
    }

    fn serve(&mut self, listener: TcpListener, endpoint: Endpoint) {
        self.servers.spawn(async move {
            loop {
                endpoint.answer(&listener).await;
            }
        });
    }
}

#[derive(Clone, Copy)]
enum Endpoint {
    Nomad,
    Exporter,
    Rpc,
}

impl Endpoint {
    async fn answer(self, listener: &TcpListener) {
        let (socket, _) = listener.accept().await.unwrap();
        let mut reader = tokio::io::BufReader::new(socket);
        let mut request = String::new();
        reader.read_line(&mut request).await.unwrap();
        let mut length = 0;
        while let Some(header) = Self::header(&mut reader).await {
            length = Self::content_length(&header).unwrap_or(length);
        }
        let mut payload = vec![0; length];
        reader.read_exact(&mut payload).await.unwrap();
        let body = self.response(request.split_whitespace().nth(1).unwrap());
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        reader
            .get_mut()
            .write_all(response.as_bytes())
            .await
            .unwrap();
    }

    async fn header(reader: &mut tokio::io::BufReader<tokio::net::TcpStream>) -> Option<String> {
        let mut header = String::new();
        reader.read_line(&mut header).await.unwrap();
        (header != "\r\n").then_some(header)
    }

    fn content_length(header: &str) -> Option<usize> {
        let (key, value) = header.split_once(':')?;
        key.eq_ignore_ascii_case("content-length")
            .then(|| value.trim().parse().unwrap())
    }

    fn response(self, path: &str) -> String {
        match self {
            Self::Nomad => Self::allocations(path),
            Self::Exporter => "kardamom_ingress_tx_received_total 1\n".to_string(),
            Self::Rpc => r#"{"jsonrpc":"2.0","id":1,"result":"0x539"}"#.to_string(),
        }
    }

    fn allocations(path: &str) -> String {
        let job = path
            .strip_prefix("/v1/job/")
            .unwrap()
            .strip_suffix("/allocations")
            .unwrap();
        let count = match job {
            "cluster" | "executor" => 3,
            "sequencer" => 4,
            "ingress" => 2,
            _ => panic!("unexpected job {job}"),
        };
        let rows: Vec<_> = (0..count)
            .map(|index| {
                serde_json::json!({
                    "ID": format!("{job}-{index}"), "JobVersion": 1,
                    "TaskGroup": job, "ClientStatus": "running", "DesiredStatus": "run",
                    "NodeName": format!("{job}-{index}"), "NodeID": index.to_string(),
                    "TaskStates": {job: {"State": "running"}}
                })
            })
            .collect();
        serde_json::to_string(&rows).unwrap()
    }
}

#[tokio::test]
async fn running_allocations_wait_for_both_rpcs_within_the_reschedule_deadline() {
    let (mut fixture, mut h) = Recovery::new().await;
    h.knobs.reschedule_slo = Duration::from_millis(200);
    let error = ControlPlane::await_workers(&mut h).await.unwrap_err();
    assert!(error.to_string().contains("reschedule budget"), "{error:#}");

    h.knobs.reschedule_slo = Duration::from_secs(10);
    let recovery = ControlPlane::await_workers(&mut h);
    tokio::pin!(recovery);
    assert!(
        tokio::time::timeout(Duration::from_millis(200), &mut recovery)
            .await
            .is_err()
    );
    fixture.start(8545, Endpoint::Rpc).await;
    recovery.await.unwrap();
}

/// The hold outlasts the heartbeat grace of the shard's deploy, so the
/// servers mark the isolated clients down before the hold ends.
#[test]
fn the_hold_outlasts_the_heartbeat_grace_of_the_shard() {
    let grace = crate::Shard::Coordinated
        .deploy_vars()
        .knobs
        .heartbeat_grace_s
        .unwrap();
    assert!(
        HOLD.as_secs() >= grace + 15,
        "hold {HOLD:?}, grace {grace}s"
    );
    assert_eq!(CLIENT_STEPS.last(), Some(&HOLD.as_secs()));
}
