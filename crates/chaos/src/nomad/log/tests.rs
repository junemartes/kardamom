use super::*;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

const LOGS: &str = "/v1/client/fs/logs/";
const NODE: &str = "/v1/node/node-0";
const FORWARD_FAILED: &str = "stream reset";

/// A fake Nomad agent. Each path prefix answers one canned status and
/// body; any other path answers 404.
#[derive(Default)]
struct FakeAgent {
    routes: Vec<(String, u16, String)>,
}

impl FakeAgent {
    fn route(mut self, prefix: &str, status: u16, body: &str) -> Self {
        self.routes.push((prefix.into(), status, body.into()));
        self
    }

    /// Serve one connection at a time on a loopback port. Returns the
    /// address.
    async fn spawn(self) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                self.answer(&listener).await;
            }
        });
        addr
    }

    async fn answer(&self, listener: &TcpListener) {
        let (sock, _) = listener.accept().await.unwrap();
        let mut reader = BufReader::new(sock);
        let request = Self::line(&mut reader).await;
        while !matches!(Self::line(&mut reader).await.as_str(), "\r\n" | "") {}
        let path = request.split_whitespace().nth(1).unwrap_or_default();
        let (status, body) = self.reply(path);
        let response = format!(
            "HTTP/1.1 {status} Fake\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        reader
            .get_mut()
            .write_all(response.as_bytes())
            .await
            .unwrap();
        reader.get_mut().shutdown().await.unwrap();
    }

    async fn line(reader: &mut BufReader<TcpStream>) -> String {
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        line
    }

    fn reply(&self, path: &str) -> (u16, String) {
        self.routes
            .iter()
            .find(|(prefix, ..)| path.starts_with(prefix.as_str()))
            .map_or((404, String::new()), |(_, status, body)| {
                (*status, body.clone())
            })
    }
}

/// An executor allocation on `node-0`.
fn alloc() -> Alloc {
    serde_json::from_value(serde_json::json!({
        "ID": "eb6b7531-7629-88bc-fc66-646f1c648ed7", "TaskGroup": "executor",
        "ClientStatus": "running", "JobVersion": 1, "DesiredStatus": "run",
        "NodeName": "executor-0", "NodeID": "node-0",
        "TaskStates": { "executor": { "State": "running" } }
    }))
    .unwrap()
}

/// A client of the fake control agent, with a short retry budget.
fn nomad(control: &str) -> Nomad {
    Nomad {
        log_budget: Budget::new(Duration::from_millis(200), Duration::from_millis(50)),
        ..Nomad::new(&format!("http://{control}")).unwrap()
    }
}

/// The control agent, with the node record that names `node_agent`.
fn control_agent(node_agent: &str) -> FakeAgent {
    FakeAgent::default().route(NODE, 200, &format!(r#"{{"HTTPAddr":"{node_agent}"}}"#))
}

/// The address of a loopback port that nothing listens on.
async fn closed_port() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().to_string()
}

async fn read(nomad: &Nomad) -> anyhow::Result<String> {
    nomad.task_log(&alloc(), "executor", "stdout").await
}

#[tokio::test]
async fn the_control_agent_serves_the_log() {
    let control = control_agent(&closed_port().await)
        .route(LOGS, 200, "restored\n")
        .spawn()
        .await;
    assert_eq!(read(&nomad(&control)).await.unwrap(), "restored\n");
}

#[tokio::test]
async fn a_5xx_from_the_control_agent_reads_the_node_agent() {
    let node = FakeAgent::default()
        .route(LOGS, 200, "restored\n")
        .spawn()
        .await;
    let control = control_agent(&node)
        .route(LOGS, 500, FORWARD_FAILED)
        .spawn()
        .await;
    assert_eq!(read(&nomad(&control)).await.unwrap(), "restored\n");
}

#[tokio::test]
async fn a_log_that_no_agent_has_reads_as_empty() {
    let gone = control_agent(&closed_port().await).spawn().await;
    assert_eq!(read(&nomad(&gone)).await.unwrap(), "");
    let node = FakeAgent::default().spawn().await;
    let control = control_agent(&node)
        .route(LOGS, 500, FORWARD_FAILED)
        .spawn()
        .await;
    assert_eq!(read(&nomad(&control)).await.unwrap(), "");
}

#[tokio::test]
async fn a_5xx_from_both_agents_fails_with_both_bodies() {
    let node = FakeAgent::default()
        .route(LOGS, 500, "failed to stream")
        .spawn()
        .await;
    let control = control_agent(&node)
        .route(LOGS, 500, FORWARD_FAILED)
        .spawn()
        .await;
    let err = format!("{:#}", read(&nomad(&control)).await.unwrap_err());
    assert!(err.contains(FORWARD_FAILED), "{err}");
    assert!(err.contains("failed to stream"), "{err}");
    assert!(err.contains("of retries"), "{err}");
}

#[tokio::test]
async fn an_unreachable_node_agent_fails_after_the_retries() {
    let control = control_agent(&closed_port().await)
        .route(LOGS, 503, FORWARD_FAILED)
        .spawn()
        .await;
    let err = format!("{:#}", read(&nomad(&control)).await.unwrap_err());
    assert!(err.contains("503 Service Unavailable"), "{err}");
    assert!(
        err.contains("the agent on executor-0 answered GET"),
        "{err}"
    );
}

#[tokio::test]
async fn a_4xx_from_the_control_agent_ends_the_read() {
    let control = control_agent(&closed_port().await)
        .route(LOGS, 400, "unknown task name")
        .spawn()
        .await;
    let err = format!("{:#}", read(&nomad(&control)).await.unwrap_err());
    assert!(err.contains("400"), "{err}");
}
