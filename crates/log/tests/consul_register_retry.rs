//! The start-up catalog calls against a fake Consul agent: a registration
//! and the read of the cluster members. The agent fails the first
//! requests and then accepts. A transient failure (a dropped connection,
//! a server error status) makes the call try again. A refusal (an ACL
//! denial) ends it at once.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use kardamom_log::config::{DiscoveryConfig, LogConfig};
use kardamom_log::discovery::{
    Instance, PublisherRecord, Registration, RegistrationSpec, Scope, ServiceId, StartRetry,
    StreamPlane, Topic, WatchTiming, catalog_from_config,
};
use kardamom_log::error::LogError;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

/// What the fake agent does with one request.
#[derive(Clone, Copy)]
enum Answer {
    /// Close the connection with no answer.
    Drop,
    /// Answer with this status line.
    Status(&'static str),
}

/// A fake Consul agent. It gives the answers of `script` in order, one
/// per request, and `200 OK` after the script ends. A `200 OK` carries
/// an index and an empty list, so a catalog read finds no record.
struct FakeAgent {
    script: Vec<Answer>,
    requests: Arc<AtomicU32>,
}

impl FakeAgent {
    /// Start the agent on a free local port. Returns its address and the
    /// count of the requests it got.
    async fn start(script: Vec<Answer>) -> (SocketAddr, Arc<AtomicU32>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let requests = Arc::new(AtomicU32::new(0));
        let agent = Self {
            script,
            requests: requests.clone(),
        };
        tokio::spawn(agent.serve(listener));
        (addr, requests)
    }

    async fn serve(self, listener: TcpListener) {
        while let Ok((stream, _)) = listener.accept().await {
            self.answer(stream).await;
        }
    }

    /// Read one request from `stream` and give the next answer.
    async fn answer(&self, stream: TcpStream) {
        let n = self.requests.fetch_add(1, Ordering::SeqCst);
        let answer = usize::try_from(n)
            .ok()
            .and_then(|i| self.script.get(i).copied())
            .unwrap_or(Answer::Status("200 OK"));
        let mut stream = BufReader::new(stream);
        let body_len = read_head(&mut stream).await;
        let mut body = vec![0; body_len];
        let _ = stream.read_exact(&mut body).await;
        if let Answer::Status(status) = answer {
            let reply = format!(
                "HTTP/1.1 {status}\r\nX-Consul-Index: 1\r\nContent-Length: 2\r\nConnection: close\r\n\r\n[]"
            );
            let _ = stream.get_mut().write_all(reply.as_bytes()).await;
        }
    }
}

/// Read the request head from `stream`, and return its body length.
async fn read_head(stream: &mut BufReader<TcpStream>) -> usize {
    let mut lines = Vec::new();
    let mut line = String::new();
    while stream.read_line(&mut line).await.is_ok_and(|n| n > 0) && line != "\r\n" {
        lines.push(std::mem::take(&mut line));
    }
    lines
        .iter()
        .find_map(|l| {
            l.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .and_then(|v| v.trim().parse().ok())
        })
        .unwrap_or(0)
}

fn config(agent: SocketAddr) -> DiscoveryConfig {
    DiscoveryConfig {
        enabled: true,
        consul_http_addr: format!("http://{agent}"),
        cluster_id: "test".into(),
        chain_id: 7,
        ..DiscoveryConfig::default()
    }
}

fn spec(cfg: &DiscoveryConfig) -> RegistrationSpec {
    let record = PublisherRecord {
        id: ServiceId::new("alloc-1:tx_errors:1015".into()),
        control: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 40_001),
        topic: Topic::TxErrors,
        stream_id: 1015,
        lane: None,
        publisher_id: "sequencer".into(),
        session_id: None,
    };
    let scope = Scope {
        cluster_id: cfg.cluster_id.clone(),
        chain_id: cfg.chain_id,
    };
    RegistrationSpec {
        entry: record.entry(&scope),
        ttl: cfg.check_ttl(),
        deregister_after: cfg.deregister_after(),
    }
}

/// Register the record of [`spec`] at `agent` with a start-up retry of a
/// 2 s limit and short pauses.
async fn register(agent: SocketAddr) -> Result<Registration, LogError> {
    let cfg = config(agent);
    let catalog = catalog_from_config(&cfg).expect("consul client");
    let spec = spec(&cfg);
    let timing = WatchTiming {
        backoff_min: Duration::from_millis(20),
        backoff_max: Duration::from_millis(100),
        ..WatchTiming::from_config(&cfg)
    };
    StartRetry::new(
        "record alloc-1:tx_errors:1015",
        Duration::from_secs(2),
        timing,
        tokio_util::sync::CancellationToken::new(),
    )
    .run(|| Registration::register(catalog.clone(), spec.clone()))
    .await
}

#[tokio::test]
async fn a_registration_tries_again_until_the_agent_accepts() {
    let (agent, requests) = FakeAgent::start(vec![
        Answer::Drop,
        Answer::Status("503 Service Unavailable"),
    ])
    .await;
    let registration = register(agent).await.expect("registered on the third try");
    // Two failed registers, then one register and one check pass.
    assert_eq!(requests.load(Ordering::SeqCst), 4);
    registration.deregister().await.expect("deregistered");
}

#[tokio::test]
async fn a_refused_registration_fails_at_once() {
    let (agent, requests) = FakeAgent::start(vec![Answer::Status("403 Forbidden")]).await;
    let error = register(agent).await.err().expect("a refusal");
    assert!(matches!(error, LogError::Discovery(_)), "{error}");
    assert!(error.to_string().contains("403 Forbidden"), "{error}");
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_registration_gives_up_on_an_agent_that_never_listens() {
    // The listener closes at the end of the block, so the port refuses.
    let agent = {
        let free = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        free.local_addr().expect("local addr")
    };
    let error = register(agent).await.err().expect("no agent");
    assert!(matches!(error, LogError::CatalogUnavailable(_)), "{error}");
    assert!(
        error
            .to_string()
            .starts_with("discovery: register alloc-1:tx_errors:1015: "),
        "{error}"
    );
}

/// A plane over the agent at `agent`, with short catalog pauses.
fn plane(agent: SocketAddr) -> StreamPlane {
    let cfg = LogConfig {
        discovery: DiscoveryConfig {
            backoff_min_ms: NonZeroU64::new(20).expect("20 != 0"),
            backoff_max_ms: NonZeroU64::new(100).expect("100 != 0"),
            ..config(agent)
        },
        ..LogConfig::default()
    };
    let catalog = catalog_from_config(&cfg.discovery).expect("consul client");
    let instance = Instance {
        id: "alloc-1".into(),
    };
    StreamPlane::with_catalog(&cfg, "test", catalog, instance, Ipv4Addr::LOCALHOST)
}

#[tokio::test]
async fn the_cluster_member_read_tries_again_while_the_agent_has_no_server() {
    let no_server = Answer::Status("500 Internal Server Error");
    let (agent, requests) = FakeAgent::start(vec![no_server, no_server]).await;
    let endpoints = plane(agent)
        .cluster_ingress_endpoints(Duration::from_secs(2))
        .await
        .expect("read on the third try");
    // The agent lists no member, so the static endpoints apply.
    assert_eq!(endpoints, None);
    assert_eq!(requests.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn the_cluster_member_read_fails_after_the_limit() {
    let (agent, requests) =
        FakeAgent::start(vec![Answer::Status("500 Internal Server Error"); 1000]).await;
    let error = plane(agent)
        .cluster_ingress_endpoints(Duration::from_millis(300))
        .await
        .expect_err("no answer within the limit");
    assert!(matches!(error, LogError::CatalogUnavailable(_)), "{error}");
    assert!(requests.load(Ordering::SeqCst) > 1);
}

#[tokio::test]
async fn a_lost_check_repeats_the_whole_registration() {
    let (agent, requests) = FakeAgent::start(vec![
        Answer::Status("200 OK"),
        Answer::Status("404 Not Found"),
    ])
    .await;
    let registration = register(agent).await.expect("lost check recreated");
    // The first register succeeds but its check is lost. Register and
    // pass both run again before a heartbeat can start.
    assert_eq!(requests.load(Ordering::SeqCst), 4);
    registration.deregister().await.expect("deregistered");
}

#[tokio::test]
async fn a_check_that_keeps_disappearing_exhausts_the_existing_limit() {
    let script = [Answer::Status("200 OK"), Answer::Status("404 Not Found")]
        .into_iter()
        .cycle()
        .take(1000)
        .collect();
    let (agent, requests) = FakeAgent::start(script).await;
    let error = register(agent).await.err().expect("check never stays");
    assert!(matches!(error, LogError::CatalogCheckMissing(_)), "{error}");
    assert!(requests.load(Ordering::SeqCst) > 2);
}

#[tokio::test]
async fn a_check_pass_acl_denial_remains_permanent() {
    let (agent, requests) = FakeAgent::start(vec![
        Answer::Status("200 OK"),
        Answer::Status("403 Forbidden"),
    ])
    .await;
    let error = register(agent).await.err().expect("check denied");
    assert!(matches!(error, LogError::Discovery(_)), "{error}");
    assert_eq!(requests.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_missing_registration_endpoint_remains_permanent() {
    let (agent, requests) = FakeAgent::start(vec![Answer::Status("404 Not Found")]).await;
    let error = register(agent).await.err().expect("endpoint missing");
    assert!(matches!(error, LogError::Discovery(_)), "{error}");
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_missing_member_query_endpoint_remains_permanent() {
    let (agent, requests) = FakeAgent::start(vec![Answer::Status("404 Not Found")]).await;
    let error = plane(agent)
        .cluster_ingress_endpoints(Duration::from_secs(2))
        .await
        .expect_err("endpoint missing");
    assert!(matches!(error, LogError::Discovery(_)), "{error}");
    assert_eq!(requests.load(Ordering::SeqCst), 1);
}
