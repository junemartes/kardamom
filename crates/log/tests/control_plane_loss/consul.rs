//! A Consul-like agent for the control-plane loss test. It keeps a real
//! catalog: the four calls of the production client (register, check
//! pass, deregister, and the blocking health query), TTL checks that turn
//! critical when no pass comes, and the deletion of a record that stays
//! critical past its `DeregisterCriticalServiceAfter`.
//!
//! A partition cuts the clients off the agent, as the chaos case cuts
//! the data nodes off Consul: every request is read and then held with
//! no answer, until the partition clears and the connection closes. The
//! agent's own clock keeps running, so the checks expire during the
//! partition as they do on a real agent.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, watch};

/// One registered service.
struct Service {
    /// The `Service` object of a health answer.
    record: Value,
    name: String,
    check_id: String,
    ttl: Duration,
    deregister_after: Duration,
    registered: Instant,
    last_pass: Option<Instant>,
}

impl Service {
    /// When the check turned critical: the TTL after the last pass, or
    /// the registration for a check that never passed.
    fn critical_since(&self) -> Option<Instant> {
        match self.last_pass {
            None => Some(self.registered),
            Some(at) => (at.elapsed() > self.ttl).then_some(at + self.ttl),
        }
    }

    fn passing(&self) -> bool {
        self.last_pass.is_some_and(|at| at.elapsed() <= self.ttl)
    }

    fn matches(&self, name: &str, filter: &BTreeMap<String, String>) -> bool {
        self.name == name
            && filter
                .iter()
                .all(|(k, v)| self.record["Meta"][k].as_str() == Some(v.as_str()))
    }
}

/// The catalog and its index. Every change raises the index.
#[derive(Default)]
struct Catalog {
    services: BTreeMap<String, Service>,
    index: u64,
    /// The passing state of each service at the last sweep, so that a
    /// check that expires also raises the index.
    seen_passing: BTreeMap<String, bool>,
    /// The services that the agent deleted after their critical window.
    reaped: Vec<String>,
}

impl Catalog {
    /// Expire and delete records, and raise the index on any change.
    fn sweep(&mut self) -> bool {
        let reap: Vec<String> = self
            .services
            .iter()
            .filter(|(_, s)| {
                s.critical_since()
                    .is_some_and(|at| at.elapsed() > s.deregister_after)
            })
            .map(|(id, _)| id.clone())
            .collect();
        self.services.retain(|id, _| !reap.contains(id));
        self.reaped.extend(reap.iter().cloned());
        let passing: BTreeMap<String, bool> = self
            .services
            .iter()
            .map(|(id, s)| (id.clone(), s.passing()))
            .collect();
        let changed = !reap.is_empty() || passing != self.seen_passing;
        self.seen_passing = passing;
        if changed {
            self.index += 1;
        }
        changed
    }
}

/// The shared state of the agent and its handle.
struct Shared {
    catalog: Mutex<Catalog>,
    changed: Notify,
    partitioned: watch::Sender<bool>,
}

/// The handle of a running agent.
#[derive(Clone)]
pub(crate) struct ConsulAgent {
    shared: Arc<Shared>,
    pub(crate) addr: SocketAddr,
}

/// One parsed request.
struct Request {
    method: String,
    path: String,
    query: BTreeMap<String, String>,
    body: Vec<u8>,
}

impl ConsulAgent {
    /// Start the agent on a free loopback port.
    pub(crate) async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shared = Arc::new(Shared {
            catalog: Mutex::new(Catalog {
                index: 1,
                ..Catalog::default()
            }),
            changed: Notify::new(),
            partitioned: watch::channel(false).0,
        });
        let agent = Self { shared, addr };
        tokio::spawn(agent.clone().accept(listener));
        tokio::spawn(agent.clone().sweep_loop());
        agent
    }

    async fn sweep_loop(self) {
        loop {
            self.sweep_after_pause().await;
        }
    }

    async fn sweep_after_pause(&self) {
        tokio::time::sleep(Duration::from_millis(100)).await;
        self.sweep();
    }

    fn sweep(&self) {
        if self.shared.catalog.lock().unwrap().sweep() {
            self.shared.changed.notify_waiters();
        }
    }

    async fn accept(self, listener: TcpListener) {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(self.clone().serve(stream));
        }
    }

    /// Cut the clients off the agent, or let them back.
    pub(crate) fn set_partitioned(&self, partitioned: bool) {
        self.shared.partitioned.send_replace(partitioned);
    }

    /// Whether the catalog holds `id` with a passing check.
    pub(crate) fn passing(&self, id_prefix: &str) -> bool {
        self.shared
            .catalog
            .lock()
            .unwrap()
            .services
            .iter()
            .any(|(id, s)| id.starts_with(id_prefix) && s.passing())
    }

    /// The services that the agent deleted after their critical window.
    pub(crate) fn reaped(&self) -> Vec<String> {
        self.shared.catalog.lock().unwrap().reaped.clone()
    }

    /// Serve one connection: one request, one answer, then close. A
    /// partitioned agent holds the request until the partition clears,
    /// and closes the connection with no answer.
    async fn serve(self, stream: TcpStream) {
        let mut stream = BufReader::new(stream);
        let Some(request) = Self::read(&mut stream).await else {
            return;
        };
        if *self.shared.partitioned.borrow() {
            let mut cleared = self.shared.partitioned.subscribe();
            let _ = cleared.wait_for(|p| !*p).await;
            return;
        }
        let (status, index, body) = self.answer(&request).await;
        let reply = format!(
            "HTTP/1.1 {status}\r\nX-Consul-Index: {index}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = stream.get_mut().write_all(reply.as_bytes()).await;
    }

    async fn read(stream: &mut BufReader<TcpStream>) -> Option<Request> {
        let mut line = String::new();
        stream.read_line(&mut line).await.ok()?;
        let mut parts = line.split_whitespace();
        let method = parts.next()?.to_string();
        let url = reqwest::Url::parse(&format!("http://agent{}", parts.next()?)).ok()?;
        let mut length = Some(0);
        let mut body_len = 0;
        while let Some(seen) = length {
            body_len = seen;
            length = Self::header(stream, seen).await?;
        }
        let mut body = vec![0; body_len];
        stream.read_exact(&mut body).await.ok()?;
        Some(Request {
            method,
            path: url.path().to_string(),
            query: url.query_pairs().into_owned().collect(),
            body,
        })
    }

    /// Read one header line. Returns the body length seen so far, updated
    /// by a `Content-Length` header, or `None` at the end of the head.
    async fn header(stream: &mut BufReader<TcpStream>, seen: usize) -> Option<Option<usize>> {
        let mut header = String::new();
        stream.read_line(&mut header).await.ok()?;
        let header = header.trim_end();
        if header.is_empty() {
            return Some(None);
        }
        let length = header
            .split_once(':')
            .filter(|(k, _)| k.eq_ignore_ascii_case("content-length"))
            .and_then(|(_, v)| v.trim().parse().ok());
        Some(Some(length.unwrap_or(seen)))
    }

    async fn answer(&self, req: &Request) -> (&'static str, u64, String) {
        let path = req.path.as_str();
        match (req.method.as_str(), path) {
            ("PUT", "/v1/agent/service/register") => self.register(&req.body),
            ("PUT", p) if p.starts_with("/v1/agent/check/pass/") => {
                self.pass(&p["/v1/agent/check/pass/".len()..])
            }
            ("PUT", p) if p.starts_with("/v1/agent/service/deregister/") => {
                self.deregister(&p["/v1/agent/service/deregister/".len()..])
            }
            ("GET", p) if p.starts_with("/v1/health/service/") => {
                self.health(&p["/v1/health/service/".len()..], &req.query)
                    .await
            }
            _ => ("404 Not Found", self.index(), String::new()),
        }
    }

    fn index(&self) -> u64 {
        self.shared.catalog.lock().unwrap().index
    }

    /// Apply `change` to the catalog, raise the index and wake the
    /// blocking queries.
    fn write(&self, change: impl FnOnce(&mut Catalog) -> bool) -> (&'static str, u64, String) {
        let (ok, index) = {
            let mut catalog = self.shared.catalog.lock().unwrap();
            let ok = change(&mut catalog);
            if ok {
                catalog.index += 1;
            }
            (ok, catalog.index)
        };
        self.shared.changed.notify_waiters();
        if ok {
            ("200 OK", index, String::new())
        } else {
            ("404 Not Found", index, "Unknown check ID".to_string())
        }
    }

    fn register(&self, body: &[u8]) -> (&'static str, u64, String) {
        let reg: Value = serde_json::from_slice(body).unwrap();
        let id = reg["ID"].as_str().unwrap().to_string();
        let service = Service {
            record: json!({
                "ID": id,
                "Service": reg["Name"],
                "Address": reg["Address"],
                "Port": reg["Port"],
                "Meta": reg["Meta"],
            }),
            name: reg["Name"].as_str().unwrap().to_string(),
            check_id: reg["Check"]["CheckID"].as_str().unwrap().to_string(),
            ttl: seconds(&reg["Check"]["TTL"]),
            deregister_after: seconds(&reg["Check"]["DeregisterCriticalServiceAfter"]),
            registered: Instant::now(),
            last_pass: None,
        };
        self.write(|c| {
            c.services.insert(id, service);
            true
        })
    }

    fn pass(&self, check_id: &str) -> (&'static str, u64, String) {
        self.write(|c| {
            c.services
                .values_mut()
                .find(|s| s.check_id == check_id)
                .map(|s| s.last_pass = Some(Instant::now()))
                .is_some()
        })
    }

    fn deregister(&self, id: &str) -> (&'static str, u64, String) {
        self.write(|c| c.services.remove(id).is_some())
    }

    /// The passing records of `name` that match the query filter. A
    /// query with an index waits until the index moves past it, or until
    /// its wait ends.
    async fn health(
        &self,
        name: &str,
        query: &BTreeMap<String, String>,
    ) -> (&'static str, u64, String) {
        let index: u64 = query.get("index").and_then(|i| i.parse().ok()).unwrap_or(0);
        let wait = query
            .get("wait")
            .map_or(Duration::ZERO, |w| seconds(&Value::from(w.as_str())));
        let filter = parse_filter(query.get("filter").map_or("", String::as_str));
        let _ = tokio::time::timeout(wait, async {
            while index > 0 && self.index() <= index {
                self.shared.changed.notified().await;
            }
        })
        .await;
        let catalog = self.shared.catalog.lock().unwrap();
        let entries: Vec<Value> = catalog
            .services
            .values()
            .filter(|s| s.passing() && s.matches(name, &filter))
            .map(|s| json!({ "Service": s.record }))
            .collect();
        ("200 OK", catalog.index, Value::from(entries).to_string())
    }
}

/// A Consul duration such as `10s`.
fn seconds(v: &Value) -> Duration {
    let text = v.as_str().unwrap_or("0s");
    Duration::from_secs(text.trim_end_matches('s').parse().unwrap_or(0))
}

/// The pairs of a `Service.Meta.k == "v" and ...` filter.
fn parse_filter(filter: &str) -> BTreeMap<String, String> {
    filter
        .split(" and ")
        .filter_map(|clause| clause.split_once(" == "))
        .filter_map(|(k, v)| {
            Some((
                k.strip_prefix("Service.Meta.")?.to_string(),
                v.trim_matches('"').replace("\\\"", "\""),
            ))
        })
        .collect()
}
