//! The locator query: where an executor's archive holds canonical index
//! `i`. One JSON-RPC POST over HTTP/1.0 to the query endpoint of one
//! executor (`kardamom_getExecLocator`, params `[index, tx_hash]`), one
//! request per connection, with no HTTP dependency. The answer is parsed
//! once here into [`LocatorAnswer`].

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use alloy_primitives::B256;
use serde::Deserialize;

/// The bound of the connect and of each read and write of one query.
const QUERY_TIMEOUT: Duration = Duration::from_secs(2);

/// The largest response the client reads. A locator answer is a few
/// hundred bytes.
const MAX_RESPONSE: u64 = 64 * 1024;

/// The JSON-RPC method of the locator query.
const METHOD: &str = "kardamom_getExecLocator";

/// Where one executor's archive holds a record: the archive, the session
/// of the recording, and a raw position at or before the start of the
/// record.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct ArchiveLocator {
    /// The archive record id (`archive_id` in discovery).
    pub archive_id: String,
    /// The Aeron session of the recorded publication.
    pub session_id: i32,
    /// A raw stream position at or before the start of the record.
    pub position: i64,
}

/// One executor's answer for canonical index `i`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum LocatorAnswer {
    /// The executor joined `i`, and a retained recording covers it.
    Located(ArchiveLocator),
    /// The executor reached `i` and did not join it.
    NotHeld,
    /// The executor has not reached `i`.
    NotReached,
    /// The executor's state includes `i`, and no retained recording
    /// covers it.
    Lost,
}

impl LocatorAnswer {
    /// The `outcome` label of the refetch metric.
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Self::Located(_) => "located",
            Self::NotHeld => "not_held",
            Self::NotReached => "not_reached",
            Self::Lost => "lost",
        }
    }
}

/// A locator query failed: no connection, no answer in time, or an
/// answer that does not parse.
#[derive(Debug, thiserror::Error)]
#[error("locator query to {endpoint}: {detail}")]
pub struct LocatorError {
    endpoint: String,
    detail: String,
}

/// The query endpoint of one executor, as `host:port`. The host can be a
/// name, such as a Consul node record; each address it resolves to is
/// tried in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryEndpoint(String);

impl QueryEndpoint {
    /// Parse `http://host:port`, `http://host:port/` or `host:port`.
    ///
    /// # Errors
    ///
    /// Returns the raw value when it names no `host:port`.
    pub fn parse(raw: &str) -> Result<Self, String> {
        let bare = raw.strip_prefix("http://").unwrap_or(raw);
        let bare = bare.strip_suffix('/').unwrap_or(bare);
        match bare.rsplit_once(':') {
            Some((host, port)) if !host.is_empty() && port.parse::<u16>().is_ok() => {
                Ok(Self(bare.to_owned()))
            }
            _ => Err(format!("executor query endpoint {raw:?} is not host:port")),
        }
    }

    fn error(&self, detail: impl Into<String>) -> LocatorError {
        LocatorError {
            endpoint: self.0.clone(),
            detail: detail.into(),
        }
    }

    /// Open a connection to the first address that accepts one.
    fn connect(&self) -> Result<TcpStream, LocatorError> {
        let addrs = self
            .0
            .to_socket_addrs()
            .map_err(|e| self.error(format!("resolve: {e}")))?;
        let mut last = None;
        addrs
            .into_iter()
            .find_map(|addr| {
                TcpStream::connect_timeout(&addr, QUERY_TIMEOUT)
                    .map_err(|e| last = Some(e))
                    .ok()
            })
            .ok_or_else(|| {
                self.error(last.map_or_else(
                    || "resolves to no address".to_owned(),
                    |e| format!("connect: {e}"),
                ))
            })
    }

    /// Send `body` as one POST and read the whole response.
    fn post(&self, body: &str) -> Result<Vec<u8>, LocatorError> {
        let mut stream = self.connect()?;
        let io = |e: std::io::Error| self.error(format!("io: {e}"));
        stream.set_read_timeout(Some(QUERY_TIMEOUT)).map_err(io)?;
        stream.set_write_timeout(Some(QUERY_TIMEOUT)).map_err(io)?;
        let request = format!(
            "POST / HTTP/1.0\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(request.as_bytes()).map_err(io)?;
        let mut response = Vec::new();
        stream
            .take(MAX_RESPONSE)
            .read_to_end(&mut response)
            .map_err(io)?;
        Ok(response)
    }

    /// The answer in an HTTP response: a `200` status, and a JSON-RPC
    /// body with a result.
    fn answer(&self, response: &[u8]) -> Result<LocatorAnswer, LocatorError> {
        let split = response
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .ok_or_else(|| self.error("no end of the response head"))?;
        let (head, body) = response.split_at(split);
        let status_line = head.split(|b| *b == b'\n').next().unwrap_or_default();
        let ok = std::str::from_utf8(status_line)
            .is_ok_and(|line| line.split_whitespace().nth(1) == Some("200"));
        if !ok {
            return Err(self.error(format!(
                "status line {:?}",
                String::from_utf8_lossy(status_line)
            )));
        }
        let parsed: RpcResponse = serde_json::from_slice(&body[4..])
            .map_err(|e| self.error(format!("parse the answer: {e}")))?;
        match (parsed.result, parsed.error) {
            (Some(answer), None) => Ok(answer),
            (_, Some(error)) => Err(self.error(format!("error answer: {error}"))),
            (None, None) => Err(self.error("no result")),
        }
    }
}

#[derive(Deserialize)]
struct RpcResponse {
    #[serde(default)]
    result: Option<LocatorAnswer>,
    #[serde(default)]
    error: Option<serde_json::Value>,
}

/// The locator client: the query endpoints of the executors, in a fixed
/// order. The index of an endpoint names its executor in the wait.
#[derive(Clone, Debug, Default)]
pub struct LocatorClient {
    endpoints: Vec<QueryEndpoint>,
}

impl LocatorClient {
    /// The client of `endpoints`, parsed once here.
    ///
    /// # Errors
    ///
    /// Returns the first endpoint that names no `host:port`.
    pub fn new(endpoints: &[String]) -> Result<Self, String> {
        let endpoints = endpoints
            .iter()
            .map(|raw| QueryEndpoint::parse(raw))
            .collect::<Result<_, _>>()?;
        Ok(Self { endpoints })
    }

    /// The count of executors the client asks.
    #[must_use]
    pub fn len(&self) -> usize {
        self.endpoints.len()
    }

    /// Whether the client asks no executor.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.endpoints.is_empty()
    }

    /// Ask executor `executor` where its archive holds `index`.
    ///
    /// # Errors
    ///
    /// Returns the failure of the query: no such executor, no
    /// connection, no answer in time, or an answer that does not parse.
    pub fn ask(
        &self,
        executor: usize,
        index: u64,
        tx_hash: B256,
    ) -> Result<LocatorAnswer, LocatorError> {
        let endpoint = self.endpoints.get(executor).ok_or_else(|| LocatorError {
            endpoint: format!("executor {executor}"),
            detail: "no such executor".into(),
        })?;
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": METHOD,
            "params": [index, format!("{tx_hash:#x}")],
        })
        .to_string();
        let response = endpoint.post(&body)?;
        endpoint.answer(&response)
    }
}
