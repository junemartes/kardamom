//! The client side of `kardamom_getExecLocator`: ask one peer executor
//! where one canonical entry is in the recordings of its stream.
//!
//! The caller is the `tx_ordering` reader, a std thread, so the client is
//! std-sync: HTTP/1.0 over a plain socket, one request per connection, the
//! same wire as the server in [`crate::nonce_query`].

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::str::FromStr;
use std::time::Duration;

use alloy_primitives::B256;
use serde::Deserialize;

use crate::exec_answers::ExecLocatorAnswer;

/// The JSON-RPC name of the locator lookup.
pub const EXEC_LOCATOR_METHOD: &str = "kardamom_getExecLocator";

/// The largest response that the client reads. A locator answer is a few
/// dozen bytes.
const MAX_RESPONSE: u64 = 64 << 10;

/// The default bound of one connect, and of each read and write.
pub const DEFAULT_PEER_TIMEOUT: Duration = Duration::from_secs(2);

/// Why a connection to a `host:port` failed.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    #[error("cannot resolve {authority}: {source}")]
    Resolve {
        authority: String,
        source: std::io::Error,
    },
    #[error("{authority} resolves to no address")]
    NoAddress { authority: String },
    #[error(transparent)]
    Refused(std::io::Error),
}

/// Why one peer gave no answer.
#[derive(Debug, thiserror::Error)]
pub enum PeerError {
    #[error(transparent)]
    Connect(#[from] ConnectError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("bad response: {0}")]
    Response(String),
    #[error("JSON-RPC error: {0}")]
    Rpc(serde_json::Value),
}

/// A `host:port` that can name a host by its DNS record, such as a Consul
/// node record.
pub(crate) struct Authority<'a>(pub(crate) &'a str);

impl Authority<'_> {
    /// A connection to the first address that accepts one. Every address
    /// that the name resolves to is tried in order.
    pub(crate) fn connect(&self, timeout: Duration) -> Result<TcpStream, ConnectError> {
        let mut last = None;
        let connected = self
            .0
            .to_socket_addrs()
            .map_err(|source| ConnectError::Resolve {
                authority: self.0.to_owned(),
                source,
            })?
            .find_map(|addr| {
                TcpStream::connect_timeout(&addr, timeout)
                    .map_err(|e| last = Some(e))
                    .ok()
            });
        match (connected, last) {
            (Some(stream), _) => Ok(stream),
            (None, Some(e)) => Err(ConnectError::Refused(e)),
            (None, None) => Err(ConnectError::NoAddress {
                authority: self.0.to_owned(),
            }),
        }
    }
}

/// One peer query endpoint, `http://host:port`, parsed once.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecPeer {
    url: String,
    authority: String,
}

impl FromStr for ExecPeer {
    type Err = String;

    fn from_str(url: &str) -> Result<Self, Self::Err> {
        let authority = url
            .strip_prefix("http://")
            .map(|rest| rest.trim_end_matches('/'))
            .filter(|rest| rest.contains(':') && !rest.contains('/'))
            .ok_or_else(|| format!("expected http://host:port, got {url}"))?;
        Ok(Self {
            url: url.to_owned(),
            authority: authority.to_owned(),
        })
    }
}

/// The reply body of one lookup.
#[derive(Deserialize)]
struct Reply {
    #[serde(default)]
    result: Option<ExecLocatorAnswer>,
    #[serde(default)]
    error: Option<serde_json::Value>,
}

impl ExecPeer {
    /// The URL, for logs.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Ask this peer where the entry at canonical `index` with `tx_hash`
    /// is. `timeout` bounds the connect and each read and write.
    ///
    /// # Errors
    ///
    /// Returns the error when the peer cannot be reached, or answers no
    /// valid result.
    pub fn ask(
        &self,
        index: u64,
        tx_hash: B256,
        timeout: Duration,
    ) -> Result<ExecLocatorAnswer, PeerError> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": EXEC_LOCATOR_METHOD,
            "params": [index, tx_hash],
        })
        .to_string();
        let mut stream = Authority(&self.authority).connect(timeout)?;
        stream.set_read_timeout(Some(timeout))?;
        stream.set_write_timeout(Some(timeout))?;
        write!(
            stream,
            "POST / HTTP/1.0\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        )?;
        let mut raw = Vec::new();
        stream.take(MAX_RESPONSE).read_to_end(&mut raw)?;
        Self::parse(&raw)
    }

    /// The result of a whole HTTP/1.0 response. A JSON-RPC error rides a
    /// `200 OK` or a `500`, so the body decides, not the status.
    fn parse(raw: &[u8]) -> Result<ExecLocatorAnswer, PeerError> {
        let split = raw
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .ok_or_else(|| PeerError::Response("no end of head".into()))?;
        let body = raw.get(split.saturating_add(4)..).unwrap_or_default();
        let reply: Reply = serde_json::from_slice(body)
            .map_err(|e| PeerError::Response(format!("body is no JSON-RPC reply: {e}")))?;
        match (reply.result, reply.error) {
            (_, Some(error)) => Err(PeerError::Rpc(error)),
            (Some(answer), None) => Ok(answer),
            (None, None) => Err(PeerError::Response("no result".into())),
        }
    }
}

/// The peer executors that a reader asks, with the bound of one ask.
#[derive(Clone, Debug, Default)]
pub struct ExecPeers {
    peers: Vec<ExecPeer>,
    timeout: Duration,
}

impl ExecPeers {
    /// The peers in `listed`, without `own`: an executor does not ask
    /// itself.
    #[must_use]
    pub fn new(listed: Vec<ExecPeer>, own: Option<&ExecPeer>, timeout: Duration) -> Self {
        let peers = listed
            .into_iter()
            .filter(|peer| Some(peer) != own)
            .collect();
        Self { peers, timeout }
    }

    /// The peers, in the configured order.
    #[must_use]
    pub fn peers(&self) -> &[ExecPeer] {
        &self.peers
    }

    /// True when no peer is configured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    /// The bound of one ask.
    #[must_use]
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

#[cfg(test)]
#[path = "exec_peers_tests.rs"]
mod tests;
