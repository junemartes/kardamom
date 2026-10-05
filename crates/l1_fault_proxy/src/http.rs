//! A small HTTP/1.1 reader and writer over one TCP connection: enough
//! for JSON-RPC clients and for the control endpoint. A connection
//! serves requests in order until the peer closes it or a reply says
//! `Connection: close`.

use anyhow::{Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// One parsed request.
pub(crate) struct Request {
    pub(crate) method: String,
    pub(crate) path: String,
    pub(crate) body: Vec<u8>,
    /// The peer asked for the connection to close after the reply, or
    /// speaks HTTP/1.0, where that is the default.
    pub(crate) close: bool,
}

/// One reply: a status, a JSON body, and whether the connection closes
/// after it.
pub(crate) struct Response {
    pub(crate) status: u16,
    pub(crate) body: Vec<u8>,
    pub(crate) close: bool,
}

impl Response {
    /// A JSON reply that keeps the connection.
    pub(crate) fn json(status: u16, body: &serde_json::Value) -> Self {
        Self {
            status,
            body: body.to_string().into_bytes(),
            close: false,
        }
    }

    fn reason(&self) -> &'static str {
        match self.status {
            200 => "OK",
            400 => "Bad Request",
            404 => "Not Found",
            429 => "Too Many Requests",
            502 => "Bad Gateway",
            503 => "Service Unavailable",
            _ => "Unknown",
        }
    }

    fn head(&self) -> String {
        let connection = if self.close { "close" } else { "keep-alive" };
        format!(
            "HTTP/1.1 {} {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
             Connection: {connection}\r\n\r\n",
            self.status,
            self.reason(),
            self.body.len()
        )
    }
}

/// One accepted connection and the bytes read ahead of the request in
/// hand, so a pipelined request is not lost.
pub(crate) struct Connection {
    sock: TcpStream,
    buf: Vec<u8>,
}

/// What one read did.
enum ReadStep<T> {
    /// The condition matched: the read is complete.
    Done(T),
    /// The peer closed the connection before the condition matched.
    Closed,
    /// More bytes arrived; the condition has not matched yet.
    Pending,
}

/// The head of a request: its line, and the body length and the close
/// flag its headers name.
struct Head {
    method: String,
    path: String,
    content_length: usize,
    close: bool,
}

impl Head {
    /// Parse the request line and the headers of `raw`, the bytes before
    /// the blank line.
    fn parse(raw: &[u8]) -> Result<Self> {
        let text = String::from_utf8_lossy(raw);
        let mut lines = text.split("\r\n");
        let line = lines.next().unwrap_or_default();
        let mut parts = line.split(' ');
        let method = parts.next().unwrap_or_default().to_string();
        let path = parts.next().unwrap_or_default().to_string();
        let version = parts.next().unwrap_or_default();
        anyhow::ensure!(
            !method.is_empty() && !path.is_empty(),
            "bad request line: {line}"
        );
        let headers: Vec<(String, String)> = lines
            .filter_map(|l| l.split_once(':'))
            .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_ascii_lowercase()))
            .collect();
        let header = |name: &str| {
            headers
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.as_str())
        };
        // No Content-Length means no body (a bare GET). A present but
        // unparseable one is a malformed request, not a missing body.
        let content_length = header("content-length")
            .map(str::parse::<usize>)
            .transpose()
            .context("Content-Length header is not a valid number")?
            .unwrap_or(0);
        let close = header("connection").is_some_and(|v| v == "close") || version == "HTTP/1.0";
        Ok(Self {
            method,
            path,
            content_length,
            close,
        })
    }
}

impl Connection {
    pub(crate) fn new(sock: TcpStream) -> Self {
        Self {
            sock,
            buf: Vec::with_capacity(4096),
        }
    }

    /// The next request. `None` on a clean close before a request line.
    ///
    /// # Errors
    /// Returns an error when the socket fails or the request is malformed.
    pub(crate) async fn read_request(&mut self) -> Result<Option<Request>> {
        let Some(header_end) = self
            .read_until(|b| find_subslice(b, b"\r\n\r\n").map(|i| i + 4))
            .await?
        else {
            return Ok(None);
        };
        let head = Head::parse(&self.buf[..header_end])?;
        // `content_length` comes from the wire; a hostile value must fail
        // the request, not overflow the bound below.
        let end = header_end
            .checked_add(head.content_length)
            .context("Content-Length header overflows the buffer size")?;
        // A close before the whole body arrived is a truncated request:
        // the peer gets no reply for it.
        if self
            .read_until(|b| (b.len() >= end).then_some(()))
            .await?
            .is_none()
        {
            return Ok(None);
        }
        let body = self.buf[header_end..end].to_vec();
        self.buf.drain(..end);
        Ok(Some(Request {
            method: head.method,
            path: head.path,
            body,
            close: head.close,
        }))
    }

    /// Write one reply.
    ///
    /// # Errors
    /// Returns an error when the socket fails.
    pub(crate) async fn write(&mut self, response: &Response) -> Result<()> {
        self.sock.write_all(response.head().as_bytes()).await?;
        self.sock.write_all(&response.body).await?;
        self.sock.flush().await?;
        Ok(())
    }

    /// Read into the buffer until `done` matches against it and returns
    /// `Some`. `None` on a clean close before `done` ever matches.
    async fn read_until<T>(
        &mut self,
        mut done: impl FnMut(&[u8]) -> Option<T>,
    ) -> Result<Option<T>> {
        if let Some(t) = done(&self.buf) {
            return Ok(Some(t));
        }
        loop {
            match self.read_chunk(&mut done).await? {
                ReadStep::Done(t) => return Ok(Some(t)),
                ReadStep::Closed => return Ok(None),
                ReadStep::Pending => (),
            }
        }
    }

    /// Read one chunk, append it, and check `done` against the buffer.
    async fn read_chunk<T>(
        &mut self,
        done: &mut impl FnMut(&[u8]) -> Option<T>,
    ) -> Result<ReadStep<T>> {
        let mut chunk = [0u8; 4096];
        let n = self.sock.read(&mut chunk).await?;
        if n == 0 {
            return Ok(ReadStep::Closed);
        }
        self.buf.extend_from_slice(&chunk[..n]);
        Ok(done(&self.buf).map_or(ReadStep::Pending, ReadStep::Done))
    }
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_head_names_its_method_path_length_and_close() {
        let head = Head::parse(
            b"POST /fault HTTP/1.1\r\nHost: x\r\nContent-Length: 12\r\nConnection: Close\r\n",
        )
        .unwrap();
        assert_eq!(
            (head.method.as_str(), head.path.as_str()),
            ("POST", "/fault")
        );
        assert_eq!(head.content_length, 12);
        assert!(head.close);
        let keep = Head::parse(b"GET /health HTTP/1.1\r\n").unwrap();
        assert_eq!(keep.content_length, 0);
        assert!(!keep.close);
        assert!(Head::parse(b"GET / HTTP/1.0\r\n").unwrap().close);
        assert!(Head::parse(b"GET / HTTP/1.1\r\nContent-Length: x\r\n").is_err());
        assert!(Head::parse(b"\r\n").is_err());
    }

    #[test]
    fn a_reply_head_carries_the_length_and_the_connection_mode() {
        let reply = Response::json(429, &serde_json::json!({"a": 1}));
        let head = reply.head();
        assert!(head.starts_with("HTTP/1.1 429 Too Many Requests\r\n"));
        assert!(head.contains("Content-Length: 7\r\n"));
        assert!(head.contains("Connection: keep-alive\r\n"));
        let closing = Response {
            close: true,
            ..Response::json(503, &serde_json::Value::Null)
        };
        assert!(closing.head().contains("Connection: close\r\n"));
    }
}
