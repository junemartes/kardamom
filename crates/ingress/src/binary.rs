//! Length-prefixed RLP binary line protocol over TCP and Unix Domain
//! Sockets.
//!
//! Frame format, big-endian:
//! - `u32 len`: payload length in bytes, at most 1 MiB.
//! - `len` bytes: RLP-encoded `TxEnvelope`.
//!
//! Reply frame:
//! - `u8 status`: 0 = ok, 1 = rate-limited, 2 = decode, 3 = sig,
//!   4 = timeout, 5 = duplicate, 9 = internal.
//! - `u32 payload_len`.
//! - `payload_len` bytes: on ok, the 32-byte `tx_hash`; on error, the
//!   UTF-8 error message.

#![cfg(feature = "binary-protocol")]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::ops::ControlFlow;
use std::path::Path;

use alloy_primitives::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
use tokio::task::JoinHandle;

use crate::channels::{IngressPublication, IngressSubscription};
use crate::error::IngressError;
use crate::proxy::IngressProxy;

pub(crate) const STATUS_OK: u8 = 0;
pub(crate) const STATUS_RATE_LIMITED: u8 = 1;
pub(crate) const STATUS_DECODE: u8 = 2;
pub(crate) const STATUS_SIG: u8 = 3;
pub(crate) const STATUS_TIMEOUT: u8 = 4;
pub(crate) const STATUS_DUPLICATE: u8 = 5;
pub(crate) const STATUS_INTERNAL: u8 = 9;

pub(crate) const MAX_FRAME_BYTES: usize = 1024 * 1024;

/// A frame length already checked against `MAX_FRAME_BYTES`. Parsed once
/// at the wire boundary, so `ConnectionLoop` never re-checks it.
#[derive(Clone, Copy)]
struct FrameLen(usize);

impl FrameLen {
    #[must_use]
    fn get(self) -> usize {
        self.0
    }
}

impl TryFrom<u32> for FrameLen {
    type Error = ();

    fn try_from(len: u32) -> Result<Self, Self::Error> {
        // Every target this workspace builds for has a `usize` at least
        // as wide as `u32`, so this widening never truncates.
        let len = len as usize;
        if len > MAX_FRAME_BYTES {
            Err(())
        } else {
            Ok(Self(len))
        }
    }
}

/// Every stream `ConnectionLoop` reads and writes frames on.
pub(crate) trait AsyncStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin {}

impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> AsyncStream for T {}

/// A listener that accepts a stream of [`AsyncStream`] connections, each
/// paired with the client IP the connection's rate-limit key should use.
/// [`accept_loop`] is generic over this, so the TCP and UDS listeners
/// share one accept-and-dispatch loop instead of two copies of it.
pub(crate) trait Accept {
    type Stream: AsyncStream + Send + 'static;

    async fn accept_one(&self) -> std::io::Result<(Self::Stream, IpAddr)>;
}

impl Accept for TcpListener {
    type Stream = TcpStream;

    async fn accept_one(&self) -> std::io::Result<(Self::Stream, IpAddr)> {
        let (sock, peer) = self.accept().await?;
        Ok((sock, peer.ip()))
    }
}

impl Accept for UnixListener {
    type Stream = UnixStream;

    async fn accept_one(&self) -> std::io::Result<(Self::Stream, IpAddr)> {
        let (sock, _) = self.accept().await?;
        // A UDS connection has no IP, so this uses loopback as the
        // rate-limit key.
        Ok((sock, IpAddr::V4(Ipv4Addr::LOCALHOST)))
    }
}

/// The accept loop's state: the bound listener and the proxy every
/// connection goes through.
pub(crate) struct Acceptor<L, P, S>
where
    P: IngressPublication + Clone,
    S: IngressSubscription + Clone,
{
    listener: L,
    proxy: IngressProxy<P, S>,
}

impl<L, P, S> Acceptor<L, P, S>
where
    L: Accept,
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    fn new(listener: L, proxy: IngressProxy<P, S>) -> Self {
        Self { listener, proxy }
    }

    /// Accept connections until the listener errors, spawning one
    /// [`ConnectionLoop`] task per connection.
    async fn run(self) -> std::io::Result<()> {
        loop {
            self.accept_one().await?;
        }
    }

    /// Accept one connection and hand it its own task.
    async fn accept_one(&self) -> std::io::Result<()> {
        let (sock, client_ip) = self.listener.accept_one().await?;
        let proxy = self.proxy.clone();
        tokio::spawn(async move {
            let _ = ConnectionLoop::new(sock, client_ip, proxy).run().await;
        });
        Ok(())
    }
}

impl<P, S> Acceptor<TcpListener, P, S>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    /// Bind `addr` on the spawned task and accept until the listener
    /// errors.
    pub(crate) fn spawn_tcp(
        proxy: IngressProxy<P, S>,
        addr: SocketAddr,
    ) -> JoinHandle<std::io::Result<()>> {
        tokio::spawn(async move {
            let listener = TcpListener::bind(addr).await?;
            Self::new(listener, proxy).run().await
        })
    }
}

impl<P, S> Acceptor<UnixListener, P, S>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    /// Bind `path` now, so a bind error shows up right away, then accept
    /// until the listener errors.
    pub(crate) fn spawn_uds(
        proxy: IngressProxy<P, S>,
        path: &Path,
    ) -> std::io::Result<JoinHandle<std::io::Result<()>>> {
        let listener = UnixListener::bind(path)?;
        Ok(tokio::spawn(Self::new(listener, proxy).run()))
    }
}

/// One connection's read/dispatch state: the socket, the client's IP (the
/// rate-limit key), and the proxy every frame goes through. Grouped so
/// [`Self::step`] is one call plus the dispatch on its result, per frame.
///
/// Generic over `P`/`S` directly (matching [`IngressProxy`]'s own
/// parameters), not over a `B: ProxyBackend`: `B`'s associated types
/// alone would leave `B` itself unconstrained by any field, which the
/// compiler cannot infer at construction.
struct ConnectionLoop<W, P, S>
where
    P: IngressPublication + Clone,
    S: IngressSubscription + Clone,
{
    sock: W,
    client_ip: IpAddr,
    proxy: IngressProxy<P, S>,
}

/// [`ConnectionLoop::read_header`]'s outcome.
enum HeaderOutcome {
    /// The peer closed the connection at a frame boundary. Clean end.
    Closed,
    /// The declared length exceeds `MAX_FRAME_BYTES`. A decode-error
    /// reply is already written; the caller reads the next header.
    TooLarge,
    /// A valid length; the caller reads that many body bytes next.
    Len(FrameLen),
}

impl<W: AsyncStream, P, S> ConnectionLoop<W, P, S>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    pub(crate) fn new(sock: W, client_ip: IpAddr, proxy: IngressProxy<P, S>) -> Self {
        Self {
            sock,
            client_ip,
            proxy,
        }
    }

    /// Serve frames until the peer closes the connection or a write
    /// fails.
    ///
    /// # Errors
    ///
    /// Returns the socket error when a reply cannot be written.
    pub(crate) async fn run(mut self) -> std::io::Result<()> {
        loop {
            match self.step().await? {
                ControlFlow::Break(()) => return Ok(()),
                ControlFlow::Continue(()) => {}
            }
        }
    }

    /// One full frame cycle: read the header, then (unless it closed the
    /// connection or was too large) the body, then dispatch it.
    /// `Break(())` means the connection ended, on any of the three read
    /// or protocol outcomes that stop it; `Continue(())` means read the
    /// next frame.
    async fn step(&mut self) -> std::io::Result<ControlFlow<()>> {
        let len = match self.read_header().await? {
            HeaderOutcome::Closed => return Ok(ControlFlow::Break(())),
            HeaderOutcome::TooLarge => return Ok(ControlFlow::Continue(())),
            HeaderOutcome::Len(len) => len,
        };
        let Some(raw) = self.read_body(len).await? else {
            return Ok(ControlFlow::Break(()));
        };
        self.dispatch_frame(raw).await?;
        Ok(ControlFlow::Continue(()))
    }

    /// Read the 4-byte big-endian length prefix and validate it against
    /// `MAX_FRAME_BYTES`. See [`HeaderOutcome`].
    async fn read_header(&mut self) -> std::io::Result<HeaderOutcome> {
        let mut len_buf = [0u8; 4];
        if self.sock.read_exact(&mut len_buf).await.is_err() {
            return Ok(HeaderOutcome::Closed); // The peer closed the connection.
        }
        if let Ok(len) = FrameLen::try_from(u32::from_be_bytes(len_buf)) {
            Ok(HeaderOutcome::Len(len))
        } else {
            self.reply(STATUS_DECODE, b"frame too large").await?;
            Ok(HeaderOutcome::TooLarge)
        }
    }

    /// Read `len` body bytes. `None` means the peer closed mid-frame
    /// (the caller stops; a header-only close already handled the clean
    /// case, so this one gets no reply either, matching that).
    async fn read_body(&mut self, len: FrameLen) -> std::io::Result<Option<Bytes>> {
        let mut payload = vec![0u8; len.get()];
        if self.sock.read_exact(&mut payload).await.is_err() {
            return Ok(None);
        }
        Ok(Some(Bytes::from(payload)))
    }

    /// Submit one decoded frame and write its reply.
    async fn dispatch_frame(&mut self, raw: Bytes) -> std::io::Result<()> {
        match self.proxy.submit_raw(self.client_ip, raw).await {
            Ok(resp) => self.reply(STATUS_OK, resp.receipt.tx_hash.as_slice()).await,
            Err(e) => {
                let (status, msg) = map_err(&e);
                self.reply(status, msg.as_bytes()).await
            }
        }
    }

    /// Write one `status, len, payload` reply frame.
    async fn reply(&mut self, status: u8, payload: &[u8]) -> std::io::Result<()> {
        self.sock.write_all(&[status]).await?;
        // `payload` is always a 32-byte tx_hash or a short UTF-8 error
        // message, both far below `u32::MAX`. This is a narrowing cast on
        // a length that goes out over the wire, so a payload that ever did
        // violate that invariant fails the write instead of silently
        // truncating the length header and corrupting the frame.
        let len = u32::try_from(payload.len()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "reply payload exceeds u32::MAX",
            )
        })?;
        self.sock.write_all(&len.to_be_bytes()).await?;
        self.sock.write_all(payload).await?;
        self.sock.flush().await
    }
}

fn map_err(e: &IngressError) -> (u8, String) {
    match e {
        IngressError::RateLimited(_) => (STATUS_RATE_LIMITED, e.to_string()),
        IngressError::Decode(_) => (STATUS_DECODE, e.to_string()),
        IngressError::SignatureInvalid => (STATUS_SIG, e.to_string()),
        IngressError::Timeout => (STATUS_TIMEOUT, e.to_string()),
        IngressError::Duplicate(_) => (STATUS_DUPLICATE, e.to_string()),
        _ => (STATUS_INTERNAL, e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channels::MockChannels;
    use crate::config::IngressConfig;

    #[tokio::test]
    async fn empty_rlp_returns_decode_error() {
        let cfg = IngressConfig::default();
        let (mock, _rx) = MockChannels::new(std::num::NonZeroUsize::new(8).unwrap());
        let proxy = IngressProxy::new(cfg, mock.clone(), mock);
        let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = TcpListener::bind(bind).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let p2 = proxy.clone();
        tokio::spawn(async move {
            let (sock, peer) = listener.accept().await.unwrap();
            ConnectionLoop::new(sock, peer.ip(), p2)
                .run()
                .await
                .unwrap();
        });
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        // Send an empty RLP list, `0xc0`.
        client.write_all(&1u32.to_be_bytes()).await.unwrap();
        client.write_all(&[0xc0]).await.unwrap();
        let mut status = [0u8; 1];
        client.read_exact(&mut status).await.unwrap();
        assert_eq!(status[0], STATUS_DECODE);
    }

    /// An over-length frame header gets a decode-error reply, and the
    /// connection stays open for the next frame: `HeaderOutcome::TooLarge`
    /// must `continue` the loop, not close it. This pins the framing
    /// behavior `ConnectionLoop::step`/`read_header` implement, across
    /// the boundary from one frame to the next.
    #[tokio::test]
    async fn oversized_frame_gets_a_reply_and_the_connection_stays_open() {
        let cfg = IngressConfig::default();
        let (mock, _rx) = MockChannels::new(std::num::NonZeroUsize::new(8).unwrap());
        let proxy = IngressProxy::new(cfg, mock.clone(), mock);
        let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = TcpListener::bind(bind).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let p2 = proxy.clone();
        tokio::spawn(async move {
            let (sock, peer) = listener.accept().await.unwrap();
            ConnectionLoop::new(sock, peer.ip(), p2)
                .run()
                .await
                .unwrap();
        });
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();

        // First frame: a declared length past MAX_FRAME_BYTES. No body
        // bytes follow it; a frame this large is rejected on the header
        // alone.
        let too_large = u32::try_from(MAX_FRAME_BYTES + 1).unwrap();
        client.write_all(&too_large.to_be_bytes()).await.unwrap();
        assert_eq!(read_reply_status(&mut client).await, STATUS_DECODE);

        // Second frame, on the same connection: an empty RLP list, same
        // as `empty_rlp_returns_decode_error`. If the loop had ended the
        // connection instead of continuing, this reply would never
        // arrive and the test would hang until its own timeout.
        client.write_all(&1u32.to_be_bytes()).await.unwrap();
        client.write_all(&[0xc0]).await.unwrap();
        assert_eq!(read_reply_status(&mut client).await, STATUS_DECODE);
    }

    /// Read one full reply frame (status, length, payload) and return
    /// just the status, draining the payload so the stream is positioned
    /// at the start of the next reply.
    async fn read_reply_status(client: &mut tokio::net::TcpStream) -> u8 {
        let mut status = [0u8; 1];
        client.read_exact(&mut status).await.unwrap();
        let mut len_buf = [0u8; 4];
        client.read_exact(&mut len_buf).await.unwrap();
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        client.read_exact(&mut payload).await.unwrap();
        status[0]
    }
}
