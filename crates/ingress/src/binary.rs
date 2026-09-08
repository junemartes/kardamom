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
use std::path::Path;

use alloy_primitives::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};
use tokio::task::JoinHandle;

use crate::channels::{IngressPublication, IngressSubscription, ProxyBackend};
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
/// at the wire boundary, so `handle_connection` never re-checks it.
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

/// Every stream `handle_connection` reads and writes frames on.
pub(crate) trait AsyncStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin {}

impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin> AsyncStream for T {}

/// A listener that accepts a stream of [`AsyncStream`] connections, each
/// paired with the client IP the connection's rate-limit key should use.
/// [`accept_loop`] is generic over this, so the TCP and UDS listeners
/// share one accept-and-dispatch loop instead of two copies of it.
trait Accept {
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

/// Accepts connections from `listener` until it errors, spawning one
/// [`handle_connection`] task per connection.
async fn accept_loop<L, P, S>(listener: L, proxy: IngressProxy<P, S>) -> std::io::Result<()>
where
    L: Accept,
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    loop {
        let (sock, client_ip) = listener.accept_one().await?;
        let proxy = proxy.clone();
        tokio::spawn(async move {
            let _ = handle_connection::<_, (P, S)>(sock, client_ip, proxy).await;
        });
    }
}

pub(crate) fn spawn_tcp_listener<P, S>(
    proxy: IngressProxy<P, S>,
    addr: SocketAddr,
) -> JoinHandle<std::io::Result<()>>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    tokio::spawn(async move {
        let listener = TcpListener::bind(addr).await?;
        accept_loop(listener, proxy).await
    })
}

pub(crate) fn spawn_uds_listener<P, S>(
    proxy: IngressProxy<P, S>,
    path: &Path,
) -> std::io::Result<JoinHandle<std::io::Result<()>>>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    // Bind now, so a bind error shows up right away.
    let listener = UnixListener::bind(path)?;
    Ok(tokio::spawn(accept_loop(listener, proxy)))
}

pub(crate) async fn handle_connection<W: AsyncStream, B: ProxyBackend>(
    mut sock: W,
    client_ip: IpAddr,
    proxy: IngressProxy<B::Pub, B::Sub>,
) -> std::io::Result<()> {
    loop {
        let mut len_buf = [0u8; 4];
        if sock.read_exact(&mut len_buf).await.is_err() {
            return Ok(()); // The peer closed the connection.
        }
        let Ok(len) = FrameLen::try_from(u32::from_be_bytes(len_buf)) else {
            write_reply(&mut sock, STATUS_DECODE, b"frame too large").await?;
            continue;
        };
        let mut payload = vec![0u8; len.get()];
        if sock.read_exact(&mut payload).await.is_err() {
            return Ok(());
        }
        let raw = Bytes::from(payload);
        let res = proxy.submit_raw(client_ip, raw).await;
        match res {
            Ok(resp) => write_reply(&mut sock, STATUS_OK, resp.receipt.tx_hash.as_slice()).await?,
            Err(e) => {
                let (status, msg) = map_err(&e);
                write_reply(&mut sock, status, msg.as_bytes()).await?;
            }
        }
    }
}

async fn write_reply<W: AsyncWriteExt + Unpin>(
    sock: &mut W,
    status: u8,
    payload: &[u8],
) -> std::io::Result<()> {
    sock.write_all(&[status]).await?;
    // `payload` is always a 32-byte tx_hash or a short UTF-8 error
    // message, both far below `u32::MAX`. This is a narrowing cast on a
    // length that goes out over the wire, so a payload that ever did
    // violate that invariant fails the write instead of silently
    // truncating the length header and corrupting the frame.
    let len = u32::try_from(payload.len()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "reply payload exceeds u32::MAX",
        )
    })?;
    sock.write_all(&len.to_be_bytes()).await?;
    sock.write_all(payload).await?;
    sock.flush().await
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
        let (mock, _rx) = MockChannels::new(8);
        let proxy = IngressProxy::new(cfg, mock.clone(), mock);
        let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = TcpListener::bind(bind).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let p2 = proxy.clone();
        tokio::spawn(async move {
            let (sock, peer) = listener.accept().await.unwrap();
            handle_connection::<_, (MockChannels, MockChannels)>(sock, peer.ip(), p2)
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
}
