//! Length-prefixed RLP binary line protocol over TCP and Unix Domain
//! Sockets.
//!
//! Frame format (big-endian):
//! - `u32 len` — payload length in bytes (≤ 1 MiB)
//! - `len` bytes — RLP-encoded `TxEnvelope`
//!
//! Reply frame:
//! - `u8 status` — 0=ok, 1=rate-limited, 2=decode, 3=sig, 4=timeout,
//!   5=duplicate, 9=internal
//! - `u32 payload_len`
//! - `payload_len` bytes — on ok, the 32-byte `tx_hash`; on error, the
//!   UTF-8 error message.

#![cfg(feature = "binary-protocol")]

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;

use alloy_primitives::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UnixListener};
use tokio::task::JoinHandle;

use crate::channels::{IngressPublication, IngressSubscription};
use crate::error::IngressError;
use crate::proxy::IngressProxy;

pub const STATUS_OK: u8 = 0;
pub const STATUS_RATE_LIMITED: u8 = 1;
pub const STATUS_DECODE: u8 = 2;
pub const STATUS_SIG: u8 = 3;
pub const STATUS_TIMEOUT: u8 = 4;
pub const STATUS_DUPLICATE: u8 = 5;
pub const STATUS_INTERNAL: u8 = 9;

pub const MAX_FRAME_BYTES: usize = 1024 * 1024;

pub fn spawn_tcp_listener<P, S>(
    proxy: IngressProxy<P, S>,
    addr: SocketAddr,
) -> JoinHandle<std::io::Result<()>>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    tokio::spawn(async move {
        let listener = TcpListener::bind(addr).await?;
        loop {
            let (sock, peer) = listener.accept().await?;
            let proxy = proxy.clone();
            tokio::spawn(async move {
                let _ = handle_connection(sock, peer.ip(), proxy).await;
            });
        }
    })
}

pub fn spawn_uds_listener<P, S>(
    proxy: IngressProxy<P, S>,
    path: &Path,
) -> std::io::Result<JoinHandle<std::io::Result<()>>>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    // Bind eagerly so binding errors surface immediately.
    let listener = UnixListener::bind(path)?;
    Ok(tokio::spawn(async move {
        loop {
            let (sock, _) = listener.accept().await?;
            let proxy = proxy.clone();
            tokio::spawn(async move {
                // UDS has no IP; use loopback for the rate-limit key.
                let _ = handle_connection(sock, IpAddr::V4(Ipv4Addr::LOCALHOST), proxy).await;
            });
        }
    }))
}

pub async fn handle_connection<W, P, S>(
    mut sock: W,
    client_ip: IpAddr,
    proxy: IngressProxy<P, S>,
) -> std::io::Result<()>
where
    W: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    loop {
        let mut len_buf = [0u8; 4];
        if sock.read_exact(&mut len_buf).await.is_err() {
            return Ok(()); // peer closed
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        if len > MAX_FRAME_BYTES {
            write_reply(&mut sock, STATUS_DECODE, b"frame too large").await?;
            continue;
        }
        let mut payload = vec![0u8; len];
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
    sock.write_all(&(payload.len() as u32).to_be_bytes())
        .await?;
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
            handle_connection(sock, peer.ip(), p2).await.unwrap();
        });
        let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
        // Send an empty RLP list `0xc0`.
        client.write_all(&1u32.to_be_bytes()).await.unwrap();
        client.write_all(&[0xc0]).await.unwrap();
        let mut status = [0u8; 1];
        client.read_exact(&mut status).await.unwrap();
        assert_eq!(status[0], STATUS_DECODE);
    }
}
