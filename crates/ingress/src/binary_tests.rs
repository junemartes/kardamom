use std::num::NonZeroUsize;

use alloy_signer_local::PrivateKeySigner;

use super::*;
use crate::channels::MockChannels;
use crate::config::{DEFAULT_MAX_TX_BYTES, IngressConfig};
use crate::test_support::{sign_legacy_sized, spawn_fake_executor};

/// The shard count of every proxy below.
const SHARDS: NonZeroUsize = NonZeroUsize::new(8).unwrap();

/// One served connection: a client stream to a `ConnectionLoop` over a
/// proxy on `MockChannels`. `executor` holds the fake executor tasks,
/// which run only when the test asks for them.
struct Served {
    client: TcpStream,
    executor: Vec<JoinHandle<()>>,
}

impl Served {
    /// Serve one connection on a loopback port. With `execute`, a fake
    /// executor answers every published transaction with a receipt.
    async fn start(execute: bool) -> Self {
        let (mock, rx) = MockChannels::new(SHARDS);
        let executor = if execute {
            spawn_fake_executor(&mock, rx)
        } else {
            Vec::new()
        };
        let proxy = IngressProxy::new(IngressConfig::default(), mock.clone(), mock);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (sock, peer) = listener.accept().await.unwrap();
            ConnectionLoop::new(sock, peer.ip(), proxy)
                .run()
                .await
                .unwrap();
        });
        let client = TcpStream::connect(addr).await.unwrap();
        Self { client, executor }
    }

    /// Write one frame: the length prefix, then `body`.
    async fn send(&mut self, body: &[u8]) {
        let len = u32::try_from(body.len()).unwrap();
        self.client.write_all(&len.to_be_bytes()).await.unwrap();
        self.client.write_all(body).await.unwrap();
    }

    /// Read one full reply frame: the status and the payload. The stream
    /// is then at the start of the next reply.
    async fn reply(&mut self) -> (u8, Vec<u8>) {
        let mut status = [0u8; 1];
        self.client.read_exact(&mut status).await.unwrap();
        let mut len_buf = [0u8; 4];
        self.client.read_exact(&mut len_buf).await.unwrap();
        let mut payload = vec![0u8; u32::from_be_bytes(len_buf) as usize];
        self.client.read_exact(&mut payload).await.unwrap();
        (status[0], payload)
    }
}

impl Drop for Served {
    fn drop(&mut self) {
        self.executor.iter().for_each(JoinHandle::abort);
    }
}

#[tokio::test]
async fn empty_rlp_returns_decode_error() {
    let mut served = Served::start(false).await;
    // Send an empty RLP list, `0xc0`.
    served.send(&[0xc0]).await;
    assert_eq!(served.reply().await.0, STATUS_DECODE);
}

/// An over-length frame header gets a decode-error reply, and the
/// connection stays open for the next frame: `HeaderOutcome::TooLarge`
/// must `continue` the loop, not close it. This pins the framing
/// behavior `ConnectionLoop::step`/`read_header` implement, across
/// the boundary from one frame to the next.
#[tokio::test]
async fn oversized_frame_gets_a_reply_and_the_connection_stays_open() {
    let mut served = Served::start(false).await;

    // First frame: a declared length past MAX_FRAME_BYTES. No body
    // bytes follow it; a frame this large is rejected on the header
    // alone.
    let too_large = u32::try_from(MAX_FRAME_BYTES + 1).unwrap();
    served
        .client
        .write_all(&too_large.to_be_bytes())
        .await
        .unwrap();
    assert_eq!(served.reply().await.0, STATUS_DECODE);

    // Second frame, on the same connection: an empty RLP list, same
    // as `empty_rlp_returns_decode_error`. If the loop had ended the
    // connection instead of continuing, this reply would never
    // arrive and the test would hang until its own timeout.
    served.send(&[0xc0]).await;
    assert_eq!(served.reply().await.0, STATUS_DECODE);
}

/// A transaction of exactly the size cap gets its hash. One byte more
/// gets a decode-status reply that states the size and the cap, and the
/// connection stays open.
#[tokio::test(flavor = "multi_thread")]
async fn the_size_cap_accepts_the_cap_and_refuses_one_byte_more() {
    let mut served = Served::start(true).await;
    let signer = PrivateKeySigner::random();

    served
        .send(&sign_legacy_sized(&signer, 0, DEFAULT_MAX_TX_BYTES.get()))
        .await;
    let (status, payload) = served.reply().await;
    assert_eq!(status, STATUS_OK, "{}", String::from_utf8_lossy(&payload));
    assert_eq!(payload.len(), 32);

    served
        .send(&sign_legacy_sized(
            &signer,
            1,
            DEFAULT_MAX_TX_BYTES.get() + 1,
        ))
        .await;
    let (status, payload) = served.reply().await;
    assert_eq!(status, STATUS_DECODE);
    assert_eq!(
        String::from_utf8(payload).unwrap(),
        "oversized data: transaction size 131073 bytes, limit 131072 bytes"
    );
}
