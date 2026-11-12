//! The real Redis client reads controlled RESP replies over a loopback connection.

use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Duration, timeout};

use super::*;
use crate::cases::cache::test_support::Commands;

const MASTER: &str = "*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n";
const REPLICA: &str = "*1\r\n$5\r\nslave\r\n";

struct Request {
    command: Vec<String>,
    reply: oneshot::Sender<String>,
}

impl Request {
    fn answer(self, response: &str) {
        self.reply.send(response.into()).unwrap();
    }
}

struct Peer {
    requests: mpsc::Receiver<Request>,
    server: JoinHandle<()>,
    read: JoinHandle<anyhow::Result<Snapshot>>,
}

impl Peer {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (requests, receive) = mpsc::channel(4);
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut stream = BufReader::new(socket);
            while Self::step(&mut stream, &requests).await.is_some() {}
        });
        Self {
            requests: receive,
            server,
            read: tokio::spawn(Snapshot::read("primary", address, Address::ZERO)),
        }
    }

    async fn step(
        stream: &mut BufReader<TcpStream>,
        requests: &mpsc::Sender<Request>,
    ) -> Option<()> {
        let command = Commands::read(stream).await?;
        let response = if command[0] == "CLIENT" {
            "+OK\r\n".into()
        } else {
            let (reply, receive) = oneshot::channel();
            requests.send(Request { command, reply }).await.ok()?;
            receive.await.ok()?
        };
        stream.get_mut().write_all(response.as_bytes()).await.ok()
    }

    async fn next(&mut self, expected: &[&str]) -> Request {
        let request = timeout(Duration::from_secs(5), self.requests.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request.command, expected);
        request
    }

    async fn exec(&mut self) -> Request {
        self.next(&["ROLE"]).await.answer(MASTER);
        self.next(&["MULTI"]).await.answer("+OK\r\n");
        self.next(&["ROLE"]).await.answer("+QUEUED\r\n");
        self.next(&["MGET", "head:0", "head:1", "head:2"])
            .await
            .answer("+QUEUED\r\n");
        self.next(&[
            "HMGET",
            "acct:0x0000000000000000000000000000000000000000",
            "nonce",
            "balance",
            "tx_idx",
        ])
        .await
        .answer("+QUEUED\r\n");
        self.next(&["EXEC"]).await
    }

    async fn result(&mut self) -> anyhow::Result<Snapshot> {
        timeout(Duration::from_secs(5), &mut self.read)
            .await
            .unwrap()
            .unwrap()
    }

    fn reply(role: &str, missing_head: bool) -> String {
        let heads = if missing_head {
            "*3\r\n$20\r\n00000000000000000010\r\n$-1\r\n$20\r\n00000000000000000030\r\n"
        } else {
            "*3\r\n$20\r\n00000000000000000010\r\n$20\r\n00000000000000000020\r\n$20\r\n00000000000000000030\r\n"
        };
        format!("*3\r\n{role}{heads}*3\r\n$1\r\n7\r\n$3\r\n123\r\n$2\r\n42\r\n")
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.read.abort();
        self.server.abort();
    }
}

#[tokio::test]
async fn the_real_client_reads_heads_and_account_in_one_primary_transaction() {
    let mut peer = Peer::new().await;
    peer.exec().await.answer(&Peer::reply(MASTER, false));
    let snapshot = peer.result().await.unwrap();
    assert_eq!(snapshot.primary, "primary");
    assert_eq!(
        snapshot.heads.0.map(std::num::NonZeroU64::get),
        [10, 20, 30]
    );
    assert_eq!(snapshot.row, "7\n123\n42");
}

#[tokio::test]
async fn demotion_after_connect_cannot_certify_the_transaction_snapshot() {
    let mut peer = Peer::new().await;
    peer.exec().await.answer(&Peer::reply(REPLICA, false));
    let error = peer.result().await.unwrap_err();
    assert!(error.to_string().contains("not a primary"));
}

#[tokio::test]
async fn a_missing_head_in_the_real_reply_cannot_certify_coverage() {
    let mut peer = Peer::new().await;
    peer.exec().await.answer(&Peer::reply(MASTER, true));
    let error = peer.result().await.unwrap_err();
    assert!(error.to_string().contains("invalid mirror head"));
}

#[tokio::test]
async fn a_silent_exec_reply_expires_the_real_client_read() {
    let mut peer = Peer::new().await;
    let _held = peer.exec().await;
    tokio::time::pause();
    let started = tokio::time::Instant::now();
    let error = peer.result().await.unwrap_err();
    assert!(error.to_string().contains("timed out"), "{error}");
    // Tokio rounds timer deadlines up to its millisecond resolution.
    assert!(
        started.elapsed() <= Duration::from_millis(3_001),
        "elapsed {:?}",
        started.elapsed()
    );
}
