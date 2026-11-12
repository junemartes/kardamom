use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};

use super::{EgressTransport, PAGE_TIMEOUT};
use crate::reader::carried::transport::{Request, Transport};

const REQUEST: Request = Request {
    from_index: 0,
    from_block: 1,
    request_id: 1,
};
const BOUND: Duration = Duration::from_secs(2);

/// The peer accepts headers and holds the response until the client leaves.
struct HeldServer {
    base: String,
    requests: mpsc::UnboundedReceiver<oneshot::Receiver<()>>,
    task: JoinHandle<()>,
}

impl HeldServer {
    async fn start() -> Self {
        Self::with_response_headers(false).await
    }

    async fn with_response_headers(response_headers: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let (opened, requests) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut handlers = JoinSet::new();
            loop {
                Self::accept(&listener, &opened, response_headers, &mut handlers).await;
            }
        });
        Self {
            base,
            requests,
            task,
        }
    }

    async fn accept(
        listener: &TcpListener,
        opened: &mpsc::UnboundedSender<oneshot::Receiver<()>>,
        response_headers: bool,
        handlers: &mut JoinSet<()>,
    ) {
        let (socket, _) = listener.accept().await.unwrap();
        handlers.spawn(Self::hold(socket, opened.clone(), response_headers));
    }

    async fn hold(
        mut socket: TcpStream,
        opened: mpsc::UnboundedSender<oneshot::Receiver<()>>,
        response_headers: bool,
    ) {
        let mut headers = Vec::new();
        while !headers.windows(4).any(|part| part == b"\r\n\r\n") {
            Self::read_headers(&mut socket, &mut headers).await;
        }
        if response_headers {
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nx-egress-after-index: 0\r\nx-egress-after-block: 1\r\nx-egress-next-index: 0\r\nx-egress-next-block: 1\r\n\r\n").await.unwrap();
        }
        let (closed, gone) = oneshot::channel();
        opened.send(gone).unwrap();
        let mut byte = [0];
        assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
        let _ = closed.send(());
    }

    async fn read_headers(socket: &mut TcpStream, headers: &mut Vec<u8>) {
        let mut buf = [0; 1024];
        let count = socket.read(&mut buf).await.unwrap();
        assert_ne!(count, 0);
        headers.extend_from_slice(&buf[..count]);
    }

    fn transport(&self) -> EgressTransport {
        let (_, live) = crossbeam_channel::unbounded();
        EgressTransport::new(self.base.clone(), live, tokio::runtime::Handle::current())
    }

    async fn accepted(&mut self) -> oneshot::Receiver<()> {
        tokio::time::timeout(BOUND, self.requests.recv())
            .await
            .expect("the peer receives a request")
            .unwrap()
    }
}

impl Drop for HeldServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn assert_closed(gone: oneshot::Receiver<()>) {
    tokio::time::timeout(BOUND, gone)
        .await
        .expect("an abandoned replay must close its HTTP request")
        .unwrap();
}

#[tokio::test]
async fn closing_a_source_cancels_its_pending_range_read() {
    let mut peer = HeldServer::start().await;
    let mut transport = peer.transport();
    transport.request(REQUEST);
    let gone = peer.accepted().await;
    transport.close();
    assert_closed(gone).await;
}

#[tokio::test]
async fn a_new_replay_cancels_the_previous_request() {
    let mut peer = HeldServer::start().await;
    let mut transport = peer.transport();
    transport.request(REQUEST);
    let first = peer.accepted().await;
    transport.request(Request {
        request_id: 2,
        ..REQUEST
    });
    let second = peer.accepted().await;
    assert_closed(first).await;
    transport.close();
    assert_closed(second).await;
}

#[tokio::test]
async fn ending_a_reader_cancels_its_pending_range_read() {
    let mut peer = HeldServer::start().await;
    let gone = {
        let mut transport = peer.transport();
        transport.request(REQUEST);
        peer.accepted().await
    };
    assert_closed(gone).await;
}

#[tokio::test]
async fn a_held_range_read_expires_and_renews_the_connection() {
    let mut peer = HeldServer::start().await;
    assert_page_deadline(&mut peer).await;
}

#[tokio::test]
async fn response_headers_do_not_remove_the_page_body_deadline() {
    let mut peer = HeldServer::with_response_headers(true).await;
    assert_page_deadline(&mut peer).await;
}

async fn assert_page_deadline(peer: &mut HeldServer) {
    let mut transport = peer.transport();
    transport.request(REQUEST);
    let gone = peer.accepted().await;
    tokio::time::timeout(PAGE_TIMEOUT + BOUND, gone)
        .await
        .expect("a peer cannot hold the page past its deadline")
        .unwrap();
    assert_eq!(transport.connection(), Some(2));
}
