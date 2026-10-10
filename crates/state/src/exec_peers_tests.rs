//! Tests of the exec locator client against the real query endpoint.

use alloy_primitives::{Address, B256};
use kardamom_types::{BPosition, Receipt};

use super::*;
use crate::exec_answers::{ExecAnswers, ExecLocator};
use crate::nonce_query::tests::env_with_receipt;
use crate::nonce_query::{NonceQueryServer, serve_nonce_queries};

fn peer(url: &str) -> ExecPeer {
    url.parse().unwrap()
}

#[test]
fn a_peer_url_is_http_host_and_port() {
    let parsed = peer("http://executor-1.node.dc1.consul:9024");
    assert_eq!(parsed.url(), "http://executor-1.node.dc1.consul:9024");
    assert_eq!(parsed.authority, "executor-1.node.dc1.consul:9024");
    assert!("executor-1:9024".parse::<ExecPeer>().is_err());
    assert!("http://executor-1".parse::<ExecPeer>().is_err());
    assert!("http://executor-1:9024/rpc".parse::<ExecPeer>().is_err());
}

#[test]
fn an_executor_does_not_ask_itself() {
    let listed = vec![peer("http://a:1"), peer("http://b:1"), peer("http://c:1")];
    let own = peer("http://b:1");
    let peers = ExecPeers::new(listed, Some(&own), DEFAULT_PEER_TIMEOUT);
    let urls: Vec<&str> = peers.peers().iter().map(ExecPeer::url).collect();
    assert_eq!(urls, vec!["http://a:1", "http://c:1"]);
}

/// A query server over a state with one committed block that holds a
/// receipt at index 4 and ends at index 5. With answers, the run starts
/// at 5, passes 5 and 6, and parks at 7; the locator log names index 0
/// and index 5.
fn server(answers: bool) -> (NonceQueryServer, tempfile::TempDir) {
    let receipt = Receipt {
        tx_idx: BPosition::from_index(4),
        tx_hash: B256::repeat_byte(0xBE),
        status: true,
        gas_used: 21_000,
        nonce: 9,
        from: Address::repeat_byte(0x11),
        block_number: 1,
        ..Receipt::default()
    };
    let dir = tempfile::tempdir().unwrap();
    let env = env_with_receipt(dir.path(), &receipt);
    let lookup = answers.then(|| {
        let (feed, lookup) = ExecAnswers::spawn("executor-0".to_owned()).unwrap();
        feed.located(ExecLocator {
            index: 0,
            session_id: 3,
            position: 0,
        });
        feed.first(5);
        feed.located(ExecLocator {
            index: 5,
            session_id: 4,
            position: 640,
        });
        feed.passed(6);
        feed.parked(7);
        lookup
    });
    let server = serve_nonce_queries("127.0.0.1:0".parse().unwrap(), env, lookup).unwrap();
    (server, dir)
}

async fn ask(server: &NonceQueryServer, index: u64) -> Result<ExecLocatorAnswer, PeerError> {
    let peer = peer(&format!("http://{}", server.addr));
    tokio::task::spawn_blocking(move || peer.ask(index, B256::repeat_byte(1), DEFAULT_PEER_TIMEOUT))
        .await
        .unwrap()
}

fn located(session_id: i32, position: i64) -> ExecLocatorAnswer {
    ExecLocatorAnswer::Located {
        archive_id: "executor-0".to_owned(),
        session_id,
        position,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_exec_locator_method_round_trips() {
    let (server, _dir) = server(true);
    // Committed with a receipt: located by the older session.
    assert_eq!(ask(&server, 4).await.unwrap(), located(3, 0));
    // Committed with no receipt: a vacant slot.
    assert_eq!(ask(&server, 3).await.unwrap(), ExecLocatorAnswer::NotHeld);
    // Joined in the current run.
    assert_eq!(ask(&server, 6).await.unwrap(), located(4, 640));
    // Parked at, then not reached.
    assert_eq!(ask(&server, 7).await.unwrap(), ExecLocatorAnswer::NotHeld);
    assert_eq!(
        ask(&server, 8).await.unwrap(),
        ExecLocatorAnswer::NotReached
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_committed_receipt_with_no_locator_is_lost() {
    let receipt = Receipt {
        tx_idx: BPosition::from_index(4),
        tx_hash: B256::repeat_byte(0xBE),
        status: true,
        gas_used: 21_000,
        block_number: 1,
        ..Receipt::default()
    };
    let dir = tempfile::tempdir().unwrap();
    let env = env_with_receipt(dir.path(), &receipt);
    let (_feed, lookup) = ExecAnswers::spawn("executor-0".to_owned()).unwrap();
    let server = serve_nonce_queries("127.0.0.1:0".parse().unwrap(), env, Some(lookup)).unwrap();
    assert_eq!(ask(&server, 4).await.unwrap(), ExecLocatorAnswer::Lost);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_with_no_exec_stream_answers_an_error() {
    let (server, _dir) = server(false);
    let err = ask(&server, 4).await.unwrap_err();
    assert!(matches!(err, PeerError::Rpc(_)), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_closed_port_is_no_answer() {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let peer = peer(&format!("http://127.0.0.1:{port}"));
    let err = tokio::task::spawn_blocking(move || {
        peer.ask(1, B256::ZERO, std::time::Duration::from_millis(500))
    })
    .await
    .unwrap()
    .unwrap_err();
    assert!(matches!(err, PeerError::Connect(_)), "{err}");
}
