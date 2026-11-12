//! The batched header read over HTTP: a fake L1 behind the L1 fault proxy,
//! the mock the e2e suites and the chaos-l1 shard read L1 through. A range
//! of headers is one request on the wire, and every header comes back in
//! block order with its parent and its timestamp.

use std::net::SocketAddr;
use std::time::Duration;

use alloy_primitives::B256;
use alloy_provider::ProviderBuilder;
use alloy_rpc_types_eth::{Block, BlockNumberOrTag, Header};
use jsonrpsee::server::{RpcModule, Server, ServerHandle};
use kardamom_da_watcher::sources::{L1Endpoints, SourceHalt};
use kardamom_da_watcher::{L1Source, L1SourceError, RpcL1Source};
use kardamom_l1_fault_proxy::{Fault, FaultProxy};

fn hash(number: u64) -> B256 {
    B256::left_padding_from(&number.to_be_bytes())
}

/// The block a fake L1 serves for `number`: hashes chain, and the block
/// time is 12 s.
fn block(number: u64) -> Block {
    Block::<alloy_rpc_types_eth::Transaction> {
        header: Header {
            hash: hash(number),
            inner: alloy_consensus::Header {
                number,
                parent_hash: hash(number.saturating_sub(1)),
                timestamp: 1_000 + 12 * number,
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    }
}

async fn fake_l1() -> (SocketAddr, ServerHandle) {
    let server = Server::builder().build("127.0.0.1:0").await.unwrap();
    let addr = server.local_addr().unwrap();
    let mut module = RpcModule::new(());
    module
        .register_method("eth_getBlockByNumber", |params, (), _| {
            let (tag, _full): (BlockNumberOrTag, bool) = params.parse().unwrap();
            let number = tag.as_number().unwrap_or(10);
            serde_json::to_value(block(number)).unwrap()
        })
        .unwrap();
    (addr, server.start(module))
}

#[tokio::test]
async fn unresponsive_sources_cannot_hold_the_follower_read_forever() {
    let (l1, _server) = fake_l1().await;
    let proxy = FaultProxy::spawn(&format!("http://{l1}"), "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let sources = L1Endpoints {
        rpcs: vec![proxy.url(), format!("{}/second", proxy.url())],
        light_client: None,
    }
    .connect()
    .await
    .unwrap()
    .backoff(Duration::from_secs(2));
    assert_eq!(sources.finalized_block_number().await.unwrap(), 10);
    proxy.set_fault(Fault::Unresponsive { all_sources: true });
    let result = ReadClock::expire(&proxy, sources.finalized_block_number()).await;
    assert!(matches!(
        result,
        Err(L1SourceError::Halt(SourceHalt::NoQuorum { .. }))
    ));
    proxy.set_fault(Fault::None);
    let requests = proxy.served();
    assert!(matches!(
        sources.finalized_block_number().await,
        Err(L1SourceError::Halt(SourceHalt::NoQuorum { .. }))
    ));
    assert_eq!(
        proxy.served(),
        requests,
        "the backoff sends no provider requests"
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(sources.finalized_block_number().await.is_err());
    assert_eq!(
        proxy.served(),
        requests,
        "the backoff still holds at one second"
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::time::resume();
    assert_eq!(sources.finalized_block_number().await.unwrap(), 10);
}

struct ReadClock;

impl ReadClock {
    async fn expire<F: std::future::Future>(proxy: &FaultProxy, read: F) -> F::Output {
        tokio::time::pause();
        let expected = proxy.served().checked_add(2).unwrap();
        let started = std::time::Instant::now();
        tokio::pin!(read);
        // Yielding stays runnable, so virtual time cannot jump ahead of real socket delivery.
        while proxy.served() < expected {
            Self::poll_request(read.as_mut(), started).await;
        }
        tokio::time::advance(Duration::from_secs(29)).await;
        assert!(
            futures::poll!(read.as_mut()).is_pending(),
            "the production deadline must not be shortened"
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::time::timeout(Duration::from_secs(1), read)
            .await
            .expect("the production deadline must return control to the follower")
    }

    async fn poll_request<F: std::future::Future>(
        mut read: std::pin::Pin<&mut F>,
        started: std::time::Instant,
    ) {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "both requests must reach the fault proxy"
        );
        assert!(futures::poll!(read.as_mut()).is_pending());
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn a_range_of_headers_is_one_request() {
    let (l1, _server) = fake_l1().await;
    let proxy = FaultProxy::spawn(&format!("http://{l1}"), "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let provider = ProviderBuilder::new().connect(&proxy.url()).await.unwrap();
    let source = RpcL1Source::new(provider);

    let headers = source.headers(101, 132).await.unwrap();
    assert_eq!(proxy.served(), 1, "32 headers ride one batch request");
    assert_eq!(headers.len(), 32);
    headers.iter().zip(101..).for_each(|(header, number)| {
        assert_eq!(header.number, number);
        assert_eq!(header.hash, hash(number));
        assert_eq!(header.parent_hash, hash(number - 1));
        assert_eq!(header.timestamp, 1_000 + 12 * number);
    });
}

/// A lie of the proxy reaches the header of the batch it touches: the
/// follower's chain check then sees the broken link.
#[tokio::test]
async fn a_lie_in_the_batch_reaches_the_header() {
    let (l1, _server) = fake_l1().await;
    let proxy = FaultProxy::spawn(&format!("http://{l1}"), "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    proxy.set_fault(Fault::BrokenParentChain { from_block: 110 });
    let provider = ProviderBuilder::new().connect(&proxy.url()).await.unwrap();
    let headers = RpcL1Source::new(provider).headers(105, 115).await.unwrap();
    let broken: Vec<u64> = headers
        .windows(2)
        .filter(|w| w[1].parent_hash != w[0].hash)
        .map(|w| w[1].number)
        .collect();
    assert!(!broken.is_empty(), "the proxy's lie shows in the batch");
    assert!(broken.iter().all(|n| *n >= 110), "{broken:?}");
}
