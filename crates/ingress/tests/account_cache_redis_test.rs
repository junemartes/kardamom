//! The admission checks and the account RPCs with the Redis layer on,
//! against a real Redis container. The local layer stays empty, so every
//! read goes to Redis: the case of an ingress that just restarted.
//!
//! Run with `cargo test -p kardamom-ingress --features docker-e2e,test-support
//! --test account_cache_redis_test -- --ignored`.

#![cfg(feature = "docker-e2e")]

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use alloy_primitives::{Address, U256};
use alloy_rpc_types_eth::BlockNumberOrTag;
use alloy_signer_local::PrivateKeySigner;
use jsonrpsee::server::{RpcModule, Server, ServerHandle};

use kardamom_cache::{AccountCache, CacheConfig, ExecutorQueryConfig};
use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::error::IngressError;
use kardamom_ingress::proxy::AccountField;
use kardamom_ingress::test_support::{receipt, sign_legacy};
use kardamom_ingress::{IngressProxy, MockChannels};
use kardamom_types::{AccountRow, BPosition};

const SHARDS: NonZeroUsize = NonZeroUsize::new(8).unwrap();
const IP: std::net::IpAddr = std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 4, 0, 1));
/// The cost of the `sign_legacy` shape: 21,000 gas at 1 gwei, no value.
const LEGACY_COST: u64 = 21_000 * 1_000_000_000;

/// A proxy with `[cache]` on, its Redis client, and the shard receivers
/// a test holds so a publish never sees a closed lane.
struct Fixture {
    proxy: IngressProxy<MockChannels, MockChannels>,
    mock: MockChannels,
    cache: AccountCache,
    shard_rx: Vec<tokio::sync::mpsc::UnboundedReceiver<kardamom_types::TxEnvelope>>,
}

impl Fixture {
    fn advance_local_head(&self) {
        let _ = self.mock.apply_rows(
            BPosition::from_index(100),
            &[AccountRow {
                address: Address::repeat_byte(0xee),
                nonce: 0,
                balance: U256::ZERO,
            }],
        );
    }

    async fn fields(&self, address: Address) -> (U256, U256) {
        let nonce = self
            .proxy
            .account_field(IP, address, BlockNumberOrTag::Latest, AccountField::Nonce)
            .await
            .unwrap();
        let balance = self
            .proxy
            .account_field(IP, address, BlockNumberOrTag::Latest, AccountField::Balance)
            .await
            .unwrap();
        (nonce, balance)
    }

    async fn new(cfg: &CacheConfig) -> Self {
        Self::with_query(cfg, ExecutorQueryConfig::default()).await
    }

    async fn with_query(cfg: &CacheConfig, executor_query: ExecutorQueryConfig) -> Self {
        let cache = AccountCache::connect(cfg).await.expect("connect");
        let (mock, shard_rx) = MockChannels::new(SHARDS);
        let ingress = IngressConfig {
            cache: cfg.clone(),
            executor_query,
            ..IngressConfig::default()
        };
        let proxy = IngressProxy::new(ingress, mock.clone(), mock.clone());
        // The reader connects in the background.
        for _ in 0..50 {
            if proxy.redis_connected() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(proxy.redis_connected(), "the reader connects");
        Self {
            proxy,
            mock,
            cache,
            shard_rx,
        }
    }

    /// Write one account row at position 10 and publish a live head at
    /// the same position. The local layer sees a frame at position 10 for
    /// another account, so it has a head, and the balance is fresh.
    async fn seed(&self, address: Address, nonce: u64, balance: u64) {
        let row = AccountRow {
            address,
            nonce,
            balance: U256::from(balance),
        };
        self.cache
            .write_rows(BPosition::from_index(10), &[row])
            .await
            .unwrap();
        self.cache
            .set_head(0, BPosition::from_index(10))
            .await
            .unwrap();
        let _ = self.mock.apply_rows(
            BPosition::from_index(10),
            &[AccountRow {
                address: Address::repeat_byte(0xee),
                nonce: 0,
                balance: U256::ZERO,
            }],
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

struct QueryServer {
    endpoint: String,
    calls: Arc<AtomicUsize>,
    handle: ServerHandle,
}

impl QueryServer {
    async fn start() -> Self {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut module = RpcModule::new(calls.clone());
        module
            .register_method("eth_getTransactionCount", |_, calls, _| {
                calls.fetch_add(1, Ordering::Relaxed);
                "0x5"
            })
            .unwrap();
        module
            .register_method("eth_getBalance", |_, calls, _| {
                calls.fetch_add(1, Ordering::Relaxed);
                "0x2a"
            })
            .unwrap();
        let server = Server::builder().build("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", server.local_addr().unwrap());
        Self {
            endpoint,
            calls,
            handle: server.start(module),
        }
    }

    fn config(&self) -> ExecutorQueryConfig {
        ExecutorQueryConfig {
            endpoints: vec![self.endpoint.clone()],
            ..ExecutorQueryConfig::default()
        }
    }
}

impl Drop for QueryServer {
    fn drop(&mut self) {
        let _ = self.handle.stop();
    }
}

#[tokio::test]
#[ignore = "requires Docker; run with --features docker-e2e -- --ignored"]
async fn fresh_redis_and_fresh_local_rows_do_not_query_executors() {
    let (_container, mut cfg) = kardamom_cache::testing::redis().await;
    cfg.max_stale_txs = NonZeroU64::new(8).unwrap();
    let query = QueryServer::start().await;
    let fixture = Fixture::with_query(&cfg, query.config()).await;
    let address = Address::repeat_byte(0xab);
    fixture.seed(address, 3, 123).await;
    assert_eq!(
        fixture.fields(address).await,
        (U256::from(3), U256::from(123))
    );
    let _ = fixture.mock.apply_rows(
        BPosition::from_index(100),
        &[AccountRow {
            address,
            nonce: 4,
            balance: U256::from(456),
        }],
    );
    assert_eq!(
        fixture.fields(address).await,
        (U256::from(4), U256::from(456))
    );
    assert_eq!(query.calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
#[ignore = "requires Docker; run with --features docker-e2e -- --ignored"]
async fn a_stale_projection_without_an_executor_cannot_answer_a_head_rpc() {
    let (_container, mut cfg) = kardamom_cache::testing::redis().await;
    cfg.max_stale_txs = NonZeroU64::new(8).unwrap();
    let fixture = Fixture::new(&cfg).await;
    let address = Address::repeat_byte(0xab);
    fixture.seed(address, 3, 0).await;
    fixture.advance_local_head();
    let err = fixture
        .proxy
        .account_field(IP, address, BlockNumberOrTag::Latest, AccountField::Nonce)
        .await
        .unwrap_err();
    assert!(matches!(err, IngressError::StateUnavailable(_)), "{err:?}");
}

#[tokio::test]
#[ignore = "requires Docker; run with --features docker-e2e -- --ignored"]
async fn validated_admission_keeps_stale_nonce_lower_bounds_without_querying() {
    let (_container, mut cfg) = kardamom_cache::testing::redis().await;
    cfg.max_stale_txs = NonZeroU64::new(8).unwrap();
    let query = QueryServer::start().await;
    let fixture = Fixture::with_query(&cfg, query.config()).await;
    let signer = PrivateKeySigner::random();
    fixture.seed(signer.address(), 3, 0).await;
    fixture.advance_local_head();
    let err = fixture
        .proxy
        .submit_validated(IP, sign_legacy(&signer, 2))
        .await
        .unwrap_err();
    assert!(matches!(err, IngressError::NonceTooLow { .. }), "{err:?}");
    assert_eq!(query.calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
#[ignore = "requires Docker; run with --features docker-e2e -- --ignored"]
async fn account_rpcs_use_the_executor_while_a_live_mirror_head_lags() {
    let (_container, mut cfg) = kardamom_cache::testing::redis().await;
    cfg.max_stale_txs = NonZeroU64::new(8).unwrap();
    cfg.head_ttl_secs = NonZeroU64::new(60).unwrap();
    let query = QueryServer::start().await;
    let fixture = Fixture::with_query(
        &cfg,
        ExecutorQueryConfig {
            endpoints: vec![query.endpoint.clone()],
            ..ExecutorQueryConfig::default()
        },
    )
    .await;
    let address = Address::repeat_byte(0xab);
    fixture.seed(address, 3, 0).await;
    let _ = fixture.mock.apply_rows(
        BPosition::from_index(100),
        &[AccountRow {
            address: Address::repeat_byte(0xee),
            nonce: 0,
            balance: U256::ZERO,
        }],
    );
    let nonce = fixture
        .proxy
        .account_field(IP, address, BlockNumberOrTag::Latest, AccountField::Nonce)
        .await
        .unwrap();
    let balance = fixture
        .proxy
        .account_field(IP, address, BlockNumberOrTag::Latest, AccountField::Balance)
        .await
        .unwrap();
    assert_eq!((nonce, balance), (U256::from(5), U256::from(42)));
    assert_eq!(query.calls.load(Ordering::Relaxed), 2);
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --features docker-e2e -- --ignored`"]
async fn a_retry_of_a_landed_tx_answers_from_the_receipt_index() {
    let (_c, cfg) = kardamom_cache::testing::redis().await;
    let f = Fixture::new(&cfg).await;
    let signer = PrivateKeySigner::random();
    let raw = sign_legacy(&signer, 0);
    let tx_hash = alloy_primitives::keccak256(&raw);
    f.seed(signer.address(), 1, LEGACY_COST * 10).await;
    f.cache
        .write_receipts(&[receipt(signer.address(), 0, tx_hash, 10)])
        .await
        .unwrap();

    // The local receipt cache is empty: the retry answers from Redis.
    let got = f.proxy.submit_raw_async(IP, raw).await.expect("landed");
    assert_eq!(got, tx_hash);
    // A different tx at the landed nonce is a duplicate.
    let err = f
        .proxy
        .submit_raw_async(IP, sign_legacy_value(&signer, 0, 1))
        .await
        .unwrap_err();
    assert!(matches!(err, IngressError::Duplicate(_)), "{err:?}");
    // The next nonce publishes.
    f.proxy
        .submit_raw_async(IP, sign_legacy(&signer, 1))
        .await
        .expect("the next nonce publishes");
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --features docker-e2e -- --ignored`"]
async fn an_unfunded_sender_rejects_only_while_the_head_is_fresh() {
    let (_c, cfg) = kardamom_cache::testing::redis().await;
    let f = Fixture::new(&cfg).await;
    let signer = PrivateKeySigner::random();
    f.seed(signer.address(), 0, LEGACY_COST - 1).await;
    let err = f
        .proxy
        .submit_raw_async(IP, sign_legacy(&signer, 0))
        .await
        .unwrap_err();
    assert!(
        matches!(err, IngressError::InsufficientFunds { .. }),
        "{err:?}"
    );

    // The head ages out (its TTL is 5 s): the balance is no longer fresh
    // and the same submit admits.
    tokio::time::sleep(Duration::from_secs(6)).await;
    f.proxy
        .submit_raw_async(IP, sign_legacy(&signer, 0))
        .await
        .expect("no live head: the balance check is skipped");
}

#[tokio::test]
#[ignore = "requires Docker; run with `cargo test --features docker-e2e -- --ignored`"]
async fn a_past_nonce_with_no_indexed_receipt_publishes() {
    let (_c, cfg) = kardamom_cache::testing::redis().await;
    let f = Fixture::new(&cfg).await;
    let signer = PrivateKeySigner::random();
    f.seed(signer.address(), 3, LEGACY_COST * 10).await;
    // The projection knows nonce 3, but no index holds a receipt for
    // nonce 2: the receipt index can lag the account rows. The sequencer
    // decides.
    f.proxy
        .submit_raw_async(IP, sign_legacy(&signer, 2))
        .await
        .expect("a past nonce with no receipt publishes");
    // An unknown sender admits, with Redis on as with it off.
    let other = PrivateKeySigner::random();
    f.proxy
        .submit_raw_async(IP, sign_legacy(&other, 7))
        .await
        .expect("no entry admits");
}

/// A legacy tx with a value, so its hash differs from `sign_legacy`'s.
fn sign_legacy_value(signer: &PrivateKeySigner, nonce: u64, value: u64) -> alloy_primitives::Bytes {
    use alloy_consensus::TxLegacy;
    use kardamom_ingress::test_support::sign_and_encode;
    let tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 1_000_000_000,
        gas_limit: 21_000,
        to: alloy_primitives::TxKind::Call(Address::ZERO),
        value: U256::from(value),
        input: alloy_primitives::Bytes::default(),
    };
    sign_and_encode(tx, signer).1
}

#[path = "account_cache_redis_test/profile.rs"]
mod profile;
