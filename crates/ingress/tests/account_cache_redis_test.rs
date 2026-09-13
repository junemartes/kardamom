//! The admission checks and the account RPCs with the Redis layer on,
//! against a real Redis container. The local layer stays empty, so every
//! read goes to Redis: the case of an ingress that just restarted.
//!
//! Run with `cargo test -p kardamom-ingress --features docker-e2e,test-support
//! --test account_cache_redis_test -- --ignored`.

#![cfg(feature = "docker-e2e")]

use std::num::NonZeroUsize;
use std::time::Duration;

use alloy_primitives::{Address, U256};
use alloy_signer_local::PrivateKeySigner;

use kardamom_cache::{AccountCache, CacheConfig};
use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::error::IngressError;
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
    cache: AccountCache,
    _shard_rx: Vec<tokio::sync::mpsc::UnboundedReceiver<kardamom_types::TxEnvelope>>,
}

impl Fixture {
    async fn new(cfg: &CacheConfig) -> Self {
        let cache = AccountCache::connect(cfg).await.expect("connect");
        let (mock, shard_rx) = MockChannels::new(SHARDS);
        let ingress = IngressConfig {
            cache: cfg.clone(),
            ..IngressConfig::default()
        };
        let proxy = IngressProxy::new(ingress, mock.clone(), mock);
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
            cache,
            _shard_rx: shard_rx,
        }
    }

    /// Write one account row at position 10 and publish a live head at
    /// the same position, so the balance is fresh.
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
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
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
