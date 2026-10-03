//! The source set's rules, on the scripted mock: agreement, a liar, a
//! rate-limited source, every source out, and the light client.

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use alloy_rpc_types_eth::{Filter, Log};
use kardamom_da_watcher::source::fakes::MockL1Source;
use kardamom_da_watcher::{L1Source, L1SourceError, L1Sources, SourceHalt};
use kardamom_obs::testkit::{free_port, scrape};

/// A set of `n` honest mocks, named `s0..`, with a one-hour backoff.
fn honest(n: usize) -> L1Sources<MockL1Source> {
    L1Sources::new(
        (0..n)
            .map(|i| (format!("s{i}"), MockL1Source::new()))
            .collect(),
    )
    .backoff(Duration::from_secs(3600))
}

fn lying_at(block: u64) -> MockL1Source {
    let liar = MockL1Source::new();
    liar.hashes
        .lock()
        .unwrap()
        .insert(block, B256::repeat_byte(0xEE));
    liar
}

fn rate_limited() -> MockL1Source {
    let source = MockL1Source::new();
    *source.rate_limited.lock().unwrap() = true;
    source
}

/// The set asks every member and returns the shared answer. The tip is
/// the lowest one, so every member has finalized through it.
#[tokio::test]
async fn two_sources_that_agree_serve_the_block_and_the_lowest_tip() {
    let a = MockL1Source::new();
    let b = MockL1Source::new();
    a.push_tip(Ok(12));
    b.push_tip(Ok(10));
    let set = L1Sources::new(vec![("a".into(), a), ("b".into(), b)]);

    assert_eq!(set.finalized_block_number().await.unwrap(), 10);
    let (hash, parent) = set.block_ids(5).await.unwrap();
    assert_eq!(hash, MockL1Source::filler_hash(5));
    assert_eq!(parent, MockL1Source::filler_hash(4));
    assert!(
        set.lockbox_logs(Address::ZERO, 5, 5)
            .await
            .unwrap()
            .is_empty()
    );
}

/// A member with no finalized block yet is the lowest tip: the set has
/// no finalized block either.
#[tokio::test]
async fn a_member_without_a_finalized_block_holds_the_tip_back() {
    let a = MockL1Source::new();
    let b = MockL1Source::new();
    a.push_tip(Ok(12));
    let set = L1Sources::new(vec![("a".into(), a), ("b".into(), b)]);
    assert!(matches!(
        set.finalized_block_number().await,
        Err(L1SourceError::NotFinalized)
    ));
}

/// Two public endpoints that disagree halt the read, every time: neither
/// one outvotes the other, both answers are reported, and the counter
/// grows. The one metrics recorder of this process lives in this test.
#[tokio::test]
async fn one_liar_halts_the_read_with_both_answers_and_the_counter() {
    let addr = free_port();
    kardamom_obs::init("da-watcher", addr, "local", "test", "test")
        .await
        .expect("init");
    let honest_source = MockL1Source::new();
    let liar = lying_at(5);
    let set = L1Sources::new(vec![
        ("honest".into(), honest_source),
        ("liar".into(), liar),
    ]);

    for _ in 0..2 {
        let err = set.block_ids(5).await.unwrap_err();
        let L1SourceError::Halt(halt) = err else {
            panic!("a halt, got {err}");
        };
        assert_eq!(halt.cause(), "l1_source_disagreement");
        let SourceHalt::Disagreement {
            what,
            a_name,
            b_name,
            ..
        } = halt
        else {
            panic!("a disagreement, got {halt}");
        };
        assert_eq!(what, "block 5");
        assert_eq!((a_name.as_str(), b_name.as_str()), ("honest", "liar"));
    }
    // Block 4 is honest on both: the set serves it.
    assert!(set.block_ids(4).await.is_ok());

    let body = scrape(&format!("http://{addr}/metrics")).await;
    let count: u64 = body
        .lines()
        .find(|l| l.starts_with("kardamom_l1_source_disagreement_total"))
        .and_then(|l| l.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
        .expect("the disagreement counter is exposed");
    assert!(count >= 2, "counter {count}");
}

/// A rate-limited source rotates out for the backoff. With two honest
/// sources left, the reads go on, and the rotated source is not asked
/// again inside the backoff.
#[tokio::test]
async fn a_rate_limited_source_rotates_out_and_the_rest_continue() {
    let limited = Arc::new(rate_limited());
    let b = Arc::new(MockL1Source::new());
    let set = L1Sources::new(vec![
        ("limited".into(), limited.clone()),
        ("b".into(), b.clone()),
        ("c".into(), Arc::new(MockL1Source::new())),
    ])
    .backoff(Duration::from_secs(3600));

    assert!(set.block_ids(5).await.is_ok());
    assert!(set.block_ids(6).await.is_ok());
    assert!(set.finalized_block_number().await.is_err());
    assert_eq!(limited.calls(), 1, "asked once, then out");
    assert_eq!(b.calls(), 3, "asked on every read");
}

/// With two sources and one out, the read waits: one public source alone
/// is not trusted, and the follower reports the shortfall until the
/// backoff ends.
#[tokio::test]
async fn one_live_source_of_two_is_short_of_the_quorum() {
    let limited = rate_limited();
    let set = L1Sources::new(vec![
        ("limited".into(), limited),
        ("b".into(), MockL1Source::new()),
    ])
    .backoff(Duration::ZERO);
    let err = set.block_ids(5).await.unwrap_err();
    assert!(
        matches!(
            err,
            L1SourceError::Halt(SourceHalt::NoQuorum {
                answered: 1,
                needed: 2,
                configured: 2
            })
        ),
        "{err}"
    );
}

/// Every source out: the read fails loudly, as a single source does.
#[tokio::test]
async fn every_source_out_halts() {
    let set = L1Sources::new(vec![
        ("a".into(), rate_limited()),
        ("b".into(), rate_limited()),
    ]);
    let err = set.finalized_block_number().await.unwrap_err();
    assert!(
        matches!(
            err,
            L1SourceError::Halt(SourceHalt::NoQuorum { answered: 0, .. })
        ),
        "{err}"
    );
    let err = set.block_ids(5).await.unwrap_err();
    let L1SourceError::Halt(halt) = err else {
        panic!("a halt, got {err}");
    };
    assert_eq!(halt.cause(), "l1_sources_out");
}

/// A rotated source comes back after the backoff: with a zero backoff it
/// is asked on every read, and once it answers again the set is whole.
#[tokio::test]
async fn a_rotated_source_returns_after_the_backoff() {
    let flaky = Arc::new(rate_limited());
    let set = L1Sources::new(vec![
        ("flaky".into(), flaky.clone()),
        ("b".into(), Arc::new(MockL1Source::new())),
    ])
    .backoff(Duration::ZERO);
    assert!(set.block_ids(5).await.is_err());
    assert!(set.block_ids(5).await.is_err());
    assert_eq!(flaky.calls(), 2, "asked on every read with a zero backoff");
    *flaky.rate_limited.lock().unwrap() = false;
    assert!(set.block_ids(5).await.is_ok());
}

/// The light client settles a disagreement: its answer is served, and
/// the public source that disagrees with it is the liar, rotated out.
#[tokio::test]
async fn the_light_client_settles_a_disagreement_and_rotates_the_liar_out() {
    let set = L1Sources::new(vec![("liar".into(), lying_at(5))])
        .with_light_client("light".into(), MockL1Source::new())
        .backoff(Duration::from_secs(3600));
    let (hash, _) = set.block_ids(5).await.unwrap();
    assert_eq!(hash, MockL1Source::filler_hash(5));
    // The liar is out; the light client answers alone.
    let (hash, _) = set.block_ids(5).await.unwrap();
    assert_eq!(hash, MockL1Source::filler_hash(5));
}

/// A light client that does not serve the block leaves the public
/// sources to agree among themselves.
#[tokio::test]
async fn a_silent_light_client_falls_back_to_the_public_quorum() {
    let light = MockL1Source::new();
    *light.block_hash_fails.lock().unwrap() = true;
    let set = L1Sources::new(vec![
        ("a".into(), MockL1Source::new()),
        ("b".into(), MockL1Source::new()),
    ])
    .with_light_client("light".into(), light)
    .backoff(Duration::from_secs(3600));
    assert!(set.block_ids(5).await.is_ok());
}

/// One configured source is trusted alone: a deployment with one
/// endpoint keeps working as before.
#[tokio::test]
async fn a_single_source_is_trusted_alone() {
    let set = honest(1);
    assert!(set.block_ids(5).await.is_ok());
}

/// Log queries are cross-checked too: a source that swallows logs
/// disagrees with one that serves them.
#[tokio::test]
async fn log_queries_are_cross_checked() {
    let a = MockL1Source::new();
    let b = MockL1Source::new();
    a.raw_logs
        .lock()
        .unwrap()
        .push_back(Ok(vec![Log::default()]));
    let set = L1Sources::new(vec![("a".into(), a), ("b".into(), b)]);
    let err = set.logs(&Filter::new()).await.unwrap_err();
    assert!(
        matches!(err, L1SourceError::Halt(SourceHalt::Disagreement { .. })),
        "{err}"
    );
    // The next query: both mocks answer no log, so they agree.
    assert!(set.logs(&Filter::new()).await.unwrap().is_empty());
}
