//! The validated ack: the duplicate cache, the nonce check against the
//! account view, no balance check, the answer of a failed offer, and an
//! answer with no receipt and no verdict. Each case runs on the tokio path
//! and on the verify agent, which must give the same answers.

use std::net::{IpAddr, Ipv4Addr};
use std::num::{NonZeroU32, NonZeroUsize};
use std::time::Duration;

use alloy_primitives::{Address, Bytes, U256, keccak256};
use alloy_signer_local::PrivateKeySigner;
use kardamom_types::{AccountRow, BPosition, TxEnvelope};
use tokio::sync::mpsc::UnboundedReceiver;

use crate::channels::MockChannels;
use crate::config::{AckMode, IngressConfig};
use crate::error::IngressError;
use crate::proxy::IngressProxy;
use crate::rendered_metrics_tests::Rendered;
use crate::test_support::sign_legacy;

const IP: IpAddr = IpAddr::V4(Ipv4Addr::new(10, 4, 0, 1));

/// A proxy with the validated ack over one mock lane, and the receiver of
/// that lane.
struct Fixture {
    proxy: IngressProxy<MockChannels, MockChannels>,
    mock: MockChannels,
    lane: UnboundedReceiver<TxEnvelope>,
}

impl Fixture {
    fn with(cfg: IngressConfig) -> Self {
        let (mock, mut lanes) = MockChannels::new(NonZeroUsize::MIN);
        Self {
            proxy: IngressProxy::new(cfg, mock.clone(), mock.clone())
                .with_verify_agent()
                .expect("the agent starts"),
            mock,
            lane: lanes.remove(0),
        }
    }

    fn on(path: Path) -> Self {
        Self::with(validated(path))
    }

    /// The account row of `signer` at `nonce`, with no balance.
    fn row(&self, signer: &PrivateKeySigner, nonce: u64) {
        let row = AccountRow {
            address: signer.address(),
            nonce,
            balance: U256::ZERO,
        };
        assert_eq!(self.mock.apply_rows(BPosition::from_index(10), &[row]), 1);
    }

    async fn submit(&self, raw: Bytes) -> Result<alloy_primitives::B256, IngressError> {
        self.proxy.submit(IP, raw).await
    }

    /// The proxy alone. The lane receiver ends here, so every later offer
    /// fails.
    fn into_proxy(self) -> IngressProxy<MockChannels, MockChannels> {
        self.proxy
    }

    /// The envelopes published so far.
    fn published(&mut self) -> usize {
        std::iter::from_fn(|| self.lane.try_recv().ok()).count()
    }
}

/// The path of a validated submit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Path {
    Tokio,
    Agent,
}

impl Path {
    const BOTH: [Self; 2] = [Self::Tokio, Self::Agent];
}

/// The validated ack over one lane, on `path`.
fn validated(path: Path) -> IngressConfig {
    IngressConfig {
        partition_count_m: NonZeroU32::MIN,
        ack: AckMode::Validated,
        verify_agent: path == Path::Agent,
        ..IngressConfig::default()
    }
}

/// Run the case `case` on both paths, the tokio path first.
async fn on_both<F, Fut>(case: F)
where
    F: Fn(Path) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    for path in Path::BOTH {
        case(path).await;
    }
}

#[tokio::test]
async fn a_copy_answers_the_hash_from_the_cache_and_publishes_once() {
    on_both(|path| async move {
        let mut f = Fixture::on(path);
        let raw = sign_legacy(&PrivateKeySigner::random(), 0);
        let first = f
            .submit(raw.clone())
            .await
            .expect("the first copy publishes");
        assert_eq!(first, keccak256(&raw));
        assert_eq!(f.submit(raw).await.expect("the cache answers"), first);
        assert_eq!(f.published(), 1, "a hit publishes nothing ({path:?})");
    })
    .await;
}

/// A hit answers before the decode, so it costs no recovery: bytes that
/// cannot decode answer their hash when the cache holds it.
#[tokio::test]
async fn a_hit_answers_before_the_decode_and_the_recovery() {
    on_both(|path| async move {
        let mut f = Fixture::on(path);
        let junk = Bytes::from_static(b"not a transaction");
        let err = f.submit(junk.clone()).await.unwrap_err();
        assert!(matches!(err, IngressError::Decode(_)), "{err:?}");
        f.proxy.seen.insert(keccak256(&junk), Address::ZERO);
        assert_eq!(f.submit(junk.clone()).await.unwrap(), keccak256(&junk));
        assert_eq!(f.published(), 0);
    })
    .await;
}

#[tokio::test]
async fn a_full_cache_forgets_the_oldest_hash() {
    on_both(|path| async move {
        let mut f = Fixture::with(IngressConfig {
            duplicate_cache_capacity: NonZeroUsize::MIN,
            ..validated(path)
        });
        let (a, b) = (
            sign_legacy(&PrivateKeySigner::random(), 0),
            sign_legacy(&PrivateKeySigner::random(), 0),
        );
        f.submit(a.clone()).await.unwrap();
        f.submit(b).await.unwrap();
        f.submit(a).await.unwrap();
        assert_eq!(f.published(), 3, "the evicted hash publishes again");
    })
    .await;
}

#[tokio::test]
async fn a_nonce_below_the_view_is_too_low() {
    on_both(|path| async move {
        let mut f = Fixture::on(path);
        let signer = PrivateKeySigner::random();
        f.row(&signer, 3);
        let err = f.submit(sign_legacy(&signer, 2)).await.unwrap_err();
        assert_eq!(
            err.to_string(),
            format!(
                "nonce too low: address {}, tx: 2 state: 3",
                signer.address()
            )
        );
        f.submit(sign_legacy(&signer, 3))
            .await
            .expect("the next nonce");
        f.submit(sign_legacy(&signer, 9))
            .await
            .expect("a gap admits");
        let unknown = PrivateKeySigner::random();
        f.submit(sign_legacy(&unknown, 5))
            .await
            .expect("no row admits");
        assert_eq!(f.published(), 3);
    })
    .await;
}

#[tokio::test]
async fn the_nonce_check_is_off_with_the_admission_checks() {
    on_both(|path| async move {
        let mut f = Fixture::with(IngressConfig {
            admission_checks: false,
            ..validated(path)
        });
        let signer = PrivateKeySigner::random();
        f.row(&signer, 3);
        f.submit(sign_legacy(&signer, 2)).await.expect("no check");
        assert_eq!(f.published(), 1);
    })
    .await;
}

/// A row with no balance does not refuse: a balance can grow.
#[tokio::test]
async fn no_balance_check_runs() {
    on_both(|path| async move {
        let mut f = Fixture::on(path);
        let signer = PrivateKeySigner::random();
        f.row(&signer, 0);
        f.submit(sign_legacy(&signer, 0))
            .await
            .expect("no balance check");
        assert_eq!(f.published(), 1);
    })
    .await;
}

/// A failed offer answers `txpool is full`, and the hash stays out of the
/// cache, so a copy offers again. The agent answers at the deadline of
/// its retry queue, 5 s after its first offer.
#[tokio::test]
async fn a_failed_offer_answers_txpool_is_full() {
    on_both(|path| async move {
        let proxy = Fixture::on(path).into_proxy();
        let raw = sign_legacy(&PrivateKeySigner::random(), 0);
        let err = proxy.submit(IP, raw.clone()).await.unwrap_err();
        assert!(matches!(err, IngressError::PoolFull), "{err:?}");
        assert_eq!(err.to_string(), "txpool is full");
        assert!(proxy.seen.get(&keccak256(&raw)).is_none());
    })
    .await;
}

/// Both submit calls answer on the publication alone: no receipt and no
/// verdict ever arrives, and no waiter stays.
#[tokio::test]
async fn both_calls_answer_with_no_receipt_and_no_verdict() {
    on_both(|path| async move {
        let mut f = Fixture::on(path);
        let signer = PrivateKeySigner::random();
        let answer = tokio::time::timeout(Duration::from_secs(5), async {
            let sync = f.submit(sign_legacy(&signer, 0)).await;
            let async_call = f.proxy.submit_raw_async(IP, sign_legacy(&signer, 1)).await;
            (sync, async_call)
        })
        .await
        .expect("no wait for the sequencer");
        assert!(answer.0.is_ok() && answer.1.is_ok(), "{answer:?}");
        assert_eq!((f.proxy.pending_len(), f.proxy.acks.len()), (0, 0));
        assert_eq!(f.published(), 2);
    })
    .await;
}

/// The metrics of four submits on `path`: a publication, a hit, a nonce
/// too low, and a failed offer.
fn counted(path: Path) -> Rendered {
    Rendered::of(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let f = Fixture::on(path);
            let signer = PrivateKeySigner::random();
            f.row(&signer, 1);
            let raw = sign_legacy(&signer, 1);
            f.submit(raw.clone()).await.unwrap();
            f.submit(raw).await.unwrap();
            f.submit(sign_legacy(&signer, 0)).await.unwrap_err();
            f.into_proxy()
                .submit(IP, sign_legacy(&signer, 2))
                .await
                .unwrap_err();
        });
    })
}

/// The samples of each of `steps` in `rendered`.
fn step_samples(rendered: &Rendered, steps: &[&str]) -> Vec<f64> {
    steps
        .iter()
        .map(|step| {
            rendered.value_or_zero(&format!("kardamom_step_seconds_count{{step=\"{step}\"}}"))
        })
        .collect()
}

/// Each answer is counted: accepted on a publication and on a hit,
/// rejected by reason. Each submit records one sample of the submit step
/// and of the validate step. The hit records no recovery. On the tokio
/// path only the two offers record a publication. The agent records no
/// publication hop, and one wait in its ring for each submit that it got.
#[test]
fn each_answer_is_counted() {
    for (path, expected) in [
        (Path::Tokio, [4.0, 4.0, 3.0, 2.0, 0.0]),
        (Path::Agent, [4.0, 4.0, 3.0, 0.0, 3.0]),
    ] {
        let rendered = counted(path);
        assert_eq!(rendered.value("kardamom_ingress_tx_received_total"), 4.0);
        assert_eq!(rendered.value("kardamom_ingress_tx_accepted_total"), 2.0);
        assert_eq!(
            rendered.value("kardamom_ingress_tx_rejected_total{reason=\"nonce-too-low\"}"),
            1.0,
            "{path:?}"
        );
        assert_eq!(
            rendered.value("kardamom_ingress_tx_rejected_total{reason=\"txpool-full\"}"),
            1.0,
            "{path:?}"
        );
        let steps = [
            "ingress.submit",
            "ingress.validate",
            "ingress.recover",
            "ingress.publish_round_trip",
            "ingress.verify_wait",
        ];
        assert_eq!(
            step_samples(&rendered, &steps),
            expected,
            "{path:?} {steps:?}"
        );
    }
}
