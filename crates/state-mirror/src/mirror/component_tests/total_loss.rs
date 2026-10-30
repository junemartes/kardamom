//! redis-total-loss-recover on a real Sentinel group: a primary, a
//! replica and three Sentinels, all killed at once, with no data on disk.

use std::time::{Duration, Instant};

use super::sentinels::Fixture as Group;
use super::support::{ACCOUNTS, Fixture};

/// The outage outlasts the mirror's bound for a lossless write retry, as
/// the deployed total loss does, so the recovery takes the rebuild path.
const OUTAGE: Duration = Duration::from_secs(11);
/// The recovery bound after the group returns: Sentinel discovery, the
/// landed retry, and a rebuild of every account.
const RECOVERY: Duration = Duration::from_secs(30);

/// The mirror keeps running through the loss. While the group is down,
/// a reader fails and the mirror's write retries mark a rebuild as due.
/// The group returns empty, the Sentinels name a primary, and the same
/// mirror lands its retried write, rebuilds every account from its
/// checkpoint, advances its head past the outage, and the same reader
/// reads exact values again.
#[tokio::test]
#[ignore = "requires Linux Docker bridge routing and five disposable containers"]
async fn a_total_loss_rebuilds_every_account_and_the_readers_recover() {
    let group = Group::new().await;
    let admin = Group::raw(&group.primary, 6379).await;
    let fixture = Fixture::on(group.cfg.clone(), admin).await;
    let (live, _writer) =
        kardamom_cache::LiveAccounts::new(&kardamom_cache::LiveAccountsConfig::default());
    let reader = kardamom_cache::CacheReader::spawn(&group.cfg, std::num::NonZeroU32::MIN, live);
    let actor = fixture.start(0, false).await;
    actor.send(&Fixture::batch(0, 1, 20));
    fixture.head(0, 20).await;
    fixture.assert_checkpoint_suffix(0).await;

    group.lose_everything().await;
    let dark = Instant::now();
    assert!(
        fixture
            .cache
            .account(Fixture::row(5, 1).address)
            .await
            .is_err(),
        "a reader must fail while the group is down"
    );
    let asked = Instant::now();
    assert_eq!(
        reader.read_account(Fixture::row(5, 1).address).await,
        Err(kardamom_cache::Degraded),
        "the production reader degrades instead of answering"
    );
    assert!(
        asked.elapsed() < Duration::from_secs(5),
        "a degraded read must not stall"
    );
    actor.send(&Fixture::batch(1, 2, 30));
    let head = fixture.head_file(0);
    tokio::time::timeout(Duration::from_secs(10), async {
        while !head.rebuild_pending().unwrap() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("a failed write must mark the rebuild before it retries");
    tokio::time::sleep(OUTAGE.saturating_sub(dark.elapsed())).await;

    group.start_everything().await;
    // The executors keep writing checkpoints; the rebuild needs one that
    // reaches the first live batch after the loss.
    fixture.checkpoint_at(2, 41, &[Fixture::row(1, 2), Fixture::row(2, 3)]);
    actor.send(&Fixture::batch(2, 3, 40));
    let back = Instant::now();
    fixture.head_within(0, 40, RECOVERY).await;
    eprintln!(
        "the mirror recovered {:?} after the group returned",
        back.elapsed()
    );
    fixture.assert_row(1, 2, 40).await;
    fixture.assert_row(2, 3, 40).await;
    fixture.assert_rows_at(3, 40).await;
    assert!(!head.rebuild_pending().unwrap(), "the rebuild finished");
    let address = Fixture::row(5, 1).address;
    let recovered = tokio::time::timeout(RECOVERY, async {
        let mut read = reader.read_account(address).await;
        while !matches!(read, Ok(Some(_))) {
            tokio::time::sleep(Duration::from_millis(50)).await;
            read = reader.read_account(address).await;
        }
        read.unwrap().unwrap()
    })
    .await
    .expect("the production reader reads the rebuilt group again");
    assert_eq!(
        (recovered.nonce, recovered.tx_idx),
        (1, 40),
        "the reader sees the rebuilt row"
    );
    assert_eq!(ACCOUNTS, 1_025);
    actor.close().await.unwrap();
}
