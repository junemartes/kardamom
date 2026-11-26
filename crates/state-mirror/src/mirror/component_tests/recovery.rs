use kardamom_types::BPosition;

use super::support::{ACCOUNTS, Actor, Fixture};
use crate::audit::AuditClock;

#[tokio::test]
#[ignore = "requires one disposable Redis container"]
async fn interrupted_partial_rebuild_restores_untouched_accounts_without_regressing_live_rows() {
    let fixture = Fixture::interrupted_scan().await;
    let newest = Fixture::batch(0, 4, 30);
    let replacement = fixture.start(0, true).await;
    replacement.send(&Fixture::batch(0, 1, 20));
    fixture.head(0, 20).await;
    fixture.assert_row(0, 4, 30).await;
    fixture.assert_checkpoint_suffix(1).await;
    assert_eq!(
        fixture
            .cache
            .receipt(newest.receipts[0].from, 4)
            .await
            .unwrap(),
        Some(newest.receipts[0].clone())
    );
    replacement.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires one disposable Redis container"]
async fn partial_live_batch_retries_without_new_input_and_publishes_head_last() {
    let mut fixture = Fixture::new().await;
    fixture.permit_prefix(1).await;
    fixture
        .cache
        .set_head(0, BPosition::from_index(20))
        .await
        .unwrap();
    let actor = fixture.start(0, false).await;
    let mut batch = Fixture::batch(0, 3, 30);
    batch.accounts.push(Fixture::row(1, 3));
    actor.send(&batch);
    fixture.denied().await;
    fixture.assert_row(0, 3, 30).await;
    assert!(
        fixture
            .cache
            .account(Fixture::row(1, 3).address)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(fixture.cache.heads(&[0]).await.unwrap(), [Some(20)]);

    fixture.permit_all().await;
    fixture.head(0, 30).await;
    fixture.assert_row(0, 3, 30).await;
    fixture.assert_row(1, 3, 30).await;
    assert!(!fixture.head_file(0).rebuild_pending().unwrap());
    assert_eq!(
        fixture
            .cache
            .receipt(batch.receipts[0].from, 3)
            .await
            .unwrap(),
        Some(batch.receipts[0].clone())
    );
    actor.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires one disposable Redis container"]
async fn script_loss_and_older_replica_frames_preserve_newest_account_state() {
    let mut fixture = Fixture::new().await;
    fixture.permit_all().await;
    fixture
        .cache
        .set_head(0, BPosition::from_index(20))
        .await
        .unwrap();
    let actor = fixture.start(0, false).await;
    actor.send(&Fixture::batch(0, 2, 25));
    fixture.head(0, 25).await;
    redis::cmd("SCRIPT")
        .arg("FLUSH")
        .query_async::<()>(&mut fixture.admin)
        .await
        .unwrap();
    actor.send(&Fixture::batch(0, 4, 30));
    fixture.head(0, 30).await;
    actor.send(&Fixture::batch(0, 3, 27));
    actor.close().await.unwrap();
    fixture.assert_row(0, 4, 30).await;
    assert_eq!(fixture.cache.heads(&[0]).await.unwrap(), [Some(30)]);
}

#[tokio::test]
#[ignore = "requires one disposable Redis container"]
async fn a_cold_mirror_restores_all_accounts_before_advancing_and_stops_on_input_close() {
    let mut fixture = Fixture::new().await;
    fixture.permit_all().await;
    let actor = fixture.start(0, false).await;
    actor.send(&Fixture::batch(0, 1, 20));
    fixture.head(0, 20).await;
    fixture.assert_checkpoint_suffix(0).await;
    assert_eq!(
        ACCOUNTS, 1_025,
        "the scan must cross two full chunk boundaries"
    );
    actor.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires one disposable Redis container"]
async fn a_transient_head_read_timeout_does_not_end_the_recovery_wait() {
    let mut fixture = Fixture::new().await;
    fixture
        .cache
        .set_head(0, BPosition::from_index(20))
        .await
        .unwrap();
    redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(400)
        .arg("ALL")
        .query_async::<()>(&mut fixture.admin)
        .await
        .unwrap();
    let started = tokio::time::Instant::now();
    fixture.head(0, 20).await;
    assert!(started.elapsed() >= std::time::Duration::from_millis(200));
}

/// All mirrors stop for longer than the TTL of the live heads, and Redis
/// keeps its data. The durable head is at the local head, and the first
/// live batch follows it, so the mirror resumes: no rebuild writes the
/// checkpoint rows.
#[tokio::test]
#[ignore = "requires one disposable Redis container"]
async fn a_restart_of_all_mirrors_on_an_intact_redis_resumes() {
    let mut fixture = Fixture::new().await;
    fixture.permit_all().await;
    fixture.all_mirrors_stopped_after(0, 20).await;
    let actor = fixture.start(0, false).await;
    actor.send(&Fixture::batch(0, 2, 21));
    fixture.head(0, 21).await;
    fixture.assert_row(0, 2, 21).await;
    fixture.assert_no_checkpoint_rows(1).await;
    assert_eq!(fixture.cache.durable_head(0).await.unwrap(), Some(21));
    actor.close().await.unwrap();
}

/// All mirrors stop, Redis keeps its data, and the chain moves on. The
/// first live batch does not follow the durable head, so no mirror
/// applied the batches between them. The mirror rebuilds the hole from a
/// checkpoint before it advances.
#[tokio::test]
#[ignore = "requires one disposable Redis container"]
async fn a_restart_of_all_mirrors_with_a_hole_rebuilds() {
    let mut fixture = Fixture::new().await;
    fixture.permit_all().await;
    fixture.all_mirrors_stopped_after(0, 10).await;
    let actor = fixture.start(0, false).await;
    actor.send(&Fixture::batch(0, 1, 20));
    fixture.head(0, 20).await;
    fixture.assert_checkpoint_suffix(1).await;
    actor.close().await.unwrap();
}

/// A Redis that lost its data holds no durable head. The local head file
/// is still there, and the start is cold.
#[tokio::test]
#[ignore = "requires one disposable Redis container"]
async fn an_empty_redis_with_a_local_head_is_cold() {
    let mut fixture = Fixture::new().await;
    fixture.permit_all().await;
    fixture.head_file(0).write(20).unwrap();
    let actor = fixture.start(0, false).await;
    actor.send(&Fixture::batch(0, 1, 20));
    fixture.head(0, 20).await;
    fixture.assert_checkpoint_suffix(1).await;
    actor.close().await.unwrap();
}

/// A failover lost the tail: the durable head is below the local head
/// file. The start rebuilds.
#[tokio::test]
#[ignore = "requires one disposable Redis container"]
async fn a_durable_head_below_the_local_head_is_a_regression() {
    let mut fixture = Fixture::new().await;
    fixture.permit_all().await;
    fixture.all_mirrors_stopped_after(0, 10).await;
    fixture.head_file(0).write(20).unwrap();
    let actor = fixture.start(0, false).await;
    actor.send(&Fixture::batch(0, 1, 20));
    fixture.head(0, 20).await;
    fixture.assert_checkpoint_suffix(1).await;
    actor.close().await.unwrap();
}

/// The audit interval counts from the persisted time of the last
/// rebuild. A restart before the interval ends does not audit. A start
/// after the persisted time plus the interval audits. An audit rewrites
/// the checkpoint rows at the end of the newer checkpoint, 40. Each batch
/// waits for the head of the one before it, so that each is its own
/// group: a due audit runs on the group after the one that found it due.
#[tokio::test]
#[ignore = "requires one disposable Redis container"]
async fn a_restart_keeps_the_audit_interval_of_the_persisted_rebuild() {
    let mut fixture = Fixture::new().await.with_audit_hours(1);
    fixture.permit_all().await;
    let rebuilt = AuditClock::unix_now();
    let first = fixture.start(0, true).await;
    fixture.apply(&first, &[(1, 20)]).await;
    first.close().await.unwrap();
    let persisted = fixture
        .head_file(0)
        .last_rebuild()
        .expect("a rebuild persists its time");
    assert!((rebuilt..=AuditClock::unix_now()).contains(&persisted));
    fixture.checkpoint_at(2, 41, &[]);

    let restarted = fixture.start(0, false).await;
    fixture.apply(&restarted, &[(2, 30), (3, 31)]).await;
    restarted.close().await.unwrap();
    fixture.assert_rows_at(1, 20).await;
    assert_eq!(fixture.head_file(0).last_rebuild(), Some(persisted));

    fixture
        .head_file(0)
        .record_rebuild(persisted.checked_sub(3_600).unwrap())
        .unwrap();
    let due = fixture.start(0, false).await;
    fixture.apply(&due, &[(4, 32), (5, 33)]).await;
    due.close().await.unwrap();
    fixture.assert_rows_at(1, 40).await;
    assert!(fixture.head_file(0).last_rebuild() >= Some(persisted));
}

/// A persisted rebuild time after now, from a clock step, does not
/// audit at once, and the mirror keeps the file as it is.
#[tokio::test]
#[ignore = "requires one disposable Redis container"]
async fn a_persisted_rebuild_time_in_the_future_does_not_audit_at_once() {
    let mut fixture = Fixture::new().await.with_audit_hours(1);
    fixture.permit_all().await;
    fixture
        .cache
        .set_head(0, BPosition::from_index(20))
        .await
        .unwrap();
    let future = AuditClock::unix_now().checked_add(10 * 24 * 3_600).unwrap();
    fixture.head_file(0).record_rebuild(future).unwrap();
    let actor = fixture.start(0, false).await;
    fixture.apply(&actor, &[(2, 21), (3, 22)]).await;
    actor.close().await.unwrap();
    fixture.assert_no_checkpoint_rows(1).await;
    assert_eq!(fixture.head_file(0).last_rebuild(), Some(future));
}

impl Fixture {
    /// Send a batch of account 0 for each `(nonce, position)` to mirror
    /// 0, each after the head of the one before it.
    async fn apply(&self, actor: &Actor, batches: &[(u64, u64)]) {
        for &(nonce, position) in batches {
            self.send_and_wait(actor, nonce, position).await;
        }
    }

    async fn send_and_wait(&self, actor: &Actor, nonce: u64, position: u64) {
        actor.send(&Self::batch(0, nonce, position));
        self.head(0, position).await;
    }
}
