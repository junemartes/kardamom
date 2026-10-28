use kardamom_types::BPosition;

use super::support::{ACCOUNTS, Fixture};

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
    assert!(fixture.head_file(0).rebuild_pending().unwrap());
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
