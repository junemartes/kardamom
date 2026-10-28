use super::support::Fixture;

/// A surviving head cannot certify accounts absent from an interrupted scan.
#[tokio::test]
#[ignore = "requires one disposable Redis container"]
async fn automatic_restart_must_restore_coverage_despite_another_live_head() {
    Fixture::interrupted_scan()
        .await
        .assert_automatic_recovery()
        .await;
}

#[tokio::test]
#[ignore = "requires one disposable Redis container"]
async fn persisted_progress_cannot_hide_an_interrupted_rebuild() {
    Fixture::interrupted_scan_from(Some(10))
        .await
        .assert_automatic_recovery()
        .await;
}

impl Fixture {
    async fn assert_automatic_recovery(self) {
        let replacement = self.start(0, false).await;
        replacement.send(&Self::batch(0, 1, 20));
        self.head(0, 20).await;
        self.assert_row(0, 4, 30).await;
        self.assert_checkpoint_suffix(1).await;
        assert!(!self.head_file(0).rebuild_pending().unwrap());
        replacement.close().await.unwrap();
    }
}
