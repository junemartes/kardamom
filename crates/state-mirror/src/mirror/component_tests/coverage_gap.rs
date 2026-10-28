use super::support::Fixture;

/// A surviving head cannot certify accounts absent from an interrupted scan.
#[tokio::test]
#[ignore = "known coverage gap; requires Redis; run component_tests::coverage_gap explicitly"]
async fn automatic_restart_must_restore_coverage_despite_another_live_head() {
    let fixture = Fixture::interrupted_scan().await;
    let replacement = fixture.start(0, false).await;
    replacement.send(&Fixture::batch(0, 1, 20));
    fixture.head(0, 20).await;
    let untouched = fixture
        .cache
        .account(Fixture::row(512, 1).address)
        .await
        .unwrap();
    replacement.close().await.unwrap();
    assert!(
        untouched.is_some(),
        "progress must not be published before missing account coverage is restored"
    );
}
