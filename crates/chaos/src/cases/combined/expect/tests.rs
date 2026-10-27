use super::*;

#[tokio::test(start_paused = true)]
async fn a_live_head_survives_other_probes_exhausting_the_observation_budget() {
    let observation = Observation::default();
    let started = tokio::time::Instant::now();
    let actual = observation
        .read("isolated producers", || async {
            tokio::join!(
                observation.head(std::future::pending()),
                observation.head(async { Some(475) }),
                std::future::pending::<()>(),
            );
        })
        .await
        .unwrap();
    assert_eq!(started.elapsed(), Duration::from_secs(12));
    assert_eq!(actual.block, 475);
    assert_eq!(actual.boundaries, None);
    assert_eq!(actual.applied, None);
    assert!(Expect::Stall.judge(actual, actual).is_ok());
    assert!(Expect::SealOnly.judge(actual, actual).is_err());
}

#[tokio::test(start_paused = true)]
async fn neither_an_absent_nor_a_hung_head_is_a_zero_height() {
    let observation = Observation::default();
    let error = observation
        .read("no observer", || async {
            tokio::join!(
                observation.head(async { None }),
                observation.head(std::future::pending()),
            );
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("no client reports a head"));
}
