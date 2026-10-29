use std::time::Duration;

use alloy_primitives::U256;
use tokio::time::Instant;

use super::*;

struct Fixture;

impl Fixture {
    fn account() -> AccountState {
        AccountState {
            address: Address::ZERO,
            nonce: 7,
            balance: U256::MAX,
        }
    }

    fn heads(values: [u64; 3]) -> Heads {
        Heads(values.map(|value| std::num::NonZeroU64::new(value).unwrap()))
    }

    fn sample(primary: &str, values: [u64; 3], valid_row: bool) -> Snapshot {
        Snapshot {
            primary: primary.into(),
            heads: Self::heads(values),
            row: if valid_row {
                format!("7\n{}\n42", U256::MAX)
            } else {
                "6\n1\n41".into()
            },
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_failed_baseline_read_retries_before_capturing_exact_state() {
    let account = Fixture::account();
    let mut replies = [
        Err(anyhow::anyhow!("MGET timed out")),
        Ok(Fixture::sample("primary", [10, 20, 30], false)),
        Ok(Fixture::sample("primary", [11, 21, 31], true)),
    ]
    .into_iter();
    let started = Instant::now();
    let snapshot = Observation::new(&account, None)
        .wait(Budget::secs(120, 2), || {
            std::future::ready(replies.next().unwrap())
        })
        .await
        .unwrap();
    assert_eq!(snapshot.heads.0, Fixture::heads([11, 21, 31]).0);
    assert_eq!(started.elapsed(), Duration::from_secs(4));
    assert!(replies.next().is_none());
}

#[tokio::test(start_paused = true)]
async fn a_transient_recovery_read_does_not_discard_the_remaining_budget() {
    let account = Fixture::account();
    let mut replies = [
        Ok(Fixture::sample("primary", [11, 21, 31], true)),
        Err(anyhow::anyhow!("connection closed")),
        Ok(Fixture::sample("primary", [12, 22, 32], true)),
    ]
    .into_iter();
    let started = Instant::now();
    Observation::new(&account, Some(Fixture::heads([10, 20, 30])))
        .wait(Budget::secs(300, 5), || {
            std::future::ready(replies.next().unwrap())
        })
        .await
        .unwrap();
    assert_eq!(started.elapsed(), Duration::from_secs(10));
    assert!(replies.next().is_none());
}

#[tokio::test(start_paused = true)]
async fn persistent_errors_keep_the_cause_at_the_original_recovery_deadline() {
    let account = Fixture::account();
    let started = Instant::now();
    let error = Observation::new(&account, Some(Fixture::heads([10, 20, 30])))
        .wait(Budget::secs(300, 5), || async {
            Err(anyhow::anyhow!("no Sentinel names a live primary"))
        })
        .await
        .unwrap_err();
    assert_eq!(started.elapsed(), Duration::from_secs(300));
    assert!(error.to_string().contains("within 300s"));
    assert!(
        error
            .to_string()
            .contains("no Sentinel names a live primary")
    );
}

#[tokio::test(start_paused = true)]
async fn a_hanging_snapshot_cannot_extend_the_baseline_deadline() {
    let account = Fixture::account();
    let started = Instant::now();
    let error = Observation::new(&account, None)
        .wait(Budget::secs(120, 2), std::future::pending)
        .await
        .unwrap_err();
    assert_eq!(started.elapsed(), Duration::from_secs(120));
    assert!(error.to_string().contains("no completed observation"));
}

#[tokio::test(start_paused = true)]
async fn every_writer_must_advance_twice_with_the_exact_account() {
    let account = Fixture::account();
    let mut replies = [
        Fixture::sample("primary", [100, 20, 300], true),
        Fixture::sample("primary", [11, 21, 31], false),
        Fixture::sample("primary", [11, 21, 31], true),
        Fixture::sample("primary", [100, 21, 300], true),
        Fixture::sample("primary", [12, 22, 32], true),
    ]
    .into_iter();
    let started = Instant::now();
    Observation::new(&account, Some(Fixture::heads([10, 20, 30])))
        .wait(Budget::secs(300, 5), || {
            std::future::ready(Ok(replies.next().unwrap()))
        })
        .await
        .unwrap();
    assert_eq!(started.elapsed(), Duration::from_secs(20));
    assert!(replies.next().is_none());
}

#[tokio::test(start_paused = true)]
async fn promotion_requires_two_complete_observations_on_the_new_primary() {
    let account = Fixture::account();
    let mut replies = [
        Fixture::sample("old", [11, 21, 31], true),
        Fixture::sample("new", [12, 22, 32], true),
        Fixture::sample("new", [12, 22, 32], true),
        Fixture::sample("new", [13, 23, 33], true),
    ]
    .into_iter();
    let started = Instant::now();
    let snapshot = Observation::new(&account, Some(Fixture::heads([10, 20, 30])))
        .wait(Budget::secs(300, 5), || {
            std::future::ready(Ok(replies.next().unwrap()))
        })
        .await
        .unwrap();
    assert_eq!(snapshot.primary, "new");
    assert_eq!(started.elapsed(), Duration::from_secs(15));
    assert!(replies.next().is_none());
}

#[tokio::test(start_paused = true)]
async fn fixed_held_heads_cannot_prove_recovery_even_with_a_correct_account() {
    let account = Fixture::account();
    let started = Instant::now();
    let error = Observation::new(&account, Some(Fixture::heads([10, 20, 30])))
        .wait(Budget::secs(300, 5), || async {
            Ok(Fixture::sample("primary", [10, 20, 30], true))
        })
        .await
        .unwrap_err();
    assert_eq!(started.elapsed(), Duration::from_secs(300));
    assert!(error.to_string().contains("observed Snapshot"));
}
