use std::time::Duration;

use alloy_primitives::{Address, U256};

use super::*;

fn accounts() -> [AccountState; 2] {
    [
        AccountState {
            address: Address::ZERO,
            nonce: 7,
            balance: U256::from(123),
        },
        AccountState {
            address: Address::repeat_byte(1),
            nonce: 0,
            balance: U256::MAX,
        },
    ]
}

fn rows() -> Vec<String> {
    vec!["7\n123\n42".into(), format!("0\n{}\n0", U256::MAX)]
}

#[tokio::test(start_paused = true)]
async fn a_failed_read_is_retried_and_only_a_complete_fresh_read_passes() {
    let expected = accounts();
    let projection = Projection::new(&expected);
    let mut readings = [
        Err(anyhow::anyhow!("the old primary stopped")),
        Ok(vec!["6\n123\n41".into(), rows()[1].clone()]),
        Ok(rows()),
    ]
    .into_iter();
    let started = tokio::time::Instant::now();
    projection
        .wait("recovery", Budget::secs(120, 3), || {
            std::future::ready(readings.next().unwrap())
        })
        .await
        .unwrap();
    assert_eq!(started.elapsed(), Duration::from_secs(6));
    assert!(readings.next().is_none());
}

#[tokio::test(start_paused = true)]
async fn separate_partial_observations_cannot_be_combined_into_a_pass() {
    let expected = accounts();
    let projection = Projection::new(&expected);
    let mut read = 0;
    let error = projection
        .wait("recovery", Budget::secs(12, 3), || {
            let mut sample = rows();
            sample[read % 2] = "\n\n".into();
            read += 1;
            std::future::ready(Ok(sample))
        })
        .await
        .unwrap_err();
    assert_eq!(read, 4);
    assert!(error.to_string().contains("after 12s"), "{error}");
    assert!(error.to_string().contains("observed"), "{error}");
}

#[tokio::test(start_paused = true)]
async fn persistent_read_failure_preserves_its_cause_at_the_original_deadline() {
    let expected = accounts();
    let projection = Projection::new(&expected);
    let started = tokio::time::Instant::now();
    let error = projection
        .wait("recovery", Budget::secs(120, 3), || async {
            Err(anyhow::anyhow!("no Sentinel names a live primary"))
        })
        .await
        .unwrap_err();
    assert_eq!(started.elapsed(), Duration::from_secs(120));
    assert!(
        error
            .to_string()
            .contains("no Sentinel names a live primary"),
        "{error}"
    );
}

#[tokio::test(start_paused = true)]
async fn a_hanging_read_cannot_extend_the_original_deadline() {
    let expected = accounts();
    let projection = Projection::new(&expected);
    let started = tokio::time::Instant::now();
    let error = projection
        .wait("recovery", Budget::secs(120, 3), std::future::pending)
        .await
        .unwrap_err();
    assert_eq!(started.elapsed(), Duration::from_secs(120));
    assert!(error.to_string().contains("after 120s"), "{error}");
}

#[test]
fn missing_extra_malformed_and_wrong_rows_cannot_certify_coverage() {
    let expected = accounts();
    let projection = Projection::new(&expected);
    for sample in [
        vec![],
        vec![rows()[0].clone()],
        [rows(), vec![rows()[0].clone()]].concat(),
        vec!["7\n122\n42".into(), rows()[1].clone()],
        vec![rows()[0].clone(), "0\n0\n0".into()],
        vec!["7\n123\ninvalid".into(), rows()[1].clone()],
    ] {
        assert!(projection.observe(Ok(sample)).is_none());
    }
    assert_eq!(projection.observe(Ok(rows())), Some(()));
}
