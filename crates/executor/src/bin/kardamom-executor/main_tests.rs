use super::*;

/// A dead publication exits with its own code, also behind a context;
/// every other failure exits with 1.
#[test]
fn a_dead_publication_has_its_own_exit_code() {
    let dead = anyhow::Error::new(ExecutorError::PublicationDead {
        topic: "tx_receipts".into(),
        unconnected_s: 140,
        reopen_after_s: 35,
    })
    .context("executor pipeline failed");
    assert_eq!(exit_code(&dead), ExitCode::from(3));
    let other = anyhow::Error::new(ExecutorError::TxOrderingClosed);
    assert_eq!(exit_code(&other), ExitCode::FAILURE);
    assert_eq!(
        exit_code(&anyhow::anyhow!("bind failed")),
        ExitCode::FAILURE
    );
}
