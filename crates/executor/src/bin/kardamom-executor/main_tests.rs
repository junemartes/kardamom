use super::*;

#[test]
fn verdict_revolves_only_on_a_staged_checkpoint() {
    let refused = || ExecutorError::ClusterReplayUnavailable {
        from_index: 10,
        oldest_index: 500,
        oldest_block: 7,
    };
    assert!(matches!(
        Verdict::of(Some(refused()), Some("peer-checkpoint"), None),
        Verdict::Revolve(None)
    ));
    assert!(matches!(
        Verdict::of(Some(refused()), Some("unrecoverable"), None),
        Verdict::Failed(_)
    ));
    assert!(matches!(
        Verdict::of(Some(refused()), None, None),
        Verdict::Failed(_)
    ));
    assert!(matches!(Verdict::of(None, None, None), Verdict::Done));
}

/// A divergence revolves only with a staged rollback; with none, the
/// executor stops for the operator. A restart of the stream source
/// always revolves.
#[test]
fn verdict_rolls_back_only_with_a_staged_checkpoint() {
    let diverged = || ExecutorError::StreamDiverged {
        block: 9,
        index: 40,
    };
    let staged = Rollback {
        restore: kardamom_state::BlockRange::below(9).unwrap(),
        record: kardamom_types::ReceiptRollback { to_block: 8 },
    };
    assert!(matches!(
        Verdict::of(Some(diverged()), None, Some(staged)),
        Verdict::Revolve(Some(r)) if r == staged
    ));
    assert!(matches!(
        Verdict::of(Some(diverged()), None, None),
        Verdict::Failed(ExecutorError::StreamDiverged { block: 9, .. })
    ));
    let restarted = ExecutorError::SourceRestarted {
        next_index: 3,
        next_block: 2,
    };
    assert!(matches!(
        Verdict::of(Some(restarted), None, None),
        Verdict::Revolve(None)
    ));
}

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
