use std::path::Path;
use std::time::Duration;

use tokio::sync::Mutex;

use super::{
    Clears, Halt, HaltCause, Record, RecoveryId, clear, cleared, current, hold_until, raise,
};
use crate::lifecycle::Slots;

/// The halt state is one per process, so the tests that write it run
/// one at a time.
static SERIAL: Mutex<()> = Mutex::const_new(());

fn repo_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("crates/obs sits two levels under the repository root")
}

#[test]
fn every_recovery_id_has_a_runbook_with_the_four_sections() {
    for id in RecoveryId::ALL {
        let path = repo_root().join(id.runbook());
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("runbook {} for {:?}: {e}", path.display(), id));
        for heading in ["## Cause", "## Confirm", "## Steps", "## Clear"] {
            assert!(
                text.contains(heading),
                "{} lacks the section {heading}",
                path.display()
            );
        }
    }
}

#[test]
fn ids_are_stable_and_distinct() {
    let mut causes: Vec<&str> = HaltCause::ALL.iter().map(|c| c.id()).collect();
    causes.sort_unstable();
    causes.dedup();
    assert_eq!(causes.len(), HaltCause::ALL.len());
    let mut runbooks: Vec<&str> = RecoveryId::ALL.iter().map(|r| r.id()).collect();
    runbooks.sort_unstable();
    runbooks.dedup();
    assert_eq!(runbooks.len(), RecoveryId::ALL.len());
    for cause in HaltCause::ALL {
        assert_eq!(
            cause.id(),
            cause.recovery().id(),
            "{cause:?} names its own runbook"
        );
    }
    assert_eq!(HaltCause::DaLag.clears(), Clears::Auto);
    assert_eq!(HaltCause::RecordLag.clears(), Clears::Auto);
    assert_eq!(HaltCause::ValidatorDivergence.clears(), Clears::Operator);
    assert_eq!(
        RecoveryId::RevertToPostedHead.runbook(),
        "docs/runbooks/revert_to_posted_head.md"
    );
}

#[test]
fn the_json_record_names_the_runbook() {
    let slots = Slots {
        halt: Some(Halt::new(HaltCause::L1ChainBreak, "block 7")),
        pause: None,
    };
    let json = super::to_json("indexer", &slots);
    assert_eq!(json["service"], "indexer");
    assert_eq!(json["state"], "halted");
    assert_eq!(json["halted"], true);
    assert_eq!(json["cause"], "l1_chain_break");
    assert_eq!(json["detail"], "block 7");
    assert_eq!(json["runbook"], "docs/runbooks/l1_chain_break.md");
    assert_eq!(json["clears"], "auto");
    assert!(json["since_unix_ms"].as_u64().unwrap() > 0);
    assert_eq!(json["pause"], serde_json::Value::Null);
    let running = super::to_json("indexer", &Slots::default());
    assert_eq!(running["halted"], false);
    assert_eq!(running["state"], "running");
    assert_eq!(slots.halt.unwrap().to_json()["recovery"], "l1_chain_break");
}

#[tokio::test]
async fn a_raise_of_the_same_cause_keeps_since_and_a_clear_returns_the_halt() {
    let _serial = SERIAL.lock().await;
    clear();
    raise(Halt::new(HaltCause::L1Unreachable, "first"));
    let first = current().unwrap();
    raise(Halt::new(HaltCause::L1Unreachable, "second"));
    let second = current().unwrap();
    assert_eq!(second.since_unix_ms, first.since_unix_ms);
    assert_eq!(second.detail, "second");
    raise(Halt::new(HaltCause::DaLag, "other"));
    assert_eq!(current().unwrap().cause, HaltCause::DaLag);
    assert_eq!(clear().unwrap().detail, "other");
    assert!(current().is_none());
    assert!(clear().is_none());
}

#[tokio::test]
async fn cleared_resolves_when_the_halt_goes() {
    let _serial = SERIAL.lock().await;
    clear();
    raise(Halt::new(HaltCause::ValidatorDivergence, "mismatch"));
    let clearer = tokio::spawn(async {
        tokio::time::sleep(Duration::from_millis(30)).await;
        clear();
    });
    tokio::time::timeout(Duration::from_secs(2), cleared())
        .await
        .expect("cleared resolves after the clear");
    clearer.await.unwrap();
}

#[tokio::test]
async fn hold_until_retries_then_clears() {
    let _serial = SERIAL.lock().await;
    clear();
    let mut attempts = 0u32;
    let value = hold_until(
        Halt::new(HaltCause::L1Unreachable, "down"),
        Duration::from_millis(5),
        || {
            attempts += 1;
            let n = attempts;
            async move {
                if n < 3 {
                    Err(format!("attempt {n} failed"))
                } else {
                    Ok(n)
                }
            }
        },
    )
    .await;
    assert_eq!(value, 3);
    assert!(current().is_none(), "a success clears the halt");
}

#[test]
fn every_cause_has_an_alert_rule() {
    let rules = std::fs::read_to_string(repo_root().join("deploy/alerts.yml")).unwrap();
    for cause in HaltCause::ALL {
        let expr = format!("kardamom_halt{{cause=\"{}\"}} == 1", cause.id());
        assert!(
            rules.contains(&expr),
            "deploy/alerts.yml lacks a rule for {expr}"
        );
        let runbook = cause.recovery().runbook();
        assert!(
            rules.contains(&runbook),
            "the rule for {} names {runbook}",
            cause.id()
        );
    }
}

#[test]
fn a_pause_has_an_info_rule_muted_by_its_root() {
    let rules = std::fs::read_to_string(repo_root().join("deploy/alerts.yml")).unwrap();
    assert!(rules.contains("alert: KardamomServicePaused"));
    assert!(rules.contains("expr: kardamom_paused == 1"));
    let inhibit =
        std::fs::read_to_string(repo_root().join("deploy/alertmanager-inhibit.yml")).unwrap();
    assert!(
        inhibit.contains(r#"'alertname =~ "KardamomHalt.*"'"#),
        "{inhibit}"
    );
    assert!(
        inhibit.contains(r#"'alertname = "KardamomServicePaused"'"#),
        "{inhibit}"
    );
    assert!(inhibit.contains("equal:\n      - cause"), "{inhibit}");
}
