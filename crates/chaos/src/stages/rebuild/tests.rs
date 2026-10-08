use super::*;

/// The error chain the tool prints when a block needs more slots than
/// its canonical range holds.
const SLOT_REFUSAL: &str = "Error: re-execute reconstructed blocks

Caused by:
    0: replay
    1: block 1536: canonical end 290001 leaves no room for its 3 slots (epoch markers, deposits, remote records, transactions) after the previous end 289999
";

#[test]
fn only_a_short_l1_record_is_retried() {
    let short = Attempt::failed(
        "",
        "Error: posted batches end at block 1630, before block 1631\n",
    );
    assert!(
        matches!(&short, Attempt::NotPosted(why) if why == "Error: posted batches end at block 1630, before block 1631"),
        "{short:?}"
    );
    let empty = Attempt::failed(
        "",
        "Error: no BatchPosted events found at 0x01 — nothing to reconstruct",
    );
    assert!(matches!(empty, Attempt::NotPosted(_)), "{empty:?}");
    let slots = Attempt::failed("", SLOT_REFUSAL);
    assert!(
        matches!(&slots, Attempt::Refused(why) if why.starts_with("Error: re-execute reconstructed blocks / Caused by: / 0: replay / 1: block 1536")),
        "{slots:?}"
    );
    let root = Attempt::failed(
        "reconstructed head=9 blocks=9 txs=1 state_root=0x01 end_tx_idx=10",
        "Error: state root mismatch: reconstructed 0x01 != expected 0x02",
    );
    assert!(
        matches!(&root, Attempt::Refused(why) if why.starts_with("reconstructed head=9") && why.ends_with("!= expected 0x02")),
        "{root:?}"
    );
}

#[test]
fn a_refusal_ends_the_wait_and_a_short_record_waits() {
    let last = RefCell::new(String::new());
    let waits = Attempt::NotPosted("short".to_string()).settle(&last, 7);
    assert!(matches!(waits, Ok(None)));
    assert_eq!(*last.borrow(), "short");

    let err = Attempt::Refused("no room".to_string())
        .settle(&last, 7)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("refused block 7") && err.ends_with("no room"),
        "{err}"
    );
    assert_eq!(*last.borrow(), "short");

    let rebuilt = Rebuilt {
        state_dir: PathBuf::from("/state"),
        report: "reconstructed head=7".to_string(),
    };
    let done = Attempt::Rebuilt(rebuilt).settle(&last, 7).unwrap();
    assert_eq!(
        done.map(|r| r.report).as_deref(),
        Some("reconstructed head=7")
    );
}
