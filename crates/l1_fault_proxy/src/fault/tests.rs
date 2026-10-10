use super::*;
use alloy_primitives::address;

const SETTLEMENT: Address = address!("00000000000000000000000000000000000000aa");
const OTHER: Address = address!("00000000000000000000000000000000000000bb");

fn block() -> Value {
    serde_json::json!({
        "number": "0x10",
        "hash": format!("0x{}", "11".repeat(32)),
        "parentHash": format!("0x{}", "22".repeat(32)),
    })
}

fn logs() -> Value {
    serde_json::json!([
        { "address": format!("{SETTLEMENT:#x}"), "blockNumber": "0x10" },
        { "address": format!("{OTHER:#x}"), "blockNumber": "0x10" },
    ])
}

/// One `faults_actually_mutate_the_proxied_reply` case: a fault paired
/// with the check that proves what it did to the reply.
type Check = fn(&Value, &Value);

/// The proxy must actually corrupt what it forwards. Without this check,
/// a green fault-injection case would only prove that nothing broke,
/// which is the exact failure mode where a drill silently stops
/// drilling.
#[test]
fn faults_actually_mutate_the_proxied_reply() {
    let cases: [(Fault, Check); 4] = [
        (Fault::None, |r, block| {
            assert_eq!(r, block, "None must pass through untouched");
        }),
        (Fault::WrongBlockHash { from_block: 0x10 }, |r, block| {
            assert_ne!(
                r["hash"], block["hash"],
                "hash must be corrupted at the threshold"
            );
            assert_eq!(r["parentHash"], block["parentHash"]);
        }),
        // Below the threshold nothing changes, so a case can arm a fault
        // without invalidating blocks a follower already accepted.
        (Fault::WrongBlockHash { from_block: 0x11 }, |r, block| {
            assert_eq!(r, block, "below the threshold must pass through");
        }),
        (Fault::BrokenParentChain { from_block: 0x10 }, |r, block| {
            assert_eq!(r["hash"], block["hash"], "the block's own hash stays");
            assert_ne!(r["parentHash"], block["parentHash"], "only ancestry lies");
        }),
    ];
    for (fault, check) in cases {
        let mut r = block();
        Faults::one(fault).apply("eth_getBlockByNumber", &mut r);
        check(&r, &block());
    }
}

#[test]
fn block_faults_leave_other_methods_alone() {
    let mut r = block();
    Faults::one(Fault::WrongBlockHash { from_block: 0 }).apply("eth_getLogs", &mut r);
    assert_eq!(r, block());
}

#[test]
fn swallow_logs_drops_only_the_named_address() {
    let mut r = logs();
    Faults::one(Fault::SwallowLogs {
        address: SETTLEMENT,
    })
    .apply("eth_getLogs", &mut r);
    assert_eq!(r.as_array().map(Vec::len), Some(1));
    assert_eq!(r[0]["address"], format!("{OTHER:#x}"));
}

#[test]
fn null_receipts_start_at_the_threshold() {
    let receipt = serde_json::json!({ "blockNumber": "0x20", "status": "0x1" });
    let mut r = receipt.clone();
    Faults::one(Fault::NullReceipts { from_block: 0x21 })
        .apply("eth_getTransactionReceipt", &mut r);
    assert_eq!(r, receipt, "a receipt below the threshold is served");
    Faults::one(Fault::NullReceipts { from_block: 0x20 })
        .apply("eth_getTransactionReceipt", &mut r);
    assert!(r.is_null(), "a receipt at the threshold is null");
    let mut block_receipts = serde_json::json!([receipt]);
    Faults::one(Fault::NullReceipts { from_block: 0x20 })
        .apply("eth_getBlockReceipts", &mut block_receipts);
    assert!(block_receipts.is_null());
}

#[test]
fn several_faults_apply_in_order_and_down_wins_the_refusal() {
    let faults = Faults(vec![
        Fault::None,
        Fault::WrongBlockHash { from_block: 0 },
        Fault::BrokenParentChain { from_block: 0 },
    ]);
    let mut r = block();
    faults.apply("eth_getBlockByNumber", &mut r);
    assert_ne!(r["hash"], block()["hash"]);
    assert_ne!(r["parentHash"], block()["parentHash"]);
    assert_eq!(faults.refusal(), None);
    assert_eq!(
        Faults(vec![Fault::RateLimit]).refusal(),
        Some(Refusal::RateLimited)
    );
    assert_eq!(
        Faults(vec![Fault::RateLimit, Fault::Down]).refusal(),
        Some(Refusal::Down)
    );
    assert!(Faults::one(Fault::None).is_faithful());
    assert!(!Faults::one(Fault::Down).is_faithful());
}

#[test]
fn the_json_form_carries_the_kind_and_the_parameters() {
    let parsed: Faults = serde_json::from_str(
        r#"[{"kind":"WrongBlockHash","from_block":5},
            {"kind":"SwallowLogs","address":"0x00000000000000000000000000000000000000aa"},
            {"kind":"Down"}]"#,
    )
    .unwrap();
    assert_eq!(
        parsed,
        Faults(vec![
            Fault::WrongBlockHash { from_block: 5 },
            Fault::SwallowLogs {
                address: SETTLEMENT
            },
            Fault::Down,
        ])
    );
    let one: Fault = serde_json::from_str(r#"{"kind":"RateLimit"}"#).unwrap();
    assert_eq!(one, Fault::RateLimit);
    assert_eq!(
        serde_json::to_value(Fault::NullReceipts { from_block: 7 }).unwrap(),
        serde_json::json!({"kind": "NullReceipts", "from_block": 7})
    );
}

/// A forked chain stays a chain: the block at the threshold keeps its
/// true parent, a later block names the forked parent, and a log carries
/// its block's forked hash. Scoped to one client, the others see L1.
#[test]
fn a_forked_chain_is_consistent_and_scoped_to_its_client() {
    let liar: IpAddr = "10.0.0.7".parse().unwrap();
    let honest: IpAddr = "10.0.0.8".parse().unwrap();
    let faults = Faults::one(Fault::ForkedChain {
        from_block: 0x10,
        client: Some(liar),
    });
    let mut at = block();
    faults.apply_from(
        Caller {
            client: Some(liar),
            second: true,
        },
        "eth_getBlockByNumber",
        &mut at,
    );
    assert_ne!(at["hash"], block()["hash"]);
    assert_eq!(
        at["parentHash"],
        block()["parentHash"],
        "the fork starts here"
    );

    let mut after = serde_json::json!({
        "number": "0x11",
        "hash": format!("0x{}", "33".repeat(32)),
        "parentHash": block()["hash"],
    });
    faults.apply_from(
        Caller {
            client: Some(liar),
            second: true,
        },
        "eth_getBlockByNumber",
        &mut after,
    );
    assert_eq!(after["parentHash"], at["hash"], "the fork is one chain");

    let mut log = serde_json::json!([{ "blockNumber": "0x10", "blockHash": block()["hash"] }]);
    faults.apply_from(
        Caller {
            client: Some(liar),
            second: true,
        },
        "eth_getLogs",
        &mut log,
    );
    assert_eq!(log[0]["blockHash"], at["hash"]);

    let mut seen = block();
    faults.apply_from(
        Caller {
            client: Some(honest),
            second: false,
        },
        "eth_getBlockByNumber",
        &mut seen,
    );
    assert_eq!(seen, block(), "another client sees L1");
    let json = serde_json::to_value(Fault::ForkedChain {
        from_block: 3,
        client: Some(liar),
    })
    .unwrap();
    assert_eq!(json["kind"], "ForkedChain");
    assert_eq!(json["client"], "10.0.0.7");
}

/// The second source serves L1 faithfully under every unscoped fault,
/// and refuses nothing.
#[test]
fn the_second_source_serves_l1_faithfully() {
    let faults = Faults(vec![Fault::WrongBlockHash { from_block: 0 }, Fault::Down]);
    let second = Caller {
        client: None,
        second: true,
    };
    let mut seen = block();
    faults.apply_from(second, "eth_getBlockByNumber", &mut seen);
    assert_eq!(seen, block());
    assert_eq!(faults.refusal_for(second), None);
    assert_eq!(faults.refusal(), Some(Refusal::Down));
}
