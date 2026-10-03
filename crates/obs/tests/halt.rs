//! The `/halt` route beside `/metrics` and `/ready`: the record as JSON,
//! the gauge, readiness failing while a halt stands, and the operator's
//! clear from the loopback.

mod common;

use std::time::Duration;

use kardamom_obs::halt::{self, Halt, HaltCause};

#[tokio::test]
async fn the_halt_route_serves_the_record_and_readiness_fails_while_halted() {
    let addr = common::free_port();
    kardamom_obs::init_with_readiness(
        "halt-test",
        addr,
        "host",
        "0.0.0",
        "deadbeef",
        kardamom_obs::Readiness::up(),
    )
    .await
    .expect("init succeeds on a free port");
    let budget = Duration::from_secs(2);

    let none = common::get(addr, "/halt", budget).await;
    assert!(none.contains(" 200 "), "{none}");
    assert!(none.contains("\"halted\":false"), "{none}");
    assert!(none.contains("\"service\":\"halt-test\""), "{none}");

    halt::raise(Halt::new(
        HaltCause::L1ChainBreak,
        "block 7 parent 0xab expected 0xcd",
    ));
    let standing = common::get(addr, "/halt", budget).await;
    assert!(
        standing.contains("\"cause\":\"l1_chain_break\""),
        "{standing}"
    );
    assert!(
        standing.contains("block 7 parent 0xab expected 0xcd"),
        "{standing}"
    );
    assert!(
        standing.contains("\"runbook\":\"docs/runbooks/l1_chain_break.md\""),
        "{standing}"
    );
    assert!(standing.contains("\"clears\":\"auto\""), "{standing}");

    let not_ready = common::get(addr, "/ready", budget).await;
    assert!(not_ready.contains(" 503 "), "{not_ready}");
    assert!(
        not_ready.contains("halted: cause=l1_chain_break"),
        "{not_ready}"
    );

    let metrics = common::get(addr, "/metrics", budget).await;
    let gauge = metrics
        .lines()
        .find(|l| l.starts_with("kardamom_halt{"))
        .expect("the halt gauge is exported");
    assert!(gauge.contains("cause=\"l1_chain_break\""), "{gauge}");
    assert!(gauge.contains("recovery=\"l1_chain_break\""), "{gauge}");
    assert!(gauge.ends_with(" 1"), "{gauge}");

    let cleared = common::post(addr, "/halt/clear", budget).await;
    assert!(cleared.contains(" 200 "), "{cleared}");
    assert!(cleared.contains("cleared l1_chain_break"), "{cleared}");
    assert!(halt::current().is_none());

    let ready = common::get(addr, "/ready", budget).await;
    assert!(ready.contains(" 200 "), "{ready}");
    let metrics = common::get(addr, "/metrics", budget).await;
    assert!(
        metrics
            .lines()
            .any(|l| l.starts_with("kardamom_halt{") && l.ends_with(" 0")),
        "{metrics}"
    );
    let again = common::post(addr, "/halt/clear", budget).await;
    assert!(again.contains("no halt stands"), "{again}");
    let get_clear = common::get(addr, "/halt/clear", budget).await;
    assert!(
        get_clear.contains(" 404 "),
        "a clear is a POST: {get_clear}"
    );
}
