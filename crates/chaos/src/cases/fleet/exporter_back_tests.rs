use std::time::Duration;

use super::{ExecutorScan, Reading};

fn reading(container: &str, body: Option<&str>) -> Reading {
    Reading {
        container: container.to_string(),
        body: body.map(str::to_string),
    }
}

fn stall_text(readings: Vec<Reading>) -> String {
    ExecutorScan(readings)
        .stall("pipeline-blackout-recover", Duration::from_secs(201))
        .to_string()
}

#[test]
fn no_answer_reads_as_dark_exporters() {
    let text = stall_text(vec![reading("exec-0", None), reading("exec-1", None)]);
    assert_eq!(
        text,
        "CHAOS FAIL: pipeline-blackout-recover: no executor exporter answers 201s after the fleet returned"
    );
}

#[test]
fn an_answer_with_no_block_names_each_executor() {
    let parked = "# TYPE kardamom_sealer_boundaries_emitted_total counter\n\
        kardamom_sealer_boundaries_emitted_total 812\n\
        kardamom_void_pending 2\n";
    let idle = "kardamom_sealer_boundaries_emitted_total 790\nkardamom_void_pending 0\n";
    let text = stall_text(vec![
        reading("exec-0", Some(parked)),
        reading("exec-1", Some(idle)),
        reading("exec-2", None),
    ]);
    assert_eq!(
        text,
        "CHAOS FAIL: pipeline-blackout-recover: executor exporters answer 201s after the fleet \
         returned, but no executor finished a block: \
         exec-0 answers with no block gauge (sealer boundaries 812); it waits for a void \
         decision on a lost entry (kardamom_void_pending 2); \
         exec-1 answers with no block gauge (sealer boundaries 790); \
         exec-2 does not answer"
    );
}

#[test]
fn a_block_after_the_budget_still_fails() {
    let text = stall_text(vec![reading(
        "exec-0",
        Some("kardamom_executor_block_number 7\n"),
    )]);
    assert_eq!(
        text,
        "CHAOS FAIL: pipeline-blackout-recover: the first executor block appeared only after the 201s budget"
    );
}
