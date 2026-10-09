//! Tests of the answers state of the executor stream.

use super::*;

fn at(index: u64, session_id: i32, position: i64) -> ExecLocator {
    ExecLocator {
        index,
        session_id,
        position,
    }
}

/// A run that started at index 10, passed every slot through 19, parked
/// at 20, and dropped 15, with locators at 0 (an older session) and 10.
fn answers() -> ExecAnswers {
    let mut answers = ExecAnswers::default();
    answers.apply(AnswersMsg::Located(at(0, 1, 64)));
    answers.apply(AnswersMsg::First(10));
    answers.apply(AnswersMsg::Located(at(10, 2, 128)));
    answers.apply(AnswersMsg::Parked(15));
    answers.apply(AnswersMsg::Passed(19));
    answers.apply(AnswersMsg::Parked(20));
    answers
}

#[test]
fn the_current_run_answers_from_its_progress() {
    let answers = answers();
    assert_eq!(answers.answer(12), RunAnswer::Joined(Some(at(10, 2, 128))));
    assert_eq!(answers.answer(15), RunAnswer::NotHeld);
    assert_eq!(answers.answer(20), RunAnswer::NotHeld);
    assert_eq!(answers.answer(21), RunAnswer::NotReached);
    assert_eq!(answers.answer(5), RunAnswer::Before(Some(at(0, 1, 64))));
}

#[test]
fn a_fetched_entry_is_joined_and_the_reached_bound_never_moves_back() {
    let mut answers = answers();
    answers.apply(AnswersMsg::Fetched(20));
    answers.apply(AnswersMsg::Passed(20));
    answers.apply(AnswersMsg::Passed(3));
    assert_eq!(answers.answer(20), RunAnswer::Joined(Some(at(10, 2, 128))));
}

#[test]
fn before_the_first_message_every_entry_is_before_the_run() {
    let answers = ExecAnswers::default();
    assert_eq!(answers.answer(0), RunAnswer::Before(None));
}

#[test]
fn the_state_settles_an_entry_before_the_run() {
    let l = at(0, 1, 64);
    let located = ExecLocatorAnswer::Located {
        archive_id: "executor-1".to_owned(),
        session_id: 1,
        position: 64,
    };
    let settle = |run: RunAnswer, slot| run.settle("executor-1", slot);
    assert_eq!(
        settle(RunAnswer::Before(Some(l)), CommittedSlot::Executed),
        located
    );
    assert_eq!(
        settle(RunAnswer::Before(None), CommittedSlot::Executed),
        ExecLocatorAnswer::Lost
    );
    assert_eq!(
        settle(RunAnswer::Before(Some(l)), CommittedSlot::Vacant),
        ExecLocatorAnswer::NotHeld
    );
    assert_eq!(
        settle(RunAnswer::Before(Some(l)), CommittedSlot::Beyond),
        ExecLocatorAnswer::NotReached
    );
    assert_eq!(
        settle(RunAnswer::Joined(None), CommittedSlot::Beyond),
        ExecLocatorAnswer::NotReached
    );
    assert_eq!(
        settle(RunAnswer::Joined(Some(l)), CommittedSlot::Beyond),
        located
    );
}

#[test]
fn the_answer_has_a_tagged_json_form() {
    let lost = serde_json::to_string(&ExecLocatorAnswer::Lost).unwrap();
    assert_eq!(lost, r#"{"status":"lost"}"#);
    let located = ExecLocatorAnswer::Located {
        archive_id: "executor-0".to_owned(),
        session_id: -7,
        position: 4096,
    };
    let json = serde_json::to_string(&located).unwrap();
    assert_eq!(
        json,
        r#"{"status":"located","archive_id":"executor-0","session_id":-7,"position":4096}"#
    );
    assert_eq!(
        serde_json::from_str::<ExecLocatorAnswer>(&json).unwrap(),
        located
    );
}

#[test]
fn a_lookup_takes_the_newest_locator_at_or_below_the_index() {
    let mut locators = Locators::default();
    locators.push(at(0, 1, 0));
    locators.push(at(1024, 1, 9000));
    locators.push(at(1500, 2, 64));
    assert_eq!(locators.lookup(1023), Some(at(0, 1, 0)));
    assert_eq!(locators.lookup(1499), Some(at(1024, 1, 9000)));
    assert_eq!(locators.lookup(9999), Some(at(1500, 2, 64)));
    assert_eq!(Locators::default().lookup(5), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_owner_thread_answers_in_feed_order() {
    let (feed, lookup) = ExecAnswers::spawn("executor-2".to_owned()).unwrap();
    feed.first(4);
    feed.located(at(4, 9, 32));
    feed.passed(6);
    // The channel keeps the order, so the question sees every message.
    assert_eq!(
        lookup.ask(5).await,
        Some(RunAnswer::Joined(Some(at(4, 9, 32))))
    );
    assert_eq!(lookup.locate(100).await, Some(at(4, 9, 32)));
    assert_eq!(lookup.archive_id(), "executor-2");
}
