use std::cell::Cell;

use super::*;
use crate::driver_budget::DriverBudget;

/// A fake add: it answers on poll `answer_at` (never with `None`), and
/// counts its polls and its cancels.
struct FakeAdd {
    answer_at: Option<u32>,
    fail: bool,
    polls: Cell<u32>,
    cancels: Cell<u32>,
}

impl FakeAdd {
    fn answering_at(answer_at: Option<u32>) -> Self {
        Self {
            answer_at,
            fail: false,
            polls: Cell::new(0),
            cancels: Cell::new(0),
        }
    }
}

impl AsyncAdd for FakeAdd {
    type Out = u32;

    fn poll(&self) -> Result<Option<u32>, AeronCError> {
        self.polls.set(self.polls.get() + 1);
        if self.fail {
            return Err(AeronErrorType::GenericError.into());
        }
        Ok(self.answer_at.filter(|at| *at == self.polls.get()))
    }

    fn cancel(&self) -> Result<(), AeronCError> {
        self.cancels.set(self.cancels.get() + 1);
        Ok(())
    }
}

fn complete(wait: &AddWait, add: &FakeAdd) -> Result<u32, LogError> {
    wait.complete(add, "add_publication", "aeron:udp?control-mode=dynamic")
}

#[test]
fn the_start_up_wait_is_the_start_up_limit() {
    let limit = |ms| {
        DriverBudget::from_driver_timeout_ms(ms)
            .unwrap()
            .start_open_limit()
    };
    let production = AddWait::start_up(limit(10_000), CancellationToken::new());
    assert_eq!(production.within(), Duration::from_secs(30));
    let ci = AddWait::start_up(limit(30_000), CancellationToken::new());
    assert_eq!(ci.within(), Duration::from_secs(70));
    assert_eq!(ci.reply_wait(), Duration::from_secs(70) + ACK_TIMEOUT);
}

#[test]
fn an_answer_ends_the_wait_with_no_cancel() {
    let add = FakeAdd::answering_at(Some(3));
    let got = complete(&AddWait::run_time(Duration::from_secs(5)), &add);
    assert_eq!(got.unwrap(), 3);
    assert_eq!(add.cancels.get(), 0);
}

#[test]
fn a_wait_with_no_answer_times_out_once_and_cancels_the_add() {
    let add = FakeAdd::answering_at(None);
    let start = Instant::now();
    let error = complete(&AddWait::run_time(Duration::from_millis(50)), &add).unwrap_err();
    assert!(start.elapsed() >= Duration::from_millis(50));
    assert!(error.to_string().contains("TimedOut"), "{error}");
    assert!(
        error
            .to_string()
            .starts_with("aeron: add_publication aeron:udp?control-mode=dynamic: "),
        "{error}"
    );
    assert_eq!(add.cancels.get(), 1, "the unanswered add is cancelled");
}

#[test]
fn a_stop_ends_the_start_up_wait_at_once() {
    let stop = CancellationToken::new();
    let add = FakeAdd::answering_at(None);
    let wait = AddWait::start_up(Duration::from_secs(60), stop.clone());
    let start = Instant::now();
    let cancel = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(50));
        stop.cancel();
    });
    let error = complete(&wait, &add).unwrap_err();
    cancel.join().unwrap();
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "the stop ends the wait"
    );
    assert_eq!(
        error.to_string(),
        "aeron: add_publication aeron:udp?control-mode=dynamic: stopped during start-up open"
    );
    assert_eq!(add.cancels.get(), 1, "the stopped add is cancelled");
}

#[test]
fn a_driver_error_fails_at_once_with_no_cancel() {
    let add = FakeAdd {
        fail: true,
        ..FakeAdd::answering_at(None)
    };
    let error = complete(&AddWait::run_time(Duration::from_secs(5)), &add).unwrap_err();
    assert!(matches!(error, LogError::Aeron(_)));
    assert_eq!(add.polls.get(), 1);
    assert_eq!(add.cancels.get(), 0);
}
