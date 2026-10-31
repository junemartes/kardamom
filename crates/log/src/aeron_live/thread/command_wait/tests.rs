use super::*;

struct Fixture;

impl Fixture {
    fn late_shutdown(worked: bool) {
        let (sender, receiver) = crossbeam_channel::unbounded();
        let mut waiting = ParkIdle::new(&receiver);
        std::thread::scope(|scope| {
            scope.spawn(move || {
                std::thread::sleep(Duration::from_millis(10));
                sender.send(RuntimeCmd::Shutdown).unwrap();
            });
            assert!(matches!(
                waiting.receive(&receiver, Duration::from_secs(1), worked),
                Ok(RuntimeCmd::Shutdown)
            ));
        });
    }

    fn disconnected() -> Receiver<RuntimeCmd> {
        let (_sender, receiver) = crossbeam_channel::unbounded();
        receiver
    }
}

#[test]
fn a_command_wakes_both_empty_and_worked_waits() {
    [false, true].into_iter().for_each(Fixture::late_shutdown);
}

#[test]
fn an_empty_timeout_does_not_consume_the_next_command() {
    let (sender, receiver) = crossbeam_channel::unbounded();
    let mut waiting = ParkIdle::new(&receiver);
    assert!(matches!(
        waiting.receive(&receiver, Duration::from_millis(1), false),
        Err(RecvTimeoutError::Timeout)
    ));
    sender.send(RuntimeCmd::Shutdown).unwrap();
    assert!(matches!(
        waiting.receive(&receiver, Duration::from_secs(1), false),
        Ok(RuntimeCmd::Shutdown)
    ));
    assert!(receiver.is_empty());
}

#[test]
fn a_disconnected_command_channel_ends_the_wait() {
    let receiver = Fixture::disconnected();
    let mut waiting = ParkIdle::new(&receiver);
    assert!(matches!(
        waiting.receive(&receiver, Duration::from_secs(1), false),
        Err(RecvTimeoutError::Disconnected)
    ));
}
