use super::*;

#[test]
fn another_load_cannot_open_the_injection_gate() {
    let tracker = Arc::new(Tracker::new().unwrap());
    let other = Tracker::new().unwrap();
    let progress = Progress::new(tracker.clone());
    {
        let _window = progress.begin();
        other.note_offered();
        other.note_accepted();
        assert!(!progress.is_flowing());
        tracker.note_offered();
        tracker.note_accepted();
        assert!(progress.is_flowing());
    }
    assert!(!progress.is_submitting());
    assert!(!progress.is_flowing());
    assert_eq!(progress.accepted(), 1);
}

#[tokio::test]
async fn a_stop_requested_before_the_window_is_not_lost() {
    let progress = Progress::new(Arc::new(Tracker::new().unwrap()));
    progress.finish_submissions();
    let mut stopped = progress.stop_requested();
    assert!(*stopped.wait_for(|stop| *stop).await.unwrap());
}

#[tokio::test]
async fn cancellation_closes_the_submission_window() {
    let progress = Progress::new(Arc::new(Tracker::new().unwrap()));
    let observed = progress.clone();
    let task = tokio::spawn(async move {
        let _window = progress.begin();
        std::future::pending::<()>().await;
    });
    tokio::task::yield_now().await;
    assert!(observed.is_submitting());
    task.abort();
    let _ = task.await;
    assert!(!observed.is_submitting());
}
