//! The root a consumer of the `l1_blocks` stream pauses on.
//!
//! The L1 follower runs as two instances. A consumer (the da-watcher, the
//! batcher) waits on it in three cases, and pauses with the follower as
//! its root:
//!
//! - every live follower instance is halted, as the `events` board shows:
//!   the root is the halt of one of them;
//! - the stream carried no record for the silence window (three finality
//!   steps in a deployment): both instances are down or stuck;
//! - the consumer waits for the record of a block that no archive holds
//!   yet.
//!
//! In the last two cases no follower names a halt, so the root is the
//! stream itself, [`HaltRef::l1_blocks_silent`]. The pause ends by itself
//! when records flow again.

use std::time::Duration;

use kardamom_types::service::HaltRef;
use tokio::sync::watch;
use tokio::time::Instant;

use crate::events::BoardView;
use crate::lifecycle::process;

/// One consumer's view of the follower.
#[derive(Debug)]
pub struct FollowerWatch {
    board: Option<watch::Receiver<BoardView>>,
    silence: Duration,
    last_record: Instant,
}

impl FollowerWatch {
    /// A watch that calls the stream silent after `silence` with no
    /// record. The silence starts now.
    #[must_use]
    pub fn new(silence: Duration) -> Self {
        Self {
            board: None,
            silence,
            last_record: Instant::now(),
        }
    }

    /// Read the follower's halts from the `events` board.
    #[must_use]
    pub fn with_board(mut self, board: watch::Receiver<BoardView>) -> Self {
        self.board = Some(board);
        self
    }

    /// A record arrived.
    pub fn saw_record(&mut self) {
        self.last_record = Instant::now();
    }

    /// The root at `now`: every live instance halted, a silent stream, or
    /// a record the consumer waits for (`waiting`). `None` while a
    /// follower serves.
    #[must_use]
    pub fn root(&self, now: Instant, waiting: bool) -> Option<HaltRef> {
        let halted = self
            .board
            .as_ref()
            .and_then(|board| board.borrow().all_halted(HaltRef::L1_FOLLOWER));
        let silent = now.saturating_duration_since(self.last_record) >= self.silence;
        halted.or_else(|| (silent || waiting).then(HaltRef::l1_blocks_silent))
    }

    /// Pause the process on the root at `now`, or end the pause.
    pub fn follow(&self, now: Instant, waiting: bool) {
        process().follow(self.root(now, waiting));
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use kardamom_types::service::{Halt, HaltCause, HaltRef, ServiceEvent, ServiceState};
    use tokio::time::Instant;

    use super::FollowerWatch;
    use crate::events::Board;

    fn halted(instance: &str) -> ServiceEvent {
        ServiceEvent {
            service: HaltRef::L1_FOLLOWER.into(),
            instance: instance.into(),
            seq: 1,
            state: ServiceState::Halted(Halt::new(HaltCause::L1ChainBreak, "block 7")),
        }
    }

    fn running(instance: &str) -> ServiceEvent {
        ServiceEvent {
            state: ServiceState::Running,
            ..halted(instance)
        }
    }

    /// One halted instance is not a root while the other serves; both
    /// halted is, with the halt as the root; a silent stream and a wait
    /// for a record name the stream.
    #[tokio::test(start_paused = true)]
    async fn the_follower_is_a_root_when_both_instances_halt_or_the_stream_is_silent() {
        let now = Instant::now();
        let mut board = Board::default();
        board.observe(halted("l1-indexer-0"), now);
        board.observe(running("l1-indexer-1"), now);
        let (tx, rx) = tokio::sync::watch::channel(board.view(now));
        let watch = FollowerWatch::new(Duration::from_secs(60)).with_board(rx);
        assert_eq!(watch.root(now, false), None);
        assert_eq!(watch.root(now, true), Some(HaltRef::l1_blocks_silent()));

        board.observe(halted("l1-indexer-1"), now);
        tx.send_replace(board.view(now));
        let root = watch.root(now, false).unwrap();
        assert_eq!(root.service, HaltRef::L1_FOLLOWER);
        assert_eq!(root.cause, HaltCause::L1ChainBreak);

        board.observe(running("l1-indexer-0"), now);
        tx.send_replace(board.view(now));
        tokio::time::advance(Duration::from_secs(61)).await;
        assert_eq!(
            watch.root(Instant::now(), false),
            Some(HaltRef::l1_blocks_silent())
        );
    }
}
