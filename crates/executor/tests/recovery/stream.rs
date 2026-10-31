//! The executor-stream checks of the executor restart cases, on a real
//! media driver and archive. Every replica records its joined
//! transactions on the local archive. A restarted replica must record a
//! new session and move its recorded cursor, and while one replica is
//! dead, the recorded cursors of its peers keep moving: the sealer takes
//! the best cursor over the executors.

use std::time::{Duration, Instant};

use kardamom_log::testing::AeronTestCluster;

use super::BLOCK_EVERY;
use super::fleet::{Fleet, NodeHandle, Wait, World};
use super::verdict::CATCH_UP;

const SESSION_ID: &str = "kardamom_executor_exec_stream_session_id";
const RECORDED_INDEX: &str = "kardamom_executor_exec_stream_recorded_index";
/// The bound of the chaos check for a new session.
const NEW_SESSION: Duration = Duration::from_secs(30);

impl NodeHandle {
    fn session(&self) -> Option<f64> {
        self.gauge(SESSION_ID)
    }

    fn recorded(&self) -> Option<f64> {
        self.gauge(RECORDED_INDEX)
    }

    /// Wait until the node shows a session other than `before`, then
    /// until its recorded cursor passes its first value.
    fn assert_records_a_new_session(&mut self, before: Option<f64>) {
        let ((session, first), _) = self.wait_for(Wait::new(NEW_SESSION, "a new session"), |n| {
            n.session().filter(|s| Some(*s) != before).zip(n.recorded())
        });
        self.assert_cursor_moves(first);
        eprintln!(
            "{}: session {before:?} -> {session}, cursor past {first}",
            self.name()
        );
    }

    fn assert_cursor_moves(&mut self, from: f64) {
        self.wait_for(Wait::new(CATCH_UP, "a recorded cursor move"), |n| {
            n.recorded().filter(|now| *now > from)
        });
    }
}

/// graceful-executor and hard-executor, the stream part: a stopped and
/// a killed replica each come back with a new recorded session whose
/// cursor moves; while the killed replica is down, the cursors of its
/// two peers keep moving; the fleet ends in the exact state.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker: a real media driver and archive"]
async fn restarted_replicas_record_new_sessions_and_live_cursors_keep_moving() {
    kardamom_log::testing::require_docker().await;
    let booted = Instant::now();
    let cluster = AeronTestCluster::single_node()
        .await
        .expect("aeron container");
    eprintln!("media driver and archive up after {:?}", booted.elapsed());
    let started = Instant::now();
    let world = World::new();
    world.seal_through(5);
    let load = world.load(5, 100_000, BLOCK_EVERY);
    let mut fleet: Fleet = world.fleet();
    fleet
        .0
        .iter_mut()
        .for_each(|n| n.cfg.aeron_dir = Some(cluster.aeron_dir_host(0).to_path_buf()));
    fleet.start();
    fleet.wait_head(10, CATCH_UP);
    let before: Vec<Option<f64>> = fleet.0.iter().map(NodeHandle::session).collect();
    assert!(
        before.iter().all(Option::is_some),
        "every replica exports its session: {before:?}"
    );

    fleet.0[0].stop();
    fleet.0[0].start();
    fleet.0[0].assert_records_a_new_session(before[0]);

    fleet.0[1].kill();
    let peers: Vec<f64> = [0, 2]
        .iter()
        .map(|&i| fleet.0[i].recorded().unwrap())
        .collect();
    [0, 2]
        .iter()
        .zip(&peers)
        .for_each(|(&i, &from)| fleet.0[i].assert_cursor_moves(from));
    fleet.0[1].start();
    fleet.0[1].assert_records_a_new_session(before[1]);

    fleet.wait_head(load.sealed() + 5, CATCH_UP);
    let head = load.halt();
    fleet.assert_converged(&world, head, CATCH_UP);
    fleet.0.iter().for_each(NodeHandle::assert_no_republished);
    eprintln!(
        "stream checks took {:?} after the driver start",
        started.elapsed()
    );
}
