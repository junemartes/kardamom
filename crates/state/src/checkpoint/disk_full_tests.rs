//! Real ENOSPC on an owned eight-MiB tmpfs, with state on a separate filesystem.

mod container;

use std::fs::File;
use std::io::Write;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use alloy_primitives::{B256, U256};
use kardamom_types::{GenesisAlloc, StateDatabase};
use metrics_exporter_prometheus::PrometheusHandle;

use super::block_log::{BlockLog, LogIdentity};
use super::log_thread::{BlockLogThread, LogConfig};
use super::tests::{ADDR, block_batch};
use super::{BlockRange, create_checkpoint, latest_checkpoint, restore_newest_readable};
use crate::{StateEnv, StateEnvBuilder, StateSnapshot, StateWriter, WriterHandle};

const BOUND: Duration = Duration::from_secs(5);

#[test]
#[ignore = "requires Linux Docker and one disposable container with an eight-MiB tmpfs"]
fn auxiliary_enospc_preserves_commits_and_recovers_checkpoint_and_log() {
    container::Container::run();
}

#[test]
#[ignore = "child entry point; the container parent supplies an isolated tmpfs"]
fn disk_full_child() {
    assert_eq!(std::env::var("KARDAMOM_CHECKPOINT_COMPONENT").unwrap(), "1");
    assert!(Path::new("/.dockerenv").exists());
    let mut fixture = Fixture::new();
    fixture.commit(1);
    fixture.wait_for_log(0, 1);
    create_checkpoint(&fixture.env, &fixture.checkpoints).unwrap();
    fixture.fill();

    fixture.commit(2);
    fixture.wait_for_failures(0);
    let error = create_checkpoint(&fixture.env, &fixture.checkpoints).unwrap_err();
    assert_eq!(
        error.to_string(),
        "recovery failed: mdbx_env_copy returned non-zero: 28"
    );
    assert_eq!(
        latest_checkpoint(&fixture.checkpoints, BlockRange::ANY)
            .unwrap()
            .unwrap()
            .block,
        1
    );
    fixture.restore("before-release", 1);
    let failed = fixture.failures();
    fixture.commit(3);
    fixture.wait_for_failures(failed);
    let logs = std::fs::read_to_string("/work/actor.log").unwrap();
    assert!(
        logs.lines()
            .any(|line| line.contains("the block log failed")
                && line.contains("No space left on device (os error 28)")),
        "{logs}"
    );

    std::fs::remove_file(fixture.checkpoints.with_file_name("filler")).unwrap();
    fixture.commit(4);
    fixture.wait_for_log(3, 4);
    assert_eq!(
        create_checkpoint(&fixture.env, &fixture.checkpoints)
            .unwrap()
            .block,
        4
    );
    fixture.commit(5);
    fixture.wait_for_log(4, 5);
    fixture.writer.shutdown().unwrap();
    fixture.log.take().unwrap().join().unwrap();
    fixture.restore("after-release", 5);
    println!("ENOSPC proof: checkpoint=28 block_log=28 committed_during=3 restored=5");
}

struct Fixture {
    env: StateEnv,
    writer: WriterHandle,
    log: Option<JoinHandle<()>>,
    checkpoints: PathBuf,
    identity: LogIdentity,
    metrics: PrometheusHandle,
}

impl Fixture {
    fn new() -> Self {
        std::fs::create_dir_all("/work").unwrap();
        tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(File::create("/work/actor.log").unwrap())
            .init();
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let metrics = recorder.handle();
        metrics::set_global_recorder(recorder).unwrap();
        let env = StateEnvBuilder::new("/work/state").open().unwrap();
        crate::genesis::seed_genesis(&env, &GenesisAlloc::of_accounts(Vec::new())).unwrap();
        let checkpoints = PathBuf::from("/dev/shm/checkpoints");
        let cfg = LogConfig {
            dir: checkpoints.join("log"),
            segment_bytes: NonZeroU64::new(1).unwrap(),
            queue: NonZeroUsize::new(2).unwrap(),
            sync_every: Duration::from_millis(10),
        };
        let identity = LogIdentity {
            genesis: super::manifest::stored_genesis_digest(&env).unwrap(),
            schema: crate::meta::SCHEMA_VERSION,
        };
        let (feed, log) = BlockLogThread::spawn(&env, &cfg).unwrap();
        let writer = StateWriter::spawn_with_log(env.clone(), feed).unwrap();
        Self {
            env,
            writer,
            log: Some(log),
            checkpoints,
            identity,
            metrics,
        }
    }

    fn commit(&self, block: u64) {
        self.writer
            .delta_tx
            .send(block_batch(ADDR, block, 2))
            .unwrap();
        let deadline = Instant::now().checked_add(BOUND).unwrap();
        let snapshot = std::iter::from_fn(|| self.writer.snapshot_rx.recv_deadline(deadline))
            .find(|snapshot| snapshot.block_number() == block)
            .expect("the same writer must commit while its auxiliary filesystem is full");
        Self::assert_state(&snapshot, block);
    }

    fn assert_state(snapshot: &StateSnapshot, block: u64) {
        assert_eq!(snapshot.block_number(), block);
        let (nonce, balance, _) = snapshot.basic(ADDR).unwrap().unwrap();
        assert_eq!(nonce, block);
        assert_eq!(balance, U256::from(block.checked_mul(100).unwrap()));
        assert_eq!(snapshot.end_tx_position().unwrap().as_index(), block);
        (1_u64..=5)
            .flat_map(|written| (0_u64..2).map(move |slot| (written, slot)))
            .for_each(|(written, slot)| {
                let key = written
                    .checked_mul(1_000_000)
                    .unwrap()
                    .checked_add(slot)
                    .unwrap();
                let expected = if written <= block {
                    U256::from(slot.checked_add(1).unwrap())
                } else {
                    U256::ZERO
                };
                assert_eq!(
                    snapshot.storage(ADDR, B256::from(U256::from(key))).unwrap(),
                    expected,
                    "storage written in block {written}, slot {slot}, after block {block}"
                );
            });
    }

    fn fill(&self) {
        let mut filler = File::create(self.checkpoints.with_file_name("filler")).unwrap();
        let error = (0..=2048)
            .try_for_each(|_| filler.write_all(&[0x5a; 4096]))
            .unwrap_err();
        assert_eq!(
            error.raw_os_error(),
            Some(28),
            "the actual tmpfs must run out of space"
        );
    }

    fn failures(&self) -> u64 {
        self.metrics
            .render()
            .lines()
            .filter(|line| line.starts_with("kardamom_checkpoint_log_failed_total{"))
            .map(|line| {
                line.split_whitespace()
                    .last()
                    .unwrap()
                    .parse::<u64>()
                    .unwrap()
            })
            .sum()
    }

    fn wait_for_failures(&self, before: u64) {
        let deadline = Instant::now().checked_add(BOUND).unwrap();
        while self.failures() <= before {
            Self::poll(deadline);
        }
    }

    fn wait_for_log(&self, base: u64, block: u64) {
        let deadline = Instant::now().checked_add(BOUND).unwrap();
        while !self.logged(base, block) {
            Self::poll(deadline);
        }
    }

    fn logged(&self, base: u64, block: u64) -> bool {
        BlockLog::open(&self.checkpoints.join("log"), self.identity)
            .unwrap()
            .after(base, block)
            .last()
            .is_some_and(|record| record.block == block)
    }

    fn poll(deadline: Instant) {
        assert!(Instant::now() < deadline, "block-log observation timed out");
        std::thread::sleep(Duration::from_millis(5));
    }

    fn restore(&self, name: &str, block: u64) {
        let path = Path::new("/work").join(name);
        let restored =
            restore_newest_readable(&self.checkpoints, &path, Some(self.identity.genesis))
                .unwrap()
                .unwrap();
        assert_eq!(restored.0, block);
        let env = StateEnvBuilder::new(path).open().unwrap();
        Self::assert_state(&StateSnapshot::open(&env).unwrap(), block);
    }
}
