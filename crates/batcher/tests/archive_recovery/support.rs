use std::num::NonZeroU32;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use alloy_primitives::{Address, B256};
use kardamom_log::aeron_live::{AeronRuntime, TxDataPublisherHandle, TxDataSubscriberHandle};
use kardamom_log::config::LogConfig;
use kardamom_log::recorder::{
    RecorderKind, RecorderThreads, connect_archive, record_stream_until_stopped,
};
use kardamom_log::refetch::{ArchiveRefetcher, EndpointSource, RefetchConfig};
use kardamom_log::testing::AeronTestCluster;
use kardamom_types::{TxDataLoc, TxEnvelope};

const STREAM: i32 = 7_600;
const COUNT: u8 = 12;
const BOUND: Duration = Duration::from_secs(20);

struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Driver {
    // The container stops before its owned files are removed.
    cluster: AeronTestCluster,
    scratch: Scratch,
}

pub(super) struct Fixture {
    driver: Driver,
    recording: Recording,
}

impl Fixture {
    pub(super) async fn recorded() -> Self {
        kardamom_log::testing::require_docker().await;
        let cluster =
            AeronTestCluster::single_node_with_archive_length(NonZeroU32::new(65_536).unwrap())
                .await
                .unwrap();
        let scratch = Scratch(cluster.aeron_dir_host(0).parent().unwrap().to_path_buf());
        let driver = Driver { cluster, scratch };
        let recording = Recording::create(driver.cluster.aeron_dir_host(0)).await;
        Self { driver, recording }
    }

    pub(super) fn archive(&self) -> &Path {
        self.driver.cluster.archive_dir_host(0)
    }

    pub(super) fn segment(&self) -> String {
        format!("{}-0.rec", self.recording.id)
    }

    pub(super) async fn stop(&mut self) {
        self.driver.cluster.stop(0).await.unwrap();
        // Host repair owns the files that the root-owned Docker archive creates.
        let owner = std::fs::metadata(&self.driver.scratch.0).unwrap();
        Tool::new()
            .run(
                self.archive(),
                "chown",
                &[
                    "-R",
                    &format!("{}:{}", owner.uid(), owner.gid()),
                    "/archive",
                ],
                0,
            )
            .await;
    }

    pub(super) fn corrupt(&self, segment: &str) {
        let path = self.archive().join(segment);
        let mut bytes = std::fs::read(&path).unwrap();
        let header = usize::try_from(self.recording.rows[3].0.position.term_offset).unwrap();
        let at = header.checked_add(40).unwrap();
        let end = at.checked_add(16).unwrap();
        bytes[at..end].iter_mut().for_each(|byte| *byte ^= 0x5a);
        std::fs::write(path, bytes).unwrap();
    }

    async fn tool(&self, args: &[&str], expected_code: i32) -> String {
        let mut command = vec![
            "--add-opens",
            "java.base/java.util.zip=ALL-UNNAMED",
            "-cp",
            "/opt/aeron/aeron-all.jar",
            "io.aeron.archive.ArchiveTool",
            "/archive",
        ];
        command.extend_from_slice(args);
        let text = Tool::new()
            .run(self.archive(), "java", &command, expected_code)
            .await;
        assert!(
            !text.contains("Exception"),
            "ArchiveTool crashes are not detection: {text}"
        );
        println!("ARCHIVE_TOOL {text}");
        text
    }

    async fn verify(&self, expected_code: i32) -> String {
        self.tool(
            &[
                "verify",
                &self.recording.id.to_string(),
                "-a",
                "-checksum",
                "io.aeron.archive.checksum.Crc32",
            ],
            expected_code,
        )
        .await
    }

    pub(super) async fn verify_clean(&self) {
        let text = self.verify(0).await;
        assert!(
            text.contains(&format!("recordingId={}) OK", self.recording.id)),
            "{text}"
        );
        assert!(!text.contains(" ERR"), "{text}");
    }

    pub(super) async fn verify_corrupt(&self) {
        let text = self.verify(255).await;
        assert!(
            text.contains(&format!("recordingId={}", self.recording.id)) && text.contains(" ERR"),
            "{text}"
        );
        assert!(
            text.to_ascii_lowercase().contains("checksum"),
            "CRC must identify the payload fault: {text}"
        );
    }

    pub(super) async fn mark_valid(&self) {
        self.tool(&["mark-valid", &self.recording.id.to_string()], 0)
            .await;
    }

    pub(super) async fn restart_and_replay(&mut self) {
        let dir = self.driver.cluster.aeron_dir_host(0).to_path_buf();
        std::fs::remove_dir_all(&dir).unwrap();
        self.driver.cluster.start(0).await.unwrap();
        tokio::time::timeout(BOUND, async {
            while !dir.join("cnc.dat").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        self.recording.replay(dir).await;
        self.stop().await;
    }
}

struct Tool {
    name: String,
    _scratch: tempfile::TempDir,
}

impl Tool {
    fn new() -> Self {
        let scratch = tempfile::Builder::new()
            .prefix("kardamom-archive-tool-")
            .tempdir()
            .unwrap();
        Self {
            name: scratch.path().file_name().unwrap().to_str().unwrap().into(),
            _scratch: scratch,
        }
    }

    async fn run(
        &self,
        archive: &Path,
        entrypoint: &str,
        args: &[&str],
        expected_code: i32,
    ) -> String {
        let mount = format!("type=bind,src={},dst=/archive", archive.display());
        let mut command = tokio::process::Command::new("docker");
        command
            .args([
                "run",
                "--rm",
                "--name",
                &self.name,
                "--network",
                "none",
                "--mount",
                &mount,
                "--entrypoint",
                entrypoint,
                "kardamom-aeron:test",
            ])
            .args(args)
            .kill_on_drop(true);
        let output = tokio::time::timeout(BOUND, command.output())
            .await
            .unwrap()
            .unwrap();
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            output.status.code(),
            Some(expected_code),
            "{entrypoint}: {text}"
        );
        text
    }
}

impl Drop for Tool {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &self.name])
            .output();
    }
}

struct Recording {
    id: i64,
    rows: Vec<(TxDataLoc, TxEnvelope)>,
}

impl Recording {
    fn config() -> LogConfig {
        let mut config = LogConfig::default();
        config.channels.tx_data_stream_id_base = STREAM;
        config.channels.tx_data_channel_template = "aeron:ipc?term-length=65536".into();
        config
    }

    fn envelope(id: u8) -> TxEnvelope {
        TxEnvelope {
            correlation_id: u64::from(id),
            sender: Address::repeat_byte(id),
            tx_hash: B256::repeat_byte(id),
            raw_tx: vec![id; 512].into(),
            max_inclusion_block: u64::MAX,
        }
    }

    async fn create(dir: &Path) -> Self {
        let runtime = AeronRuntime::spawn_with_dir(dir).unwrap();
        let config = Self::config();
        let mut subscriber = TxDataSubscriberHandle::open(&runtime, &config.channels, 0).unwrap();
        let publisher = TxDataPublisherHandle::open(&runtime, &config.channels, 0).unwrap();
        let mut recorders = RecorderThreads::new();
        let (ready, receive) = tokio::sync::oneshot::channel();
        let directory = dir.to_path_buf();
        recorders
            .spawn("archive-recovery-fixture".into(), move |stop| {
                record_stream_until_stopped(
                    Some(&directory),
                    &config.aeron,
                    &config.channels.tx_data_channel(0),
                    STREAM,
                    RecorderKind::TxData { sequencer_id: 0 },
                    &stop,
                    |result| {
                        let _ = ready.send(result);
                    },
                )
                .unwrap();
            })
            .unwrap();
        let id = tokio::time::timeout(BOUND, receive)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let mut rows = Vec::new();
        let mut end = 0;
        for sequence in 1..=COUNT {
            end = Self::round_trip(&publisher, &mut subscriber, sequence, &mut rows).await;
        }
        let directory = dir.to_path_buf();
        tokio::task::spawn_blocking(move || Self::seal(&directory, id, end))
            .await
            .unwrap();
        tokio::task::spawn_blocking(move || recorders.join())
            .await
            .unwrap();
        Self { id, rows }
    }

    async fn round_trip(
        publisher: &TxDataPublisherHandle,
        subscriber: &mut TxDataSubscriberHandle,
        id: u8,
        rows: &mut Vec<(TxDataLoc, TxEnvelope)>,
    ) -> i64 {
        let envelope = Self::envelope(id);
        let end = tokio::time::timeout(BOUND, publisher.publish_async(&envelope))
            .await
            .unwrap()
            .unwrap();
        let row = tokio::time::timeout(BOUND, subscriber.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.1, envelope);
        if let Some((first, _)) = rows.first() {
            assert_eq!(row.0.session_id, first.session_id);
            assert_eq!(row.0.position.term_id, first.position.term_id);
        }
        rows.push(row);
        i64::from(end.term_offset)
    }

    fn seal(dir: &Path, id: i64, end: i64) {
        let session = connect_archive(Some(dir), &Self::config().aeron).unwrap();
        let started = Instant::now();
        while session.archive.get_recording_position(id).unwrap() < end {
            Self::wait_for_archive(started, "archive never records the final envelope");
        }
        assert!(session.archive.try_stop_recording_by_identity(id).unwrap());
        while session.archive.get_stop_position(id).unwrap() < end {
            Self::wait_for_archive(started, "archive recording never stops");
        }
    }

    fn wait_for_archive(started: Instant, message: &str) {
        assert!(started.elapsed() < BOUND, "{message}");
        std::thread::sleep(Duration::from_millis(10));
    }

    async fn replay(&self, dir: PathBuf) {
        let from = self.rows[0].0;
        let expected = self.rows.clone();
        tokio::task::spawn_blocking(move || {
            let mut reader = ArchiveRefetcher::new(RefetchConfig {
                tx_data_endpoints: EndpointSource::Static(vec!["127.0.0.1:8010".into()]),
                tx_deposits_endpoints: EndpointSource::Static(vec![]),
                response_endpoint: "127.0.0.1:8012".into(),
                replay_endpoint: "127.0.0.1:8013".into(),
                aeron_dir: Some(dir),
                aeron: Self::config().aeron,
            });
            let mut rows = Vec::new();
            let count = reader
                .fetch_tx_data(STREAM, from.session_id, from.position, |loc, envelope| {
                    rows.push((loc, envelope));
                })
                .unwrap();
            assert_eq!(count, u64::from(COUNT));
            assert_eq!(
                rows, expected,
                "recovery preserves every payload, position and publisher session"
            );
        })
        .await
        .unwrap();
    }
}
