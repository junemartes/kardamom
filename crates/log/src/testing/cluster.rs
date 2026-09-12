//! `AeronTestCluster`: testcontainers-driven real Aeron for e2e tests.
//!
//! Other crates depend on `kardamom-log` with `features = ["docker-e2e"]`
//! and reuse this struct from their own `tests/` directory. Public API:
//!
//! ```ignore
//! let cluster = AeronTestCluster::single_node().await?;
//! let endpoint = cluster.archive_control_endpoint(0).await;
//! // ... build LogConfig pointing at endpoint, run scenario ...
//! drop(cluster); // tears down container
//! ```
//!
//! This whole module is gated behind `docker-e2e` (which implies
//! `testing`), so the testcontainers dependency does not reach hosts
//! without a Docker daemon.

use std::path::PathBuf;
use std::time::Duration;

use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

/// Tag for the locally built Aeron image.
const AERON_IMAGE_NAME: &str = "kardamom-aeron";
const AERON_IMAGE_TAG: &str = "test";

/// One Aeron node. This is either backed by a Docker container this
/// code spawns (the default; works on Linux, fails on macOS Docker
/// because virtiofs breaks `cnc.dat` shared-memory semantics), or an
/// "external" mode where the test points at an already-running Media
/// Driver through the `KARDAMOM_AERON_DIR` env var. External mode lets
/// macOS developers run the MD natively (`just aeron-driver-up`).
///
/// The `Container` variant is boxed to keep the enum small. Its inner
/// `ContainerAsync` is about 200 bytes versus the `External` variant's
/// two `PathBuf`s; clippy's `large_enum_variant` flags that gap.
enum Node {
    Container(Box<ContainerNode>),
    External {
        aeron_dir: PathBuf,
        archive_dir: PathBuf,
    },
}

struct ContainerNode {
    container: ContainerAsync<GenericImage>,
    aeron_dir_host: PathBuf,
    archive_dir_host: PathBuf,
}

impl Node {
    fn aeron_dir(&self) -> &std::path::Path {
        match self {
            Node::Container(c) => &c.aeron_dir_host,
            Node::External { aeron_dir, .. } => aeron_dir,
        }
    }
    fn archive_dir(&self) -> &std::path::Path {
        match self {
            Node::Container(c) => &c.archive_dir_host,
            Node::External { archive_dir, .. } => archive_dir,
        }
    }
}

/// Reusable Aeron e2e harness. Each instance owns one or more real
/// Aeron containers and exposes the host ports the Rust code should
/// connect to.
pub struct AeronTestCluster {
    nodes: Vec<Node>,
}

impl AeronTestCluster {
    /// Bring up a single Aeron node. If the env var
    /// `KARDAMOM_AERON_DIR` is set, adopt the already-running Media
    /// Driver that owns that directory instead. This env path lets a
    /// macOS developer run `just aeron-driver-up` and then run the e2e
    /// tests against a host-native MD, avoiding the Docker-on-macOS
    /// shared-memory limitation.
    ///
    /// # Errors
    ///
    /// Returns an error if building the Aeron Docker image fails, or
    /// if the container fails to start.
    pub async fn single_node() -> anyhow::Result<Self> {
        if let Ok(dir) = std::env::var("KARDAMOM_AERON_DIR") {
            let aeron_dir = PathBuf::from(dir);
            // Convention: the archive dir lives next to aeron.dir,
            // matching the layout `aeron-driver-up` writes.
            let archive_dir = aeron_dir
                .parent()
                .map_or_else(|| aeron_dir.clone(), |p| p.join("archive"));
            return Ok(Self {
                nodes: vec![Node::External {
                    aeron_dir,
                    archive_dir,
                }],
            });
        }
        ensure_image_built().await?;
        let node = spawn_node().await?;
        Ok(Self { nodes: vec![node] })
    }

    /// Bring up `n` Aeron nodes for multi-recorder tests. `n` cannot be
    /// zero: an empty cluster is never a valid multi-recorder test, and
    /// `NonZeroUsize` rules it out at the call site instead of panicking
    /// later on an empty `nodes` index.
    ///
    /// # Errors
    ///
    /// Returns an error if building the Aeron Docker image fails, or
    /// if any of the `n` containers fails to start.
    pub async fn multi_node(n: std::num::NonZeroUsize) -> anyhow::Result<Self> {
        ensure_image_built().await?;
        let n = n.get();
        let mut nodes = Vec::with_capacity(n);
        for _ in 0..n {
            nodes.push(spawn_node().await?);
        }
        Ok(Self { nodes })
    }

    /// "host:port" the test should pass as the Aeron Archive control
    /// channel endpoint for node `i`. Container mode resolves the
    /// dynamically allocated host port. External mode returns the
    /// fixed port that `just aeron-driver-up` configures (8010).
    ///
    /// # Panics
    ///
    /// Panics if `i` is out of range, or if Docker never reports the
    /// container's mapped host port for the control channel.
    pub async fn archive_control_endpoint(&self, i: usize) -> String {
        match &self.nodes[i] {
            Node::Container(c) => {
                let port = c
                    .container
                    .get_host_port_ipv4(8010_u16.udp())
                    .await
                    .unwrap();
                format!("127.0.0.1:{port}")
            }
            Node::External { .. } => "127.0.0.1:8010".to_string(),
        }
    }

    /// "host:port" the test should pass as the Aeron Archive control
    /// response endpoint for node `i`. See
    /// [`archive_control_endpoint`](Self::archive_control_endpoint).
    ///
    /// # Panics
    ///
    /// Panics if `i` is out of range, or if Docker never reports the
    /// container's mapped host port for the response channel.
    pub async fn archive_response_endpoint(&self, i: usize) -> String {
        match &self.nodes[i] {
            Node::Container(c) => {
                let port = c
                    .container
                    .get_host_port_ipv4(8011_u16.udp())
                    .await
                    .unwrap();
                format!("127.0.0.1:{port}")
            }
            Node::External { .. } => "127.0.0.1:8011".to_string(),
        }
    }

    /// Host filesystem path of `aeron.dir` for node `i`. Pass this to
    /// `AeronRuntime::spawn_with_dir(...)` so a host Aeron client joins
    /// the Media Driver.
    #[must_use]
    pub fn aeron_dir_host(&self, i: usize) -> &std::path::Path {
        self.nodes[i].aeron_dir()
    }

    /// Host filesystem path of `aeron.archive.dir` for node `i`.
    #[must_use]
    pub fn archive_dir_host(&self, i: usize) -> &std::path::Path {
        self.nodes[i].archive_dir()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Stop node `i` (simulates recorder failure for quorum tests).
    /// In external mode this is a no-op, since the operator manages
    /// the MD.
    ///
    /// # Errors
    ///
    /// Returns an error if the Docker container fails to stop.
    pub async fn stop(&mut self, i: usize) -> anyhow::Result<()> {
        if let Node::Container(c) = &mut self.nodes[i] {
            c.container.stop().await?;
        }
        Ok(())
    }

    /// Bring up a single-node cluster, spawn an [`AeronRuntime`] against
    /// its bind-mounted `aeron.dir`, and build a [`LogConfig`] whose
    /// `tx_data` channel is a plain IPC template rooted at
    /// `stream_id_base`. This is the bring-up every docker-e2e test in
    /// this crate repeats (start the cluster, read `aeron_dir_host`,
    /// build the config, spawn the runtime).
    ///
    /// [`AeronRuntime`]: crate::aeron_live::AeronRuntime
    /// [`LogConfig`]: crate::config::LogConfig
    ///
    /// # Panics
    ///
    /// Panics if the cluster fails to start, or if the runtime fails to
    /// spawn against its `aeron.dir`. Test setup: a panic here is the
    /// correct failure mode, same as the `.expect()` calls this
    /// replaces.
    pub async fn single_node_runtime(stream_id_base: i32) -> SingleNodeRig {
        let cluster = Self::single_node().await.expect("aeron container started");
        let aeron_dir = cluster.aeron_dir_host(0).to_string_lossy().to_string();
        let rt =
            crate::aeron_live::AeronRuntime::spawn_with_dir(&aeron_dir).expect("aeron runtime");
        let mut cfg = crate::config::LogConfig::default();
        cfg.channels.tx_data_channel_template = "aeron:ipc?alias=a-{sid}".to_string();
        cfg.channels.tx_data_stream_id_base = stream_id_base;
        SingleNodeRig { cluster, rt, cfg }
    }
}

/// The bring-up [`AeronTestCluster::single_node_runtime`] returns: the
/// cluster (keep it alive for the test's duration; dropping it tears down
/// the container), the runtime, and a config already pointed at the
/// cluster's `tx_data` IPC channel.
pub struct SingleNodeRig {
    pub cluster: AeronTestCluster,
    pub rt: crate::aeron_live::AeronRuntime,
    pub cfg: crate::config::LogConfig,
}

/// Assert that Docker is reachable, with a clear failure message. Every
/// docker-e2e test shares this one `docker info` check. Every docker-e2e
/// test is explicit opt-in (`--ignored`), an environment where Docker is
/// required; its absence is an error, not a skip condition, so a caller
/// runs this before doing anything else, rather than let a missing
/// daemon fail some later step with a confusing error.
///
/// # Panics
///
/// Panics if `docker info` does not succeed.
pub async fn require_docker() {
    use tokio::process::Command;
    let available = Command::new("docker")
        .arg("info")
        .output()
        .await
        .is_ok_and(|o| o.status.success());
    assert!(
        available,
        "docker not available — required for this --ignored docker-e2e test"
    );
}

/// Receive from `sub` until `want` matches or `budget` elapses. Polls
/// `sub.recv()` with a 50 ms per-attempt timeout, matching the
/// deadline-with-timeout receive loop every docker-e2e test in this
/// crate repeats. Returns `None` on a timed-out budget.
///
/// # Panics
///
/// Panics if `budget` added to the current instant overflows the
/// clock's addable range (not reachable with any budget a test passes).
pub async fn recv_within(
    sub: &mut crate::aeron_live::TxDataSubscriberHandle,
    budget: Duration,
    want: impl Fn(&kardamom_types::TxEnvelope) -> bool,
) -> Option<kardamom_types::TxEnvelope> {
    let deadline = std::time::Instant::now()
        .checked_add(budget)
        .expect("test-fixture budget stays well under the clock's addable range");
    while std::time::Instant::now() < deadline {
        if let Some(env) = recv_attempt(sub, &want).await {
            return Some(env);
        }
    }
    None
}

/// One [`recv_within`] poll attempt: a single `sub.recv()` with a 50 ms
/// timeout. Returns the envelope only when `want` also matches it.
async fn recv_attempt(
    sub: &mut crate::aeron_live::TxDataSubscriberHandle,
    want: impl Fn(&kardamom_types::TxEnvelope) -> bool,
) -> Option<kardamom_types::TxEnvelope> {
    let (_pos, env) = tokio::time::timeout(Duration::from_millis(50), sub.recv())
        .await
        .ok()
        .flatten()?;
    want(&env).then_some(env)
}

/// Build the host tempdir and bind-mounted container.
///
/// Aeron stores absolute paths inside `cnc.dat` (the
/// command-and-control shared-memory file). When the host client opens
/// the buffer the MD has prepared, it follows those paths exactly. So
/// the bind-mount source and destination must share the same absolute
/// path on both host and container.
///
/// Layout (identical on host and inside container):
/// ```text
///   /tmp/kardamom-aeron-<suffix>/
///     dir/              <-- Aeron's `aeron.dir` (cnc.dat, etc.)
///     archive/dir/      <-- Aeron Archive's segment files
/// ```
/// Aeron's `MediaDriver.ensureDirectoryIsRecreated` removes and
/// recreates the inner `dir/` subdir on every start, so the
/// bind-mounted parent stays intact.
async fn spawn_node() -> anyhow::Result<Node> {
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let root = std::path::PathBuf::from(format!("/tmp/kardamom-aeron-{suffix:x}"));
    let aeron_dir = root.join("dir");
    let archive_dir = root.join("archive").join("dir");
    std::fs::create_dir_all(root.join("archive"))?;
    for d in [&root, &root.join("archive")] {
        let mut p = std::fs::metadata(d)?.permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            p.set_mode(0o777);
        }
        std::fs::set_permissions(d, p)?;
    }

    let root_str = root
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("mount path not utf-8"))?
        .to_string();
    let aeron_dir_in_container = format!("{root_str}/dir");
    let archive_dir_in_container = format!("{root_str}/archive/dir");

    let image = GenericImage::new(AERON_IMAGE_NAME, AERON_IMAGE_TAG)
        .with_exposed_port(8010_u16.udp())
        .with_exposed_port(8011_u16.udp())
        .with_exposed_port(8020_u16.udp())
        .with_exposed_port(8021_u16.udp())
        .with_wait_for(WaitFor::message_on_stdout("ArchiveAgent: started"))
        .with_mount(Mount::bind_mount(root_str.clone(), root_str.clone()))
        .with_env_var("AERON_DIR", aeron_dir_in_container)
        .with_env_var("AERON_ARCHIVE_DIR", archive_dir_in_container);

    let container = image.with_shm_size(256 * 1024 * 1024).start().await?;

    Ok(Node::Container(Box::new(ContainerNode {
        container,
        aeron_dir_host: aeron_dir,
        archive_dir_host: archive_dir,
    })))
}

/// Runs `docker build` for the Aeron image once per test run; this is
/// idempotent. This shells out to the docker CLI because
/// testcontainers has no "build if missing" helper. Cached layers make
/// repeat runs fast.
async fn ensure_image_built() -> anyhow::Result<()> {
    use tokio::process::Command;
    let image_ref = format!("{AERON_IMAGE_NAME}:{AERON_IMAGE_TAG}");
    let out = Command::new("docker")
        .args(["image", "inspect", &image_ref])
        .output()
        .await?;
    if out.status.success() {
        return Ok(());
    }
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let ctx = format!("{manifest_dir}/docker/aeron");
    let status = Command::new("docker")
        .args(["build", "-t", &image_ref, &ctx])
        .status()
        .await?;
    if !status.success() {
        return Err(anyhow::anyhow!("docker build failed (status {status:?})"));
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    Ok(())
}
