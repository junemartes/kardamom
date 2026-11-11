//! The parent's side: the executor processes, the sealer load, and the
//! waits and verdicts on them.

use std::net::SocketAddr;
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use super::chain::Chain;
use super::node::NodeConfig;
use super::sealer::SealerLog;

/// The environment variable that names the config of a child process.
pub(crate) const NODE_ENV: &str = "KARDAMOM_RECOVERY_NODE";
/// The tmpfs of the host.
const SHM: &str = "/dev/shm";
/// The arguments that run the child entry point of the test binary.
pub(crate) const CHILD_ARGS: [&str; 5] = [
    "--exact",
    "recovery_node",
    "--ignored",
    "--nocapture",
    "--test-threads=1",
];
/// The pause of each wait poll.
const POLL: Duration = Duration::from_millis(20);

/// The ports this test process handed out. Tests run in parallel, and
/// the kernel can give a released port to the next bind of another test
/// before the child binds it.
static HANDED_OUT: std::sync::Mutex<std::collections::BTreeSet<u16>> =
    std::sync::Mutex::new(std::collections::BTreeSet::new());

/// A free loopback address that no other test of this process holds.
fn free_addr() -> SocketAddr {
    std::iter::repeat_with(|| {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
    })
    .find(|addr| HANDED_OUT.lock().unwrap().insert(addr.port()))
    .unwrap()
}

/// The name prefix of the storage of one test process. The process id
/// follows, so a later run can remove the storage of a killed run.
const STORAGE_PREFIX: &str = "kardamom-exec-recovery-";
/// The tmpfs room a new node needs: its state, checkpoints and logs stay
/// far below this in these small chains.
const NODE_ROOM: u64 = 256 << 20;

/// One test's world: owned storage, the chain, and the sealer. The
/// storage goes with the world, also when a test panics.
pub(crate) struct World {
    pub(crate) root: tempfile::TempDir,
    /// Storage on disk, for a node that finds no tmpfs room.
    disk: tempfile::TempDir,
    pub(crate) chain: Arc<Chain>,
    pub(crate) sealer: SealerLog,
    genesis: PathBuf,
}

impl World {
    pub(crate) fn new() -> Self {
        Self::remove_storage_of_dead_runs();
        let disk = Self::storage_in(&std::env::temp_dir());
        // Storage lives on tmpfs where it has room: a process kill keeps
        // the page cache, so the fault stays the same, and the busy
        // host's fsync latency stays out of the recovery bound.
        let root = if Self::tmpfs_has_room() {
            Self::storage_in(Path::new(SHM))
        } else {
            Self::storage_in(&std::env::temp_dir())
        };
        if std::env::var_os("KARDAMOM_RECOVERY_KEEP").is_some() {
            eprintln!("recovery storage: {}", root.path().display());
        }
        let chain = Arc::new(Chain::new());
        let genesis = root.path().join("genesis.toml");
        chain.write_genesis(&genesis);
        let sealer = SealerLog::create(&root.path().join("sealer"));
        Self {
            root,
            disk,
            chain,
            sealer,
            genesis,
        }
    }

    fn storage_in(dir: &Path) -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix(&format!("{STORAGE_PREFIX}{}-", std::process::id()))
            .tempdir_in(dir)
            .unwrap()
    }

    /// Remove the storage that a killed test process left: a directory
    /// whose process no longer runs. A kill skips the drop of its world.
    fn remove_storage_of_dead_runs() {
        [Path::new(SHM).to_path_buf(), std::env::temp_dir()]
            .iter()
            .filter_map(|dir| std::fs::read_dir(dir).ok())
            .flatten()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .and_then(|name| {
                        name.strip_prefix(STORAGE_PREFIX)?
                            .split('-')
                            .next()?
                            .parse::<i32>()
                            .ok()
                    })
                    .is_some_and(Self::process_gone)
            })
            .for_each(|entry| {
                let _ = std::fs::remove_dir_all(entry.path());
            });
    }

    /// Whether process `pid` no longer exists. Only ESRCH says so: EPERM
    /// means a live process of another user.
    fn process_gone(pid: i32) -> bool {
        // SAFETY: signal 0 checks only that the process exists.
        let failed = unsafe { libc::kill(pid, 0) } != 0;
        failed && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
    }

    /// Whether `/dev/shm` exists with room for one more node.
    fn tmpfs_has_room() -> bool {
        let path = std::ffi::CString::new(SHM).unwrap();
        // SAFETY: `path` is a valid C string, and `stat` is a valid
        // out-buffer for the call.
        let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
        let ok = unsafe { libc::statvfs(path.as_ptr(), &raw mut stat) } == 0;
        ok && stat.f_bavail.saturating_mul(stat.f_frsize) >= NODE_ROOM
    }

    /// The storage roots of this world, for a container that mounts them.
    pub(crate) fn roots(&self) -> [&Path; 2] {
        [self.root.path(), self.disk.path()]
    }

    /// Seal blocks `1..=n` at once.
    pub(crate) fn seal_through(&self, n: u64) {
        (1..=n).for_each(|b| self.sealer.append_block(&self.chain, b));
    }

    /// Start a load that seals one block every `every` after `from`,
    /// until `last`.
    pub(crate) fn load(&self, from: u64, last: u64, every: Duration) -> Load {
        let sealed = Arc::new(AtomicU64::new(from));
        let stop = Arc::new(AtomicBool::new(false));
        let (chain, log, count, halt) = (
            self.chain.clone(),
            self.sealer.clone(),
            sealed.clone(),
            stop.clone(),
        );
        let thread = std::thread::spawn(move || {
            (from + 1..=last)
                .take_while(|_| !halt.load(Ordering::Acquire))
                .for_each(|b| {
                    std::thread::sleep(every);
                    log.append_block(&chain, b);
                    count.store(b, Ordering::Release);
                });
        });
        Load {
            sealed,
            stop,
            thread: Some(thread),
        }
    }

    /// A new executor node named `name` with its own storage and address.
    /// Each node checks the tmpfs room again: tests run in parallel, and
    /// a node that finds no room lives on disk.
    pub(crate) fn node(&self, name: &str) -> NodeHandle {
        let shm = self.root.path().starts_with(SHM) && Self::tmpfs_has_room();
        let dir = if shm || !self.root.path().starts_with(SHM) {
            self.root.path().join(name)
        } else {
            self.disk.path().join(name)
        };
        std::fs::create_dir_all(dir.join("state")).unwrap();
        std::fs::create_dir_all(dir.join("checkpoints")).unwrap();
        NodeHandle {
            cfg: NodeConfig {
                state_dir: dir.join("state"),
                checkpoint_dir: dir.join("checkpoints"),
                peers: Vec::new(),
                serve: free_addr(),
                rpc: free_addr(),
                checkpoints: true,
                block_log: false,
                base_max_age_secs: std::num::NonZeroU64::new(21_600).unwrap(),
                sealer: self.sealer.dir().to_path_buf(),
                genesis: self.genesis.clone(),
                receipts: dir.join("receipts.log"),
                metrics: free_addr(),
                aeron_dir: None,
            },
            dir,
            child: None,
            runs: 0,
        }
    }

    /// Three nodes that serve their checkpoints to each other.
    pub(crate) fn fleet(&self) -> Fleet {
        let mut nodes: Vec<NodeHandle> = (0..3)
            .map(|i| self.node(&format!("executor-{i}")))
            .collect();
        let addrs: Vec<String> = nodes.iter().map(|n| n.cfg.serve.to_string()).collect();
        nodes.iter_mut().enumerate().for_each(|(i, n)| {
            n.cfg.peers = addrs
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .map(|(_, a)| a.clone())
                .collect();
        });
        Fleet(nodes)
    }
}

/// The executor replicas of one test.
pub(crate) struct Fleet(pub(crate) Vec<NodeHandle>);

impl Fleet {
    pub(crate) fn start(&mut self) {
        self.0.iter_mut().for_each(NodeHandle::start);
    }

    pub(crate) fn stop(&mut self) {
        self.0.iter_mut().for_each(NodeHandle::stop);
    }

    pub(crate) fn kill(&mut self) {
        self.0.iter_mut().for_each(NodeHandle::kill);
    }

    /// Wait until every replica published boundary `block`. Returns the
    /// slowest wait.
    pub(crate) fn wait_head(&mut self, block: u64, within: Duration) -> Duration {
        self.0
            .iter_mut()
            .map(|n| n.wait_head(block, within))
            .max()
            .unwrap_or_default()
    }

    /// Stop every replica at `head` and require the exact state of an
    /// undisturbed replica on each.
    pub(crate) fn assert_converged(&mut self, world: &World, head: u64, within: Duration) {
        self.wait_head(head, within);
        self.stop();
        let reference = world.reference(head);
        self.0
            .iter()
            .for_each(|n| n.assert_matches(&reference, head));
    }
}

/// The bound of one wait, and what it waits for.
#[derive(Clone, Copy)]
pub(crate) struct Wait<'a> {
    started: Instant,
    within: Duration,
    what: &'a str,
}

impl<'a> Wait<'a> {
    pub(crate) fn new(within: Duration, what: &'a str) -> Self {
        Self {
            started: Instant::now(),
            within,
            what,
        }
    }

    /// Pass `found` on. With no answer yet, fail past the bound with the
    /// `context`, or pause before the next poll.
    pub(crate) fn step<T>(&self, found: Option<T>, context: impl FnOnce() -> String) -> Option<T> {
        if found.is_none() {
            assert!(
                self.started.elapsed() < self.within,
                "{} not within {:?}: {}",
                self.what,
                self.within,
                context()
            );
            std::thread::sleep(POLL);
        }
        found
    }
}

/// The background sealer load.
pub(crate) struct Load {
    sealed: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Load {
    /// The last sealed block.
    pub(crate) fn sealed(&self) -> u64 {
        self.sealed.load(Ordering::Acquire)
    }

    /// Stop the load now, and return the last sealed block.
    pub(crate) fn halt(mut self) -> u64 {
        self.stop.store(true, Ordering::Release);
        self.thread.take().unwrap().join().unwrap();
        self.sealed()
    }
}

impl Drop for Load {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// The parent's handle on one executor node: its storage and its
/// current process.
pub(crate) struct NodeHandle {
    pub(crate) cfg: NodeConfig,
    pub(crate) dir: PathBuf,
    child: Option<Child>,
    runs: u32,
}

impl NodeHandle {
    /// Start a new process on the node's storage.
    pub(crate) fn start(&mut self) {
        let config = self.write_config();
        let mut command = Command::new(std::env::current_exe().unwrap());
        // SAFETY: the hook only calls `prctl`, which is async-signal-safe.
        unsafe {
            command.pre_exec(|| {
                // The child ends with the test binary, also when CI kills it.
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) == 0 {
                    Ok(())
                } else {
                    Err(std::io::Error::last_os_error())
                }
            });
        }
        command
            .args(CHILD_ARGS)
            .env(NODE_ENV, &config)
            .env("NO_COLOR", "1")
            .env("RUST_LOG", "info");
        self.spawn(command);
    }

    /// Write the node's config for its next process, and return its path.
    pub(crate) fn write_config(&self) -> PathBuf {
        let path = self.dir.join("node.toml");
        std::fs::write(&path, toml::to_string(&self.cfg).unwrap()).unwrap();
        path
    }

    /// Spawn `command` as the node's next process, with its output in
    /// the log of the run.
    pub(crate) fn spawn(&mut self, mut command: Command) {
        assert!(self.child.is_none(), "{} already runs", self.name());
        self.runs += 1;
        let log = std::fs::File::create(self.run_log(self.runs)).unwrap();
        let child = command
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .unwrap();
        self.child = Some(child);
    }

    pub(crate) fn name(&self) -> String {
        self.dir.file_name().unwrap().to_string_lossy().into_owned()
    }

    fn run_log(&self, run: u32) -> PathBuf {
        self.dir.join(format!("run-{run}.log"))
    }

    /// The log of the current or last process.
    pub(crate) fn last_log(&self) -> String {
        std::fs::read_to_string(self.run_log(self.runs)).unwrap_or_default()
    }

    /// The logs of every process of the node.
    pub(crate) fn all_logs(&self) -> String {
        (1..=self.runs)
            .map(|run| std::fs::read_to_string(self.run_log(run)).unwrap_or_default())
            .collect()
    }

    /// The receipt lines of every process.
    pub(crate) fn receipts(&self) -> String {
        std::fs::read_to_string(&self.cfg.receipts).unwrap_or_default()
    }

    /// The highest boundary that the current process published: its
    /// exported head. A new process starts at 0.
    pub(crate) fn head(&self) -> u64 {
        let receipts = self.receipts();
        let current = receipts.rsplit_once("S ").map_or("", |(_, run)| run);
        current
            .lines()
            .filter_map(|l| l.strip_prefix("B "))
            .filter_map(|l| l.split(' ').next()?.parse().ok())
            .max()
            .unwrap_or(0)
    }

    /// Wait until the node published boundary `block`.
    pub(crate) fn wait_head(&mut self, block: u64, within: Duration) -> Duration {
        let what = format!("boundary {block}");
        self.wait_for(Wait::new(within, &what), |n| {
            (n.head() >= block).then_some(())
        })
        .1
    }

    /// Poll `probe` until it answers, while the process runs, within the
    /// bound of `wait`. Returns the answer and the time it took.
    pub(crate) fn wait_for<T>(
        &mut self,
        wait: Wait<'_>,
        mut probe: impl FnMut(&Self) -> Option<T>,
    ) -> (T, Duration) {
        let found = std::iter::repeat_with(|| self.poll(&wait, &mut probe))
            .find_map(std::convert::identity)
            .unwrap();
        (found, wait.started.elapsed())
    }

    /// One poll of [`Self::wait_for`].
    fn poll<T>(
        &mut self,
        wait: &Wait<'_>,
        probe: &mut impl FnMut(&Self) -> Option<T>,
    ) -> Option<T> {
        let found = probe(self);
        if found.is_none() {
            self.assert_alive();
        }
        wait.step(found, || {
            format!(
                "{} (head {}):\n{}",
                self.name(),
                self.head(),
                self.last_log()
            )
        })
    }

    /// The process is still running.
    pub(crate) fn assert_alive(&mut self) {
        let status = self.child.as_mut().map(|c| c.try_wait().unwrap());
        assert!(
            matches!(status, Some(None)),
            "{} is not running ({status:?}):\n{}",
            self.name(),
            self.last_log()
        );
    }

    /// Stop the process as the orchestrator does: SIGTERM, then a clean
    /// exit.
    pub(crate) fn stop(&mut self) {
        let pid = i32::try_from(self.child.as_ref().expect("a running process").id()).unwrap();
        // SAFETY: `pid` is a child of this process that has not been
        // reaped, so the signal reaches that process.
        assert_eq!(unsafe { libc::kill(pid, libc::SIGTERM) }, 0);
        let status = self.wait_exit(Duration::from_secs(20));
        assert!(
            status.success(),
            "{} stopped with {status:?}:\n{}",
            self.name(),
            self.last_log()
        );
    }

    /// Kill the process with no warning, as a node loss does.
    pub(crate) fn kill(&mut self) {
        let mut child = self.child.take().expect("a running process");
        child.kill().unwrap();
        assert_eq!(child.wait().unwrap().signal(), Some(libc::SIGKILL));
    }

    /// Wait for a process that must end by itself, and return its status.
    pub(crate) fn wait_failed(&mut self, within: Duration) -> ExitStatus {
        self.wait_exit(within)
    }

    /// Wait until the process exits, and reap it. Past the bound the
    /// wait fails, and the drop of the handle kills the process.
    fn wait_exit(&mut self, within: Duration) -> ExitStatus {
        let wait = Wait::new(within, "the exit of a process");
        let child = self.child.as_mut().expect("a running process");
        let status = std::iter::repeat_with(|| wait.step(child.try_wait().unwrap(), String::new))
            .find_map(std::convert::identity)
            .unwrap();
        self.child = None;
        status
    }

    /// One HTTP GET to the node's exporter: the status code and the body,
    /// or `None` while no exporter answers.
    pub(crate) fn get(&self, path: &str) -> Option<(u16, String)> {
        use std::io::{Read, Write};
        let mut tcp = std::net::TcpStream::connect(self.cfg.metrics).ok()?;
        write!(tcp, "GET {path} HTTP/1.0\r\n\r\n").ok()?;
        let mut answer = String::new();
        tcp.read_to_string(&mut answer).ok()?;
        let status = answer.split(' ').nth(1)?.parse().ok()?;
        Some((status, answer.split_once("\r\n\r\n")?.1.to_string()))
    }

    /// The sum of every series of `name`, from `/metrics`.
    pub(crate) fn metric_sum(&self, name: &str) -> Option<f64> {
        let (_, body) = self.get("/metrics")?;
        Some(
            body.lines()
                .filter(|l| {
                    l.strip_prefix(name)
                        .is_some_and(|rest| rest.starts_with(' ') || rest.starts_with('{'))
                })
                .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
                .sum(),
        )
    }

    /// The value of the unlabelled gauge `name`, from `/metrics`.
    pub(crate) fn gauge(&self, name: &str) -> Option<f64> {
        let (_, body) = self.get("/metrics")?;
        body.lines()
            .filter(|l| {
                l.strip_prefix(name)
                    .is_some_and(|rest| rest.starts_with(' ') || rest.starts_with('{'))
            })
            .find_map(|l| l.rsplit(' ').next()?.parse().ok())
    }

    /// Wait until `/ready` answers 200: the deployment health check.
    pub(crate) fn wait_ready(&mut self, within: Duration) -> Duration {
        self.wait_for(Wait::new(within, "/ready"), |n| {
            (n.get("/ready").map(|(status, _)| status) == Some(200)).then_some(())
        })
        .1
    }

    /// Delete the state DB; keep the checkpoints.
    pub(crate) fn wipe_state(&self) {
        Self::empty(&self.cfg.state_dir);
    }

    /// Delete the state DB and the checkpoints.
    pub(crate) fn wipe_all(&self) {
        Self::empty(&self.cfg.state_dir);
        Self::empty(&self.cfg.checkpoint_dir);
    }

    /// Install a checkpoint copied from a donor: stage it under a
    /// hidden name, then rename it into place, as the harness copy does.
    pub(crate) fn install_checkpoint(&self, donor: &Path, with_log: bool) {
        let staging = self.cfg.checkpoint_dir.join(".harness-staging");
        // The log goes in before the base: a restore reads the log when
        // the base becomes visible.
        let log = donor.with_file_name(kardamom_state::checkpoint::replay::LOG_DIR);
        let sources = if with_log {
            vec![log, donor.to_path_buf()]
        } else {
            vec![donor.to_path_buf()]
        };
        for source in &sources {
            self.install_dir(source, &staging);
        }
        std::fs::remove_dir_all(staging).unwrap();
    }

    /// Stage a copy of `source`, then rename it into the checkpoint
    /// directory.
    fn install_dir(&self, source: &Path, staging: &Path) {
        let name = source.file_name().unwrap();
        Self::copy_dir(source, &staging.join(name));
        std::fs::rename(staging.join(name), self.cfg.checkpoint_dir.join(name)).unwrap();
    }

    /// Copy the files of `from` into a new directory `to`.
    pub(crate) fn copy_dir(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).unwrap();
        std::fs::read_dir(from)
            .unwrap()
            .try_for_each(|entry| {
                let entry = entry?;
                std::fs::copy(entry.path(), to.join(entry.file_name())).map(|_| ())
            })
            .unwrap();
    }

    /// Every file of the state directory with its bytes.
    pub(crate) fn state_bytes(&self) -> Vec<(std::ffi::OsString, Vec<u8>)> {
        let mut files: Vec<_> = std::fs::read_dir(&self.cfg.state_dir)
            .unwrap()
            .map(|e| {
                let e = e.unwrap();
                (e.file_name(), std::fs::read(e.path()).unwrap_or_default())
            })
            .collect();
        files.sort();
        files
    }

    fn empty(dir: &Path) {
        std::fs::remove_dir_all(dir).unwrap();
        std::fs::create_dir_all(dir).unwrap();
    }

    /// The newest complete checkpoint, as `(block, path)`.
    pub(crate) fn newest_checkpoint(&self) -> Option<(u64, PathBuf)> {
        std::fs::read_dir(&self.cfg.checkpoint_dir)
            .unwrap()
            .filter_map(|e| {
                let path = e.ok()?.path();
                let block = path
                    .file_name()?
                    .to_str()?
                    .strip_prefix("checkpoint-")?
                    .parse()
                    .ok()?;
                path.join("MANIFEST").exists().then_some((block, path))
            })
            .max_by_key(|(block, _)| *block)
    }

    /// Wait for a complete checkpoint at or above `block`.
    pub(crate) fn wait_checkpoint(&mut self, block: u64, within: Duration) -> (u64, PathBuf) {
        let what = format!("a checkpoint at or above block {block}");
        self.wait_for(Wait::new(within, &what), |n| {
            n.newest_checkpoint().filter(|(b, _)| *b >= block)
        })
        .0
    }
}

impl Drop for World {
    fn drop(&mut self) {
        if std::env::var_os("KARDAMOM_RECOVERY_KEEP").is_some() {
            let _ = std::fs::rename(self.root.path(), self.root.path().with_extension("kept"));
        }
    }
}

impl Drop for NodeHandle {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
