//! checkpoint-disk-full on a real full filesystem. One executor process
//! runs in a disposable container, with its checkpoints and block log on
//! a dedicated 16-MiB tmpfs and its state on the host. The parent fills
//! the tmpfs until the kernel refuses a write.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use super::BLOCK_EVERY;
use super::fleet::{CHILD_ARGS, NODE_ENV, NodeHandle, Wait, World};
use super::verdict::CATCH_UP;

/// The checkpoint volume inside the container.
const VOLUME: &str = "/ckpt";
const VOLUME_BYTES: u64 = 16 << 20;
/// A base image without glibc; the host's loader and libraries come in
/// as read-only mounts.
const IMAGE: &str = "redis:7-alpine";
const BLOCK: &str = "kardamom_executor_block_number";
const APPLIED: &str = "kardamom_executor_tx_applied_total";
const LOG_FAILED: &str = "kardamom_checkpoint_log_failed_total";
const LOG_ENOSPC: &str = "the block log failed; the next batch starts a new segment";
const BASE_ENOSPC: &str = "checkpoint base round failed";

/// The disposable container of the victim. Its drop removes it, also
/// after a failed assertion.
struct Container {
    name: String,
}

impl Drop for Container {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "--force", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

impl Container {
    /// The `docker run` of the victim's executor process.
    fn run_command(&self, roots: [&Path; 2], config: &Path) -> Command {
        let binary = std::env::current_exe().unwrap();
        let mut command = Command::new("docker");
        command.args(["run", "--rm", "--name", &self.name, "--network", "host"]);
        command.args(["--user", &format!("{}:{}", Self::id("-u"), Self::id("-g"))]);
        command.args([
            "--mount",
            &format!("type=tmpfs,destination={VOLUME},tmpfs-size={VOLUME_BYTES},tmpfs-mode=1777"),
        ]);
        for root in roots {
            Self::bind(&mut command, root, false);
        }
        Self::bind(&mut command, &binary, true);
        Self::libraries(&mut command, &binary);
        command.args(["--env", &format!("{NODE_ENV}={}", config.display())]);
        command.args(["--env", "NO_COLOR=1", "--env", "RUST_LOG=info"]);
        command.args([
            "--env",
            &format!("LD_LIBRARY_PATH={}", Self::library_path()),
        ]);
        command.args(["--entrypoint", binary.to_str().unwrap(), IMAGE]);
        command.args(CHILD_ARGS);
        command
    }

    fn id(flag: &str) -> String {
        let out = Command::new("id").arg(flag).output().unwrap();
        String::from_utf8(out.stdout).unwrap().trim().to_string()
    }

    fn library_path() -> String {
        std::env::var("LD_LIBRARY_PATH").unwrap_or_default()
    }

    /// Mount `path` at the same path in the container. The source is the
    /// resolved file, so a symlinked loader path still works.
    fn bind(command: &mut Command, path: &Path, readonly: bool) {
        let source = std::fs::canonicalize(path).unwrap();
        let mode = if readonly { ",readonly" } else { "" };
        command.args([
            "--mount",
            &format!(
                "type=bind,source={},target={}{mode}",
                source.display(),
                path.display()
            ),
        ]);
    }

    /// Mount every shared library of the test binary at its host path.
    fn libraries(command: &mut Command, binary: &Path) {
        let output = Command::new("ldd").arg(binary).output().unwrap();
        assert!(
            output.status.success(),
            "ldd lists the test binary's libraries"
        );
        String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .filter_map(|line| line.split_whitespace().find(|word| word.starts_with('/')))
            .for_each(|library| Self::bind(command, Path::new(library), true));
    }

    /// Run `script` in the container, and return its output.
    fn exec(&self, script: &str) -> String {
        let out = Command::new("docker")
            .args(["exec", &self.name, "sh", "-c", script])
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).into_owned()
    }

    /// The free bytes of the volume.
    fn available(&self) -> u64 {
        self.exec(&format!("df -k {VOLUME} | tail -1"))
            .split_whitespace()
            .nth(3)
            .and_then(|kib| kib.parse::<u64>().ok())
            .map_or(u64::MAX, |kib| kib * 1024)
    }

    /// Write until the kernel refuses with ENOSPC; require the refusal.
    fn fill(&self) {
        let out = self.exec(&format!(
            "cat /dev/zero 2>&1 >{VOLUME}/.fill; echo status=$?"
        ));
        assert!(out.contains("No space left on device"), "no ENOSPC: {out}");
        assert_eq!(self.available(), 0, "the volume keeps free space");
    }

    fn clear(&self) {
        self.exec(&format!("rm -f {VOLUME}/.fill"));
        assert!(self.available() > 0, "clearing the filler frees no space");
    }

    /// Copy the volume out to `to` through `docker exec` and tar.
    fn copy_out(&self, to: &Path) {
        std::fs::create_dir_all(to).unwrap();
        let tar = Command::new("docker")
            .args([
                "exec",
                &self.name,
                "tar",
                "-C",
                VOLUME,
                "--exclude",
                ".fill",
                "-cf",
                "-",
                ".",
            ])
            .output()
            .unwrap();
        assert!(tar.status.success(), "tar of the volume");
        let mut untar = Command::new("tar")
            .args(["-C", to.to_str().unwrap(), "-xf", "-"])
            .stdin(Stdio::piped())
            .spawn()
            .unwrap();
        std::io::Write::write_all(untar.stdin.as_mut().unwrap(), &tar.stdout).unwrap();
        assert!(untar.wait().unwrap().success(), "untar of the volume");
    }

    /// The newest complete base on the volume.
    fn newest_base(&self) -> Option<u64> {
        self.exec(&format!("ls {VOLUME}"))
            .split_whitespace()
            .filter_map(|n| n.strip_prefix("checkpoint-")?.parse().ok())
            .max()
    }
}

/// One reading of the victim: its block and applied count from
/// `/metrics`, and its ENOSPC failures from its log.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Progress {
    block: f64,
    applied: f64,
    failed: f64,
    log_enospc: usize,
    base_enospc: usize,
}

impl Progress {
    fn read(node: &NodeHandle) -> Option<Self> {
        let log = node.last_log();
        let enospc = |message: &str| {
            log.lines()
                .filter(|l| l.contains(message) && l.contains("os error 28"))
                .count()
        };
        Some(Self {
            block: node.gauge(BLOCK)?,
            applied: node.metric_sum(APPLIED)?,
            failed: node.metric_sum(LOG_FAILED).unwrap_or(0.0),
            log_enospc: enospc(LOG_ENOSPC),
            base_enospc: enospc(BASE_ENOSPC),
        })
    }

    /// Whether base `block` lies above the block of this reading.
    #[allow(
        clippy::cast_precision_loss,
        reason = "block numbers of these chains stay far below 2^52"
    )]
    fn below(self, block: u64) -> bool {
        block as f64 > self.block
    }

    fn failed_after(self, before: Self) -> bool {
        self.failed > before.failed
            && self.log_enospc > before.log_enospc
            && self.base_enospc > before.base_enospc
    }

    fn advanced_after(self, before: Self) -> bool {
        self.block > before.block && self.applied > before.applied
    }

    fn continued_during(self, before: Self) -> bool {
        self.advanced_after(before)
            && self.failed > before.failed
            && self.log_enospc > before.log_enospc
    }
}

impl NodeHandle {
    fn wait_progress(&mut self, what: &str, test: impl Fn(Progress) -> bool) -> Progress {
        self.wait_for(Wait::new(CATCH_UP, what), |n| {
            Progress::read(n).filter(|p| test(*p))
        })
        .0
    }
}

/// Restore the copied volume with its block log on the host, and return
/// the restored block.
fn restored_block(copy: &Path, state: &Path) -> u64 {
    kardamom_state::restore_best_checkpoint(copy, state, kardamom_state::BlockRange::ANY, None)
        .unwrap()
        .expect("a verified base")
        .0
}

/// checkpoint-disk-full: the checkpoint volume of a running executor
/// fills up. The block log and the base round both fail with ENOSPC,
/// while the same process keeps applying transactions and committing
/// blocks. After the filler goes, a new base above the block during the
/// fault restores with its log past the base, the same process advances
/// again, and the replica ends in the exact state.
#[test]
#[ignore = "requires Linux Docker: one container with a 16-MiB tmpfs"]
fn a_full_checkpoint_volume_keeps_the_same_executor_committing_and_recovers() {
    let world = World::new();
    world.seal_through(5);
    let load = world.load(5, 100_000, BLOCK_EVERY);
    let mut victim = world.node("executor-0");
    victim.cfg.checkpoint_dir = PathBuf::from(VOLUME);
    victim.cfg.block_log = true;
    victim.cfg.base_max_age_secs = std::num::NonZeroU64::MIN;
    let container = Container {
        name: format!(
            "executor-enospc-{}-{}",
            std::process::id(),
            victim.cfg.metrics.port()
        ),
    };
    let config = victim.write_config();
    victim.spawn(container.run_command(world.roots(), &config));
    victim.wait_head(10, CATCH_UP);
    let wait = Wait::new(CATCH_UP, "a baseline base");
    let baseline = std::iter::repeat_with(|| wait.step(container.newest_base(), String::new))
        .find_map(std::convert::identity)
        .unwrap();
    let before = victim.wait_progress("a first reading", |_| true);
    container.fill();
    let failed = victim.wait_progress("ENOSPC of the log and the base round", |p| {
        p.failed_after(before)
    });
    let during = victim.wait_progress("progress during ENOSPC", |p| p.continued_during(failed));
    assert_eq!(
        container.available(),
        0,
        "the fault held through the progress proof"
    );
    container.clear();
    let recovered = {
        let wait = Wait::new(CATCH_UP, "a new base above the fault");
        std::iter::repeat_with(|| {
            wait.step(
                container.newest_base().filter(|b| during.below(*b)),
                String::new,
            )
        })
        .find_map(std::convert::identity)
        .unwrap()
    };
    std::thread::sleep(Duration::from_secs(2));
    let copy = world.root.path().join("volume-copy");
    container.copy_out(&copy);
    let restored = restored_block(&copy, &world.root.path().join("restored"));
    assert!(
        restored > recovered,
        "the log reaches past base {recovered}: {restored}"
    );
    victim.wait_progress("same-process progress after release", |p| {
        p.advanced_after(during)
    });
    eprintln!(
        "baseline {baseline}, fault at {}, base {recovered}, restored {restored}",
        during.block
    );
    victim.wait_head(load.sealed() + 3, CATCH_UP);
    let head = load.halt();
    victim.wait_head(head, CATCH_UP);
    victim.stop();
    let reference = world.reference(head);
    victim.assert_matches(&reference, head);
    victim.assert_no_republished();
    drop(container);
}
