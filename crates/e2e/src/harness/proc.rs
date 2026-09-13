//! Child-process management for the local stack.
//!
//! Every stack component (JVMs and service binaries) runs as a plain OS
//! child. Its stdout and stderr are teed to a per-process log file under
//! the stack's temp root. This code polls readiness from those log files
//! or from the component's network surface, never from a fixed sleep.

use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

/// Ask the kernel to `SIGKILL` this child when the parent dies, and
/// guard against the race where the parent already died before this
/// call runs.
///
/// Runs as a `pre_exec` hook: between `fork` and `exec`, in the child,
/// before any other code.
#[cfg(target_os = "linux")]
fn set_pdeathsig() -> std::io::Result<()> {
    // SAFETY: `prctl`, `getppid`, and `raise` are each one
    // async-signal-safe syscall, the only kind of call allowed here.
    unsafe {
        if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
            return Err(std::io::Error::last_os_error());
        }
        // Guard against the race where the parent died between fork and
        // here. Without this check, the child would keep running with no
        // signal pending, exactly the leak PDEATHSIG exists to prevent.
        if libc::getppid() == 1 {
            libc::raise(libc::SIGKILL);
        }
        Ok(())
    }
}

/// A supervised child process. This is killed (SIGKILL) and reaped on
/// drop, so a panicking test never leaks JVMs or service binaries.
pub struct Proc {
    pub name: String,
    child: Child,
    pub log_path: PathBuf,
}

impl Proc {
    /// Spawn `cmd`, with stdout and stderr appended to `log_path`.
    ///
    /// The child gets `PR_SET_PDEATHSIG(SIGKILL)`, so it dies with the
    /// test process. `Drop` handles the normal exit paths, but it cannot
    /// run if something kills the test binary outright: `cargo test |
    /// head` closing the pipe (SIGPIPE), a CI step timeout, or a panic
    /// during runtime shutdown. Without this signal, any such
    /// interruption would strand a media driver, a sealer JVM, and four
    /// services. They would then compete for CPU and Aeron resources, and
    /// the next run would fail at bring-up looking like a flake.
    ///
    /// # Errors
    /// Returns an error when the log file cannot be created, or when
    /// spawning the child process fails.
    pub fn spawn(name: &str, mut cmd: Command, log_path: PathBuf) -> Result<Self> {
        let log = std::fs::File::create(&log_path)
            .with_context(|| format!("create log file {}", log_path.display()))?;
        let log_err = log.try_clone().context("clone log handle")?;
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::process::CommandExt;
            // SAFETY: `pre_exec` runs `set_pdeathsig` between fork and exec,
            // in the child, before any other code.
            unsafe {
                cmd.pre_exec(set_pdeathsig);
            }
        }
        let child = cmd
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(log_err))
            .spawn()
            .with_context(|| format!("spawn {name}: {cmd:?}"))?;
        Ok(Self {
            name: name.to_string(),
            child,
            log_path,
        })
    }

    /// True while the process has not exited.
    pub fn is_alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    /// Send SIGKILL, then reap the process. Safe to call more than once.
    pub fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// Send SIGTERM, then wait up to `grace` for the process to exit.
    /// Send SIGKILL if it overruns. Returns true when the process exited
    /// within the grace window.
    pub fn terminate(&mut self, grace: Duration) -> bool {
        if !self.is_alive() {
            return true;
        }
        #[cfg(unix)]
        unsafe {
            #[allow(
                clippy::cast_possible_wrap,
                reason = "Linux bounds pid_t well under i32::MAX (the default \
                           /proc/sys/kernel/pid_max is 4_194_304), so this never wraps"
            )]
            libc::kill(self.child.id() as i32, libc::SIGTERM);
        }
        let exited = super::metrics::poll_sync(
            "process exit after SIGTERM",
            grace,
            Duration::from_millis(50),
            || Ok((!self.is_alive()).then_some(())),
        )
        .is_ok();
        if !exited {
            self.kill();
        }
        exited
    }

    /// Send SIGSTOP to freeze the process without killing it (the
    /// divergence-detection test uses this to silence the executor's
    /// genuine publications while injecting).
    pub fn suspend(&self) {
        #[cfg(unix)]
        unsafe {
            #[allow(
                clippy::cast_possible_wrap,
                reason = "see the pid_t bound noted in terminate"
            )]
            libc::kill(self.child.id() as i32, libc::SIGSTOP);
        }
    }

    /// Send SIGCONT to resume a suspended process.
    pub(crate) fn resume(&self) {
        #[cfg(unix)]
        unsafe {
            #[allow(
                clippy::cast_possible_wrap,
                reason = "see the pid_t bound noted in terminate"
            )]
            libc::kill(self.child.id() as i32, libc::SIGCONT);
        }
    }

    /// Wait up to `timeout` for the process to exit on its own. Returns
    /// its exit code (the inner `None` means a signal killed it), or
    /// `None` on timeout.
    pub fn wait_exit(&mut self, timeout: Duration) -> Option<Option<i32>> {
        super::metrics::poll_sync(
            "process exit",
            timeout,
            Duration::from_millis(50),
            || match self.child.try_wait() {
                Ok(Some(status)) => Ok(Some(status.code())),
                Ok(None) => Ok(None),
                Err(e) => Err(anyhow::anyhow!(e)),
            },
        )
        .ok()
    }

    /// Last `n` lines of the process log (best-effort, for failure dumps).
    /// Block until `needle` appears in the log, the process exits, or
    /// `timeout` passes. A process that exits first, or a timeout, is an
    /// error that carries the log tail.
    ///
    /// # Errors
    /// Returns an error when the process exits before it logs `needle`,
    /// or when `timeout` passes first.
    pub fn wait_for_log_line(&mut self, needle: &str, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if let ControlFlow::Break(result) = self.poll_log_line(needle, deadline, timeout) {
                return result;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// One [`Self::wait_for_log_line`] poll: `Break` when the line is
    /// there, the process is gone, or the deadline passed.
    fn poll_log_line(
        &mut self,
        needle: &str,
        deadline: Instant,
        timeout: Duration,
    ) -> ControlFlow<Result<()>> {
        let found = std::fs::read_to_string(&self.log_path).is_ok_and(|s| s.contains(needle));
        if found {
            return ControlFlow::Break(Ok(()));
        }
        if !self.is_alive() {
            return ControlFlow::Break(Err(anyhow::anyhow!(
                "{} exited before logging {needle:?}; log tail:\n{}",
                self.name,
                self.log_tail(40)
            )));
        }
        if Instant::now() >= deadline {
            return ControlFlow::Break(Err(anyhow::anyhow!(
                "{}: timed out ({timeout:?}) waiting for {needle:?}; log tail:\n{}",
                self.name,
                self.log_tail(40)
            )));
        }
        ControlFlow::Continue(())
    }

    #[must_use]
    pub fn log_tail(&self, n: usize) -> String {
        match std::fs::read_to_string(&self.log_path) {
            Ok(s) => {
                let lines: Vec<&str> = s.lines().collect();
                let start = lines.len().saturating_sub(n);
                lines[start..].join("\n")
            }
            Err(e) => format!("<unreadable log {}: {e}>", self.log_path.display()),
        }
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        self.kill();
    }
}

/// Poll `path` until it contains `needle`, or until the deadline passes.
/// Also fails fast if `proc` exits first. A component that dies during
/// startup should fail the bring-up right away, with its log tail, not
/// after a timeout.
///
/// # Errors
/// Returns an error when `proc` exits before logging `needle`, or when
/// `timeout` passes first.
pub(crate) fn wait_for_log_line(proc: &mut Proc, needle: &str, timeout: Duration) -> Result<()> {
    super::metrics::poll_sync(
        &format!("{}: {needle:?}", proc.name),
        timeout,
        Duration::from_millis(100),
        || {
            if let Ok(s) = std::fs::read_to_string(&proc.log_path)
                && s.contains(needle)
            {
                return Ok(Some(()));
            }
            anyhow::ensure!(
                proc.is_alive(),
                "{} exited before logging {needle:?}",
                proc.name
            );
            Ok(None)
        },
    )
    .map_err(|e| anyhow::anyhow!("{e}; log tail:\n{}", proc.log_tail(40)))
}

/// Poll until `path` exists (media-driver readiness files).
///
/// # Errors
/// Returns an error when `proc` exits before `path` appears, or when
/// `timeout` passes first.
pub(crate) fn wait_for_file(proc: &mut Proc, path: &Path, timeout: Duration) -> Result<()> {
    super::metrics::poll_sync(
        &format!("{}: {}", proc.name, path.display()),
        timeout,
        Duration::from_millis(100),
        || {
            if path.exists() {
                return Ok(Some(()));
            }
            anyhow::ensure!(
                proc.is_alive(),
                "{} exited before {} appeared",
                proc.name,
                path.display()
            );
            Ok(None)
        },
    )
    .map_err(|e| anyhow::anyhow!("{e}; log tail:\n{}", proc.log_tail(40)))
}

/// A path checked, once, to exist and be a regular file.
///
/// Several lookups across the harness (jar files, the genesis TOML) ran
/// the same `ensure!(path.is_file(), ...)` check right before use. This
/// type moves that check to one constructor, so a caller that holds an
/// `ExistingFile` never has to check again.
pub struct ExistingFile(PathBuf);

impl ExistingFile {
    /// Check that `path` exists and is a regular file. `hint` names what
    /// to do when it is missing (a command to run, or an env var to set).
    ///
    /// # Errors
    /// Returns an error naming `path` and `hint` when `path` is not a file.
    pub(crate) fn new(path: PathBuf, hint: &str) -> Result<Self> {
        anyhow::ensure!(path.is_file(), "{} not found — {hint}", path.display());
        Ok(Self(path))
    }

    pub(crate) fn path(&self) -> &Path {
        &self.0
    }
}

/// Find a build artifact: use the path in the environment variable `var`
/// if it is set, else fall back to `fallback`. Either way, the result
/// must be an existing file. `hint` names what to do when the fallback
/// path is missing (a command to run).
///
/// # Errors
/// Returns an error when neither location holds a file.
pub(crate) fn resolve_artifact(var: &str, fallback: PathBuf, hint: &str) -> Result<ExistingFile> {
    if let Ok(p) = std::env::var(var) {
        return ExistingFile::new(PathBuf::from(p), &format!("set via {var}"));
    }
    ExistingFile::new(fallback, hint)
}

impl AsRef<Path> for ExistingFile {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl AsRef<std::ffi::OsStr> for ExistingFile {
    fn as_ref(&self) -> &std::ffi::OsStr {
        self.0.as_ref()
    }
}

impl std::fmt::Display for ExistingFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.display())
    }
}
