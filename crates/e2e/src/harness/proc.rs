//! Child-process management for the local stack.
//!
//! Every stack component (JVMs and service binaries) runs as a plain OS
//! child. Its stdout and stderr are teed to a per-process log file under
//! the stack's temp root. This code polls readiness from those log files
//! or from the component's network surface, never from a fixed sleep.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

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
    pub fn spawn(name: &str, mut cmd: Command, log_path: PathBuf) -> Result<Self> {
        let log = std::fs::File::create(&log_path)
            .with_context(|| format!("create log file {}", log_path.display()))?;
        let log_err = log.try_clone().context("clone log handle")?;
        #[cfg(target_os = "linux")]
        unsafe {
            use std::os::unix::process::CommandExt;
            cmd.pre_exec(|| {
                // SAFETY: this is one async-signal-safe syscall, the only kind
                // of call allowed between fork and exec.
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                // Guard against the race where the parent died between fork
                // and here. Without this check, the child would keep
                // running with no signal pending, exactly the leak
                // PDEATHSIG exists to prevent.
                if libc::getppid() == 1 {
                    libc::raise(libc::SIGKILL);
                }
                Ok(())
            });
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

    /// The process id (for diagnostics).
    pub fn pid(&self) -> u32 {
        self.child.id()
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
            libc::kill(self.child.id() as i32, libc::SIGTERM);
        }
        let deadline = Instant::now() + grace;
        while Instant::now() < deadline {
            if !self.is_alive() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        self.kill();
        false
    }

    /// Send SIGSTOP to freeze the process without killing it (the
    /// divergence-detection test uses this to silence the executor's
    /// genuine publications while injecting).
    pub fn suspend(&self) {
        #[cfg(unix)]
        unsafe {
            libc::kill(self.child.id() as i32, libc::SIGSTOP);
        }
    }

    /// Send SIGCONT to resume a suspended process.
    pub fn resume(&self) {
        #[cfg(unix)]
        unsafe {
            libc::kill(self.child.id() as i32, libc::SIGCONT);
        }
    }

    /// Wait up to `timeout` for the process to exit on its own. Returns
    /// its exit code (the inner `None` means a signal killed it), or
    /// `None` on timeout.
    pub fn wait_exit(&mut self, timeout: Duration) -> Option<Option<i32>> {
        let deadline = Instant::now() + timeout;
        loop {
            match self.child.try_wait() {
                Ok(Some(status)) => return Some(status.code()),
                Ok(None) => {
                    if Instant::now() >= deadline {
                        return None;
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(_) => return None,
            }
        }
    }

    /// Last `n` lines of the process log (best-effort, for failure dumps).
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
pub fn wait_for_log_line(proc: &mut Proc, needle: &str, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(s) = std::fs::read_to_string(&proc.log_path)
            && s.contains(needle)
        {
            return Ok(());
        }
        if !proc.is_alive() {
            anyhow::bail!(
                "{} exited before logging {needle:?}; log tail:\n{}",
                proc.name,
                proc.log_tail(40)
            );
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "{}: timed out ({timeout:?}) waiting for {needle:?}; log tail:\n{}",
                proc.name,
                proc.log_tail(40)
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Poll until `path` exists (media-driver readiness files).
pub fn wait_for_file(proc: &mut Proc, path: &Path, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if path.exists() {
            return Ok(());
        }
        if !proc.is_alive() {
            anyhow::bail!(
                "{} exited before {} appeared; log tail:\n{}",
                proc.name,
                path.display(),
                proc.log_tail(40)
            );
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "{}: timed out ({timeout:?}) waiting for {}",
                proc.name,
                path.display()
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Reserve a free TCP port on loopback. The listener drops before this
/// function returns, so there is a small reuse race. This is fine for
/// tests, since the OS cycles ephemeral ports instead of reissuing the
/// same one right away.
pub fn free_tcp_port() -> Result<u16> {
    let l = std::net::TcpListener::bind("127.0.0.1:0").context("reserve tcp port")?;
    Ok(l.local_addr()?.port())
}

/// Reserve a free UDP port on loopback (same caveat as [`free_tcp_port`]).
pub fn free_udp_port() -> Result<u16> {
    let s = std::net::UdpSocket::bind("127.0.0.1:0").context("reserve udp port")?;
    Ok(s.local_addr()?.port())
}
