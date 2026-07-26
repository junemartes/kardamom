//! Child-process management for the local stack.
//!
//! Every stack component (JVMs and service binaries) runs as a plain OS child
//! with stdout+stderr teed to a per-process log file under the stack's temp
//! root. Readiness is polled from those log files or from the component's
//! network surface — never fixed sleeps.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

/// A supervised child process. Killed (SIGKILL) and reaped on drop so a
/// panicking test never leaks JVMs or service binaries.
pub struct Proc {
    pub name: String,
    child: Child,
    pub log_path: PathBuf,
}

impl Proc {
    /// Spawn `cmd` with stdout+stderr appended to `log_path`.
    pub fn spawn(name: &str, mut cmd: Command, log_path: PathBuf) -> Result<Self> {
        let log = std::fs::File::create(&log_path)
            .with_context(|| format!("create log file {}", log_path.display()))?;
        let log_err = log.try_clone().context("clone log handle")?;
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

    /// SIGKILL + reap. Idempotent.
    pub fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// SIGTERM, then wait up to `grace` for exit; SIGKILL on overrun.
    /// Returns true when the process exited within the grace window.
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

    /// SIGSTOP — freeze the process without killing it (S7 uses this to
    /// silence the executor's genuine publications while injecting).
    pub fn suspend(&self) {
        #[cfg(unix)]
        unsafe {
            libc::kill(self.child.id() as i32, libc::SIGSTOP);
        }
    }

    /// SIGCONT — resume a suspended process.
    pub fn resume(&self) {
        #[cfg(unix)]
        unsafe {
            libc::kill(self.child.id() as i32, libc::SIGCONT);
        }
    }

    /// Wait up to `timeout` for the process to exit on its own; returns its
    /// exit code (`None` element when killed by signal) or `None` on timeout.
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

/// Poll `path` until it contains `needle` or the deadline passes. Also fails
/// fast if `proc` exits first (a component that dies during startup should
/// fail the bring-up immediately, with its log tail, not after a timeout).
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

/// Reserve a free TCP port on loopback. The listener is dropped before
/// returning, so there is a small reuse race — acceptable for tests, and the
/// OS cycles ephemeral ports rather than re-issuing the same one immediately.
pub fn free_tcp_port() -> Result<u16> {
    let l = std::net::TcpListener::bind("127.0.0.1:0").context("reserve tcp port")?;
    Ok(l.local_addr()?.port())
}

/// Reserve a free UDP port on loopback (same caveat as [`free_tcp_port`]).
pub fn free_udp_port() -> Result<u16> {
    let s = std::net::UdpSocket::bind("127.0.0.1:0").context("reserve udp port")?;
    Ok(s.local_addr()?.port())
}
