//! [`CopyRate`]: the copy of a checkpoint, at full speed or at a rate
//! limit.
//!
//! With no limit, libmdbx writes a compacted copy to the image file.
//!
//! With a limit, libmdbx writes the copy into a pipe, and a thread writes
//! the pipe to the image file at the limit. So the copy of a large state
//! does not take the whole disk from the processes that share it. The copy
//! still reads an online read-only snapshot, so it never blocks the
//! writer. That snapshot keeps the pages that it reads, so the database
//! file can grow during a slow copy.
//!
//! The copy into a pipe keeps the pages as they are, with no compaction.
//! A compacted copy into a pipe writes the meta pages first, with a root
//! page that libmdbx predicts before it walks the tree. That prediction
//! can be wrong with no error, and the image then opens empty. A copy that
//! keeps the pages needs no prediction. Its image holds the free pages of
//! the database too, so it is larger than a compacted one.
//!
//! Into a pipe, libmdbx also writes the zero pages up to the size of the
//! map. The thread turns each chunk of zeros into a hole in the file, so
//! only the bytes of data count for the limit.
//!
//! When libmdbx refuses the copy into a pipe, the copy writes a compacted
//! file directly with no limit, and counts
//! `kardamom_checkpoint_copy_unlimited_total`.

use std::fs::File;
use std::io::{PipeReader, PipeWriter, Read, Seek, SeekFrom, Write};
use std::num::NonZeroU64;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

use signet_libmdbx::ffi;
use tracing::warn;

use crate::compaction::{compact_to, prepare_dest};
use crate::env::StateEnv;
use crate::error::StateError;

/// The bytes that the thread moves from the pipe at a time.
const CHUNK: u64 = 1 << 20;
/// The copies that libmdbx refused into a pipe, and that ran with no limit.
const COPY_UNLIMITED: &str = "kardamom_checkpoint_copy_unlimited_total";

/// How fast the copy of a checkpoint writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CopyRate {
    /// As fast as the disk takes it.
    Unlimited,
    /// At most this many bytes of data a second.
    Limited(NonZeroU64),
}

impl CopyRate {
    /// Copy `env` to the file `dest`: compacted with no limit, and with
    /// the pages as they are at a limit.
    ///
    /// # Errors
    ///
    /// Returns an error when `dest` exists, when the copy fails, or when
    /// the file does not write.
    pub(crate) fn copy(self, env: &StateEnv, dest: &Path) -> Result<(), StateError> {
        let Self::Limited(rate) = self else {
            return compact_to(env, dest);
        };
        prepare_dest(dest)?;
        let (reader, writer) = std::io::pipe()?;
        let sink = RateSink::new(File::create(dest)?, rate);
        let drain = thread::Builder::new()
            .name("kardamom-checkpoint-copy".into())
            .spawn(move || sink.drain(reader))?;
        let rc = Self::copy_into(env, writer);
        drain
            .join()
            .map_err(|_| StateError::Recovery("the checkpoint copy thread panicked".into()))??;
        if rc == 0 {
            return Ok(());
        }
        warn!(
            rc,
            "libmdbx refused the copy into a pipe; copying with no limit"
        );
        metrics::counter!(COPY_UNLIMITED).increment(1);
        std::fs::remove_file(dest)?;
        compact_to(env, dest)
    }

    /// Write the copy of `env` into `pipe`, then close the pipe, so the
    /// thread at the other end reads the end. Returns the code of libmdbx.
    fn copy_into(env: &StateEnv, pipe: PipeWriter) -> std::ffi::c_int {
        // SAFETY: the env pointer is valid for the closure. The closure
        // owns `pipe`, so its descriptor is open for the whole call, and
        // the pipe closes when the closure ends.
        env.raw().with_raw_env_ptr(move |env_ptr| unsafe {
            ffi::mdbx_env_copy2fd(env_ptr, pipe.as_raw_fd(), ffi::MDBX_CP_DEFAULTS)
        })
    }
}

/// The end of the pipe that writes the image file at the rate limit.
struct RateSink {
    file: File,
    rate: NonZeroU64,
    started: Instant,
    /// The bytes of data written.
    data: u64,
    /// The length of the file, holes included.
    len: u64,
}

impl RateSink {
    fn new(file: File, rate: NonZeroU64) -> Self {
        Self {
            file,
            rate,
            started: Instant::now(),
            data: 0,
            len: 0,
        }
    }

    /// Write the pipe to the file until the pipe ends, then sync the file.
    fn drain(mut self, mut pipe: PipeReader) -> std::io::Result<()> {
        let mut chunk = Vec::new();
        while self.step(&mut pipe, &mut chunk)? {}
        self.file.set_len(self.len)?;
        self.file.sync_all()
    }

    /// Move one chunk from the pipe to the file. Returns `false` at the end
    /// of the pipe.
    fn step(&mut self, pipe: &mut PipeReader, chunk: &mut Vec<u8>) -> std::io::Result<bool> {
        chunk.clear();
        pipe.by_ref().take(CHUNK).read_to_end(chunk)?;
        let len = u64::try_from(chunk.len()).map_err(std::io::Error::other)?;
        if len == 0 {
            return Ok(false);
        }
        self.len = Self::grow(self.len, len)?;
        if chunk.iter().all(|b| *b == 0) {
            self.file.seek(SeekFrom::Start(self.len))?;
            return Ok(true);
        }
        self.file.write_all(chunk)?;
        self.data = Self::grow(self.data, len)?;
        self.pace();
        Ok(true)
    }

    /// Wait until the data written is not ahead of the rate.
    fn pace(&self) {
        let due_nanos = u128::from(self.data) * 1_000_000_000 / u128::from(self.rate.get());
        let due = Duration::from_nanos(u64::try_from(due_nanos).unwrap_or(u64::MAX));
        if let Some(wait) = due.checked_sub(self.started.elapsed()) {
            thread::sleep(wait);
        }
    }

    fn grow(total: u64, by: u64) -> std::io::Result<u64> {
        total
            .checked_add(by)
            .ok_or_else(|| std::io::Error::other("the copy is larger than u64"))
    }
}

#[cfg(test)]
#[path = "copy_rate_tests.rs"]
mod tests;
