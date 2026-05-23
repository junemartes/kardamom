//! Continuous io_uring fsync sidecar.
//!
//! Polls a [`PositionSource`] (in production: the Aeron Archive
//! `recording-position` counter). Whenever the source advances past the
//! mirror's tail, the sidecar:
//!
//!   1. reads the new bytes from the recorder's segment file,
//!   2. submits an `IORING_OP_WRITE` of those bytes to the mirror file
//!      (opened `O_DIRECT`),
//!   3. submits an `IORING_OP_FSYNC` (with `IORING_FSYNC_DATASYNC`) linked
//!      after the write,
//!   4. waits for the fsync CQE,
//!   5. returns the new fsynced position so the caller can publish a
//!      [`kardamom_types::FsyncWatermark`].
//!
//! Buffers are 4 KiB aligned to satisfy `O_DIRECT`.
//!
//! ## Filesystem requirements
//!
//! The mirror file must live on a filesystem that supports `O_DIRECT` —
//! ext4, xfs, btrfs, etc. tmpfs returns `EINVAL` on `O_DIRECT` opens, so the
//! unit test in `tests/fsync_sidecar.rs` probes for support and skips on
//! tmpfs. Production deployments must point `FsyncConfig::mirror_path` at
//! durable enterprise NVMe (ideally with PLP).

use std::alloc::{Layout, alloc, dealloc};
use std::fs::OpenOptions;
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use io_uring::{IoUring, opcode, types};
use libc::O_DIRECT;

use crate::error::LogError;
use kardamom_types::BPosition;

pub trait PositionSource: Send {
    /// Returns the (monotonically increasing) Aeron stream position in bytes
    /// that has been written into the recorder's segment file.
    fn current(&self) -> i64;
}

pub struct FsyncSidecar {
    source_fd: std::fs::File,
    mirror_fd: std::fs::File,
    position: Box<dyn PositionSource>,
    ring: IoUring,
    /// Bytes mirrored + fsynced so far.
    fsynced: i64,
    /// 4 KiB aligned bounce buffer.
    bounce: AlignedBuf,
}

struct AlignedBuf {
    ptr: *mut u8,
    cap: usize,
    layout: Layout,
}

// SAFETY: AlignedBuf owns its allocation; the raw pointer is only handed to
// io_uring SQEs while the buffer is alive, and `tick()` blocks for the
// completion before returning, so no aliasing occurs across ticks.
unsafe impl Send for AlignedBuf {}

impl AlignedBuf {
    fn new(cap: usize) -> Self {
        let layout = Layout::from_size_align(cap, 4096).expect("alignable");
        // SAFETY: layout is valid (non-zero, alignment power-of-two).
        let ptr = unsafe { alloc(layout) };
        assert!(!ptr.is_null(), "alloc failed");
        Self { ptr, cap, layout }
    }

    fn as_mut(&mut self) -> &mut [u8] {
        // SAFETY: ptr is valid for cap bytes; exclusive access through &mut self.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.cap) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        // SAFETY: ptr was allocated with the stored layout.
        unsafe { dealloc(self.ptr, self.layout) };
    }
}

impl FsyncSidecar {
    pub fn open(
        source: &Path,
        mirror: &Path,
        position: Box<dyn PositionSource>,
        uring_entries: u32,
    ) -> Result<Self, LogError> {
        let source_fd = OpenOptions::new().read(true).open(source)?;
        let mirror_fd = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .custom_flags(O_DIRECT)
            .open(mirror)?;

        let ring = IoUring::new(uring_entries).map_err(LogError::Io)?;

        // 1 MiB bounce buffer per tick. Sized to amortize syscall overhead
        // while keeping per-tick latency well under the 250ms boundary cadence.
        let bounce = AlignedBuf::new(1 << 20);

        Ok(Self {
            source_fd,
            mirror_fd,
            position,
            ring,
            fsynced: 0,
            bounce,
        })
    }

    /// How many bytes have been mirrored + fsynced so far.
    pub fn fsynced(&self) -> i64 {
        self.fsynced
    }

    /// One iteration: copy any newly-available bytes through the bounce buffer,
    /// submit write + fdatasync linked SQEs, wait for fsync completion.
    /// Returns the new fsynced position, or `None` if nothing advanced.
    pub fn tick(&mut self) -> Result<Option<BPosition>, LogError> {
        let avail = self.position.current();
        if avail <= self.fsynced {
            return Ok(None);
        }
        let want = (avail - self.fsynced) as usize;
        let chunk = want.min(self.bounce.cap);
        // Round down to 4 KiB to satisfy O_DIRECT length alignment.
        let chunk = chunk & !4095;
        if chunk == 0 {
            // We have <4 KiB of unaligned tail; wait until more arrives.
            return Ok(None);
        }

        // Read from source (buffered, not O_DIRECT — kernel page cache absorbs).
        let read_off = self.fsynced as u64;
        let read = read_at(&self.source_fd, self.bounce.as_mut(), read_off, chunk)?;
        if read == 0 {
            return Ok(None);
        }
        assert_eq!(read & 4095, 0, "source must yield 4 KiB-aligned bytes");

        let write_off = self.fsynced as u64;
        self.submit_write_then_fsync(read, write_off)?;
        self.fsynced += read as i64;

        Ok(Some(stream_position_to_bposition(self.fsynced)))
    }

    fn submit_write_then_fsync(&mut self, len: usize, offset: u64) -> Result<(), LogError> {
        let write = opcode::Write::new(
            types::Fd(self.mirror_fd.as_raw_fd()),
            self.bounce.ptr,
            len as u32,
        )
        .offset(offset)
        .build()
        .user_data(0xAA)
        .flags(io_uring::squeue::Flags::IO_LINK);

        let fsync = opcode::Fsync::new(types::Fd(self.mirror_fd.as_raw_fd()))
            .flags(types::FsyncFlags::DATASYNC)
            .build()
            .user_data(0xBB);

        // SAFETY: SQEs reference `self.bounce.ptr` and `self.mirror_fd`; both
        // outlive the submission because `tick` blocks until the CQE arrives
        // before returning.
        unsafe {
            let mut sq = self.ring.submission();
            sq.push(&write)
                .map_err(|_| LogError::Io(std::io::Error::other("uring sq full (write)")))?;
            sq.push(&fsync)
                .map_err(|_| LogError::Io(std::io::Error::other("uring sq full (fsync)")))?;
        }
        // Submit and wait for the fsync (the linked write completes first;
        // its CQE arrives but we only need to ensure the fsync is durable).
        self.ring.submit_and_wait(2).map_err(LogError::Io)?;

        let mut cq = self.ring.completion();
        while let Some(cqe) = cq.next() {
            if cqe.result() < 0 {
                return Err(LogError::Io(std::io::Error::from_raw_os_error(
                    -cqe.result(),
                )));
            }
        }
        Ok(())
    }
}

fn read_at(
    f: &std::fs::File,
    buf: &mut [u8],
    offset: u64,
    len: usize,
) -> Result<usize, LogError> {
    use std::os::unix::fs::FileExt;
    let n = f.read_at(&mut buf[..len], offset)?;
    Ok(n)
}

/// Aeron stream position decomposes to (term_id, term_offset). We use a fixed
/// term length of 16 MiB (Aeron default `aeron.term.buffer.length=16777216`).
pub const TERM_LEN: i64 = 16 * 1024 * 1024;

pub fn stream_position_to_bposition(pos: i64) -> BPosition {
    let term_id = (pos / TERM_LEN) as i32;
    let term_offset = (pos % TERM_LEN) as i32;
    BPosition {
        term_id,
        term_offset,
    }
}

// ---------------------------------------------------------------------------
// Aeron-backed PositionSource
// ---------------------------------------------------------------------------

/// `PositionSource` backed by an Aeron counter (the recording-position counter
/// exposed by the Aeron Archive). Only available with `aeron-live`.
#[cfg(feature = "aeron-live")]
pub struct AeronPositionSource {
    counter: rusteron_client::AtomicCounter,
}

#[cfg(feature = "aeron-live")]
impl AeronPositionSource {
    pub fn new(
        aeron: &rusteron_client::Aeron,
        counter_id: i32,
    ) -> Result<Self, crate::error::LogError> {
        let counter = aeron
            .counter_for_id(counter_id)
            .map_err(|e| crate::error::LogError::Aeron(format!("counter_for_id {counter_id}: {e}")))?;
        Ok(Self { counter })
    }
}

#[cfg(feature = "aeron-live")]
impl PositionSource for AeronPositionSource {
    fn current(&self) -> i64 {
        self.counter.get()
    }
}
