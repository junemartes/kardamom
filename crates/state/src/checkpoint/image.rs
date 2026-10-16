//! The data file of a new checkpoint, cut to the pages that it uses.
//!
//! `mdbx_env_copy` sizes the copy to the geometry of the live env: the
//! used pages, rounded up to the growth step. The rest of the file is a
//! hole that reads as zeros. A checkpoint of a small state is about
//! 128 KiB of pages in a 256 MiB file. The manifest hashes every byte of
//! the file, and a peer sends every byte of it, so the copy is cut to
//! its used pages before the hash.
//!
//! The cut keeps the image valid. libmdbx accepts a data file that is
//! shorter than its geometry and not shorter than its used pages. A
//! read-write open grows the file back to the geometry, and a read-only
//! open maps only the file.

use std::mem::MaybeUninit;
use std::path::Path;

use signet_libmdbx::ffi;

use crate::compaction::MdbxPath;
use crate::error::StateError;

/// The cut length is a multiple of 64 KiB. A read-only open refuses a
/// file whose length is not a multiple of the allocation granularity of
/// the host. 64 KiB is the largest granularity of a supported host:
/// Linux pages are 4, 16 or 64 KiB, and the Windows granularity is
/// 64 KiB. So the cut image opens on every host.
const CUT_ALIGN: u64 = 64 * 1024;

/// A compacted mdbx data file that no process has open.
pub(crate) struct CompactedImage<'a> {
    path: &'a Path,
}

impl<'a> CompactedImage<'a> {
    pub(crate) fn new(path: &'a Path) -> Self {
        Self { path }
    }

    /// Cut the file to its used pages, rounded up to [`CUT_ALIGN`], and
    /// sync it. Returns the new length.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`] if libmdbx cannot read the image header, if
    /// the header gives a size that overflows, or if the cut fails.
    pub(crate) fn cut_to_used_pages(&self) -> Result<u64, StateError> {
        let len = self.cut_len()?;
        let file = std::fs::OpenOptions::new().write(true).open(self.path)?;
        file.set_len(len)?;
        file.sync_all()?;
        Ok(len)
    }

    /// The bytes of the used pages, rounded up to [`CUT_ALIGN`]. The page
    /// count and the page size come from the newest meta page of the
    /// image. `mdbx_preopen_snapinfo` reads them without a map and
    /// without a lock file.
    fn cut_len(&self) -> Result<u64, StateError> {
        let info = self.header_info()?;
        info.mi_last_pgno
            .checked_add(1)
            .and_then(|pages| pages.checked_mul(u64::from(info.mi_dxb_pagesize)))
            .and_then(|used| used.checked_next_multiple_of(CUT_ALIGN))
            .ok_or_else(|| {
                StateError::Recovery(format!(
                    "checkpoint image {} states a used size that overflows",
                    self.path.display()
                ))
            })
    }

    fn header_info(&self) -> Result<ffi::MDBX_envinfo, StateError> {
        let c_path = MdbxPath::new(self.path, "checkpoint image")?;
        let mut info = MaybeUninit::<ffi::MDBX_envinfo>::uninit();
        // SAFETY: `c_path` is a NUL-terminated path that lives for the
        // call. `info` is valid for writes of `size_of::<MDBX_envinfo>()`
        // bytes, the size that the call is given.
        let rc = unsafe {
            ffi::mdbx_preopen_snapinfo(
                c_path.as_ptr(),
                info.as_mut_ptr(),
                size_of::<ffi::MDBX_envinfo>(),
            )
        };
        if rc != 0 {
            return Err(StateError::Recovery(format!(
                "mdbx_preopen_snapinfo on {} returned non-zero: {rc}",
                self.path.display()
            )));
        }
        // SAFETY: a zero return means that the call zeroed the whole
        // struct and then wrote its fields.
        Ok(unsafe { info.assume_init() })
    }
}
