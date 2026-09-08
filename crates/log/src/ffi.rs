//! Small FFI string helpers shared across `aeron_live`, `recorder`, and
//! `refetch`: turning a `Path` or a channel/endpoint URI into a `CString`
//! rusteron's C bindings take, with one error message each instead of a
//! dozen slightly different ones written by hand at each call site.

use std::ffi::CString;
use std::path::Path;

use crate::error::LogError;

/// Turn `dir` into a `CString` for an Aeron `set_dir` call.
///
/// # Errors
///
/// Returns an error if `dir` is not UTF-8, or contains a NUL byte.
pub(crate) fn dir_cstring(dir: &Path) -> Result<CString, LogError> {
    let s = dir
        .to_str()
        .ok_or_else(|| LogError::Aeron(format!("aeron.dir not UTF-8: {}", dir.display())))?;
    CString::new(s).map_err(|_| LogError::Aeron(format!("aeron.dir contains a NUL byte: {s}")))
}

/// Turn a channel or endpoint URI into a `CString`. `what` names the URI
/// in the error message (for example `"tx_data channel"`,
/// `"destination uri"`), so every NUL-byte rejection across the crate
/// reads the same way, tagged with what failed.
///
/// # Errors
///
/// Returns an error if `uri` contains a NUL byte.
pub(crate) fn c_uri(uri: &str, what: &str) -> Result<CString, LogError> {
    CString::new(uri).map_err(|e| LogError::Aeron(format!("{what} contains NUL: {e}")))
}
