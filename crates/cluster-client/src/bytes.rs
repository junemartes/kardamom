//! Little-endian byte-reading primitives shared across the cluster crates.
//!
//! Two hand-written codecs read the same exact-width LE scalars out of raw
//! buffers: the SBE session codec here ([`crate::protocol`], error type
//! `DecodeError`) and the app-envelope codec in `kardamom-cluster-adapter`'s
//! `wire` module (error type `WireError`). Their `TooShort { at, need, have }`
//! variants are structurally identical but deliberately distinct types — a
//! session-protocol decode failure and an app-envelope decode failure are
//! different faults with different handlers. So the shared primitives return
//! `Option`: `None` means the read fell off the end of the buffer, and each
//! codec maps that to its OWN error with its own `at`/`need`/`have` values.
//! Keep this module error-type-free.

/// The exact-width `[u8; N]` at `at..at + N`, or `None` if `buf` is too short.
fn chunk<const N: usize>(buf: &[u8], at: usize) -> Option<[u8; N]> {
    buf.get(at..at + N)?.try_into().ok()
}

/// Read a little-endian `u16` at byte offset `at`.
pub fn u16_le(buf: &[u8], at: usize) -> Option<u16> {
    Some(u16::from_le_bytes(chunk(buf, at)?))
}

/// Read a little-endian `i32` at byte offset `at`.
pub fn i32_le(buf: &[u8], at: usize) -> Option<i32> {
    Some(i32::from_le_bytes(chunk(buf, at)?))
}

/// Read a little-endian `u32` at byte offset `at`.
pub fn u32_le(buf: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_le_bytes(chunk(buf, at)?))
}

/// Read a little-endian `i64` at byte offset `at`.
pub fn i64_le(buf: &[u8], at: usize) -> Option<i64> {
    Some(i64::from_le_bytes(chunk(buf, at)?))
}

/// Read a little-endian `u64` at byte offset `at`.
pub fn u64_le(buf: &[u8], at: usize) -> Option<u64> {
    Some(u64::from_le_bytes(chunk(buf, at)?))
}
