//! Small numeric conversions shared across this crate.

use core::num::{NonZeroU32, NonZeroUsize};

/// Convert a `usize` length or count to `u64`, for a wire field that is
/// always `u64` regardless of the host's pointer width.
///
/// `usize` is at most 64 bits on every target this workspace builds for
/// (32-bit and 64-bit hosts only), so the conversion never loses a value.
/// The `const` assert pins that invariant at compile time instead of
/// leaving it as an unstated assumption at each call site.
#[must_use]
pub fn usize_to_u64(n: usize) -> u64 {
    const _: () = assert!(usize::BITS <= u64::BITS);
    n as u64
}

/// Widen a `u32` count to `usize`, for a value that is `u32` on the wire
/// but `usize` once held in memory. `usize` is at least 32 bits on every
/// target this workspace builds for; the `const` assert pins it.
#[must_use]
pub fn u32_to_usize(n: u32) -> usize {
    const _: () = assert!(usize::BITS >= u32::BITS);
    n as usize
}

/// Widen a `NonZeroU32` count or index to `NonZeroUsize`, for a value
/// that is `u32` on the wire but `usize` once held in memory.
///
/// # Panics
///
/// Never: `usize` is at least 32 bits on every target this workspace
/// builds for, so a non-zero `u32` widened this way is always non-zero.
/// The `const` assert pins the bound that makes it so.
#[must_use]
pub fn nonzero_u32_to_usize(n: NonZeroU32) -> NonZeroUsize {
    const _: () = assert!(usize::BITS >= u32::BITS);
    NonZeroUsize::new(n.get() as usize).expect("a widened NonZeroU32 is never zero")
}

#[cfg(test)]
mod tests {
    use core::num::{NonZeroU32, NonZeroUsize};

    use super::{nonzero_u32_to_usize, usize_to_u64};

    #[test]
    fn converts_without_loss() {
        assert_eq!(usize_to_u64(0), 0);
        assert_eq!(usize_to_u64(42), 42);
        assert_eq!(usize_to_u64(usize::MAX), usize::MAX as u64);
    }

    #[test]
    fn widens_nonzero_u32_without_loss() {
        assert_eq!(
            nonzero_u32_to_usize(NonZeroU32::new(1).unwrap()),
            NonZeroUsize::new(1).unwrap()
        );
        assert_eq!(
            nonzero_u32_to_usize(NonZeroU32::MAX),
            NonZeroUsize::new(u32::MAX as usize).unwrap()
        );
    }
}
