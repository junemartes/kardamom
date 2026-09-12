//! Millisecond-precision duration conversion, shared by every crate that
//! turns a measured [`Duration`] into a wire or metric value.

use core::time::Duration;

/// Milliseconds in `d`, saturated to `u64::MAX`. `Duration` itself never
/// overflows; the saturation only guards the (unreachable in practice,
/// about 584 million years) case where the millisecond count would not
/// fit `u64`. A threshold comparison against the saturated value still
/// fires correctly in that case.
#[must_use]
pub fn duration_to_ms_saturating(d: Duration) -> u64 {
    d.as_secs()
        .saturating_mul(1_000)
        .saturating_add(u64::from(d.subsec_millis()))
}
