//! A shared `pprof` CPU-sample guard and flamegraph writer.
//!
//! [`harness::Harness`](crate::harness::Harness) and the `kardamom-stm-p2`
//! binary both start an on-CPU profile around one measured phase, and
//! render it as a flamegraph SVG when a caller asks for one. This module
//! holds the plain form of both steps: start a guard at a fixed sample
//! frequency, and render its report straight to a file. A caller that
//! needs to filter or merge the report first, such as `Harness`
//! separating ingress frames from client frames, builds on
//! [`pprof::ProfilerGuard`] directly instead.

use std::path::Path;

/// The blocklist every `pprof` guard in this crate uses: runtime and
/// libc frames that appear in every stack and add nothing to a
/// kardamom-specific flamegraph.
const BLOCKLIST: &[&str] = &["libc", "libgcc", "pthread", "vdso"];

/// `pprof` sampling frequency in Hz, for every guard in this crate.
/// Use 999, not 1000, to avoid resonance with the kernel tick or
/// scheduler quantum on most platforms.
pub const PPROF_HZ: i32 = 999;

/// Start a CPU-sample guard at `hz` samples per second, or `None` if
/// `out` is `None`. A caller passes its own `--pprof-out`-style option
/// straight through, so profiling stays off when no output path is set.
///
/// # Errors
///
/// Returns an error if the profiler fails to start.
pub fn pprof_guard(
    out: Option<&impl AsRef<Path>>,
    hz: i32,
) -> anyhow::Result<Option<pprof::ProfilerGuard<'static>>> {
    if out.is_none() {
        return Ok(None);
    }
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(hz)
        .blocklist(BLOCKLIST)
        .build()
        .map_err(|e| anyhow::anyhow!("pprof guard build failed: {e}"))?;
    Ok(Some(guard))
}

/// Render `guard`'s CPU sample as a flamegraph SVG at `path`. Does
/// nothing if either is missing, so a caller can pass both straight
/// through from its own `Option`s with no branch of its own.
///
/// # Errors
///
/// Returns an error if the report fails to build, `path` cannot be
/// created, or rendering fails.
pub fn write_pprof_svg(
    guard: Option<pprof::ProfilerGuard<'static>>,
    path: Option<&Path>,
) -> anyhow::Result<()> {
    let (Some(guard), Some(path)) = (guard, path) else {
        return Ok(());
    };
    let report = guard
        .report()
        .build()
        .map_err(|e| anyhow::anyhow!("pprof report build failed: {e}"))?;
    let file = std::fs::File::create(path)
        .map_err(|e| anyhow::anyhow!("create {}: {e}", path.display()))?;
    report
        .flamegraph(file)
        .map_err(|e| anyhow::anyhow!("flamegraph: {e}"))?;
    Ok(())
}
