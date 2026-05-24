//! kardamom-sequencer: per-partition CLI binary.
//!
//! The full Aeron-backed wiring is implemented in Task 18 once `aeron-live`
//! becomes available on this host's CI matrix. The default-features build of
//! this binary refuses to run with a clear error so operators don't ship a
//! no-op binary by accident.

fn main() -> std::process::ExitCode {
    eprintln!(
        "kardamom-sequencer: this build was produced without the `aeron-live` \
         feature; rebuild with `cargo build -p kardamom-sequencer \
         --features aeron-live` to enable real Aeron wiring."
    );
    std::process::ExitCode::from(2)
}
