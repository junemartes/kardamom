//! A smoke test. `compact_to` produces a directory containing a
//! libmdbx data file. This test does not verify byte-for-byte
//! equivalence; that is libmdbx's own responsibility.

use kardamom_state::compact_to;

mod common;

#[test]
fn compact_emits_a_directory() {
    let (_src_dir, env) = common::temp_env();

    let dst_dir = tempfile::tempdir().unwrap();
    let dst = dst_dir.path().join("compacted");
    compact_to(&env, &dst).unwrap();
    // mdbx creates one of:
    //   - A directory containing `mdbx.dat` (subdir mode, the default).
    //   - A single file named `compacted` (NOSUBDIR mode).
    // Both are acceptable for this smoke test. Assert non-empty in either case.
    assert!(
        dst.exists(),
        "expected dest path to exist: {}",
        dst.display()
    );
    let size = if dst.is_dir() {
        std::fs::metadata(dst.join("mdbx.dat")).map_or(0, |m| m.len())
    } else {
        std::fs::metadata(&dst).map_or(0, |m| m.len())
    };
    assert!(
        size > 0,
        "expected non-empty mdbx data at {}",
        dst.display()
    );
}

#[test]
fn compact_refuses_existing_destination() {
    let (_src_dir, env) = common::temp_env();

    let dst_dir = tempfile::tempdir().unwrap();
    let dst = dst_dir.path().join("preexisting");
    std::fs::create_dir_all(&dst).unwrap();
    let err = compact_to(&env, &dst).unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("already exists"),
        "expected refusal message, got: {msg}"
    );
}
