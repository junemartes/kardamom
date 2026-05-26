//! Smoke test: open an env, verify each table is openable inside a read txn,
//! then close it cleanly.

use state::env::{Durability, StateEnvBuilder};
use state::schema::ALL_TABLES;

#[test]
fn env_opens_and_closes() {
    let tmp = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(tmp.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    // Verify every table is openable via a read txn.
    let txn = env.raw().begin_ro_sync().unwrap();
    for name in ALL_TABLES {
        txn.open_db(Some(name))
            .unwrap_or_else(|e| panic!("expected table `{name}` to be openable, got error: {e}"));
    }
    drop(txn);
}

#[test]
fn env_reopens_existing_path() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let _env = StateEnvBuilder::new(tmp.path())
            .durability(Durability::SafeNoSync)
            .open()
            .unwrap();
        // first open creates schema, then closes
    }
    // re-open should find the existing tables
    let env = StateEnvBuilder::new(tmp.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    let txn = env.raw().begin_ro_sync().unwrap();
    for name in ALL_TABLES {
        txn.open_db(Some(name)).unwrap();
    }
}
