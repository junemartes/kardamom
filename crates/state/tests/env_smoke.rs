//! A smoke test. Open an env, verify each table is openable inside a
//! read transaction, then close it cleanly.

use kardamom_state::schema::ALL_TABLES;

mod common;

#[test]
fn env_opens_and_closes() {
    let (_tmp, env) = common::temp_env();
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
        let _env = common::open_env(tmp.path());
        // The first open creates the schema, then closes.
    }
    // The reopen should find the existing tables.
    let env = common::open_env(tmp.path());
    let txn = env.raw().begin_ro_sync().unwrap();
    for name in ALL_TABLES {
        txn.open_db(Some(name)).unwrap();
    }
}
