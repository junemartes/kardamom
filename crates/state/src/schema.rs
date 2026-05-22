//! On-disk schema: table names, version, and meta keys.
//!
//! Bump `SCHEMA_VERSION` on any incompatible layout change. Opening an env
//! whose stored version differs from this constant refuses to start.

/// Incompatible layout changes bump this.
pub const SCHEMA_VERSION: u32 = 1;

/// All MDBX named subdatabases live here.
pub mod tables {
    pub const ACCOUNTS: &str = "accounts";
    pub const STORAGE: &str = "storage";
    pub const CODE: &str = "code";
    pub const HEADERS: &str = "headers";
    pub const RECEIPTS: &str = "receipts";
    pub const APPLIED_DEPOSITS: &str = "applied_deposits";
    pub const META: &str = "meta";

    /// Order matters: `open_or_create` walks this list to create every table
    /// on a fresh env in one txn.
    pub const ALL: &[&str] = &[
        ACCOUNTS,
        STORAGE,
        CODE,
        HEADERS,
        RECEIPTS,
        APPLIED_DEPOSITS,
        META,
    ];
}

/// Keys in the `meta` table. Values are tag-specific byte encodings (BE
/// integers for numeric tags; raw 32-byte hashes; etc).
pub mod meta {
    pub const SCHEMA_VERSION: &[u8] = b"schema_version";
    pub const CHAIN_ID: &[u8] = b"chain_id";
    pub const LATEST_BLOCK: &[u8] = b"latest_block";
    pub const GENESIS_HASH: &[u8] = b"genesis_hash";
}
