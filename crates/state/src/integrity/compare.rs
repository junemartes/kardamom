//! Deep table-level comparison between two state DBs: [`deep_compare`]
//! and its receipt field-diff helper.

use signet_libmdbx::tx::aliases::RwTxSync;

use crate::env::StateEnv;
use crate::error::StateError;
use crate::meta::{
    KEY_GENESIS_DIGEST, KEY_LAST_COMMITTED_BLOCK, KEY_LAST_COMMITTED_END_TX_POSITION,
    KEY_SCHEMA_VERSION,
};
use crate::schema::{
    TABLE_ACCOUNTS, TABLE_CODE, TABLE_HEADERS, TABLE_META, TABLE_RECEIPTS, TABLE_STORAGE,
    TABLE_TX_HASH_INDEX, decode_receipt_value,
};

/// One cursor row, or `None` at end of table.
type Row = Option<(Vec<u8>, Vec<u8>)>;

/// One cursor row's key and value, already known present — unlike
/// [`Row`], which also allows end-of-table. [`TableCompare::compare_both`]
/// takes one of these per side instead of two loose `Vec<u8>` scalars.
struct KeyValue {
    key: Vec<u8>,
    value: Vec<u8>,
}

const SHARED_TABLES: &[&str] = &[
    TABLE_ACCOUNTS,
    TABLE_STORAGE,
    TABLE_CODE,
    TABLE_HEADERS,
    TABLE_RECEIPTS,
    TABLE_TX_HASH_INDEX,
];

/// A byte-level comparison of the chain-state tables of two DBs.
/// Canonically, one is an executor's DB and the other is a validator's.
///
/// Returns human-readable differences. An empty result means the two
/// databases hold identical chain state.
///
/// This comparison excludes trie tables and per-node meta, such as the
/// fsync watermark and `state_root`. Only the validator maintains a
/// trie, and [`super::sweep`] verifies it.
///
/// # Errors
///
/// Returns [`StateError`] if either DB's transaction, table open, or
/// cursor read fails.
pub fn deep_compare(a: &StateEnv, b: &StateEnv) -> Result<Vec<String>, StateError> {
    let ta = a.raw().begin_rw_sync()?;
    let tb = b.raw().begin_rw_sync()?;
    let mut diffs = Vec::new();
    for table in SHARED_TABLES {
        diffs.extend(TableCompare::new(&ta, &tb, table).run()?);
    }
    diffs.extend(TableCompare::meta_keys(&ta, &tb)?);
    Ok(diffs)
}

/// A dual-cursor merge of one shared table between two DBs' transactions.
/// Advances the smaller side on a key mismatch to resynchronize, and stops
/// early once the table has reported `MAX_DIFFS_PER_TABLE` differences.
struct TableCompare<'a> {
    ta: &'a RwTxSync,
    tb: &'a RwTxSync,
    table: &'a str,
    diffs: Vec<String>,
}

impl<'a> TableCompare<'a> {
    fn new(ta: &'a RwTxSync, tb: &'a RwTxSync, table: &'a str) -> Self {
        Self {
            ta,
            tb,
            table,
            diffs: Vec::new(),
        }
    }

    fn push(&mut self, message: String) {
        self.diffs.push(message);
    }

    fn run(mut self) -> Result<Vec<String>, StateError> {
        const MAX_DIFFS_PER_TABLE: usize = 8;

        let da = self.ta.open_db(Some(self.table))?;
        let db = self.tb.open_db(Some(self.table))?;
        let mut ca = self.ta.cursor(da)?;
        let mut cb = self.tb.cursor(db)?;
        let mut ia = ca.first::<Vec<u8>, Vec<u8>>()?;
        let mut ib = cb.first::<Vec<u8>, Vec<u8>>()?;
        while self.diffs.len() < MAX_DIFFS_PER_TABLE {
            let Some(next) = self.compare_step(&mut ca, &mut cb, ia, ib)? else {
                break;
            };
            (ia, ib) = next;
        }
        if self.diffs.len() >= MAX_DIFFS_PER_TABLE {
            self.push(format!("{}: further diffs truncated", self.table));
        }
        Ok(self.diffs)
    }

    /// One step of the synchronized two-cursor walk: compare the current
    /// pair, push any diff, and advance. `None` means both cursors are
    /// exhausted.
    fn compare_step<K: signet_libmdbx::TransactionKind>(
        &mut self,
        ca: &mut signet_libmdbx::Cursor<'_, K>,
        cb: &mut signet_libmdbx::Cursor<'_, K>,
        ia: Row,
        ib: Row,
    ) -> Result<Option<(Row, Row)>, StateError> {
        match (ia, ib) {
            (None, None) => Ok(None),
            (Some((ka, va)), Some((kb, vb))) => self.compare_both(
                ca,
                cb,
                KeyValue { key: ka, value: va },
                KeyValue { key: kb, value: vb },
            ),
            (Some((ka, _)), None) => {
                self.push(format!(
                    "{}: extra key in a: {:02x?}",
                    self.table,
                    super::head(&ka)
                ));
                Ok(Some((ca.next::<Vec<u8>, Vec<u8>>()?, None)))
            }
            (None, Some((kb, _))) => {
                self.push(format!(
                    "{}: extra key in b: {:02x?}",
                    self.table,
                    super::head(&kb)
                ));
                Ok(Some((None, cb.next::<Vec<u8>, Vec<u8>>()?)))
            }
        }
    }

    /// The `(Some, Some)` arm of [`Self::compare_step`]: a key mismatch
    /// pushes one diff and resyncs by advancing the smaller side; a
    /// matching key with differing values pushes a value diff and
    /// advances both.
    fn compare_both<K: signet_libmdbx::TransactionKind>(
        &mut self,
        ca: &mut signet_libmdbx::Cursor<'_, K>,
        cb: &mut signet_libmdbx::Cursor<'_, K>,
        a: KeyValue,
        b: KeyValue,
    ) -> Result<Option<(Row, Row)>, StateError> {
        let KeyValue { key: ka, value: va } = a;
        let KeyValue { key: kb, value: vb } = b;
        if ka != kb {
            self.push(format!(
                "{}: key mismatch a={:02x?} b={:02x?}",
                self.table,
                super::head(&ka),
                super::head(&kb)
            ));
            // Advance the smaller side to resynchronize.
            return Ok(Some(if ka < kb {
                (ca.next::<Vec<u8>, Vec<u8>>()?, Some((kb, vb)))
            } else {
                (Some((ka, va)), cb.next::<Vec<u8>, Vec<u8>>()?)
            }));
        }
        if va != vb {
            let message = self.value_diff_message(&ka, &va, &vb);
            self.push(message);
        }
        Ok(Some((
            ca.next::<Vec<u8>, Vec<u8>>()?,
            cb.next::<Vec<u8>, Vec<u8>>()?,
        )))
    }

    /// One value-mismatch message for `key` in this table. Byte lengths
    /// alone do not say what diverged. "224 vs 224 bytes" is the shape a
    /// fixed-width field mismatch takes, and chasing it from CI logs is
    /// guesswork. Receipts decode, so this names the fields instead.
    fn value_diff_message(&self, key: &[u8], va: &[u8], vb: &[u8]) -> String {
        let detail = if self.table == TABLE_RECEIPTS {
            receipt_field_diff(va, vb)
        } else {
            None
        };
        match detail {
            Some(d) => format!("{}[{:02x?}]: {d}", self.table, super::head(key)),
            None => format!(
                "{}[{:02x?}]: values differ ({} vs {} bytes)",
                self.table,
                super::head(key),
                va.len(),
                vb.len()
            ),
        }
    }

    /// Compare the shared meta cursors. Per-node keys, such as the fsync
    /// watermark and `state_root`, are excluded by design.
    fn meta_keys(ta: &'a RwTxSync, tb: &'a RwTxSync) -> Result<Vec<String>, StateError> {
        let mut diffs = Vec::new();
        let ma = ta.open_db(Some(TABLE_META))?;
        let mb = tb.open_db(Some(TABLE_META))?;
        for key in [
            KEY_LAST_COMMITTED_BLOCK,
            KEY_LAST_COMMITTED_END_TX_POSITION,
            KEY_GENESIS_DIGEST,
            KEY_SCHEMA_VERSION,
        ] {
            let va = ta.get::<Vec<u8>>(ma.dbi(), key)?;
            let vb = tb.get::<Vec<u8>>(mb.dbi(), key)?;
            if va != vb {
                diffs.push(format!(
                    "meta[{}]: {:02x?} vs {:02x?}",
                    String::from_utf8_lossy(key),
                    va.as_deref().map(super::head),
                    vb.as_deref().map(super::head)
                ));
            }
        }
        Ok(diffs)
    }
}

/// A field-level diff of two encoded receipts, for the deep-compare
/// report.
///
/// Returns `None` when either side does not decode. The caller then
/// falls back to the byte-length message, which is still true, just
/// less informative.
fn receipt_field_diff(a: &[u8], b: &[u8]) -> Option<String> {
    let ra = decode_receipt_value(a).ok()?;
    let rb = decode_receipt_value(b).ok()?;
    let mut fields: Vec<String> = Vec::new();
    macro_rules! cmp {
        ($f:ident) => {
            if ra.$f != rb.$f {
                fields.push(format!("{}: {:?} vs {:?}", stringify!($f), ra.$f, rb.$f));
            }
        };
    }
    cmp!(tx_idx);
    cmp!(tx_type);
    cmp!(tx_hash);
    cmp!(status);
    cmp!(gas_used);
    cmp!(write_set_hash);
    cmp!(nonce);
    cmp!(from);
    cmp!(to);
    cmp!(contract_address);
    cmp!(effective_gas_price);
    cmp!(block_number);
    cmp!(transaction_index);
    if ra.logs != rb.logs {
        fields.push(format!(
            "logs: {} vs {} entries",
            ra.logs.len(),
            rb.logs.len()
        ));
    }
    if fields.is_empty() {
        // The decoded values are equal, but the bytes differ. This encoding
        // difference is worth reporting, instead of saying there is no diff.
        return Some(format!(
            "receipts decode EQUAL but encode differently ({} vs {} bytes) — an \
             encoding-level divergence",
            a.len(),
            b.len()
        ));
    }
    Some(format!("receipt fields differ — {}", fields.join("; ")))
}
