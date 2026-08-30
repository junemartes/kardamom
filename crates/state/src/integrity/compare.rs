//! Deep table-level comparison between two state DBs: [`deep_compare`]
//! and its receipt field-diff helper.

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

/// A byte-level comparison of the chain-state tables of two DBs.
/// Canonically, one is an executor's DB and the other is a validator's.
///
/// Returns human-readable differences. An empty result means the two
/// databases hold identical chain state.
///
/// This comparison excludes trie tables and per-node meta, such as the
/// fsync watermark and `state_root`. Only the validator maintains a
/// trie, and [`super::sweep`] verifies it.
pub fn deep_compare(a: &StateEnv, b: &StateEnv) -> Result<Vec<String>, StateError> {
    const SHARED_TABLES: &[&str] = &[
        TABLE_ACCOUNTS,
        TABLE_STORAGE,
        TABLE_CODE,
        TABLE_HEADERS,
        TABLE_RECEIPTS,
        TABLE_TX_HASH_INDEX,
    ];
    const MAX_DIFFS_PER_TABLE: usize = 8;

    let ta = a.raw().begin_rw_sync()?;
    let tb = b.raw().begin_rw_sync()?;
    let mut diffs = Vec::new();

    for table in SHARED_TABLES {
        let da = ta.open_db(Some(table))?;
        let db = tb.open_db(Some(table))?;
        let mut ca = ta.cursor(da)?;
        let mut cb = tb.cursor(db)?;
        let mut ia = ca.first::<Vec<u8>, Vec<u8>>()?;
        let mut ib = cb.first::<Vec<u8>, Vec<u8>>()?;
        let mut table_diffs = 0usize;
        while table_diffs < MAX_DIFFS_PER_TABLE {
            match (&ia, &ib) {
                (None, None) => break,
                (Some((ka, va)), Some((kb, vb))) => {
                    if ka != kb {
                        diffs.push(format!(
                            "{table}: key mismatch a={:02x?} b={:02x?}",
                            &ka[..ka.len().min(8)],
                            &kb[..kb.len().min(8)]
                        ));
                        table_diffs += 1;
                        // Advance the smaller side to resynchronize.
                        if ka < kb {
                            ia = ca.next::<Vec<u8>, Vec<u8>>()?;
                        } else {
                            ib = cb.next::<Vec<u8>, Vec<u8>>()?;
                        }
                        continue;
                    }
                    if va != vb {
                        // Byte lengths alone do not say what diverged.
                        // "224 vs 224 bytes" is the shape a fixed-width field
                        // mismatch takes, and chasing it from CI logs is
                        // guesswork. Receipts decode, so name the fields instead.
                        let detail = if *table == TABLE_RECEIPTS {
                            receipt_field_diff(va, vb)
                        } else {
                            None
                        };
                        match detail {
                            Some(d) => {
                                diffs.push(format!("{table}[{:02x?}]: {d}", &ka[..ka.len().min(8)]))
                            }
                            None => diffs.push(format!(
                                "{table}[{:02x?}]: values differ ({} vs {} bytes)",
                                &ka[..ka.len().min(8)],
                                va.len(),
                                vb.len()
                            )),
                        }
                        table_diffs += 1;
                    }
                    ia = ca.next::<Vec<u8>, Vec<u8>>()?;
                    ib = cb.next::<Vec<u8>, Vec<u8>>()?;
                }
                (Some((ka, _)), None) => {
                    diffs.push(format!(
                        "{table}: extra key in a: {:02x?}",
                        &ka[..ka.len().min(8)]
                    ));
                    table_diffs += 1;
                    ia = ca.next::<Vec<u8>, Vec<u8>>()?;
                }
                (None, Some((kb, _))) => {
                    diffs.push(format!(
                        "{table}: extra key in b: {:02x?}",
                        &kb[..kb.len().min(8)]
                    ));
                    table_diffs += 1;
                    ib = cb.next::<Vec<u8>, Vec<u8>>()?;
                }
            }
        }
        if table_diffs >= MAX_DIFFS_PER_TABLE {
            diffs.push(format!("{table}: further diffs truncated"));
        }
    }

    // Shared meta cursors must agree. Per-node keys, such as the fsync
    // watermark and `state_root`, are excluded by design.
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
                va.as_deref().map(|v| &v[..v.len().min(8)]),
                vb.as_deref().map(|v| &v[..v.len().min(8)])
            ));
        }
    }

    Ok(diffs)
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
