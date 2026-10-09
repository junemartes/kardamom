//! The frozen journal and data directory layouts against the format
//! registry (`formats.toml` at the workspace root).

use alloy_primitives::{Address, B256, Bytes};
use kardamom_formats::Registry;

use crate::ring::journal::InFlight;
use crate::store::Contracts;

#[test]
fn the_journal_layout_matches_the_registry() {
    let entry = InFlight {
        nonce: 1,
        hash: B256::repeat_byte(2),
        raw: Bytes::from_static(&[3]),
        published: true,
    };
    let json = serde_json::to_string(&entry).unwrap();
    Registry::assert_layout("canary-journal", "InFlight", &json);
}

#[test]
fn the_store_layout_matches_the_registry() {
    let contracts = Contracts {
        counter: Some(Address::repeat_byte(4)),
    };
    let json = serde_json::to_string(&contracts).unwrap();
    Registry::assert_layout("canary-store", "Contracts", &json);
    let anchor = serde_json::to_string(&B256::repeat_byte(5)).unwrap();
    Registry::assert_layout("canary-store", "anchor", &anchor);
}
