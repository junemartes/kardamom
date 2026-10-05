//! The rebuild from references on a map of envelopes: the property that
//! matters, that a rebuilt range packs to the bytes the live path packs,
//! and the two refusals, an envelope not served and an envelope that
//! does not hash to its reference.

use alloy_primitives::{Address, B256, keccak256};
use bytes::Bytes;
use kardamom_types::TxDataLoc;

use super::*;
use crate::batcher::{BatcherConfig, pack_blocks};

/// A signed-looking envelope: the hash is the keccak of the bytes, as the
/// proxy computes it.
pub(crate) fn envelope(seed: u8) -> TxEnvelope {
    let raw_tx = Bytes::from(vec![seed; 16]);
    TxEnvelope {
        correlation_id: u64::from(seed),
        tx_hash: keccak256(&raw_tx),
        raw_tx,
        sender: Address::repeat_byte(seed),
        max_inclusion_block: 99,
    }
}

/// A live closed block as the feed loop builds it from the sealer, with
/// `txs` transactions at the end of its range.
pub(crate) fn live_block(number: u64, end: u64, txs: u8) -> ClosedBlock {
    ClosedBlock {
        block_number: number,
        l2_timestamp: 1_700_000_000 + number,
        end_tx_idx: BPosition::from_index(end),
        l1_origin: 40 + number,
        remote_epochs: Vec::new(),
        txs: (0..txs)
            .map(|i| RecordedTx {
                position: BPosition::from_index(end - u64::from(txs) + u64::from(i)),
                envelope: envelope(u8::try_from((end * 8 + u64::from(i)) % 251).unwrap()),
            })
            .collect(),
    }
}

/// The references the query endpoint serves for `block`, with each
/// transaction on shard `tx_idx % 2`, session 7, at archive position
/// `1000 + tx_idx`.
pub(crate) fn refs_of(block: &ClosedBlock) -> BlockRefs {
    BlockRefs {
        block_number: block.block_number,
        end_tx_idx: block.end_tx_idx.as_index(),
        l1_origin: block.l1_origin,
        l2_timestamp: block.l2_timestamp,
        refs: block
            .txs
            .iter()
            .map(|t| BlockTxRef {
                tx_hash: t.envelope.tx_hash,
                tx_idx: t.position.as_index(),
                shard_id: u8::try_from(t.position.as_index() % 2).unwrap(),
                session_id: 7,
                position: 1000 + t.position.as_index(),
            })
            .collect(),
    }
}

/// The archive's content for `blocks`: every envelope at its location.
pub(crate) fn archive_of(blocks: &[ClosedBlock]) -> HashMap<ArchiveLoc, TxEnvelope> {
    blocks
        .iter()
        .flat_map(|b| refs_of(b).refs.into_iter().zip(b.txs.iter().cloned()))
        .map(|(r, t)| {
            (
                loc_of(
                    r.shard_id,
                    TxDataLoc::new(r.session_id, BPosition::from_index(r.position)),
                ),
                t.envelope,
            )
        })
        .collect()
}

/// The rebuilt range packs to the same payload and commitment as the
/// live blocks: the archive's bytes are the posted bytes.
#[test]
fn a_rebuilt_range_packs_to_the_live_bytes() {
    let live = vec![
        live_block(11, 12, 2),
        live_block(12, 12, 0),
        live_block(13, 15, 3),
    ];
    let mut archive = archive_of(&live);
    let rebuilt = rebuild(&live.iter().map(refs_of).collect::<Vec<_>>(), &mut archive).unwrap();
    assert_eq!(rebuilt, live);
    let cfg = BatcherConfig {
        compress: false,
        ..BatcherConfig::default()
    };
    let from_live = pack_blocks(&cfg, &live).unwrap();
    let from_rebuilt = pack_blocks(&cfg, &rebuilt).unwrap();
    assert_eq!(from_rebuilt.payload, from_live.payload);
    assert_eq!(
        from_rebuilt.records_commitment,
        from_live.records_commitment
    );
}

/// An envelope the archives do not serve names its transaction and
/// block.
#[test]
fn an_envelope_not_served_is_named() {
    let live = vec![live_block(11, 12, 2)];
    let mut archive = archive_of(&live);
    let lost = live[0].txs[1].envelope.tx_hash;
    archive.retain(|_, env| env.tx_hash != lost);
    let err = rebuild(&live.iter().map(refs_of).collect::<Vec<_>>(), &mut archive).unwrap_err();
    let text = format!("{err:#}");
    assert!(
        text.contains(&format!("{lost}")) && text.contains("block 11"),
        "{text}"
    );
}

/// An envelope whose bytes do not hash to the reference is refused: the
/// archive is checked the way the live stream is.
#[test]
fn an_envelope_that_does_not_hash_to_its_reference_is_refused() {
    let live = vec![live_block(11, 12, 2)];
    let mut archive = archive_of(&live);
    let forged = live[0].txs[0].envelope.tx_hash;
    archive
        .values_mut()
        .filter(|env| env.tx_hash == forged)
        .for_each(|env| env.raw_tx = Bytes::from_static(b"not those bytes"));
    let err = rebuild(&live.iter().map(refs_of).collect::<Vec<_>>(), &mut archive).unwrap_err();
    assert!(
        format!("{err:#}").contains("whose bytes hash to"),
        "{err:#}"
    );

    let mut archive = archive_of(&live);
    archive
        .values_mut()
        .filter(|env| env.tx_hash == forged)
        .for_each(|env| env.tx_hash = B256::repeat_byte(0xEE));
    let err = rebuild(&live.iter().map(refs_of).collect::<Vec<_>>(), &mut archive).unwrap_err();
    assert!(format!("{err:#}").contains("with hash"), "{err:#}");
}
