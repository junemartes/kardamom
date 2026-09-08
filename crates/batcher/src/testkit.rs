//! Shared anvil e2e test scaffold for the optimistic and proof-submission
//! suites: the dev accounts, the deterministic tx builder, the clock
//! helper, and the accepting-verifier binding that `optimistic_e2e.rs`,
//! `proof_submission_e2e.rs`, and `optimistic_proof_e2e.rs` all need.
//! Behind the `test-support` feature, so those integration tests (each its
//! own crate, from cargo's point of view) can share this instead of
//! duplicating it.

use std::collections::HashMap;
use std::io::Write;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use alloy_primitives::{Address, B256, address};
use alloy_provider::Provider;
use alloy_sol_types::sol;
use bytes::Bytes;
use kardamom_types::{BPosition, BlockBoundaryStart, TxEnvelope, TxOrderingMessage, TxRef};

use crate::archive_reader::append_frame;

sol!(
    #[sol(rpc)]
    AcceptingVerifier,
    concat!(
        env!("CARGO_WORKSPACE_DIR"),
        "/contracts/out/KardamomProofOracle.t.sol/AcceptingVerifier.json"
    )
);

/// The dev owner the optimistic/proof e2e tests deploy and claim from:
/// anvil's well-known default account 0.
pub const DEV_OWNER: Address = address!("f39Fd6e51aad88F6F4ce6aB8827279cffFb92266");
/// The batcher EOA these tests post batches from.
pub const BATCHER: Address = address!("00000000000000000000000000000000000000BA");
/// The L2 chain id these tests deploy for.
pub const L2_CHAIN_ID: u64 = 412_346;

/// A minimal, deterministic `TxEnvelope`: distinct per `i`, otherwise
/// arbitrary.
///
/// # Panics
/// Panics if `i` does not fit in a `u8`; test callers only ever pass small
/// indices.
#[must_use]
pub fn env_tx(i: u64) -> TxEnvelope {
    TxEnvelope {
        correlation_id: i,
        raw_tx: vec![0xF0u8, u8::try_from(i).unwrap(), 0xBA, 0x12].into(),
        sender: Address::repeat_byte(0x11),
        tx_hash: B256::repeat_byte(u8::try_from(i).unwrap() + 1),
    }
}

/// Serialize `frames` in order, each at its own `BPosition`, into `name`
/// under `dir`, and return the file's path. Shared by every fixture
/// builder in this workspace that writes one archive segment file
/// (`tests/multi_archive_reader.rs` had its own copy of exactly this).
///
/// # Panics
/// Panics on any file I/O failure; this is a test helper, so a failure to
/// write the fixture should fail the test loudly.
pub fn write_segment<T>(dir: &Path, name: &str, frames: &[(BPosition, T)]) -> PathBuf
where
    T: for<'a> rkyv::Serialize<
            rkyv::api::high::HighSerializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::rancor::Error,
            >,
        >,
{
    let mut buf = Vec::new();
    for (p, v) in frames {
        append_frame(&mut buf, *p, v);
    }
    let path = dir.join(name);
    std::fs::File::create(&path)
        .unwrap()
        .write_all(&buf)
        .unwrap();
    path
}

/// `n` deterministic `(BPosition, TxEnvelope)` frames for one sequencer,
/// at positions `0, 256, 512, ...`: correlation ids `base_id..base_id +
/// n`, `fill`-byte calldata, and a `sender`/`tx_hash` derived from
/// `sender_byte` and the index. Shared by every A-archive this fixture
/// builds — the two sequencers differ only in these three parameters.
///
/// # Panics
/// Panics if `n` is large enough to overflow the position, correlation
/// id, or hash-byte arithmetic below; this is a test-fixture bound, never
/// hit by any caller in this workspace.
fn sequencer_frames(
    base_id: u64,
    fill: u8,
    sender_byte: u8,
    n: usize,
) -> Vec<(BPosition, TxEnvelope)> {
    (0..n)
        .map(|i| {
            let idx = u64::try_from(i).expect("fixture bound");
            let pos = BPosition::from_index(256u64.checked_mul(idx).expect("fixture bound"));
            let env = TxEnvelope {
                correlation_id: base_id.checked_add(idx).expect("fixture bound"),
                raw_tx: Bytes::from(vec![fill; 80]),
                sender: Address::repeat_byte(sender_byte),
                tx_hash: B256::repeat_byte(
                    sender_byte
                        .checked_add(u8::try_from(i).expect("fixture bound"))
                        .expect("fixture bound"),
                ),
            };
            (pos, env)
        })
        .collect()
}

/// Appends the `tx_ordering` (B) archive's records in canonical order:
/// one `TxRef` per push, then one final `BoundaryStart`. Bundles the
/// running byte offset and hash seed so [`write_m_plus_one_archives`]'s
/// build loop is two calls to [`BWriter::push_ref`] per position, instead
/// of repeating the offset/hash bookkeeping for each sequencer.
struct BWriter {
    frames: Vec<(BPosition, TxOrderingMessage)>,
    off: u64,
    hash_seed: u8,
}

impl BWriter {
    fn new() -> Self {
        Self {
            frames: Vec::new(),
            off: 0,
            hash_seed: 0,
        }
    }

    /// Push one `TxRef` at `a_pos`, on sequencer `seq_id`. Each ref gets a
    /// distinct `tx_hash`, so a dedup pass would not collapse them, even
    /// though this drives an offline-reader test with no executor
    /// involved.
    ///
    /// # Panics
    /// Panics if `off` overflows `u64`; a test-fixture bound, never hit by
    /// any caller in this workspace.
    fn push_ref(&mut self, seq_id: u8, a_pos: BPosition) {
        self.hash_seed = self.hash_seed.wrapping_add(1);
        self.frames.push((
            BPosition::from_index(self.off),
            TxOrderingMessage::TxRef(TxRef::new(
                B256::repeat_byte(self.hash_seed),
                seq_id,
                a_pos,
                0,
            )),
        ));
        self.off = self.off.checked_add(16).expect("fixture bound");
    }

    /// Close the block with one `BoundaryStart` at the current offset, and
    /// return the finished frame list.
    fn finish(
        mut self,
        block_number: u64,
        l2_timestamp: u64,
    ) -> Vec<(BPosition, TxOrderingMessage)> {
        let boundary_pos = BPosition::from_index(self.off);
        self.frames.push((
            boundary_pos,
            TxOrderingMessage::BoundaryStart(BlockBoundaryStart {
                block_number,
                end_tx_idx: boundary_pos,
                l2_timestamp,
                l1_origin: 0,
            }),
        ));
        self.frames
    }
}

/// An M+1 archive fixture: two per-sequencer `tx_data` archives and one
/// `tx_ordering` archive, built by [`write_m_plus_one_archives`].
pub struct MPlusOneArchives {
    pub b_segment: PathBuf,
    pub a_segments: HashMap<u8, PathBuf>,
    /// The correlation-id order the B archive's refs record, in canonical
    /// (round-robin) order. The caller asserts a `reconstruct` result's
    /// tx order against this, instead of duplicating the fixture's own id
    /// scheme.
    pub canonical_order: Vec<u64>,
}

/// Build an M+1 archive fixture: two per-sequencer `tx_data` archives (A0,
/// A1, `txs_per_sequencer` txs each) and one `tx_ordering` archive (B) that
/// round-robins A0/A1 refs in the canonical alternation order and closes
/// with one `BoundaryStart` at `(block_number, l2_timestamp)`.
///
/// Correlation ids are `1000 + i` on A0 and `2000 + i` on A1, for `i` in
/// `0..txs_per_sequencer`; senders and tx hashes are otherwise arbitrary
/// but deterministic.
///
/// Shared by `section6_conformance.rs` and `docker_e2e.rs`, which drive
/// the same offline-reader pipeline at different fixture sizes.
///
/// # Panics
/// Panics on any archive-file I/O failure; this is a test helper, so a
/// failure to write the fixture should fail the test loudly.
#[must_use]
pub fn write_m_plus_one_archives(
    dir: &Path,
    txs_per_sequencer: usize,
    block_number: u64,
    l2_timestamp: u64,
) -> MPlusOneArchives {
    let frames_a0 = sequencer_frames(1000, 0xAA, 0x10, txs_per_sequencer);
    let frames_a1 = sequencer_frames(2000, 0xBB, 0x20, txs_per_sequencer);
    let a0_path = write_segment(dir, "a0.rec", &frames_a0);
    let a1_path = write_segment(dir, "a1.rec", &frames_a1);

    // Canonical order on B: round-robin a0/a1 for `2 * txs_per_sequencer`
    // refs, then a boundary.
    let mut b = BWriter::new();
    let mut canonical_order = Vec::with_capacity(2 * txs_per_sequencer);
    for ((pos, e0), (_, e1)) in frames_a0.iter().zip(&frames_a1) {
        b.push_ref(0, *pos);
        canonical_order.push(e0.correlation_id);
        b.push_ref(1, *pos);
        canonical_order.push(e1.correlation_id);
    }
    let b_path = write_segment(dir, "b.rec", &b.finish(block_number, l2_timestamp));

    let mut a_segments = HashMap::new();
    a_segments.insert(0u8, a0_path);
    a_segments.insert(1u8, a1_path);
    MPlusOneArchives {
        b_segment: b_path,
        a_segments,
        canonical_order,
    }
}

/// Advance anvil's clock by `window_secs + 1`, so a challenge window of
/// that length has fully elapsed.
///
/// # Panics
/// Panics if either anvil RPC call fails, or if `window_secs` is already
/// `u64::MAX` (a test-fixture bound, never hit by any caller in this
/// workspace); this is a test helper, so a failure should fail the test
/// loudly.
pub async fn advance_past_window<P: Provider>(provider: &P, window_secs: NonZeroU64) {
    let elapsed = window_secs.get().checked_add(1).expect("fixture bound");
    let _: serde_json::Value = provider
        .raw_request("evm_increaseTime".into(), serde_json::json!([elapsed]))
        .await
        .unwrap();
    let _: serde_json::Value = provider
        .raw_request("evm_mine".into(), serde_json::json!([]))
        .await
        .unwrap();
}
