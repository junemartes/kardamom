//! Shared test fixtures, behind the `testing` feature: a deterministic
//! signer, a signed `TxEnvelope` builder, an A-position builder, and a
//! one-partition config. Six test files and one bench built the same
//! `signer`/`signed_envelope`/`pos`/`one_partition_cfg` shapes by hand;
//! this is the one copy.
//!
//! `signer`/`envelope_with` need `alloy-network` and `alloy-signer-local`
//! as real (optional) dependencies of this crate, not dev-dependencies —
//! see the `testing` feature's doc comment in `Cargo.toml` for why.

use std::num::NonZeroU32;

use alloy_consensus::{SignableTransaction, TxEnvelope as ConsensusEnvelope, TxLegacy};
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, B256, U256, keccak256};
use alloy_rlp::Encodable;
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use kardamom_types::{BPosition, TxDataLoc, TxEnvelope, TxError, TxRef};

use crate::config::SequencerConfig;
use crate::error::SequencerError;
use crate::inbound::fakes::ScriptedTxData;
use crate::outbound::fakes::{InMemoryTxErrorPublisher, InMemoryTxOrderingRefPublisher};
use crate::partition::PartitionCount;
use crate::sequencer::{Ports, Sequencer};

/// A deterministic signer from a small integer seed, so tests get stable,
/// distinct senders without a real RNG.
///
/// # Panics
///
/// Panics if `seed` is 0: a private key of 0 is not a valid secp256k1
/// scalar. Every caller in this crate seeds from 1.
#[must_use]
pub fn signer(seed: u64) -> PrivateKeySigner {
    let mut k = [0u8; 32];
    k[24..].copy_from_slice(&seed.to_be_bytes());
    PrivateKeySigner::from_bytes(&k.into()).unwrap()
}

/// What varies between test envelopes: everything else (chain id, gas
/// price, recipient, value) is fixed, because no test asserts on it.
#[derive(Clone, Copy)]
pub struct EnvelopeSpec {
    pub gas_limit: u64,
    /// Length of a filler calldata payload (`0xAB` repeated). Most tests
    /// want zero; the allocation-profile harness wants a realistic size
    /// to exercise RLP decode cost.
    pub calldata_len: usize,
    /// `false` leaves `tx_hash` at its default (single-replica tests
    /// never key off it). `true` stamps the real `keccak256` of the
    /// encoded envelope — required wherever `tx_hash` is a dedup key
    /// (for example, racing-replica tests).
    pub real_hash: bool,
}

impl Default for EnvelopeSpec {
    fn default() -> Self {
        Self {
            gas_limit: 21_000,
            calldata_len: 0,
            real_hash: false,
        }
    }
}

/// Build a signed legacy transaction, wrapped the way the proxy publishes
/// it onto `tx_data`: RLP `raw_tx`, plus a proxy-stamped `sender` and
/// `tx_hash`. The sequencer trusts both fields and never recovers or
/// hashes them.
///
/// # Panics
///
/// Never: signing a well-formed legacy transaction with an already-valid
/// `PrivateKeySigner` cannot fail.
#[must_use]
pub fn envelope_with(
    s: &PrivateKeySigner,
    nonce: u64,
    correlation_id: u64,
    spec: EnvelopeSpec,
) -> TxEnvelope {
    let mut tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 1_000_000_000,
        gas_limit: spec.gas_limit,
        to: Address::ZERO.into(),
        value: U256::ZERO,
        input: vec![0xAB; spec.calldata_len].into(),
    };
    let sig = s.sign_transaction_sync(&mut tx).unwrap();
    let alloy_env: ConsensusEnvelope = tx.into_signed(sig).into();
    let mut buf = Vec::with_capacity(256 + spec.calldata_len);
    alloy_env.encode(&mut buf);
    let tx_hash = if spec.real_hash {
        keccak256(&buf)
    } else {
        B256::default()
    };
    TxEnvelope {
        correlation_id,
        raw_tx: Bytes::from(buf),
        sender: s.address(),
        tx_hash,
    }
}

/// [`envelope_with`] at [`EnvelopeSpec::default`]: 21,000 gas, no
/// calldata, defaulted `tx_hash`. This is what most tests want — they
/// exercise the nonce-check state machine, not the payload.
#[must_use]
pub fn signed_envelope(s: &PrivateKeySigner, nonce: u64, correlation_id: u64) -> TxEnvelope {
    envelope_with(s, nonce, correlation_id, EnvelopeSpec::default())
}

/// Build an increasing `BPosition` for the in-memory subscription, so
/// every observed envelope has a distinct A-position. This is the way
/// Aeron-archive fragment offsets work in production.
#[must_use]
pub fn pos(offset: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: offset,
    }
}

/// The three fakes a [`Sequencer`] drives against in every test:
/// scripted `tx_data` in, in-memory `tx_ordering` refs and `tx_errors`
/// out. Building one `Rig` and calling [`Self::step`] (or, for the `run`
/// callers, [`Self::ports`]) replaces the `seq.run_once(&mut Ports {
/// tx_data: &mut a, refs: &mut b, errors: &mut rc })` shape repeated
/// across this crate's tests and its throughput bench.
#[derive(Default)]
pub struct Rig {
    pub tx_data: ScriptedTxData,
    pub refs: InMemoryTxOrderingRefPublisher,
    pub errors: InMemoryTxErrorPublisher,
}

impl Rig {
    /// Borrow this rig's three fakes as one [`Ports`]. For callers that
    /// drive [`Sequencer::run`] or [`Sequencer::run_once`] themselves —
    /// [`Self::step`] does not fit a test that interleaves further sends
    /// or assertions between steps, or that passes a [`crate::sequencer::
    /// Shutdown`] token to `run`.
    pub fn ports(
        &mut self,
    ) -> Ports<'_, ScriptedTxData, InMemoryTxOrderingRefPublisher, InMemoryTxErrorPublisher> {
        Ports {
            tx_data: &mut self.tx_data,
            refs: &mut self.refs,
            errors: &mut self.errors,
        }
    }

    /// One [`Sequencer::run_once`] step against this rig's fakes.
    ///
    /// # Errors
    ///
    /// Returns an error exactly when [`Sequencer::run_once`] does.
    pub fn step(&mut self, seq: &mut Sequencer) -> Result<bool, SequencerError> {
        seq.run_once(&mut self.ports())
    }

    /// Queue one scripted `tx_data` envelope at `loc`.
    pub fn push(&mut self, loc: TxDataLoc, env: TxEnvelope) {
        self.tx_data.queue.push_back((loc, env));
    }

    /// A cloned snapshot of every ref published so far, in arrival order.
    ///
    /// # Panics
    ///
    /// Panics if the refs mutex is poisoned.
    #[must_use]
    pub fn refs(&self) -> Vec<TxRef> {
        self.refs.refs.lock().unwrap().clone()
    }

    /// A cloned snapshot of every error emitted so far, in arrival order.
    ///
    /// # Panics
    ///
    /// Panics if the errors mutex is poisoned.
    #[must_use]
    pub fn errors(&self) -> Vec<TxError> {
        self.errors.errors.lock().unwrap().clone()
    }
}

/// Build a [`Rig`], feed it `stream`, and drive [`Rig::step`] until it
/// returns `false` (input and rebuffered backlog both drained). Returns
/// every published ref and every emitted error, in arrival order.
///
/// Fits a test that builds its whole input up front, drives once to
/// completion, and inspects the result — not one that interleaves
/// assertions or further sends between `run_once` calls, or drives more
/// than one sequencer against a shared publisher.
///
/// # Panics
///
/// Panics if `cfg` is invalid ([`Sequencer::new`]), or if [`Rig::step`]
/// returns an error (a real bug the caller should see, not swallow).
#[must_use]
pub fn drive_to_idle(
    cfg: SequencerConfig,
    stream: &[(TxDataLoc, TxEnvelope)],
) -> (Vec<TxRef>, Vec<TxError>) {
    let mut seq = Sequencer::new(cfg).unwrap();
    let mut rig = Rig::default();
    for (loc, env) in stream {
        rig.push(*loc, env.clone());
    }
    while rig.step(&mut seq).unwrap() {}
    (rig.refs(), rig.errors())
}

/// A `SequencerConfig` for a single-partition deployment (`M = 1`):
/// every sender routes to partition 0. The default for every field this
/// crate's tests do not care about.
///
/// # Panics
///
/// Never: 1 is a non-zero `u32`.
#[must_use]
pub fn one_partition_cfg() -> SequencerConfig {
    SequencerConfig {
        partition_count: PartitionCount::new(NonZeroU32::new(1).unwrap()),
        partition_index: 0,
        sequencer_id: 0,
        ..Default::default()
    }
}
