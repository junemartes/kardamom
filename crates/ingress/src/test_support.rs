//! Shared test and bench helpers: sign a tx, build the `Receipt` a fake
//! executor would send back, or start an in-process server over
//! [`MockChannels`]. Behind the `test-support` feature (and always on
//! for this crate's own `#[cfg(test)]` code), so this crate's unit
//! tests, its `tests/` and `benches/`, and `crates/bench` share one copy
//! instead of each repeating it.

use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};

use alloy_consensus::transaction::Transaction;
use alloy_consensus::{
    SignableTransaction, Signed, TxEip4844, TxEnvelope as ConsensusEnvelope, TxLegacy,
};
use alloy_primitives::{Address, B256, Bytes, Signature, TxKind, U256};
use alloy_rlp::{Decodable, Encodable};
use alloy_signer_local::PrivateKeySigner;
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use jsonrpsee::server::ServerHandle;
use k256::ecdsa::{RecoveryId, signature::hazmat::PrehashSigner};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;

use kardamom_types::{BPosition, QuorumWatermark, Receipt, TxEnvelope};

use crate::channels::MockChannels;
use crate::config::IngressConfig;
use crate::json_rpc::start_jsonrpc_server;
use crate::proxy::IngressProxy;
use crate::routing::partition_for;

/// A signed tx: its decoded envelope, its RLP bytes, and the address
/// that signed it. Returned by [`sign_legacy_tx`].
pub struct SignedTx {
    pub env: ConsensusEnvelope,
    pub raw: Bytes,
    pub sender: Address,
}

/// An in-process JSON-RPC server, started over [`MockChannels`] with no
/// fake executor attached. Returned by [`start_test_server`]. Dropping
/// `handle` stops the server.
pub struct TestServer {
    pub mock: MockChannels,
    pub shard_rx: Vec<UnboundedReceiver<TxEnvelope>>,
    pub addr: SocketAddr,
    pub handle: ServerHandle,
}

/// The one-sender-to-itself legacy tx shape every `sign_legacy*` helper
/// signs, with an explicit `gas_limit` since `sign_legacy_with_gas`
/// needs one other than the default.
fn legacy_tx(nonce: u64, gas_limit: u64) -> TxLegacy {
    TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 1_000_000_000,
        gas_limit,
        to: TxKind::Call(Address::ZERO),
        value: U256::ZERO,
        input: Bytes::default(),
    }
}

/// Signs `tx`'s hash with `s`, and RLP-encodes the resulting envelope.
/// The shared tail of every `sign_*` helper below.
///
/// # Panics
///
/// Panics if signing fails, which does not happen for a freshly
/// generated in-memory `PrivateKeySigner`.
#[must_use]
pub fn sign_and_encode<T>(tx: T, s: &PrivateKeySigner) -> (ConsensusEnvelope, Bytes)
where
    T: SignableTransaction<Signature>,
    Signed<T, Signature>: Into<ConsensusEnvelope>,
{
    let (k256_sig, rid): (k256::ecdsa::Signature, RecoveryId) = s
        .credential()
        .sign_prehash(tx.signature_hash().as_slice())
        .unwrap();
    let alloy_sig = Signature::from_signature_and_parity(k256_sig, rid.is_y_odd());
    let env: ConsensusEnvelope = tx.into_signed(alloy_sig).into();
    let mut buf = Vec::new();
    env.encode(&mut buf);
    (env, Bytes::from(buf))
}

/// Signs a fresh legacy tx with `signer` at `nonce`. Returns the decoded
/// envelope, its RLP bytes, and the signer address.
#[must_use]
pub fn sign_legacy_tx(signer: &PrivateKeySigner, nonce: u64) -> SignedTx {
    let sender = signer.address();
    let (env, raw) = sign_and_encode(legacy_tx(nonce, 21_000), signer);
    SignedTx { env, raw, sender }
}

/// Signs a fresh legacy tx and returns only the RLP bytes.
#[must_use]
pub fn sign_legacy(signer: &PrivateKeySigner, nonce: u64) -> Bytes {
    sign_legacy_tx(signer, nonce).raw
}

/// Signs a legacy tx with an explicit gas limit, for protocol-limit tests.
#[must_use]
pub fn sign_legacy_with_gas(signer: &PrivateKeySigner, nonce: u64, gas_limit: u64) -> Bytes {
    sign_and_encode(legacy_tx(nonce, gas_limit), signer).1
}

/// Signs a minimal EIP-4844 (type-3) tx and returns the encoded bytes.
#[must_use]
pub fn sign_eip4844(signer: &PrivateKeySigner, nonce: u64) -> Bytes {
    // `AccessList` is not a direct dependency of this crate; naming it
    // here would need one just for this default value.
    #[allow(clippy::default_trait_access)]
    let access_list = Default::default();
    let tx = TxEip4844 {
        chain_id: 1,
        nonce,
        gas_limit: 21_000,
        max_fee_per_gas: 1_000_000_000,
        max_priority_fee_per_gas: 0,
        to: Address::ZERO,
        value: U256::ZERO,
        access_list,
        blob_versioned_hashes: vec![B256::repeat_byte(0x01)],
        max_fee_per_blob_gas: 1,
        input: Bytes::default(),
    };
    sign_and_encode(tx, signer).1
}

/// Decodes only the `nonce` field from an RLP-encoded tx. Generic over
/// the byte-buffer type, since callers hold both `bytes::Bytes` (the
/// wire `TxEnvelope::raw_tx` field) and `alloy_primitives::Bytes` (this
/// module's own `sign_*` return type).
///
/// # Panics
///
/// Panics if `raw` does not decode as a legacy tx envelope.
#[must_use]
pub fn nonce_of(raw: &impl AsRef<[u8]>) -> u64 {
    ConsensusEnvelope::decode(&mut raw.as_ref())
        .unwrap()
        .nonce()
}

/// A signer whose address routes to `target_shard` under `m` shards.
#[must_use]
pub fn signer_for_shard(target_shard: u32, m: NonZeroU32) -> PrivateKeySigner {
    loop {
        if let Some(s) = try_signer_for_shard(target_shard, m) {
            return s;
        }
    }
}

/// One [`signer_for_shard`] draw: a random signer, kept only if it routes
/// to `target_shard`.
fn try_signer_for_shard(target_shard: u32, m: NonZeroU32) -> Option<PrivateKeySigner> {
    let s = PrivateKeySigner::random();
    (partition_for(s.address(), m) == target_shard).then_some(s)
}

/// Builds the `BPosition` a fake test executor assigns to the
/// `offset`-th entry in its single, shared position stream (`term_id`
/// `0`).
#[must_use]
pub fn pos(offset: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: offset,
    }
}

/// Builds the `Receipt` a fake test executor sends back for a `tx_data`
/// `envelope`, at position `pos`. Every field but `tx_idx`, `tx_hash`,
/// `from`, and `nonce` (recovered from `envelope.raw_tx`) takes the
/// "always succeeds" default a fake executor uses.
#[must_use]
pub fn receipt_for(envelope: &TxEnvelope, pos: BPosition) -> Receipt {
    Receipt {
        tx_idx: pos,
        tx_hash: envelope.tx_hash,
        status: true,
        gas_used: 21_000,
        logs: Vec::new(),
        write_set_hash: B256::ZERO,
        from: envelope.sender,
        nonce: nonce_of(&envelope.raw_tx),
        ..Default::default()
    }
}

/// Builds a success `Receipt` directly from `(sender, nonce, tx_hash)`,
/// for tests that check receipt-cache and subscription behavior without
/// publishing a real `tx_data` envelope.
#[must_use]
pub fn receipt(sender: Address, nonce: u64, tx_hash: B256, at: i32) -> Receipt {
    Receipt {
        tx_idx: pos(at),
        tx_hash,
        status: true,
        gas_used: 21_000,
        from: sender,
        nonce,
        ..Default::default()
    }
}

/// A `Receipt` with no meaningful identity, at `pos`. For pending-registry
/// tests that only care about release timing, not receipt content.
#[must_use]
pub fn dummy_receipt(pos: BPosition) -> Receipt {
    Receipt {
        tx_idx: pos,
        tx_hash: B256::ZERO,
        status: true,
        gas_used: 21_000,
        logs: Vec::new(),
        write_set_hash: B256::ZERO,
        ..Default::default()
    }
}

/// Spawns one task per shard receiver, each of which builds a
/// [`receipt_for`] for every envelope it drains and sends it, and a
/// matching watermark, so a `MockChannels`-backed proxy sees each
/// submission "execute" immediately.
///
/// All shards share one increasing position space: a per-shard `term_id`
/// would let the quorum watermark go backward, since `BPosition` orders
/// `(term_id, term_offset)`, and any receipt parked above where the last
/// watermark lands would never release. Returns the join handles so the
/// caller can abort them at teardown.
///
/// # Panics
///
/// Panics if more than `i32::MAX` envelopes are drained across every
/// shard combined, which no fixture in this workspace approaches.
#[must_use]
pub fn spawn_fake_executor(
    mock: &MockChannels,
    rx: Vec<UnboundedReceiver<TxEnvelope>>,
) -> Vec<JoinHandle<()>> {
    let next_pos = Arc::new(AtomicI32::new(0));
    rx.into_iter()
        .map(|mut rx| {
            let receipt_bus = mock.receipt_bus.clone();
            let watermark_bus = mock.watermark_bus.clone();
            let next_pos = next_pos.clone();
            tokio::spawn(async move {
                while let Some(envelope) = rx.recv().await {
                    let offset = next_pos
                        .fetch_add(1, Ordering::SeqCst)
                        .checked_add(1)
                        .expect("fixture positions fit i32");
                    let position = pos(offset);
                    let receipt = receipt_for(&envelope, position);
                    let _ = receipt_bus.send(receipt);
                    let _ = watermark_bus.send(QuorumWatermark { position });
                }
            })
        })
        .collect()
}

/// Starts an in-process JSON-RPC server over [`MockChannels`], with no
/// fake executor attached.
///
/// # Panics
///
/// Panics if the server fails to bind.
pub async fn start_test_server(cfg: IngressConfig) -> TestServer {
    let shards = std::num::NonZeroUsize::try_from(cfg.partition_count_m)
        .expect("NonZeroU32 fits NonZeroUsize on every platform this targets");
    let (mock, shard_rx) = MockChannels::new(shards);
    let proxy = IngressProxy::new(cfg, mock.clone(), mock.clone());
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (addr, handle) = start_jsonrpc_server(proxy, bind).await.unwrap();
    TestServer {
        mock,
        shard_rx,
        addr,
        handle,
    }
}

/// An `HttpClient` pointed at `addr`.
///
/// # Panics
///
/// Panics if building the client fails, which does not happen for a
/// plain `http://` URL built from a real bound `SocketAddr`.
#[must_use]
pub fn http_client(addr: SocketAddr) -> HttpClient {
    HttpClientBuilder::default()
        .build(format!("http://{addr}"))
        .unwrap()
}
