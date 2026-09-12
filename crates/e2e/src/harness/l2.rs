//! JSON-RPC client and transaction signing for the scenario drivers.
//!
//! This is a thin wrapper over jsonrpsee's HTTP client. It does two
//! things: it surfaces JSON-RPC error codes as data, so the scenarios can
//! check them, and it measures the latency of each call, since the
//! "RPC never hangs" checks are latency limits.
//!
//! Signing reuses `kardamom_deployer`'s mnemonic derivation (the anvil dev
//! mnemonic, funded by `deploy/cluster/config/genesis/dev.toml`) and the
//! same `TxLegacy` to sign to `encode_2718` pattern that `kardamom-load`
//! uses.

use std::num::{NonZeroU32, NonZeroU64};
use std::time::{Duration, Instant};

use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, B256, Bytes, TxKind, U256};
use anyhow::{Context, Result};
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use jsonrpsee::rpc_params;
use kardamom_deployer::mnemonic;
pub use kardamom_deployer::signers::DerivedSigner;

/// The anvil/hardhat dev mnemonic. Accounts #0 through #17 are prefunded
/// by `deploy/cluster/config/genesis/dev.toml`.
pub const DEV_MNEMONIC: &str = "test test test test test test test test test test test junk";

/// Outcome of a JSON-RPC call, with the wall-clock latency of the round trip.
#[derive(Debug)]
pub struct RpcOutcome<T> {
    pub result: std::result::Result<T, RpcError>,
    pub elapsed: Duration,
}

/// A JSON-RPC layer error. This is either a server-side error object
/// (code and message) or a transport-level failure (timeout, refused
/// connection, and so on).
#[derive(Debug, Clone)]
pub enum RpcError {
    /// The server answered with a JSON-RPC error object.
    Call { code: i32, message: String },
    /// The call failed below the JSON-RPC layer (connect refused, client
    /// timeout, connection dropped).
    Transport(String),
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RpcError::Call { code, message } => write!(f, "rpc error {code}: {message}"),
            RpcError::Transport(m) => write!(f, "transport error: {m}"),
        }
    }
}

fn classify(e: jsonrpsee::core::client::Error) -> RpcError {
    match e {
        jsonrpsee::core::client::Error::Call(obj) => RpcError::Call {
            code: obj.code(),
            message: obj.message().to_string(),
        },
        other => RpcError::Transport(other.to_string()),
    }
}

/// JSON-RPC client bound to one ingress endpoint.
#[derive(Clone)]
pub struct L2Client {
    http: HttpClient,
    pub url: String,
}

impl L2Client {
    /// `request_timeout` limits every call at the client. Scenarios set it
    /// above the ingress-side pending-receipt timeout, so a server timeout
    /// error can be told apart from a client-side abort.
    ///
    /// # Errors
    /// Returns an error when the HTTP client fails to build for `url`.
    pub fn new(url: &str, request_timeout: Duration) -> Result<Self> {
        let http = HttpClientBuilder::default()
            .request_timeout(request_timeout)
            .max_concurrent_requests(4096)
            .build(url)
            .with_context(|| format!("build http client for {url}"))?;
        Ok(Self {
            http,
            url: url.to_string(),
        })
    }

    async fn call<T: jsonrpsee::core::DeserializeOwned>(
        &self,
        method: &str,
        params: jsonrpsee::core::params::ArrayParams,
    ) -> RpcOutcome<T> {
        let start = Instant::now();
        let result = self
            .http
            .request::<T, _>(method, params)
            .await
            .map_err(classify);
        RpcOutcome {
            result,
            elapsed: start.elapsed(),
        }
    }

    /// `eth_sendRawTransaction`. The server blocks until the receipt lands,
    /// or until the ingress pending-receipt timeout fires.
    pub async fn send_raw(&self, raw: &Bytes) -> RpcOutcome<B256> {
        self.call(
            "eth_sendRawTransaction",
            rpc_params![format!("0x{}", hex::encode(raw))],
        )
        .await
    }

    /// `eth_getTransactionReceipt`. Returns `None` on a cache miss.
    pub async fn receipt(&self, hash: B256) -> RpcOutcome<Option<serde_json::Value>> {
        self.call("eth_getTransactionReceipt", rpc_params![format!("{hash}")])
            .await
    }

    pub async fn chain_id(&self) -> RpcOutcome<String> {
        self.call("eth_chainId", rpc_params![]).await
    }

    pub async fn block_number(&self) -> RpcOutcome<String> {
        self.call("eth_blockNumber", rpc_params![]).await
    }

    /// `eth_getBalance`. This is a deferred endpoint: it must answer with
    /// a clean error, and never hang. Kept for the RPC liveness matrix.
    pub async fn get_balance(&self, addr: Address) -> RpcOutcome<String> {
        self.call("eth_getBalance", rpc_params![format!("{addr}"), "latest"])
            .await
    }

    /// `eth_getTransactionCount`. Has the same deferred-endpoint contract.
    pub async fn get_transaction_count(&self, addr: Address) -> RpcOutcome<String> {
        self.call(
            "eth_getTransactionCount",
            rpc_params![format!("{addr}"), "latest"],
        )
        .await
    }

    /// A raw JSON-RPC call, with untyped params and result. This is the
    /// transport the rpc-vectors driver uses. Errors surface as
    /// `RpcError::Call{code,message}`, so vectors can match the full
    /// error contract.
    ///
    /// # Panics
    /// Panics when a param in `params` fails to serialize. Every caller
    /// passes plain `serde_json::Value`s, which always serialize.
    pub async fn raw_call(
        &self,
        method: &str,
        params: &[serde_json::Value],
    ) -> RpcOutcome<serde_json::Value> {
        let mut p = jsonrpsee::core::params::ArrayParams::new();
        for v in params {
            p.insert(v).expect("vector param must serialize");
        }
        self.call(method, p).await
    }
}

/// Derive `count` dev signers (mnemonic index 0..count). A zero count
/// would return an empty `Vec` that every caller then indexes into and
/// panics on, so the type rules it out.
///
/// # Errors
/// Returns an error when signer derivation fails.
pub fn dev_signers(count: NonZeroU32) -> Result<Vec<DerivedSigner>> {
    mnemonic::derive_signers(DEV_MNEMONIC, count.get())
}

/// Derive `total` dev signers (mnemonic index `0..total`). The shape
/// every caller with a total headcount (as opposed to a highest-used
/// index; see [`dev_signers_through`]) needs; computing the `u32` and
/// nonzero checks once here removes the six-line
/// `NonZeroU32::try_from(u32::try_from(..)?)?` shape from each call site.
///
/// # Errors
/// Returns an error when `total` does not fit in a `u32`, when `total` is
/// zero, or under the same conditions as [`dev_signers`].
pub fn dev_signers_total(total: usize) -> Result<Vec<DerivedSigner>> {
    let count = u32::try_from(total)
        .ok()
        .and_then(NonZeroU32::new)
        .context("signer count out of range")?;
    dev_signers(count)
}

/// Derive dev signers covering every mnemonic index `0..=max_index`. This
/// is the `total = max_index + 1` shape every caller with a highest-used
/// index needs; computing that arithmetic once here (checked, not a bare
/// `+ 1`) removes the off-by-one drift risk of writing it out at each
/// call site.
///
/// # Errors
/// Returns an error when `max_index + 1` overflows, or under the same
/// conditions as [`dev_signers_total`]. The arithmetic case is
/// effectively unreachable for the mnemonic-sized indices this crate
/// uses.
pub fn dev_signers_through(max_index: usize) -> Result<Vec<DerivedSigner>> {
    dev_signers_total(max_index.checked_add(1).context("max_index overflows")?)
}

/// A signed legacy transfer ready for `eth_sendRawTransaction`.
#[derive(Debug, Clone)]
pub struct SignedTransfer {
    pub raw: Bytes,
    pub hash: B256,
    pub sender: Address,
    pub nonce: u64,
}

/// A legacy call with the standard 1 gwei gas price and no input, varying
/// only in nonce, gas limit, recipient, and value. For callers that build
/// an unsigned or deliberately mis-signed transaction directly, instead
/// of through [`sign_transfer`] or the other `sign_*` helpers.
#[must_use]
pub fn legacy_tx(chain_id: u64, nonce: u64, gas_limit: u64, to: Address, value: U256) -> TxLegacy {
    TxLegacy {
        chain_id: Some(chain_id),
        nonce,
        gas_price: 1_000_000_000,
        gas_limit,
        to: TxKind::Call(to),
        value,
        input: Bytes::new(),
    }
}

/// The parts of a legacy transaction that vary by call shape: everything
/// [`sign_legacy`] needs beyond the signer, chain id, and nonce every
/// caller already carries.
struct LegacyTxShape {
    gas_limit: u64,
    to: TxKind,
    value: U256,
    input: Bytes,
    /// Names the shape, for the sign error's context (e.g. "transfer").
    what: &'static str,
}

/// Sign one legacy transaction. Every legacy-shaped signer
/// ([`sign_transfer`], [`sign_create`], [`sign_call`]) is a thin wrapper
/// around this, differing only in `spec`.
///
/// # Errors
/// Returns an error when signing the transaction fails.
fn sign_legacy(
    signer: &DerivedSigner,
    chain_id: u64,
    nonce: u64,
    spec: LegacyTxShape,
) -> Result<SignedTransfer> {
    let mut tx = TxLegacy {
        chain_id: Some(chain_id),
        nonce,
        gas_price: 1_000_000_000,
        gas_limit: spec.gas_limit,
        to: spec.to,
        value: spec.value,
        input: spec.input,
    };
    let signature = signer
        .signer
        .sign_transaction_sync(&mut tx)
        .with_context(|| format!("sign {} nonce {nonce}", spec.what))?;
    let signed_tx = tx.into_signed(signature);
    let hash = *signed_tx.hash();
    let envelope: TxEnvelope = signed_tx.into();
    // Every legacy shape this crate signs fits well under this bound; a
    // few wasted bytes of slack cost nothing next to one shared body.
    let mut bytes = Vec::with_capacity(220);
    envelope.encode_2718(&mut bytes);
    Ok(SignedTransfer {
        raw: Bytes::from(bytes),
        hash,
        sender: signer.address,
        nonce,
    })
}

/// Sign a 21k-gas legacy value transfer (the `kardamom-load` shape).
/// `value_wei` changes the payload, so two transactions at the same nonce
/// can be told apart. The RPC liveness past-nonce probe needs a
/// different transaction, not an idempotent resubmit.
///
/// # Errors
/// Returns an error when signing the transaction fails.
pub fn sign_transfer(
    signer: &DerivedSigner,
    chain_id: u64,
    nonce: u64,
    to: Address,
    value_wei: u64,
) -> Result<SignedTransfer> {
    sign_legacy(
        signer,
        chain_id,
        nonce,
        LegacyTxShape {
            gas_limit: 21_000,
            to: TxKind::Call(to),
            value: U256::from(value_wei),
            input: Bytes::new(),
            what: "transfer",
        },
    )
}

/// A `(signer, payee, running nonce)` triple that sends 1-wei nudge
/// transfers, advancing its nonce only when a send lands. This is the one
/// "sign, send, and advance the nonce on success" operation every
/// scenario that nudges a chain with real transfers — to make a
/// pipelined commit durable, or just to keep L2 busy — otherwise
/// duplicated.
pub struct NudgeSender {
    signer: DerivedSigner,
    payee: Address,
    nonce: u64,
}

impl NudgeSender {
    #[must_use]
    pub fn new(signer: DerivedSigner, payee: Address, nonce: u64) -> Self {
        Self {
            signer,
            payee,
            nonce,
        }
    }

    /// The current nonce — the one the next [`Self::send`] will attempt.
    #[must_use]
    pub fn nonce(&self) -> u64 {
        self.nonce
    }

    /// The signer this sender signs and sends with.
    #[must_use]
    pub fn signer(&self) -> &DerivedSigner {
        &self.signer
    }

    /// The payee [`Self::send`] sends 1-wei transfers to.
    #[must_use]
    pub fn payee(&self) -> Address {
        self.payee
    }

    /// The current nonce, advancing it unconditionally. For a caller that
    /// signs its own transaction shape (not a plain 1-wei transfer) but
    /// needs the same nonce sequence [`Self::send`] advances.
    ///
    /// # Errors
    /// Returns an error when the nonce overflows.
    pub fn next_nonce(&mut self) -> Result<u64> {
        let n = self.nonce;
        self.nonce = self
            .nonce
            .checked_add(1)
            .context("sender nonce overflows")?;
        Ok(n)
    }

    /// Sign and send one 1-wei transfer at the current nonce, against
    /// `chain_id` through `rpc`. Returns the transfer if the send
    /// landed — the nonce advances only then, so a failed send retries
    /// the same nonce next time.
    ///
    /// # Errors
    /// Returns an error when signing the transfer fails.
    pub async fn send(&mut self, rpc: &L2Client, chain_id: u64) -> Result<Option<SignedTransfer>> {
        let tx = sign_transfer(&self.signer, chain_id, self.nonce, self.payee, 1)?;
        if rpc.send_raw(&tx.raw).await.result.is_ok() {
            self.nonce += 1;
            return Ok(Some(tx));
        }
        Ok(None)
    }
}

/// Sign a legacy contract-creation transaction. `init_code` is the
/// deployment bytecode. 100k gas covers the trivial creates the
/// scenarios deploy.
///
/// # Errors
/// Returns an error when signing the transaction fails.
pub fn sign_create(
    signer: &DerivedSigner,
    chain_id: u64,
    nonce: u64,
    init_code: &[u8],
) -> Result<SignedTransfer> {
    sign_legacy(
        signer,
        chain_id,
        nonce,
        LegacyTxShape {
            gas_limit: 100_000,
            to: TxKind::Create,
            value: U256::ZERO,
            input: Bytes::copy_from_slice(init_code),
            what: "create",
        },
    )
}

/// Sign a legacy call to `to`, carrying `value` and `input`. This covers
/// contract calls such as the withdrawal predeploy's
/// `initiateWithdrawal`.
///
/// # Errors
/// Returns an error when signing the transaction fails.
pub fn sign_call(
    signer: &DerivedSigner,
    chain_id: u64,
    nonce: u64,
    to: Address,
    value: U256,
    input: &[u8],
) -> Result<SignedTransfer> {
    sign_legacy(
        signer,
        chain_id,
        nonce,
        LegacyTxShape {
            gas_limit: 200_000,
            to: TxKind::Call(to),
            value,
            input: Bytes::copy_from_slice(input),
            what: "call",
        },
    )
}

/// A deterministic, in-place Fisher-Yates shuffle (xorshift64*). This
/// makes scenario orderings reproducible from a seed, with no need for
/// `rand`. The seed is a [`NonZeroU64`]: xorshift's all-zero state never
/// leaves zero, so a zero seed would produce no shuffle at all.
pub fn seeded_shuffle<T>(items: &mut [T], seed: NonZeroU64) {
    let mut seed = seed.get();
    let mut next = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for i in (1..items.len()).rev() {
        #[allow(
            clippy::cast_possible_truncation,
            reason = "j < i + 1, and i < items.len() (already a valid usize), so this never truncates"
        )]
        let j = (next() % (i as u64).saturating_add(1)) as usize;
        items.swap(i, j);
    }
}
