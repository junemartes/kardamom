//! Errors raised by the engine actor and its helpers.

use alloc::string::String;

use alloy_primitives::B256;

use crate::exec_types::TxIndex;
use kardamom_types::BPosition;

#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    #[error("state backend error: {0}")]
    State(String),

    /// A proven divergence between a validator's re-execution and the
    /// sequencer's published output (write-set does not match BAL, or a
    /// receipt mismatch). This error is fatal and not retryable. The commit
    /// thread's must-deliver retry must not retry it. The error must
    /// propagate so the pipeline halts and the process stops.
    #[error("proven divergence: {0}")]
    Divergence(String),

    /// A write to a local file that a published output depends on failed,
    /// for example the receipt ledger of an executor. This error is fatal
    /// and not retryable: a full or broken disk does not heal by a retry.
    /// The commit thread's must-deliver retry must not retry it, so the run
    /// ends and the process exits.
    #[error("local write failed: {0}")]
    LocalWrite(String),

    #[error("revm execution failure at tx {idx:?}: {detail}")]
    Execution { idx: TxIndex, detail: String },

    /// A stateless (guest-shaped) execution re-derived a tx record's
    /// identity from its raw bytes, and it contradicts the envelope. This
    /// means a forged or corrupt `sender` or `tx_hash`. The live pipeline
    /// never produces this error, because it trusts the proxy's fields.
    #[error("record identity mismatch: {0}")]
    RecordIdentity(String),

    /// The witness could not be tied to `pre_state_root`. Causes include a
    /// missing or undecodable proof node, a witness entry the trie refutes,
    /// or a post-root recompute that the node set cannot complete. This
    /// error aborts a stateless execution, either before the first EVM step
    /// (verification) or after it (recompute). Both cases fail closed, like
    /// the witness's own incompleteness errors.
    #[error("witness unanchored: {0}")]
    WitnessUnanchored(String),

    #[error(
        "block boundary closes before observed end_tx_idx: end={end:?} last_seen={last_seen:?}"
    )]
    BoundaryMisaligned {
        end: BPosition,
        last_seen: BPosition,
    },

    #[error("tx_ordering subscription closed")]
    TxOrderingClosed,

    #[error(
        "cluster replay unavailable: need from index {from_index}, oldest retained index {oldest_index} (block {oldest_block}) — full resync required"
    )]
    ClusterReplayUnavailable {
        from_index: u64,
        oldest_index: u64,
        oldest_block: u64,
    },

    /// The cluster head lies below this consumer's delivery cursor: the
    /// sealer lost canonical records that this consumer already applied,
    /// for example after a wipe. The consumer stops, because each new record
    /// falls below its cursor and drops as a duplicate. No repair path
    /// handles this error: a peer checkpoint is ahead of the head too.
    #[error(
        "cluster head (index {head_index}, block {head_block}) is behind the delivery cursor (index {next_index}, block {next_block}) — the sealer lost applied records"
    )]
    ClusterBehindCursor {
        next_index: u64,
        next_block: u64,
        head_index: u64,
        head_block: u64,
    },

    /// The canonical stream differs from the committed history of this
    /// consumer. `block` and `index` name the first difference: the
    /// divergence point. Every block below `block` is equal on both sides,
    /// so a checkpoint below `block` is a valid restart point.
    /// `stream` is the fingerprint of the first frame of `block` in the
    /// stream. Every consumer of that stream has that frame, so every
    /// consumer that finds one divergence gets the same fingerprint.
    #[error(
        "the canonical stream diverges from the committed history at block {block} (index {index})"
    )]
    StreamDiverged {
        block: u64,
        index: u64,
        stream: B256,
    },

    /// The canonical stream differs from the receipts that this consumer
    /// published above its committed block. The committed history is
    /// equal, so the state stays. The receipts at and above `block` are
    /// void. `index` and `stream` are as in [`Self::StreamDiverged`].
    #[error(
        "the canonical stream differs from the receipts published above the committed history at block {block} (index {index})"
    )]
    PublishedDiverged {
        block: u64,
        index: u64,
        stream: B256,
    },

    /// The canonical stream differs from the committed history at the
    /// floor of the compared window, so the divergence can be below the
    /// window. With `widen`, a wider window can find it. Without it, the
    /// floor is the first block of the chain or the retention of the
    /// source, and the operator restores the consumer.
    #[error(
        "the canonical stream diverges from the committed history at or below block {floor_block}, the floor of the compared window (a wider window can find it: {widen})"
    )]
    DivergedBelowWindow { floor_block: u64, widen: bool },

    /// The source of the canonical stream has a new connection, or its head
    /// went below the delivery cursor, while this consumer has entries in
    /// flight above its committed block. The stream of the new connection
    /// can differ from the entries in flight, and only the committed
    /// history can be compared. The pipeline starts again from the state
    /// DB, and the comparison at the start runs again.
    #[error(
        "the stream source restarted (delivery cursor index {next_index}, block {next_block}): restart from the state DB"
    )]
    SourceRestarted { next_index: u64, next_block: u64 },

    /// Every source of the carried reader holds a transaction reference at
    /// `index`, and the reader has no `tx_data` lane to join it. Another
    /// source or a restart gets the same reference, so the consumer stops.
    /// Only a sequencer that offers a reference makes such an entry.
    #[error(
        "no source holds the bytes of the transaction reference at index {index}: the carried reader cannot join it"
    )]
    UnjoinableReference { index: u64 },

    /// `sequencer_id` is the `tx_data` lane index, `TxRef::shard_id`. It
    /// names the archive that holds the envelope, not the process that
    /// published the ref.
    #[error("tx_data[{sequencer_id}] subscription closed")]
    TxDataClosed { sequencer_id: u8 },

    #[error("tx_receipts publication closed")]
    TxReceiptsClosed,

    /// A must-deliver publication has no connected subscriber. The commit
    /// thread retries the publish and runs its escalation clock on this
    /// error alone: any other transient error means a subscriber exists.
    #[error("{topic} publication not connected: {detail}")]
    NotConnected { topic: String, detail: String },

    /// A must-deliver publication stayed unconnected past the exit
    /// threshold of the escalation, after one reopen on a new session.
    /// The process exits with a distinct code, so the supervisor restarts
    /// it. The receipts of this replica that are not yet delivered are
    /// lost here; the other replicas deliver theirs.
    #[error(
        "{topic} publication unconnected for {unconnected_s} s, reopened after {reopen_after_s} s: the process exits so the supervisor restarts it"
    )]
    PublicationDead {
        topic: String,
        unconnected_s: u64,
        reopen_after_s: u64,
    },

    /// The executor-local `TxIndex` counter reached `u64::MAX` and cannot
    /// advance. This would need more than `u64::MAX` transactions in one
    /// process lifetime.
    #[error("TxIndex counter overflowed u64")]
    TxIndexOverflow,

    /// The `tx_ordering` reader pulled a [`kardamom_types::TxRef`], but the
    /// referenced envelope never appeared on `tx_data` within the join
    /// timeout. Either the `tx_data` publisher failed, or the sequencer
    /// published a ref to a position it never wrote. Both are upstream bugs.
    /// `sequencer_id` is the lane the ref names, `TxRef::shard_id`.
    #[error(
        "join timeout: TxRef(lane={sequencer_id}, tx_data_position={tx_data_position:?}) not found within {timeout_ms} ms"
    )]
    JoinTimeout {
        sequencer_id: u8,
        tx_data_position: BPosition,
        timeout_ms: u128,
    },

    /// The canonical order removes an entry that this replica executed. A
    /// void record is legal only while every voter still waits at the entry,
    /// so the state of this replica is different from the state of its
    /// peers. There is no local repair: the replica stops.
    #[error("void record for executed entry: index={index} tx_hash={tx_hash:?}")]
    VoidOfExecutedEntry { index: u64, tx_hash: B256 },
}

/// Role-agnostic alias for the engine error. New engine and validator code
/// should use `EngineError`. `ExecutorError` stays for existing executor call
/// sites; both names refer to the same type.
pub type EngineError = ExecutorError;
