package io.kardamom.sealer.cluster;

import io.kardamom.sealer.CanonicalSealerState;

/**
 * The Java side of the Kardamom cluster wire protocol: app-envelope offsets,
 * ingress and egress message kinds, minimum frame lengths, and the shared
 * dedup/retention defaults. This class is the ONE home for the Java&harr;Rust
 * wire sync — the Rust mirror is {@code crates/cluster-adapter/src/wire.rs}.
 *
 * <p><b>App envelope framing.</b> The Rust side defines the application envelope
 * as {@code { kind: u8, sender: 20B, nonce: u64 LE, canonical_id: 32B, payload }}
 * — the guard header ({@code sender}/{@code nonce}, #85 fix B) and the 32-byte
 * canonical id sit at FIXED offsets after the 1-byte {@code kind} tag, and the
 * opaque {@code payload} follows. We match that exact layout: sender at
 * {@link #SENDER_OFFSET}, nonce at {@link #NONCE_OFFSET}, id at
 * {@link #CANONICAL_ID_OFFSET}, relay from {@link #RELAY_OFFSET}.</p>
 *
 * <p>TODO(envelope): keep this byte framing in lockstep with the Rust app
 * envelope in {@code crates/cluster-adapter/src/wire.rs} (every field is at a
 * fixed offset; do not invent a different layout). If the Rust {@code kind}
 * discriminant gains variants, branch on {@code buffer.getByte(offset + KIND_OFFSET)}
 * in {@link SealerClusteredService#onSessionMessage}.</p>
 */
public final class SealerWire {

    /** Offset of the 1-byte {@code kind} tag within the app envelope. */
    public static final int KIND_OFFSET = 0;
    /** Offset of the 20-byte sender in the guard header (#85 fix B). */
    public static final int SENDER_OFFSET = KIND_OFFSET + Byte.BYTES;
    /** Offset of the u64 LE nonce in the guard header. */
    public static final int NONCE_OFFSET = SENDER_OFFSET + CanonicalSealerState.SENDER_LEN;
    /** Offset of the 32-byte canonical id within the app envelope. */
    public static final int CANONICAL_ID_OFFSET = NONCE_OFFSET + Long.BYTES;
    /**
     * Offset from which the relayed payload is forwarded to egress. It starts at
     * the canonical id (NOT after it) so the relayed payload is
     * {@code [canonical_id:32][record_type][fields…]} — the executor needs the
     * canonical id (the tx/source hash) to reconstruct the record and to dedup.
     * The id is still parsed for dedup from {@link #CANONICAL_ID_OFFSET}.
     */
    public static final int RELAY_OFFSET = CANONICAL_ID_OFFSET;

    /** Minimum valid ingress length: kind + canonical id (payload may be empty). */
    static final int MIN_INGRESS_LEN = CANONICAL_ID_OFFSET + CanonicalSealerState.CANONICAL_ID_LEN;

    /** Ingress message kinds (first byte of every ingress app message). */
    public static final byte KIND_INGRESS_RECORD = 0;
    /** Replay request: {@code [kind:1][from_index:u64 LE][from_block:u64 LE]}. */
    public static final byte KIND_REPLAY_REQUEST = 1;
    /**
     * Egress-subscribe announcement: {@code [kind:2]} — the sending session is
     * a canonical-stream consumer and wants the per-record/per-boundary egress
     * broadcast. Publisher-only sessions (sequencers) never send it, so the
     * leader stops paying one unicast offer per record for sessions that drop
     * the payload client-side anyway.
     */
    public static final byte KIND_SUBSCRIBE = 2;
    /**
     * Batch of ingress records:
     * {@code [kind:3][count:u16 LE][per entry: len:u32 LE + entry bytes]},
     * each entry a complete single-record frame ({@code [kind:0][id:32][payload…]}).
     * Entries are processed EXACTLY like individually-offered records (same
     * dedup, same per-record relay), so determinism and the egress format are
     * unchanged — the batch only amortizes the ingress offer round trip.
     */
    public static final byte KIND_BATCH = 3;
    /**
     * Origin-advancing record:
     * {@code [kind:4][canonical_id:32][l1_origin:u64 LE][slot_count:u32 LE][payload…]}.
     *
     * <p>Kind 4, not 3: {@link #KIND_BATCH} took 3. Deliberately does NOT
     * carry the guard header {@link #KIND_INGRESS_RECORD} does — epochs are
     * deposits, which are not nonce-gated, so there is no sender/nonce to
     * contiguity-check.</p>
     *
     * <p>The service does NOT parse the payload: it stays schema-agnostic
     * about what an epoch contains, reading only the origin and the slot count
     * from their fixed offsets and handing them to
     * {@link CanonicalSealerState#onOriginRecord}, which closes the open block,
     * adopts the origin and relays. See
     * {@code docs/agents/l1-origin-deposit-derivation-spec.md}.</p>
     */
    public static final byte KIND_ORIGIN_RECORD = 4;

    /**
     * Remote-origin record — a batch of cross-chain messages a PEER chain
     * produced:
     * {@code [kind:5][canonical_id:32][origin_chain_id:u64 LE][anchor_number:u64 LE][slot_count:u32 LE][payload…]}.
     *
     * <p>A distinct kind byte, not a record type inside a
     * {@link #KIND_ORIGIN_RECORD} payload: the service branches to its own
     * handler on the tag alone and stays schema-agnostic — peeking into the
     * payload to tell an epoch from a remote batch is exactly the parsing this
     * layer must never do. It also lets the header differ, which it must: TWO
     * u64 fields (WHICH peer, and WHERE in that peer's chain) where an epoch
     * needs one, because there is exactly one L1 but any number of peers.</p>
     *
     * <p>Like {@link #KIND_ORIGIN_RECORD} it carries no guard header —
     * cross-chain messages are not nonce-gated by an L2 sender — and its
     * {@code slot_count} is {@code 1 + message count} (marker plus one slot per
     * message), taken on trust and re-derived by consumers that do parse the
     * payload. See {@code docs/specs/interop-outbox-messaging-spec.md} §7.</p>
     */
    public static final byte KIND_REMOTE_ORIGIN_RECORD = 5;

    /** Offset of the 32-byte canonical id in a {@link #KIND_ORIGIN_RECORD} frame. */
    static final int ORIGIN_ID_OFFSET = KIND_OFFSET + Byte.BYTES;
    /** Offset of the u64 L1 origin within a {@link #KIND_ORIGIN_RECORD} frame. */
    static final int ORIGIN_OFFSET =
            ORIGIN_ID_OFFSET + CanonicalSealerState.CANONICAL_ID_LEN;
    /** Offset of the u32 slot count within a {@link #KIND_ORIGIN_RECORD} frame. */
    static final int SLOT_COUNT_OFFSET = ORIGIN_OFFSET + Long.BYTES;
    /** Minimum valid origin-record length: kind + canonical id + origin + slots. */
    static final int MIN_ORIGIN_RECORD_LEN = SLOT_COUNT_OFFSET + Integer.BYTES;

    /** Offset of the 32-byte canonical id in a {@link #KIND_REMOTE_ORIGIN_RECORD} frame. */
    static final int REMOTE_ID_OFFSET = KIND_OFFSET + Byte.BYTES;
    /** Offset of the u64 LE origin chain id within a {@link #KIND_REMOTE_ORIGIN_RECORD} frame. */
    static final int REMOTE_CHAIN_ID_OFFSET =
            REMOTE_ID_OFFSET + CanonicalSealerState.CANONICAL_ID_LEN;
    /** Offset of the u64 LE anchor number within a {@link #KIND_REMOTE_ORIGIN_RECORD} frame. */
    static final int REMOTE_ANCHOR_OFFSET = REMOTE_CHAIN_ID_OFFSET + Long.BYTES;
    /** Offset of the u32 LE slot count within a {@link #KIND_REMOTE_ORIGIN_RECORD} frame. */
    static final int REMOTE_SLOT_COUNT_OFFSET = REMOTE_ANCHOR_OFFSET + Long.BYTES;
    /**
     * Minimum valid remote-origin length: kind + canonical id + chain id +
     * anchor + slots. Eight bytes longer than {@link #MIN_ORIGIN_RECORD_LEN} —
     * the second u64 is the whole header difference.
     */
    static final int MIN_REMOTE_ORIGIN_RECORD_LEN = REMOTE_SLOT_COUNT_OFFSET + Integer.BYTES;

    /** Minimum valid replay-request length: kind + from_index + from_block. */
    static final int MIN_REPLAY_REQUEST_LEN = Byte.BYTES + Long.BYTES + Long.BYTES;

    /** Egress message kinds (first byte of every egress frame). */
    public static final byte EGRESS_KIND_RELAYED = 1;
    public static final byte EGRESS_KIND_BOUNDARY = 2;
    /** Replay refused: {@code [kind:3][oldest_index:u64][oldest_block:u64]}. */
    public static final byte EGRESS_KIND_REPLAY_UNAVAILABLE = 3;
    /** Replay complete: {@code [kind:4][up_to_index:u64][up_to_block:u64]}. */
    public static final byte EGRESS_KIND_REPLAY_DONE = 4;
    /**
     * Contiguity reject (#85 fix B):
     * {@code [kind:5][sender:20][nonce:u64][expected:u64]}, offered to the
     * OFFERING session only — the sequencer whose ref would have sealed a
     * canonical nonce gap rewinds its unconfirmed ledger to {@code expected}
     * and republishes the missing refs.
     */
    public static final byte EGRESS_KIND_CONTIGUITY_REJECT = 5;

    /** Bounded in-memory retention of framed egress bytes for client replay. */
    static final int DEFAULT_RETENTION = 65536;

    /**
     * Default first-seen dedup window. SAFETY INVARIANT: the window must be
     * larger than (worst-case racing-replica stall × peak unique-record
     * throughput), or a replica that resumes after its ids were FIFO-evicted
     * gets its re-offers accepted as fresh — the same tx ordered TWICE in the
     * canonical log. At 10k unique tx/s the previous default of 8192 tolerated
     * a stall of only ~0.8s (a single GC pause / cgroup throttle); 1&lt;&lt;17
     * tolerates ~13s for ~20MB of heap and a ~4MB snapshot (snapshot I/O is
     * chunked, see {@link SnapshotIo#writeSnapshot}). All members MUST agree on
     * the window ({@code -Dkardamom.cluster.dedupCapacity}) — it is part of the
     * deterministic state machine.
     */
    public static final int DEFAULT_DEDUP_CAPACITY = 1 << 17;

    private SealerWire() {
    }
}
