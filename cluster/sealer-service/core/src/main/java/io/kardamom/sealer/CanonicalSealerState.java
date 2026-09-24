package io.kardamom.sealer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Deterministic canonical-ordering state machine for the Kardamom sealer.
 *
 * <p>This is a pure POJO. It has no Aeron dependency, no wall clock, and no
 * threads. Every output is a deterministic function of the input sequence.
 * This is the pipeline's one dedup point: the executor trusts the relayed
 * stream and keeps no window of its own.</p>
 *
 * <p>Responsibilities:</p>
 * <ul>
 *   <li><b>Dedup</b> — a bounded, FIFO-evicted first-seen window over 32-byte
 *       canonical ids ({@link #firstSeen(byte[], long)}).</li>
 *   <li><b>Canonical count</b> — {@link #onRecord(byte[], byte[], long, byte[])}
 *       relays each first-seen record with its 0-based index and increases
 *       {@code canonicalCount}. Duplicates are dropped and never counted.</li>
 *   <li><b>Contiguity guard</b> — a bounded per-sender expected-nonce map. If
 *       a known sender's first-seen record has a nonce other than the
 *       expected next one, the guard rejects the record (it is never counted
 *       or deduped). This turns a voided-offer gap into a recoverable signal,
 *       instead of a silently sealed canonical nonce gap. Unknown senders
 *       seed at any nonce. The all-zero sender (deposits) is exempt.</li>
 *   <li><b>Boundaries</b> — {@link #onTick(long)} stamps a {@link Boundary}
 *       with the current count and a timestamp floored to 250 ms, then
 *       advances the block number.</li>
 *   <li><b>Remote origins</b> — {@link #onRemoteOriginRecord} tracks a
 *       per-peer anchor and a per-peer lane cursor ({@code nextSeq}). A
 *       record is accepted only if the origin is in the configured
 *       allowlist, its {@code firstSeq} equals the lane cursor, its
 *       {@code slotCount} matches its seq range, and its anchor advances.
 *       A rejected record is answered with a reject outcome and never
 *       enters the dedup window. The peer position is independent of the
 *       L1 origin, and it is not stamped into boundaries.</li>
 *   <li><b>Snapshot</b> — {@link #takeSnapshot()} and {@link #load(byte[], int)}
 *       round-trip the full state for cluster snapshots.</li>
 * </ul>
 */
public final class CanonicalSealerState {

    /** L2 tick alignment in milliseconds. This matches the Rust sealer's 250 ms tick. */
    public static final long TICK_INTERVAL_MS = 250L;

    /** Length, in bytes, of a canonical id (a 32-byte hash). */
    public static final int CANONICAL_ID_LEN = 32;

    /** Length, in bytes, of a sender address in the contiguity guard. */
    public static final int SENDER_LEN = 20;

    /** Default genesis block number. */
    public static final long GENESIS_BLOCK_NUMBER = 1L;

    /**
     * Default inclusion horizon, in blocks. It matches the proxy's
     * {@code --inclusion-horizon-blocks} default: 16 s at the 250 ms
     * production tick, 128 s at the 2000 ms container tick.
     */
    public static final long DEFAULT_INCLUSION_HORIZON_BLOCKS = 64L;

    private static final int SNAPSHOT_MAGIC = 0x4B53_4541; // "KSEA"
    /**
     * Version 2 added the contiguity-guard sender map. Version 3 adds the
     * L1-origin trio ({@code l1Origin}, {@code lastL2Timestamp},
     * {@code lastBoundaryCount}) after it. This keeps v1 and v2 parsing
     * unchanged, so older snapshots still load. The trio defaults to zero,
     * which is exactly the pre-origin state. Version 4 adds the per-peer
     * remote-origin map after that, on the same terms: an older snapshot
     * restores an empty peer map, which is exactly the state a pre-interop
     * chain was in. Version 5 widens each peer entry with the lane cursor
     * ({@code nextSeqKnown} + {@code nextSeq}). A v4 entry loads with an
     * unknown cursor, so the peer re-seeds its cursor on its next record
     * (trust-on-first-sight). A cluster can upgrade in place without a
     * coordinated snapshot migration.
     */
    private static final int SNAPSHOT_VERSION = 7;

    /** Remote-origin reject reason: {@code firstSeq} is not the lane cursor. */
    public static final byte REMOTE_REJECT_SEQ_MISMATCH = 1;
    /** Remote-origin reject reason: the anchor does not advance. */
    public static final byte REMOTE_REJECT_ANCHOR_REGRESSED = 2;
    /** Remote-origin reject reason: {@code slotCount != 2 + lastSeq - firstSeq}. */
    public static final byte REMOTE_REJECT_SLOT_COUNT_MISMATCH = 3;
    /** Remote-origin reject reason: the origin is not in the allowlist. */
    public static final byte REMOTE_REJECT_UNKNOWN_ORIGIN = 4;
    /** Remote-origin reject reason: {@code lastSeq < firstSeq}, or the range overflows. */
    public static final byte REMOTE_REJECT_BAD_RANGE = 5;

    /** Snapshot bytes per remote-origin entry from version 5 on. */
    private static final int REMOTE_ENTRY_LEN_V5 = 8 + 8 + 1 + 8;
    /** Snapshot bytes per remote-origin entry in version 4. */
    private static final int REMOTE_ENTRY_LEN_V4 = 8 + 8;

    /**
     * FIFO first-seen window. It is insertion-ordered, so the oldest inserted
     * id is the first element, and eviction removes it. Keys are 32-byte
     * ids, wrapped in a read-only {@link ByteBuffer} for value-based
     * equality.
     */
    private final LinkedHashMap<ByteBuffer, Long> dedup;
    /**
     * The ids of {@link #dedup}, grouped by the deadline that frees them.
     * {@link #onTick} drops every group below the new block number in one
     * step, instead of walking the whole window each tick.
     */
    private final TreeMap<Long, LinkedHashSet<ByteBuffer>> byDeadline;
    private final int dedupCapacity;
    /**
     * How far past the open block the sealer holds an id, and the deadline
     * it assigns a marker. Replicated configuration: every member must
     * agree on it, like {@link #dedupCapacity}, because it decides
     * accept-or-reject inside the replicated state machine.
     */
    private final long inclusionHorizonBlocks;

    /**
     * Per-sender expected next nonce. This map is LRU-bounded at the dedup
     * capacity (one shared setting all members must already agree on; about
     * 5MB at the default of 1&lt;&lt;17). The map is access-ordered and
     * mutated only by the replicated record sequence, and is snapshotted in
     * iteration order. So every member holds an identical map with an
     * identical eviction order.
     *
     * <p>An evicted sender that reappears is treated as unknown. It re-seeds
     * at whatever nonce arrives, and its gap protection restarts there. This
     * is the same trust-on-first-sight rule as for a brand new sender, so
     * eviction never causes a false reject.</p>
     *
     * <p>Keys are 20-byte senders, wrapped in read-only {@link ByteBuffer}s
     * for value-based equality.</p>
     */
    private final LinkedHashMap<ByteBuffer, Long> expectedNonce;

    /**
     * Per-peer remote-origin state: the anchor position of the last remote
     * batch adopted from the peer, and the peer's lane cursor. Immutable, so
     * the map replaces an entry instead of mutating it.
     */
    public static final class RemotePeer {
        /** The anchor last adopted from this peer. */
        public final long anchorNumber;
        /**
         * True when {@link #nextSeq} is known. False only for a peer loaded
         * from a version-4 snapshot, which had no cursor. Such a peer seeds
         * its cursor from its next record.
         */
        public final boolean nextSeqKnown;
        /** The first seq of this lane that is not yet ordered (unsigned). */
        public final long nextSeq;

        RemotePeer(long anchorNumber, boolean nextSeqKnown, long nextSeq) {
            this.anchorNumber = anchorNumber;
            this.nextSeqKnown = nextSeqKnown;
            this.nextSeq = nextSeq;
        }
    }

    /**
     * Per-peer remote origin: {@code originChainId} → {@link RemotePeer}
     * (see {@link #onRemoteOriginRecord}). Peers are INDEPENDENT — one
     * peer's position never gates another's, which is why this is a map and
     * not the single scalar {@link #l1Origin} is (there is exactly one L1,
     * but any number of peers).
     *
     * <p>Insertion-ordered so {@link #takeSnapshot()} serialises it
     * deterministically. Bounded by {@link #remoteOriginAllowlist}: a record
     * from an origin outside the allowlist is rejected before it can add an
     * entry, so the map never grows past the configured peer set. An LRU
     * floor here would silently re-seed an evicted peer at whatever anchor
     * arrives, which is a monotonicity hole, so the bound is the allowlist
     * and not the dedup capacity.</p>
     */
    private final LinkedHashMap<Long, RemotePeer> remoteOrigins;

    /**
     * The peer chain ids this sealer accepts remote-origin records from. An
     * empty set disables interop: every kind-5 record is rejected. This is
     * configuration that every member must agree on, like the dedup
     * capacity, because it decides accept-or-reject in the replicated state
     * machine. It is not part of the snapshot.
     */
    private final Set<Long> remoteOriginAllowlist;

    /**
     * The void ledger: which ordered transaction references can still be
     * removed, and which voters have asked for a removal. See
     * {@link #onVoidRequest}.
     */
    private VoidLedger voids;

    /** Cumulative count of canonical (first-seen) records relayed. */
    private long canonicalCount;

    /** Block number the next {@link #onTick(long)} will stamp. */
    private long blockNumber;

    /**
     * L1 block number stamped into every boundary until the next
     * origin-advancing record. This value is echoed from ordered input. The
     * state machine never reads L1 itself, which keeps it deterministic
     * across replicas.
     */
    private long l1Origin;

    /**
     * Timestamp of the last boundary stamped. Boundaries are forced to
     * strictly increase. A tick-aligned timestamp alone is not always
     * unique, because {@link #onOriginRecord} can force a second boundary
     * inside the same 250 ms window. Two blocks that share a timestamp would
     * confuse any code that reasons about block time.
     */
    private long lastL2Timestamp;

    /**
     * The {@code canonicalCount} value at the last boundary. This is how
     * many records the currently open block already holds. It lets
     * {@link #onOriginRecord} skip forcing a boundary when the open block is
     * still empty, so a burst of epochs (L1 catch-up) does not create a run
     * of empty blocks.
     */
    private long lastBoundaryCount;

    /** Create a state at genesis (block number {@value #GENESIS_BLOCK_NUMBER}). */
    public CanonicalSealerState(int dedupCapacity) {
        this(dedupCapacity, GENESIS_BLOCK_NUMBER);
    }

    /**
     * Create a state with an EMPTY remote-origin allowlist: interop is
     * disabled, and every remote-origin record is rejected.
     */
    public CanonicalSealerState(int dedupCapacity, long initialBlockNumber) {
        this(dedupCapacity, initialBlockNumber, Set.of());
    }

    /**
     * Create a state at genesis with the given remote-origin allowlist.
     *
     * @param remoteOrigins the peer chain ids this sealer accepts remote
     *        batches from; empty disables interop
     */
    public CanonicalSealerState(int dedupCapacity, long initialBlockNumber, Set<Long> remoteOrigins) {
        this(dedupCapacity, initialBlockNumber, remoteOrigins, VoidLedger.Config.DISABLED);
    }

    /**
     * Create a state at genesis with a remote-origin allowlist and a void
     * configuration.
     *
     * @param voidConfig the void window and the voter set;
     *        {@link VoidLedger.Config#DISABLED} refuses every void request
     */
    public CanonicalSealerState(
            int dedupCapacity, long initialBlockNumber, Set<Long> remoteOrigins, VoidLedger.Config voidConfig) {
        this(dedupCapacity, initialBlockNumber, remoteOrigins, voidConfig, DEFAULT_INCLUSION_HORIZON_BLOCKS);
    }

    /**
     * The full constructor. {@code inclusionHorizonBlocks} bounds how long
     * an id stays in the dedup window, and is the deadline the sealer
     * assigns a marker. It is replicated configuration: every member must
     * agree on it.
     *
     * @param dedupCapacity          hard cap on the window; a fresh record
     *                               past it is refused, never evicted
     * @param initialBlockNumber     the first block this state stamps
     * @param remoteOrigins          the peer-chain allowlist
     * @param voidConfig             the void ledger's configuration
     * @param inclusionHorizonBlocks the deadline horizon, in blocks
     */
    public CanonicalSealerState(
            int dedupCapacity,
            long initialBlockNumber,
            Set<Long> remoteOrigins,
            VoidLedger.Config voidConfig,
            long inclusionHorizonBlocks) {
        if (dedupCapacity <= 0) {
            throw new IllegalArgumentException("dedupCapacity must be > 0, got " + dedupCapacity);
        }
        if (inclusionHorizonBlocks <= 0) {
            throw new IllegalArgumentException(
                    "inclusionHorizonBlocks must be > 0, got " + inclusionHorizonBlocks);
        }
        this.remoteOriginAllowlist = Set.copyOf(remoteOrigins);
        this.dedupCapacity = dedupCapacity;
        this.inclusionHorizonBlocks = inclusionHorizonBlocks;
        this.dedup = new LinkedHashMap<>();
        this.byDeadline = new TreeMap<>();
        this.expectedNonce = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(final Map.Entry<ByteBuffer, Long> eldest) {
                return size() > dedupCapacity;
            }
        };
        this.remoteOrigins = new LinkedHashMap<>();
        this.voids = new VoidLedger(voidConfig);
        this.canonicalCount = 0L;
        this.blockNumber = initialBlockNumber;
        this.l1Origin = 0L;
        this.lastL2Timestamp = 0L;
        this.lastBoundaryCount = 0L;
    }

    /** What the window did with an offered id. */
    public enum Admission {
        /** The id was not in the window, and is now. */
        FRESH,
        /** The id is already in the window. */
        DUPLICATE,
        /** The open block has passed the id's deadline. */
        PAST_DEADLINE,
        /** The window is at capacity; nothing was forgotten to make room. */
        WINDOW_FULL
    }

    /**
     * Record {@code id32} in the dedup window under {@code deadline}, the
     * last block this record may be ordered into.
     *
     * <p>The window holds an id until its deadline passes, and never
     * evicts. So an id leaves only when no offer carrying it can be
     * accepted again, which is what makes the dedup exact: a stalled
     * replica's re-offer can never be read as fresh. {@link #dedupCapacity}
     * stays a hard cap, but it means back-pressure now, not eviction.</p>
     *
     * <p>The stored deadline is clamped to {@code blockNumber +
     * inclusionHorizonBlocks}. The clamp only shortens, so it cannot admit
     * a record the proxy's deadline would refuse, and it bounds the window
     * by the horizon even if a proxy stamps a deadline far in the future.</p>
     *
     * @param id32     a 32-byte canonical id (defensively copied)
     * @param deadline the last block this record may be ordered into
     */
    public Admission firstSeen(byte[] id32, long deadline) {
        checkId(id32);
        // Copy the array so the caller cannot change a stored key later.
        ByteBuffer key = ByteBuffer.wrap(id32.clone()).asReadOnlyBuffer();
        if (dedup.containsKey(key)) {
            return Admission.DUPLICATE;
        }
        if (blockNumber > deadline) {
            return Admission.PAST_DEADLINE;
        }
        if (dedup.size() >= dedupCapacity) {
            return Admission.WINDOW_FULL;
        }
        insertFresh(key, deadline);
        return Admission.FRESH;
    }

    /**
     * Drop every id whose deadline is below the open block. No offer
     * carrying one of them can be accepted again, so forgetting them is
     * safe by construction, and the window is bounded by the horizon
     * rather than by a count.
     */
    private void pruneExpired() {
        SortedMap<Long, LinkedHashSet<ByteBuffer>> expired = byDeadline.headMap(blockNumber);
        for (LinkedHashSet<ByteBuffer> ids : expired.values()) {
            for (ByteBuffer id : ids) {
                dedup.remove(id);
            }
        }
        expired.clear();
    }

    /** The deadline this sealer assigns a record that carries none. */
    private long assignedDeadline() {
        return saturatingAdd(blockNumber, inclusionHorizonBlocks);
    }

    /** Remove one id from the window and from its deadline group. */
    private void forget(ByteBuffer key) {
        Long held = dedup.remove(key);
        if (held == null) {
            return;
        }
        LinkedHashSet<ByteBuffer> group = byDeadline.get(held);
        group.remove(key);
        if (group.isEmpty()) {
            byDeadline.remove(held);
        }
    }

    /** {@code a + b}, held at {@link Long#MAX_VALUE} instead of wrapping. */
    private static long saturatingAdd(long a, long b) {
        long sum = a + b;
        return ((a ^ sum) & (b ^ sum)) < 0 ? Long.MAX_VALUE : sum;
    }

    private static void checkId(byte[] id32) {
        if (id32 == null || id32.length != CANONICAL_ID_LEN) {
            throw new IllegalArgumentException(
                    "canonical id must be " + CANONICAL_ID_LEN + " bytes, got "
                            + (id32 == null ? "null" : id32.length));
        }
    }

    /**
     * Insert a new key, held until {@code deadline} passes. The caller has
     * already checked the capacity, so this never forgets an id.
     */
    private void insertFresh(ByteBuffer key, long deadline) {
        long held = Math.min(deadline, assignedDeadline());
        dedup.put(key, held);
        byDeadline.computeIfAbsent(held, d -> new LinkedHashSet<>()).add(key);
    }

    /**
     * Outcome of {@link #onRecord(byte[], byte[], long, byte[])}. It is
     * exactly one of:
     * <ul>
     *   <li>dropped duplicate — {@code relayed} is empty, {@code rejected}
     *       is false;</li>
     *   <li>relayed — {@code relayed} holds the record;</li>
     *   <li>contiguity-rejected — {@code rejected} is true, and
     *       {@code expectedNonce} carries the nonce that the guard
     *       wanted.</li>
     * </ul>
     */
    public static final class RecordOutcome {
        /** Which of the five outcomes this is. */
        public enum Kind {
            /** The id was already in the window: dropped, never counted. */
            DUPLICATE,
            /** Accepted: {@link RecordOutcome#relayed} holds the record. */
            RELAYED,
            /** The nonce was not the sender's expected next one. */
            CONTIGUITY_REJECT,
            /** The open block had passed the record's deadline. */
            PAST_DEADLINE,
            /** The window was at capacity, so no decision was taken. */
            WINDOW_FULL
        }

        public final Kind kind;
        public final Optional<Relayed> relayed;
        public final long expectedNonce;
        /** The deadline that was passed. Meaningful for {@link Kind#PAST_DEADLINE}. */
        public final long maxInclusionBlock;

        private RecordOutcome(
                Kind kind, Optional<Relayed> relayed, long expectedNonce, long maxInclusionBlock) {
            this.kind = kind;
            this.relayed = relayed;
            this.expectedNonce = expectedNonce;
            this.maxInclusionBlock = maxInclusionBlock;
        }

        static RecordOutcome duplicate() {
            return new RecordOutcome(Kind.DUPLICATE, Optional.empty(), 0L, 0L);
        }

        static RecordOutcome relayed(Relayed r) {
            return new RecordOutcome(Kind.RELAYED, Optional.of(r), 0L, 0L);
        }

        static RecordOutcome rejected(long expectedNonce) {
            return new RecordOutcome(Kind.CONTIGUITY_REJECT, Optional.empty(), expectedNonce, 0L);
        }

        static RecordOutcome pastDeadline(long maxInclusionBlock) {
            return new RecordOutcome(Kind.PAST_DEADLINE, Optional.empty(), 0L, maxInclusionBlock);
        }

        static RecordOutcome windowFull() {
            return new RecordOutcome(Kind.WINDOW_FULL, Optional.empty(), 0L, 0L);
        }
    }

    /**
     * Process one application record.
     *
     * <p>Order matters: the dedup check runs first, so a re-offered copy of
     * a record that already committed is absorbed as a duplicate before the
     * contiguity guard sees its now-stale nonce. Then, for a non-zero
     * sender, the guard runs:</p>
     * <ul>
     *   <li>a known sender whose nonce is not the expected next one is
     *       rejected — the record is not deduped, counted, or relayed,
     *       because the sequencer must resend the same canonical id after
     *       it recovers from the gap, and that resend must then be accepted
     *       as fresh;</li>
     *   <li>an unknown sender — new, or evicted from the bounded map —
     *       seeds at whatever nonce arrives.</li>
     * </ul>
     *
     * <p>{@code payload} is relayed as-is and is never parsed.</p>
     */
    public RecordOutcome onRecord(
            byte[] canonicalId32, byte[] sender20, long nonce, long deadline, byte[] payload) {
        checkId(canonicalId32);
        if (sender20 == null || sender20.length != SENDER_LEN) {
            throw new IllegalArgumentException(
                    "sender must be " + SENDER_LEN + " bytes, got "
                            + (sender20 == null ? "null" : sender20.length));
        }
        ByteBuffer key = ByteBuffer.wrap(canonicalId32.clone()).asReadOnlyBuffer();
        if (dedup.containsKey(key)) {
            return RecordOutcome.duplicate();
        }
        // The deadline check runs after the dedup lookup, so a re-offer of
        // a record that is still in the window is absorbed as a duplicate,
        // exactly as before, and never reaches this check with a stale
        // nonce. It runs before the contiguity guard, so an expired offer
        // does not move a sender's expected nonce.
        if (blockNumber > deadline) {
            return RecordOutcome.pastDeadline(deadline);
        }
        if (dedup.size() >= dedupCapacity) {
            // Back-pressure, not a verdict: nothing is forgotten to make
            // room. The next tick that passes a deadline frees space, and
            // the sequencer republishes.
            return RecordOutcome.windowFull();
        }
        if (!isZeroSender(sender20)) {
            ByteBuffer senderKey = ByteBuffer.wrap(sender20.clone()).asReadOnlyBuffer();
            Long expected = expectedNonce.get(senderKey);
            if (expected != null && expected.longValue() != nonce) {
                return RecordOutcome.rejected(expected.longValue());
            }
            expectedNonce.put(senderKey, nonce + 1);
            voids.onReference(canonicalCount, canonicalId32, sender20, nonce);
        }
        insertFresh(key, deadline);
        long index = canonicalCount;
        canonicalCount++;
        return RecordOutcome.relayed(new Relayed(index, payload));
    }

    /** The result of one void request: the vote, and the void record when the vote decided it. */
    public static final class VoidOutcome {
        public final VoidLedger.Vote vote;
        public final Optional<Relayed> relayed;

        private VoidOutcome(VoidLedger.Vote vote, Optional<Relayed> relayed) {
            this.vote = vote;
            this.relayed = relayed;
        }
    }

    /**
     * Count the vote of one consumer for the removal of the transaction
     * reference at {@code index}. The consumer votes when it has no envelope
     * for the entry and every archive refuses the range.
     *
     * <p>When every configured voter has voted, the state removes the entry:</p>
     * <ul>
     *   <li>It appends a void record, which takes the next canonical index.
     *       Every consumer drops the entry when it reads the record.</li>
     *   <li>It removes the hash from the dedup window. The sender submits
     *       the same signed bytes again, and the window must not absorb them
     *       as a duplicate.</li>
     *   <li>It sets the sender's expected nonce back to the nonce of the
     *       entry. If it did not, the contiguity guard would refuse the new
     *       submit. An evicted sender re-seeds on its next record, so it
     *       needs no change.</li>
     * </ul>
     *
     * <p>A void never waits on a clock. The decision is a function of the
     * ordered votes only, so every member appends the same record at the
     * same index.</p>
     */
    public VoidOutcome onVoidRequest(int voterId, long index, byte[] txHash32) {
        checkId(txHash32);
        VoidLedger.Tally tally = voids.onVote(voterId, index, txHash32, canonicalCount);
        return new VoidOutcome(tally.vote, tally.entry.map(this::appendVoid));
    }

    private Relayed appendVoid(VoidLedger.Entry entry) {
        forget(ByteBuffer.wrap(entry.id).asReadOnlyBuffer());
        expectedNonce.computeIfPresent(
            ByteBuffer.wrap(entry.sender).asReadOnlyBuffer(), (sender, expected) -> entry.nonce);
        long voidIndex = canonicalCount;
        canonicalCount++;
        return new Relayed(voidIndex, VoidLedger.payload(entry));
    }

    /** The void ledger, for the vote counts that the service logs. */
    public VoidLedger voids() {
        return voids;
    }

    /**
     * Process a record without the contiguity guard (no sender identity)
     * and without a deadline. This is the pre-guard contract, kept for
     * deposit-only callers and existing tests. It is equivalent to
     * {@link #onRecord(byte[], byte[], long, long, byte[])} with the
     * all-zero sender and a deadline this sealer assigns.
     */
    public Optional<Relayed> onRecord(byte[] canonicalId32, byte[] payload) {
        return onRecord(canonicalId32, new byte[SENDER_LEN], 0L, assignedDeadline(), payload)
                .relayed;
    }

    private static boolean isZeroSender(byte[] sender20) {
        for (byte b : sender20) {
            if (b != 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Stamp a block boundary at this tick.
     * <ul>
     *   <li>{@code endTxIdx} is the current {@code canonicalCount} — every
     *       record counted so far belongs to a block at or before this
     *       boundary.</li>
     *   <li>{@code l2Timestamp} is {@code leaderClockMillis} floored to
     *       {@link #TICK_INTERVAL_MS}.</li>
     * </ul>
     * <p>The block number advances after the stamp, so successive ticks
     * produce block numbers that increase by one each time, with no
     * gaps.</p>
     */
    public Boundary onTick(long leaderClockMillis) {
        long floored = (leaderClockMillis / TICK_INTERVAL_MS) * TICK_INTERVAL_MS;
        // Under a normal tick cadence, `floored` always exceeds the previous
        // stamp by a full interval, so this max() call has no effect. It
        // matters only after onOriginRecord forces an extra boundary in the
        // same window.
        long l2Timestamp = Math.max(floored, lastL2Timestamp + 1);
        Boundary boundary = new Boundary(blockNumber, canonicalCount, l2Timestamp, l1Origin);
        blockNumber++;
        pruneExpired();
        lastL2Timestamp = l2Timestamp;
        lastBoundaryCount = canonicalCount;
        return boundary;
    }

    /**
     * Process one origin-advancing record: a record whose frame also
     * declares a new L1 origin (see {@code KIND_ORIGIN_RECORD} in
     * {@code crates/cluster-adapter/src/wire.rs}). The steps, in order:
     *
     * <ol>
     *   <li>drop the record if its canonical id is a duplicate. Every
     *       sequencer forwards every epoch, so most offers are re-offers of
     *       a record that is already ordered, carrying the origin it
     *       already adopted;</li>
     *   <li>close the currently open block, if it holds any records, so the
     *       record leads a block instead of landing mid-block. The forced
     *       boundary still carries the old origin, because it closes a
     *       block that belongs to the old epoch;</li>
     *   <li>adopt {@code newL1Origin}, so every later boundary carries
     *       it;</li>
     *   <li>relay the payload as-is, exactly like {@link #onRecord}.</li>
     * </ol>
     *
     * <p>{@code slotCount} is how many canonical slots this record claims
     * (see {@code epoch_slots} in {@code crates/cluster-adapter/src/wire.rs}).
     * An epoch record expands to a contiguous range of slots — the marker
     * plus one slot per deposit — because each slot must map to at most one
     * transaction downstream. The sealer trusts this count and never parses
     * the payload. Consumers that do parse it re-derive the count and fail
     * stop on a mismatch.</p>
     *
     * <p>The origin is echoed, never validated against L1, because this
     * state machine has no L1 access by design. Monotonicity is enforced
     * locally, since that check needs only the replicated state.</p>
     *
     * @param newL1Origin the L1 block number for this record's epoch
     * @param slotCount canonical slots claimed; must be at least 1
     * @return empty if the record was a duplicate; otherwise the forced
     *         boundary (if any) and the relayed record
     * @throws IllegalArgumentException if {@code newL1Origin} does not
     *         advance, or {@code slotCount} is below 1
     */
    public Optional<OriginAdvance> onOriginRecord(
            byte[] canonicalId32,
            long newL1Origin,
            long slotCount,
            byte[] payload,
            long leaderClockMillis) {
        if (slotCount < 1) {
            // A zero-width record would make the next record reuse this
            // index. The consumer's dense cursor keys records by index.
            throw new IllegalArgumentException("slotCount must be >= 1, got " + slotCount);
        }
        // Check dedup first. Checking monotonicity before dedup would reject
        // normal re-offers from racing sequencers as regressions.
        // A marker carries no deadline of its own: it is not a user
        // submission. The sealer assigns one, for pruning only. A re-offer
        // of a pruned marker id meets the origin guard below, which is
        // what refuses it.
        if (firstSeen(canonicalId32, assignedDeadline()) != Admission.FRESH) {
            return Optional.empty();
        }
        if (newL1Origin <= l1Origin) {
            // This is not a duplicate, but it claims an origin at or below
            // the current one, so two producers disagree about L1. Reject
            // it to keep l1Origin increasing, which the derivation rules
            // depend on.
            throw new IllegalArgumentException(
                    "l1Origin must advance: have " + l1Origin + ", got " + newL1Origin);
        }
        Boundary forced = null;
        if (canonicalCount > lastBoundaryCount) {
            forced = onTick(leaderClockMillis);
        }
        l1Origin = newL1Origin;
        long index = canonicalCount;
        // The record is relayed at the first slot of its range. The rest of
        // the range is consumed here, so the next record starts past the
        // deposits.
        canonicalCount += slotCount;
        return Optional.of(new OriginAdvance(forced, new Relayed(index, payload)));
    }

    /**
     * Outcome of {@link #onRemoteOriginRecord}. It is exactly one of:
     * <ul>
     *   <li>dropped duplicate — {@code advance} is empty, {@code rejected}
     *       is false;</li>
     *   <li>relayed — {@code advance} holds the forced boundary (if any)
     *       and the record to relay;</li>
     *   <li>rejected — {@code rejected} is true, {@code reason} is one of
     *       the {@code REMOTE_REJECT_*} codes, and {@code expectedNextSeq}
     *       carries the lane cursor (0 unless the reason is a seq
     *       mismatch).</li>
     * </ul>
     */
    public static final class RemoteOriginOutcome {
        public final Optional<RemoteOriginAdvance> advance;
        public final boolean rejected;
        public final byte reason;
        public final long expectedNextSeq;

        private RemoteOriginOutcome(
                Optional<RemoteOriginAdvance> advance, boolean rejected, byte reason, long expectedNextSeq) {
            this.advance = advance;
            this.rejected = rejected;
            this.reason = reason;
            this.expectedNextSeq = expectedNextSeq;
        }

        static RemoteOriginOutcome duplicate() {
            return new RemoteOriginOutcome(Optional.empty(), false, (byte) 0, 0L);
        }

        static RemoteOriginOutcome relayed(RemoteOriginAdvance a) {
            return new RemoteOriginOutcome(Optional.of(a), false, (byte) 0, 0L);
        }

        static RemoteOriginOutcome rejected(byte reason, long expectedNextSeq) {
            return new RemoteOriginOutcome(Optional.empty(), true, reason, expectedNextSeq);
        }
    }

    /**
     * Process one REMOTE-ORIGIN record: a batch of cross-chain messages a peer
     * chain produced, carried by its own ingress kind (see
     * {@code KIND_REMOTE_ORIGIN_RECORD} in
     * {@code crates/cluster-adapter/src/wire}). The steps, in order:
     *
     * <ol>
     *   <li>drop it if the canonical id is already in the dedup window. The
     *       Rust-side canonical id mixes the origin chain id, the anchor, and
     *       the seq range into its preimage, so ONE dedup window covers every
     *       peer, and the M-watcher fan-in (every watcher forwards every
     *       remote batch) is absorbed exactly as the epoch fan-in is. This
     *       check runs FIRST, so a re-offer of an adopted record is never
     *       read as a lane regression;</li>
     *   <li>reject it if {@code originChainId} is not in the allowlist;</li>
     *   <li>reject it if the seq range is malformed, or if
     *       {@code slotCount != 2 + lastSeq - firstSeq} (the marker plus one
     *       slot per message). The sealer never parses the payload, so this
     *       is the only place the claimed slot count meets the claimed
     *       range;</li>
     *   <li>reject it if THIS peer's lane cursor is known and
     *       {@code firstSeq} differs from it. This is the lane contiguity
     *       guard: a record that skips or repeats a seq never seals. An
     *       unknown peer seeds its cursor at {@code firstSeq}
     *       (trust-on-first-sight, like the contiguity guard's unknown
     *       senders);</li>
     *   <li>reject it if {@code anchorNumber} does not advance THIS peer's
     *       position (the second guard). Peers are independent, so the
     *       checks read and write only that peer's entry;</li>
     *   <li>only now insert the id into the dedup window. A rejected id
     *       never enters the window, so a racing copy of the same record is
     *       rejected the same way and never absorbed as a "duplicate";</li>
     *   <li>close the currently open block (if it holds any records) so the
     *       batch LEADS a block, keeping its contiguous slot range inside one
     *       block and its marker aligned with the block start;</li>
     *   <li>adopt the anchor, set the lane cursor to {@code lastSeq + 1},
     *       relay the payload verbatim at the first slot, and consume
     *       {@code slotCount} slots, exactly as the epoch path does.</li>
     * </ol>
     *
     * <p><b>Remote origins are NOT stamped into boundaries.</b> {@link Boundary}
     * carries {@code l1Origin} because there is exactly one L1; a per-peer
     * stamp would grow EVERY boundary by the peer count for data that is
     * already recoverable from the stream itself (the relayed markers, which
     * replay preserves). Do not add fields to {@link Boundary} for it.</p>
     *
     * <p>All seq and anchor comparisons are unsigned: the Rust side sends
     * u64 values.</p>
     *
     * @param originChainId the peer chain this batch came from
     * @param anchorNumber the peer-side position this batch is anchored at
     * @param slotCount canonical slots claimed (1 + message count)
     * @param firstSeq the seq of the batch's first message
     * @param lastSeq the seq of the batch's last message
     * @return the outcome: duplicate, relayed, or rejected with a reason
     */
    public RemoteOriginOutcome onRemoteOriginRecord(
            byte[] canonicalId32,
            long originChainId,
            long anchorNumber,
            long slotCount,
            long firstSeq,
            long lastSeq,
            byte[] payload,
            long leaderClockMillis) {
        checkId(canonicalId32);
        ByteBuffer key = ByteBuffer.wrap(canonicalId32.clone()).asReadOnlyBuffer();
        // Dedup LOOKUP first: the racing watchers' normal re-offers carry
        // the position already adopted and would read as lane regressions
        // if checked before this.
        if (dedup.containsKey(key)) {
            return RemoteOriginOutcome.duplicate();
        }
        if (!remoteOriginAllowlist.contains(originChainId)) {
            return RemoteOriginOutcome.rejected(REMOTE_REJECT_UNKNOWN_ORIGIN, 0L);
        }
        if (Long.compareUnsigned(lastSeq, firstSeq) < 0) {
            return RemoteOriginOutcome.rejected(REMOTE_REJECT_BAD_RANGE, 0L);
        }
        long span = lastSeq - firstSeq; // unsigned, no overflow: lastSeq >= firstSeq
        if (Long.compareUnsigned(span, Long.MAX_VALUE - 2) > 0) {
            return RemoteOriginOutcome.rejected(REMOTE_REJECT_BAD_RANGE, 0L);
        }
        if (slotCount != span + 2) {
            // A zero-width or over-wide record would let the next record
            // reuse a slot, or leave slots no message fills. The consumer
            // keys BY index, so this must fail here and not downstream.
            return RemoteOriginOutcome.rejected(REMOTE_REJECT_SLOT_COUNT_MISMATCH, 0L);
        }
        RemotePeer peer = remoteOrigins.get(originChainId);
        if (peer != null) {
            if (peer.nextSeqKnown && peer.nextSeq != firstSeq) {
                // Not a duplicate, yet not the next slice of this lane: the
                // record skips or repeats a seq. Sealing it would commit a
                // permanent lane hole that no retry can fill.
                return RemoteOriginOutcome.rejected(REMOTE_REJECT_SEQ_MISMATCH, peer.nextSeq);
            }
            if (Long.compareUnsigned(anchorNumber, peer.anchorNumber) <= 0) {
                // Claiming a position at or below the one already adopted
                // FOR THIS PEER: two producers disagree about that peer's
                // chain. Other peers' entries are untouched.
                return RemoteOriginOutcome.rejected(REMOTE_REJECT_ANCHOR_REGRESSED, 0L);
            }
        }
        // Every check passed. Only now does the id enter the window.
        //
        // A marker does not meet the window cap. Markers are a trickle (one
        // per L1 block, one per peer batch) where transactions are a flood,
        // and refusing one would stall that lane rather than shed load. The
        // cap bounds the flood. Every member takes the same branch, so the
        // replicated state stays identical.
        insertFresh(key, assignedDeadline());
        Boundary forced = null;
        if (canonicalCount > lastBoundaryCount) {
            forced = onTick(leaderClockMillis);
        }
        remoteOrigins.put(originChainId, new RemotePeer(anchorNumber, true, lastSeq + 1));
        long index = canonicalCount;
        // Relayed at the FIRST slot of its range; the rest is consumed here so
        // the next record starts past this batch's messages.
        canonicalCount += slotCount;
        return RemoteOriginOutcome.relayed(new RemoteOriginAdvance(forced, new Relayed(index, payload)));
    }

    /** L1 origin currently stamped into boundaries. */
    public long l1Origin() {
        return l1Origin;
    }

    /**
     * The anchor position last adopted from {@code originChainId}, or empty if
     * no batch from that peer has been ordered yet.
     */
    public Optional<Long> remoteOriginOf(long originChainId) {
        RemotePeer p = remoteOrigins.get(originChainId);
        return p == null ? Optional.empty() : Optional.of(p.anchorNumber);
    }

    /**
     * The lane cursor for {@code originChainId}: the first seq not yet
     * ordered. Empty if the peer is unknown, or if its cursor is unknown
     * (loaded from a version-4 snapshot).
     */
    public Optional<Long> remoteNextSeqOf(long originChainId) {
        RemotePeer p = remoteOrigins.get(originChainId);
        return (p == null || !p.nextSeqKnown) ? Optional.empty() : Optional.of(p.nextSeq);
    }

    /** The configured remote-origin allowlist (read-only). */
    public Set<Long> remoteOriginAllowlist() {
        return remoteOriginAllowlist;
    }

    /** Number of peer chains with an adopted remote-origin position. */
    public int trackedRemoteOrigins() {
        return remoteOrigins.size();
    }

    /** Cumulative count of canonical records relayed so far. */
    public long canonicalCount() {
        return canonicalCount;
    }

    /** Block number the next {@link #onTick(long)} will stamp. */
    public long blockNumber() {
        return blockNumber;
    }

    /** Capacity of the dedup window. */
    public int dedupCapacity() {
        return dedupCapacity;
    }

    /** Current number of ids held in the dedup window. */
    public int dedupSize() {
        return dedup.size();
    }

    /** Current number of senders tracked by the contiguity guard. */
    public int trackedSenders() {
        return expectedNonce.size();
    }

    /**
     * The guard's expected next nonce for {@code sender20}, or empty if the
     * sender is not tracked. This method iterates on purpose: a {@code get}
     * call on the access-ordered map would reorder the LRU. This accessor
     * is for tests and local observability, and must not change the
     * replicated eviction order.
     */
    public Optional<Long> expectedNonceOf(byte[] sender20) {
        ByteBuffer key = ByteBuffer.wrap(sender20).asReadOnlyBuffer();
        for (Map.Entry<ByteBuffer, Long> e : expectedNonce.entrySet()) {
            if (e.getKey().equals(key)) {
                return Optional.of(e.getValue());
            }
        }
        return Optional.empty();
    }

    /**
     * Serialize the full state: dedup ids (32 bytes each, in FIFO/insertion
     * order), {@code canonicalCount}, {@code blockNumber}, and (from
     * version 2) the contiguity-guard sender map in LRU iteration order
     * (eldest first, so a restore rebuilds the identical eviction order).
     * The capacity is not encoded. It is supplied at
     * {@link #load(byte[], int)} time, matching the cluster's configured
     * window.
     *
     * <p>Layout (big-endian): magic(4) | version(4) | canonicalCount(8) |
     * blockNumber(8) | idCount(4) | idCount * 32 | senderCount(4) |
     * senderCount * (sender 20 + expectedNonce 8) | l1Origin(8) |
     * lastL2Timestamp(8) | lastBoundaryCount(8) | remoteCount(4) |
     * remoteCount * (originChainId 8 + anchorNumber 8 + nextSeqKnown 1 +
     * nextSeq 8).</p>
     *
     * <p>The version-3 origin trio is added after the version-2 sender map,
     * and the version-4 peer map after the trio, so version-1 through
     * version-3 parsing stays byte-identical and older snapshots keep
     * loading. Version 5 widens each peer entry by 9 bytes (the lane
     * cursor); a version-4 snapshot is parsed with the 16-byte entry.
     * Version 6 adds the void ledger after the peer map: see
     * {@link VoidLedger#writeTo}.</p>
     */
    public byte[] takeSnapshot() {
        int idCount = dedup.size();
        int senderCount = expectedNonce.size();
        int remoteCount = remoteOrigins.size();
        int size = 4 + 4 + 8 + 8 + 4 + idCount * (CANONICAL_ID_LEN + 8)
                + 4 + senderCount * (SENDER_LEN + 8)
                + 8 + 8 + 8
                + 4 + remoteCount * REMOTE_ENTRY_LEN_V5
                + voids.snapshotLen(canonicalCount);
        ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(SNAPSHOT_MAGIC);
        buf.putInt(SNAPSHOT_VERSION);
        buf.putLong(canonicalCount);
        buf.putLong(blockNumber);
        buf.putInt(idCount);
        // v7: each id carries the deadline that frees it. The map is
        // insertion-ordered and mutated only by the replicated record
        // sequence, so every member writes the same bytes.
        for (Map.Entry<ByteBuffer, Long> e : dedup.entrySet()) {
            ByteBuffer dup = e.getKey().duplicate();
            dup.rewind();
            byte[] raw = new byte[CANONICAL_ID_LEN];
            dup.get(raw);
            buf.put(raw);
            buf.putLong(e.getValue());
        }
        buf.putInt(senderCount);
        for (Map.Entry<ByteBuffer, Long> e : expectedNonce.entrySet()) {
            ByteBuffer dup = e.getKey().duplicate();
            dup.rewind();
            byte[] raw = new byte[SENDER_LEN];
            dup.get(raw);
            buf.put(raw);
            buf.putLong(e.getValue());
        }
        // Version-3 fields.
        buf.putLong(l1Origin);
        buf.putLong(lastL2Timestamp);
        buf.putLong(lastBoundaryCount);
        // v5 tail: the per-peer remote origins, in insertion order.
        buf.putInt(remoteCount);
        for (Map.Entry<Long, RemotePeer> e : remoteOrigins.entrySet()) {
            buf.putLong(e.getKey());
            buf.putLong(e.getValue().anchorNumber);
            buf.put(e.getValue().nextSeqKnown ? (byte) 1 : (byte) 0);
            buf.putLong(e.getValue().nextSeq);
        }
        // v6 tail: the void window, then the open votes.
        voids.writeTo(buf, canonicalCount);
        return buf.array();
    }

    /**
     * Restore a state that {@link #takeSnapshot()} produced earlier. Later
     * behavior matches the snapshotted instance exactly: the dedup window
     * rebuilds in the same FIFO order, and {@code canonicalCount} and
     * {@code blockNumber} resume from the same values.
     */
    public static CanonicalSealerState load(byte[] snapshot, int dedupCapacity) {
        return load(snapshot, dedupCapacity, Set.of());
    }

    /**
     * Restore a state with the given remote-origin allowlist. The allowlist
     * is configuration, not snapshot content: a peer entry whose origin is
     * no longer allowlisted stays in the map (its history is replicated
     * state), but its next record is rejected.
     */
    public static CanonicalSealerState load(byte[] snapshot, int dedupCapacity, Set<Long> remoteOrigins) {
        return load(snapshot, dedupCapacity, remoteOrigins, VoidLedger.Config.DISABLED);
    }

    /**
     * Restore a state with a remote-origin allowlist and a void
     * configuration. The void configuration is not snapshot content. A
     * snapshot from before version 6 restores an empty ledger: an entry from
     * before the snapshot cannot be removed, and a consumer that waits at
     * one stops as it did before.
     */
    public static CanonicalSealerState load(
            byte[] snapshot, int dedupCapacity, Set<Long> remoteOrigins, VoidLedger.Config voidConfig) {
        return load(snapshot, dedupCapacity, remoteOrigins, voidConfig, DEFAULT_INCLUSION_HORIZON_BLOCKS);
    }

    /**
     * {@link #load(byte[], int, Set, VoidLedger.Config)} with this member's
     * inclusion horizon. The horizon is replicated configuration, so it
     * comes from the deploy, not from the snapshot.
     *
     * @param snapshot               the bytes {@link #takeSnapshot()} wrote
     * @param dedupCapacity          this member's configured window cap
     * @param remoteOrigins          the peer-chain allowlist
     * @param voidConfig             the void ledger's configuration
     * @param inclusionHorizonBlocks the deadline horizon, in blocks
     * @return the restored state
     */
    public static CanonicalSealerState load(
            byte[] snapshot,
            int dedupCapacity,
            Set<Long> remoteOrigins,
            VoidLedger.Config voidConfig,
            long inclusionHorizonBlocks) {
        ByteBuffer buf = ByteBuffer.wrap(snapshot).order(ByteOrder.BIG_ENDIAN);
        int magic = buf.getInt();
        if (magic != SNAPSHOT_MAGIC) {
            throw new IllegalArgumentException(
                    "bad snapshot magic: 0x" + Integer.toHexString(magic));
        }
        int version = buf.getInt();
        if (version < 1 || version > SNAPSHOT_VERSION) {
            throw new IllegalArgumentException("unsupported snapshot version: " + version);
        }
        long canonicalCount = buf.getLong();
        long blockNumber = buf.getLong();
        int idCount = buf.getInt();
        if (idCount < 0 || idCount > dedupCapacity) {
            // A snapshot taken with a larger configured window than this
            // member's would silently rebuild an oversized window. firstSeen
            // only shrinks it by one entry per insert, so dedup behavior
            // would differ from a fresh state with the same config — a
            // determinism hazard if members disagree on the capacity. Fail
            // loudly instead of truncating silently. Shrinking the window
            // across a restart needs an explicit migration.
            throw new IllegalArgumentException(
                    "snapshot idCount " + idCount + " outside [0, dedupCapacity="
                            + dedupCapacity + "] — members must agree on the configured window");
        }
        int idEntryLen = version >= 7 ? CANONICAL_ID_LEN + 8 : CANONICAL_ID_LEN;
        if ((long) idCount * idEntryLen > buf.remaining()) {
            throw new IllegalArgumentException(
                    "truncated snapshot: idCount " + idCount + " needs "
                            + ((long) idCount * idEntryLen) + " bytes, only "
                            + buf.remaining() + " remaining");
        }

        CanonicalSealerState state = new CanonicalSealerState(
                dedupCapacity, blockNumber, remoteOrigins, voidConfig, inclusionHorizonBlocks);
        for (int i = 0; i < idCount; i++) {
            byte[] raw = new byte[CANONICAL_ID_LEN];
            buf.get(raw);
            // A version below 7 has no deadline per id. Hold each one for a
            // full horizon: the conservative direction, since an id held too
            // long only absorbs a duplicate, while one freed too early could
            // let a stale copy in.
            long held = version >= 7 ? buf.getLong() : state.assignedDeadline();
            state.insertFresh(ByteBuffer.wrap(raw).asReadOnlyBuffer(), held);
        }
        if (version >= 2) {
            int senderCount = buf.getInt();
            if (senderCount < 0 || senderCount > dedupCapacity) {
                throw new IllegalArgumentException(
                        "snapshot senderCount " + senderCount + " outside [0, capacity="
                                + dedupCapacity + "] — members must agree on the configured window");
            }
            if ((long) senderCount * (SENDER_LEN + 8) > buf.remaining()) {
                throw new IllegalArgumentException(
                        "truncated snapshot: senderCount " + senderCount + " needs "
                                + ((long) senderCount * (SENDER_LEN + 8)) + " bytes, only "
                                + buf.remaining() + " remaining");
            }
            for (int i = 0; i < senderCount; i++) {
                byte[] raw = new byte[SENDER_LEN];
                buf.get(raw);
                long expected = buf.getLong();
                // Put entries into the fresh access-ordered map in snapshot
                // order (eldest first). This rebuilds the identical LRU
                // order.
                state.expectedNonce.put(ByteBuffer.wrap(raw).asReadOnlyBuffer(), expected);
            }
        }
        if (version >= 3) {
            state.l1Origin = buf.getLong();
            state.lastL2Timestamp = buf.getLong();
            state.lastBoundaryCount = buf.getLong();
        }
        if (version >= 4) {
            int remoteCount = buf.getInt();
            int entryLen = version >= 5 ? REMOTE_ENTRY_LEN_V5 : REMOTE_ENTRY_LEN_V4;
            if (remoteCount < 0 || (long) remoteCount * entryLen > buf.remaining()) {
                throw new IllegalArgumentException(
                        "truncated snapshot: remoteCount " + remoteCount + " needs "
                                + ((long) remoteCount * entryLen) + " bytes, only "
                                + buf.remaining() + " remaining");
            }
            for (int i = 0; i < remoteCount; i++) {
                long originChainId = buf.getLong();
                long anchorNumber = buf.getLong();
                boolean nextSeqKnown = false;
                long nextSeq = 0L;
                if (version >= 5) {
                    nextSeqKnown = buf.get() != 0;
                    nextSeq = buf.getLong();
                }
                // A v4 entry has no cursor. The peer keeps its anchor guard
                // and seeds its cursor from its next record.
                state.remoteOrigins.put(originChainId, new RemotePeer(anchorNumber, nextSeqKnown, nextSeq));
            }
        }
        if (version >= 6) {
            state.voids = VoidLedger.readFrom(buf, voidConfig);
        }
        // A version-1 snapshot (before the guard existed) restores an empty
        // guard map, so every sender re-seeds on its next record. This is
        // trust-on-first-sight, and it causes no false rejects. A version-1
        // or version-2 snapshot (before origin tracking) restores origin 0,
        // which is exactly the state that chain was in. A version-1 through
        // version-3 snapshot (before interop) restores an empty peer map,
        // and each peer re-seeds on its next batch, the same
        // trust-on-first-sight behavior. A version-4 snapshot restores each
        // peer's anchor with an unknown lane cursor. The cursor seeds from
        // the peer's next record.
        state.canonicalCount = canonicalCount;
        return state;
    }
}
