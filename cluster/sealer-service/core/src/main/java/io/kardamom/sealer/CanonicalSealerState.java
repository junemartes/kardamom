package io.kardamom.sealer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;

/**
 * Deterministic canonical-ordering state machine for the Kardamom sealer.
 *
 * <p>Pure POJO with NO Aeron dependency, NO wall clock and NO threads — every
 * output is a deterministic function of the input sequence. This is a faithful
 * Java port of the Rust sealer's canonical republish logic
 * ({@code crates/sealer/src/bin/kardamom-sealer.rs}), the boundary emitter
 * ({@code crates/sealer/src/emitter.rs}) and the executor-side
 * {@code DedupWindow} ({@code crates/executor/src/reader.rs}).</p>
 *
 * <p>Responsibilities:</p>
 * <ul>
 *   <li><b>Dedup</b> — a bounded, FIFO-evicted first-seen window over 32-byte
 *       canonical ids ({@link #firstSeen(byte[])}), mirroring Rust
 *       {@code DedupWindow}/{@code CanonicalDedup}.</li>
 *   <li><b>Canonical count</b> — {@link #onRecord(byte[], byte[], long, byte[])}
 *       relays each first-seen record with its 0-based index and bumps
 *       {@code canonicalCount}; duplicates are dropped and never counted.</li>
 *   <li><b>Contiguity guard</b> (#85 fix B) — a bounded per-sender
 *       expected-nonce map. A known sender's first-seen record whose nonce is
 *       not the expected next one is REJECTED (never counted, never deduped)
 *       so a voided-offer gap surfaces as a recoverable signal instead of a
 *       silently sealed canonical nonce gap. Unknown senders seed at any
 *       nonce; the all-zero sender (deposits) is exempt.</li>
 *   <li><b>Boundaries</b> — {@link #onTick(long)} stamps a {@link Boundary} with
 *       the current count and a 250 ms-floored timestamp, then advances the
 *       block number.</li>
 *   <li><b>Snapshot</b> — {@link #takeSnapshot()} / {@link #load(byte[], int)}
 *       round-trip the full state for cluster snapshots.</li>
 * </ul>
 */
public final class CanonicalSealerState {

    /** L2 tick alignment in milliseconds. Mirrors the Rust sealer's 250 ms tick. */
    public static final long TICK_INTERVAL_MS = 250L;

    /** Length, in bytes, of a canonical id (a 32-byte hash). */
    public static final int CANONICAL_ID_LEN = 32;

    /** Length, in bytes, of a sender address in the contiguity guard. */
    public static final int SENDER_LEN = 20;

    /** Default genesis block number. */
    public static final long GENESIS_BLOCK_NUMBER = 1L;

    private static final int SNAPSHOT_MAGIC = 0x4B53_4541; // "KSEA"
    /**
     * v2 appended the contiguity-guard sender map; v3 appends the L1-origin
     * trio ({@code l1Origin}, {@code lastL2Timestamp}, {@code lastBoundaryCount})
     * AFTER it, so v1 and v2 parsing is untouched and older snapshots still
     * load — the trio defaults to zero, which is exactly the pre-origin state.
     * A cluster upgrades in place without a coordinated snapshot migration.
     */
    private static final int SNAPSHOT_VERSION = 3;

    /**
     * FIFO first-seen window. Insertion-ordered, so the oldest inserted id is
     * the first element — exactly the {@code VecDeque} front the Rust
     * {@code DedupWindow} pops on eviction. Keys are 32-byte ids wrapped in a
     * read-only {@link ByteBuffer} for value-based equality.
     */
    private final LinkedHashSet<ByteBuffer> dedup;
    private final int dedupCapacity;

    /**
     * Per-sender expected next nonce (#85 fix B), LRU-bounded at the dedup
     * capacity (one shared knob all members already must agree on; ~5MB at
     * the 1&lt;&lt;17 default). DETERMINISM: access-ordered, mutated only by
     * the replicated record sequence, and snapshotted in iteration order, so
     * every member holds the identical map AND identical eviction order. The
     * eviction floor is honest degradation: an evicted sender that reappears
     * is treated as unknown and re-seeds at whatever nonce arrives (its gap
     * protection restarts there) — the same trust-on-first-sight as a brand
     * new sender, never a false reject. Keys are 20-byte senders wrapped in
     * read-only {@link ByteBuffer}s for value-based equality.
     */
    private final LinkedHashMap<ByteBuffer, Long> expectedNonce;

    /** Cumulative count of canonical (first-seen) records relayed. */
    private long canonicalCount;

    /** Block number the next {@link #onTick(long)} will stamp. */
    private long blockNumber;

    /**
     * L1 block number stamped into every boundary until the next
     * origin-advancing record. Echoed from ordered input — this state machine
     * NEVER reads L1, which is what keeps it deterministic across replicas.
     */
    private long l1Origin;

    /**
     * Timestamp of the last boundary stamped. Boundaries are forced strictly
     * increasing: a tick-aligned timestamp alone stopped being unique once
     * {@link #onOriginRecord} can force a second boundary inside the same
     * 250 ms window, and two blocks sharing a timestamp is a trap for anything
     * reasoning about block time.
     */
    private long lastL2Timestamp;

    /**
     * {@code canonicalCount} at the last boundary — i.e. how many records the
     * currently open block already holds. Lets {@link #onOriginRecord} skip
     * forcing a boundary when the open block is still empty, so a burst of
     * epochs (L1 catch-up) does not emit a run of empty blocks.
     */
    private long lastBoundaryCount;

    /** Construct at genesis (block number {@value #GENESIS_BLOCK_NUMBER}). */
    public CanonicalSealerState(int dedupCapacity) {
        this(dedupCapacity, GENESIS_BLOCK_NUMBER);
    }

    public CanonicalSealerState(int dedupCapacity, long initialBlockNumber) {
        if (dedupCapacity <= 0) {
            throw new IllegalArgumentException("dedupCapacity must be > 0, got " + dedupCapacity);
        }
        this.dedupCapacity = dedupCapacity;
        this.dedup = new LinkedHashSet<>();
        this.expectedNonce = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(final Map.Entry<ByteBuffer, Long> eldest) {
                return size() > dedupCapacity;
            }
        };
        this.canonicalCount = 0L;
        this.blockNumber = initialBlockNumber;
        this.l1Origin = 0L;
        this.lastL2Timestamp = 0L;
        this.lastBoundaryCount = 0L;
    }

    /**
     * Record {@code id32} in the dedup window; returns {@code false} if it is
     * already present (a duplicate), {@code true} if it is freshly inserted.
     *
     * <p>On a fresh insert, if the window now exceeds its capacity the OLDEST
     * inserted id is evicted. Mirrors Rust {@code DedupWindow::first_seen}
     * exactly: an evicted id becomes "fresh" again on a later sighting.</p>
     *
     * @param id32 a 32-byte canonical id (defensively copied)
     */
    public boolean firstSeen(byte[] id32) {
        checkId(id32);
        // Defensive copy so the caller's array can't mutate a stored key.
        ByteBuffer key = ByteBuffer.wrap(id32.clone()).asReadOnlyBuffer();
        if (dedup.contains(key)) {
            return false; // already present
        }
        insertFresh(key);
        return true;
    }

    private static void checkId(byte[] id32) {
        if (id32 == null || id32.length != CANONICAL_ID_LEN) {
            throw new IllegalArgumentException(
                    "canonical id must be " + CANONICAL_ID_LEN + " bytes, got "
                            + (id32 == null ? "null" : id32.length));
        }
    }

    /** Insert a NOT-present key into the dedup window, FIFO-evicting. */
    private void insertFresh(ByteBuffer key) {
        dedup.add(key);
        if (dedup.size() > dedupCapacity) {
            Iterator<ByteBuffer> it = dedup.iterator();
            it.next(); // oldest inserted (FIFO front)
            it.remove();
        }
    }

    /**
     * Outcome of {@link #onRecord(byte[], byte[], long, byte[])}: exactly one
     * of dropped-duplicate ({@code relayed} empty, not rejected), relayed
     * ({@code relayed} present), or contiguity-rejected ({@code rejected}
     * true, {@code expectedNonce} carries the nonce the guard wanted).
     */
    public static final class RecordOutcome {
        public final Optional<Relayed> relayed;
        public final boolean rejected;
        public final long expectedNonce;

        private RecordOutcome(Optional<Relayed> relayed, boolean rejected, long expectedNonce) {
            this.relayed = relayed;
            this.rejected = rejected;
            this.expectedNonce = expectedNonce;
        }

        static RecordOutcome duplicate() {
            return new RecordOutcome(Optional.empty(), false, 0L);
        }

        static RecordOutcome relayed(Relayed r) {
            return new RecordOutcome(Optional.of(r), false, 0L);
        }

        static RecordOutcome rejected(long expectedNonce) {
            return new RecordOutcome(Optional.empty(), true, expectedNonce);
        }
    }

    /**
     * Process one application record.
     *
     * <p>Order matters (#85 fix B interplay with the #114 republish ledger):
     * the dedup check runs FIRST, so a re-offered copy of a record that
     * already committed is absorbed as a duplicate BEFORE the contiguity
     * guard sees its (by then stale) nonce. Then, for a non-zero sender, the
     * guard: a known sender whose nonce is not the expected next one is
     * REJECTED — no dedup insert (the sequencer republishes the same
     * canonical id after recovering the gap, and it must then be accepted as
     * fresh), no count, no relay. Unknown senders (first sight, or evicted
     * from the bounded map) seed at whatever nonce arrives.</p>
     *
     * <p>{@code payload} is relayed VERBATIM and is never parsed.</p>
     */
    public RecordOutcome onRecord(byte[] canonicalId32, byte[] sender20, long nonce, byte[] payload) {
        checkId(canonicalId32);
        if (sender20 == null || sender20.length != SENDER_LEN) {
            throw new IllegalArgumentException(
                    "sender must be " + SENDER_LEN + " bytes, got "
                            + (sender20 == null ? "null" : sender20.length));
        }
        ByteBuffer key = ByteBuffer.wrap(canonicalId32.clone()).asReadOnlyBuffer();
        if (dedup.contains(key)) {
            return RecordOutcome.duplicate();
        }
        if (!isZeroSender(sender20)) {
            ByteBuffer senderKey = ByteBuffer.wrap(sender20.clone()).asReadOnlyBuffer();
            Long expected = expectedNonce.get(senderKey);
            if (expected != null && expected.longValue() != nonce) {
                return RecordOutcome.rejected(expected.longValue());
            }
            expectedNonce.put(senderKey, nonce + 1);
        }
        insertFresh(key);
        long index = canonicalCount;
        canonicalCount++;
        return RecordOutcome.relayed(new Relayed(index, payload));
    }

    /**
     * Guard-exempt record processing (no sender identity — the pre-guard
     * contract, kept for deposits-only callers and the existing tests).
     * Equivalent to {@link #onRecord(byte[], byte[], long, byte[])} with the
     * all-zero sender.
     */
    public Optional<Relayed> onRecord(byte[] canonicalId32, byte[] payload) {
        return onRecord(canonicalId32, new byte[SENDER_LEN], 0L, payload).relayed;
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
     * Stamp a block boundary at this tick. The boundary's {@code endTxIdx} is the
     * current {@code canonicalCount} (every record counted so far belongs to a
     * block ≤ this boundary); {@code l2Timestamp} is {@code leaderClockMillis}
     * floored to {@link #TICK_INTERVAL_MS}. The block number is advanced AFTER
     * stamping, so successive ticks produce monotonically increasing,
     * gap-free block numbers.
     */
    public Boundary onTick(long leaderClockMillis) {
        long floored = (leaderClockMillis / TICK_INTERVAL_MS) * TICK_INTERVAL_MS;
        // Under pure tick cadence `floored` always clears the previous stamp by
        // a full interval and this max() is a no-op; it only bites after
        // onOriginRecord forced an extra boundary inside the same window.
        long l2Timestamp = Math.max(floored, lastL2Timestamp + 1);
        Boundary boundary = new Boundary(blockNumber, canonicalCount, l2Timestamp, l1Origin);
        blockNumber++;
        lastL2Timestamp = l2Timestamp;
        lastBoundaryCount = canonicalCount;
        return boundary;
    }

    /**
     * Process one ORIGIN-ADVANCING record: a record whose carrying frame also
     * declares a new L1 origin (see {@code KIND_ORIGIN_RECORD} in
     * {@code crates/cluster-adapter/src/wire.rs}). Semantics, in order:
     *
     * <ol>
     *   <li>drop it if the canonical id is a duplicate — every sequencer
     *       forwards every epoch, so most offers are re-offers of a record
     *       already ordered, carrying the origin it already adopted;</li>
     *   <li>close the currently open block (if it holds any records) so the
     *       record LEADS a block rather than landing mid-block. The forced
     *       boundary still carries the OLD origin: it closes a block that
     *       belongs to the old epoch;</li>
     *   <li>adopt {@code newL1Origin}, so every subsequent boundary carries
     *       it;</li>
     *   <li>relay the payload verbatim, exactly like {@link #onRecord}.</li>
     * </ol>
     *
     * <p>{@code slotCount} is how many canonical slots this record claims (see
     * {@code epoch_slots} in {@code crates/cluster-adapter/src/wire.rs}): an
     * epoch is the one record that expands to a contiguous RANGE — the marker
     * plus one slot per deposit — because every slot must map to at most one
     * transaction downstream. The sealer takes the count on trust since it
     * never parses the payload; consumers that do parse it re-derive and
     * fail-stop on a mismatch.</p>
     *
     * <p>The origin is echoed, never validated against L1 — this state machine
     * has no L1 access by design. Monotonicity is enforced because that check
     * is purely local to replicated state.</p>
     *
     * @param newL1Origin the L1 block number this record's epoch belongs to
     * @param slotCount canonical slots claimed; must be >= 1
     * @return empty if the record was a duplicate; otherwise the forced
     *         boundary (if any) and the relayed record
     * @throws IllegalArgumentException if {@code newL1Origin} does not advance
     *         or {@code slotCount} is below 1
     */
    public Optional<OriginAdvance> onOriginRecord(
            byte[] canonicalId32,
            long newL1Origin,
            long slotCount,
            byte[] payload,
            long leaderClockMillis) {
        if (slotCount < 1) {
            // A zero-width record would make the next record reuse this index,
            // and the consumer's dense cursor keys records BY index.
            throw new IllegalArgumentException("slotCount must be >= 1, got " + slotCount);
        }
        // Dedup FIRST. Checking monotonicity before dedup would reject the M
        // racing sequencers' normal re-offers as regressions.
        if (!firstSeen(canonicalId32)) {
            return Optional.empty();
        }
        if (newL1Origin <= l1Origin) {
            // Not a duplicate, yet claiming an origin at or below the current
            // one: two producers disagree about L1. Rejecting keeps l1Origin
            // monotonic, which the derivation rules depend on.
            throw new IllegalArgumentException(
                    "l1Origin must advance: have " + l1Origin + ", got " + newL1Origin);
        }
        Boundary forced = null;
        if (canonicalCount > lastBoundaryCount) {
            forced = onTick(leaderClockMillis);
        }
        l1Origin = newL1Origin;
        long index = canonicalCount;
        // The record is relayed at the FIRST slot of its range; the rest of the
        // range is consumed here so the next record starts past the deposits.
        canonicalCount += slotCount;
        return Optional.of(new OriginAdvance(forced, new Relayed(index, payload)));
    }

    /** L1 origin currently stamped into boundaries. */
    public long l1Origin() {
        return l1Origin;
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
     * The guard's expected next nonce for {@code sender20}, or empty if
     * untracked. Iteration-based on purpose: a {@code get} on the
     * access-ordered map would reorder the LRU — this accessor is for tests
     * and member-local observability, which must NOT perturb the replicated
     * eviction order.
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
     * Serialise the full state: dedup ids (32 bytes each, in FIFO/insertion
     * order), {@code canonicalCount}, {@code blockNumber} and (v2) the
     * contiguity-guard sender map in LRU iteration order (eldest first, so a
     * restore rebuilds the identical eviction order). The capacity is NOT
     * encoded — it is supplied at {@link #load(byte[], int)} time (matching
     * the cluster's configured window).
     *
     * <p>Layout (big-endian): magic(4) | version(4) | canonicalCount(8) |
     * blockNumber(8) | idCount(4) | idCount * 32 | senderCount(4) |
     * senderCount * (sender 20 + expectedNonce 8) | l1Origin(8) |
     * lastL2Timestamp(8) | lastBoundaryCount(8).</p>
     *
     * <p>The v3 origin trio is APPENDED after the v2 sender map so v1/v2
     * parsing is byte-identical and older snapshots keep loading.</p>
     */
    public byte[] takeSnapshot() {
        int idCount = dedup.size();
        int senderCount = expectedNonce.size();
        int size = 4 + 4 + 8 + 8 + 4 + idCount * CANONICAL_ID_LEN
                + 4 + senderCount * (SENDER_LEN + 8)
                + 8 + 8 + 8;
        ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(SNAPSHOT_MAGIC);
        buf.putInt(SNAPSHOT_VERSION);
        buf.putLong(canonicalCount);
        buf.putLong(blockNumber);
        buf.putInt(idCount);
        for (ByteBuffer id : dedup) {
            ByteBuffer dup = id.duplicate();
            dup.rewind();
            byte[] raw = new byte[CANONICAL_ID_LEN];
            dup.get(raw);
            buf.put(raw);
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
        // v3 tail.
        buf.putLong(l1Origin);
        buf.putLong(lastL2Timestamp);
        buf.putLong(lastBoundaryCount);
        return buf.array();
    }

    /**
     * Restore a state previously produced by {@link #takeSnapshot()}. Subsequent
     * behaviour is identical to the snapshotted instance: the dedup window is
     * rebuilt in the same FIFO order and {@code canonicalCount}/{@code blockNumber}
     * resume exactly.
     */
    public static CanonicalSealerState load(byte[] snapshot, int dedupCapacity) {
        ByteBuffer buf = ByteBuffer.wrap(snapshot).order(ByteOrder.BIG_ENDIAN);
        int magic = buf.getInt();
        if (magic != SNAPSHOT_MAGIC) {
            throw new IllegalArgumentException(
                    "bad snapshot magic: 0x" + Integer.toHexString(magic));
        }
        int version = buf.getInt();
        if (version != 1 && version != 2 && version != SNAPSHOT_VERSION) {
            throw new IllegalArgumentException("unsupported snapshot version: " + version);
        }
        long canonicalCount = buf.getLong();
        long blockNumber = buf.getLong();
        int idCount = buf.getInt();
        if (idCount < 0 || idCount > dedupCapacity) {
            // A snapshot taken with a LARGER configured window than this
            // member's would silently rebuild an oversized window that
            // firstSeen shrinks by only one entry per insert — dedup behaviour
            // would differ from a fresh state with the same config, a
            // determinism hazard if members ever disagree on the capacity.
            // Fail loudly instead; shrinking the window across a restart
            // requires an explicit migration, not a silent truncation.
            throw new IllegalArgumentException(
                    "snapshot idCount " + idCount + " outside [0, dedupCapacity="
                            + dedupCapacity + "] — members must agree on the configured window");
        }
        if ((long) idCount * CANONICAL_ID_LEN > buf.remaining()) {
            throw new IllegalArgumentException(
                    "truncated snapshot: idCount " + idCount + " needs "
                            + ((long) idCount * CANONICAL_ID_LEN) + " bytes, only "
                            + buf.remaining() + " remaining");
        }

        CanonicalSealerState state = new CanonicalSealerState(dedupCapacity, blockNumber);
        for (int i = 0; i < idCount; i++) {
            byte[] raw = new byte[CANONICAL_ID_LEN];
            buf.get(raw);
            // Insert directly in FIFO order without eviction or counting; the
            // snapshot already reflects a window within capacity.
            state.dedup.add(ByteBuffer.wrap(raw).asReadOnlyBuffer());
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
                // put() into the fresh access-ordered map in snapshot order
                // (eldest first) rebuilds the identical LRU order.
                state.expectedNonce.put(ByteBuffer.wrap(raw).asReadOnlyBuffer(), expected);
            }
        }
        if (version >= 3) {
            state.l1Origin = buf.getLong();
            state.lastL2Timestamp = buf.getLong();
            state.lastBoundaryCount = buf.getLong();
        }
        // A v1 snapshot (pre-guard deploy) restores an EMPTY guard map: every
        // sender re-seeds on its next record — trust-on-first-sight, honest
        // degradation, no false rejects. A v1/v2 snapshot (pre-origin)
        // restores origin 0, which is exactly the state that chain was in.
        state.canonicalCount = canonicalCount;
        return state;
    }
}
