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
 * <p>This is a pure POJO. It has no Aeron dependency, no wall clock, and no
 * threads. Every output is a deterministic function of the input sequence.
 * This class is a faithful Java port of the Rust sealer's canonical republish
 * logic ({@code crates/sealer/src/bin/kardamom-sealer.rs}), the boundary
 * emitter ({@code crates/sealer/src/emitter.rs}), and the executor-side
 * {@code DedupWindow} ({@code crates/executor/src/reader.rs}).</p>
 *
 * <p>Responsibilities:</p>
 * <ul>
 *   <li><b>Dedup</b> — a bounded, FIFO-evicted first-seen window over 32-byte
 *       canonical ids ({@link #firstSeen(byte[])}). This mirrors the Rust
 *       {@code DedupWindow}/{@code CanonicalDedup}.</li>
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

    private static final int SNAPSHOT_MAGIC = 0x4B53_4541; // "KSEA"
    /**
     * Version 2 added the contiguity-guard sender map. Version 3 adds the
     * L1-origin trio ({@code l1Origin}, {@code lastL2Timestamp},
     * {@code lastBoundaryCount}) after it. This keeps v1 and v2 parsing
     * unchanged, so older snapshots still load. The trio defaults to zero,
     * which is exactly the pre-origin state. A cluster can upgrade in place
     * without a coordinated snapshot migration.
     */
    private static final int SNAPSHOT_VERSION = 3;

    /**
     * FIFO first-seen window. It is insertion-ordered, so the oldest inserted
     * id is the first element. This is exactly the {@code VecDeque} front
     * that the Rust {@code DedupWindow} pops on eviction. Keys are 32-byte
     * ids, wrapped in a read-only {@link ByteBuffer} for value-based
     * equality.
     */
    private final LinkedHashSet<ByteBuffer> dedup;
    private final int dedupCapacity;

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
     * Record {@code id32} in the dedup window. Returns {@code false} if the
     * id is already present (a duplicate), or {@code true} if it is freshly
     * inserted.
     *
     * <p>On a fresh insert, if the window then exceeds its capacity, the
     * oldest inserted id is evicted. This matches the Rust
     * {@code DedupWindow::first_seen} exactly: an evicted id becomes "fresh"
     * again if it is seen later.</p>
     *
     * @param id32 a 32-byte canonical id (defensively copied)
     */
    public boolean firstSeen(byte[] id32) {
        checkId(id32);
        // Copy the array so the caller cannot change a stored key later.
        ByteBuffer key = ByteBuffer.wrap(id32.clone()).asReadOnlyBuffer();
        if (dedup.contains(key)) {
            return false; // the id is already present
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

    /** Insert a new key into the dedup window. Evict the oldest entry if the window is full. */
    private void insertFresh(ByteBuffer key) {
        dedup.add(key);
        if (dedup.size() > dedupCapacity) {
            Iterator<ByteBuffer> it = dedup.iterator();
            it.next(); // the oldest inserted id (front of the FIFO)
            it.remove();
        }
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
     * Process a record without the contiguity guard (no sender identity).
     * This is the pre-guard contract, kept for deposit-only callers and
     * existing tests. It is equivalent to
     * {@link #onRecord(byte[], byte[], long, byte[])} with the all-zero
     * sender.
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
        if (!firstSeen(canonicalId32)) {
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
     * lastL2Timestamp(8) | lastBoundaryCount(8).</p>
     *
     * <p>The version-3 origin trio is added after the version-2 sender map,
     * so version-1 and version-2 parsing stays byte-identical and older
     * snapshots keep loading.</p>
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
        // Version-3 fields.
        buf.putLong(l1Origin);
        buf.putLong(lastL2Timestamp);
        buf.putLong(lastBoundaryCount);
        return buf.array();
    }

    /**
     * Restore a state that {@link #takeSnapshot()} produced earlier. Later
     * behavior matches the snapshotted instance exactly: the dedup window
     * rebuilds in the same FIFO order, and {@code canonicalCount} and
     * {@code blockNumber} resume from the same values.
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
            // Insert directly in FIFO order, without eviction or counting.
            // The snapshot already reflects a window within capacity.
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
        // A version-1 snapshot (before the guard existed) restores an empty
        // guard map, so every sender re-seeds on its next record. This is
        // trust-on-first-sight, and it causes no false rejects. A version-1
        // or version-2 snapshot (before origin tracking) restores origin 0,
        // which is exactly the state that chain was in.
        state.canonicalCount = canonicalCount;
        return state;
    }
}
