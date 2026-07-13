package io.kardamom.sealer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;
import java.util.LinkedHashSet;
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
 *   <li><b>Canonical count</b> — {@link #onRecord(byte[], byte[])} relays each
 *       first-seen record with its 0-based index and bumps {@code canonicalCount};
 *       duplicates are dropped and never counted.</li>
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

    /** Default genesis block number. */
    public static final long GENESIS_BLOCK_NUMBER = 1L;

    private static final int SNAPSHOT_MAGIC = 0x4B53_4541; // "KSEA"
    private static final int SNAPSHOT_VERSION = 1;

    /**
     * FIFO first-seen window. Insertion-ordered, so the oldest inserted id is
     * the first element — exactly the {@code VecDeque} front the Rust
     * {@code DedupWindow} pops on eviction. Keys are 32-byte ids wrapped in a
     * read-only {@link ByteBuffer} for value-based equality.
     */
    private final LinkedHashSet<ByteBuffer> dedup;
    private final int dedupCapacity;

    /** Cumulative count of canonical (first-seen) records relayed. */
    private long canonicalCount;

    /** Block number the next {@link #onTick(long)} will stamp. */
    private long blockNumber;

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
        this.canonicalCount = 0L;
        this.blockNumber = initialBlockNumber;
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
        if (id32 == null || id32.length != CANONICAL_ID_LEN) {
            throw new IllegalArgumentException(
                    "canonical id must be " + CANONICAL_ID_LEN + " bytes, got "
                            + (id32 == null ? "null" : id32.length));
        }
        // Defensive copy so the caller's array can't mutate a stored key.
        ByteBuffer key = ByteBuffer.wrap(id32.clone()).asReadOnlyBuffer();
        if (!dedup.add(key)) {
            return false; // already present
        }
        if (dedup.size() > dedupCapacity) {
            Iterator<ByteBuffer> it = dedup.iterator();
            it.next(); // oldest inserted (FIFO front)
            it.remove();
        }
        return true;
    }

    /**
     * Process one application record. If {@code canonicalId32} is a duplicate the
     * record is dropped and nothing is counted ({@link Optional#empty()}).
     * Otherwise it is relayed: the returned {@link Relayed} carries the current
     * 0-based {@code canonicalCount} as its index, after which the count is bumped.
     *
     * <p>{@code payload} is relayed VERBATIM and is never parsed.</p>
     */
    public Optional<Relayed> onRecord(byte[] canonicalId32, byte[] payload) {
        if (!firstSeen(canonicalId32)) {
            return Optional.empty();
        }
        long index = canonicalCount;
        canonicalCount++;
        return Optional.of(new Relayed(index, payload));
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
        long l2Timestamp = (leaderClockMillis / TICK_INTERVAL_MS) * TICK_INTERVAL_MS;
        Boundary boundary = new Boundary(blockNumber, canonicalCount, l2Timestamp);
        blockNumber++;
        return boundary;
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

    /**
     * Serialise the full state: dedup ids (32 bytes each, in FIFO/insertion
     * order), {@code canonicalCount} and {@code blockNumber}. The capacity is NOT
     * encoded — it is supplied at {@link #load(byte[], int)} time (matching the
     * cluster's configured window).
     *
     * <p>Layout (big-endian): magic(4) | version(4) | canonicalCount(8) |
     * blockNumber(8) | idCount(4) | idCount * 32 bytes.</p>
     */
    public byte[] takeSnapshot() {
        int idCount = dedup.size();
        int size = 4 + 4 + 8 + 8 + 4 + idCount * CANONICAL_ID_LEN;
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
        if (version != SNAPSHOT_VERSION) {
            throw new IllegalArgumentException("unsupported snapshot version: " + version);
        }
        long canonicalCount = buf.getLong();
        long blockNumber = buf.getLong();
        int idCount = buf.getInt();

        CanonicalSealerState state = new CanonicalSealerState(dedupCapacity, blockNumber);
        for (int i = 0; i < idCount; i++) {
            byte[] raw = new byte[CANONICAL_ID_LEN];
            buf.get(raw);
            // Insert directly in FIFO order without eviction or counting; the
            // snapshot already reflects a window within capacity.
            state.dedup.add(ByteBuffer.wrap(raw).asReadOnlyBuffer());
        }
        state.canonicalCount = canonicalCount;
        return state;
    }
}
