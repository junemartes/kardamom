package io.kardamom.sealer;

/**
 * A block boundary stamped at a tick. Immutable value class.
 *
 * <p>Port of Rust's {@code BlockBoundaryStart} as produced by the sealer's
 * boundary emitter (see {@code crates/sealer/src/emitter.rs}):</p>
 * <ul>
 *   <li>{@code blockNumber} — the block this boundary opens.</li>
 *   <li>{@code endTxIdx} — the cumulative count of canonical records published
 *       BEFORE this tick (mirrors {@code end_tx_idx = BPosition::from_index(canonical_count)}).</li>
 *   <li>{@code l2Timestamp} — leader clock floored to the 250 ms tick interval.</li>
 * </ul>
 */
public final class Boundary {
    public final long blockNumber;
    public final long endTxIdx;
    public final long l2Timestamp;

    public Boundary(long blockNumber, long endTxIdx, long l2Timestamp) {
        this.blockNumber = blockNumber;
        this.endTxIdx = endTxIdx;
        this.l2Timestamp = l2Timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Boundary)) {
            return false;
        }
        Boundary other = (Boundary) o;
        return blockNumber == other.blockNumber
                && endTxIdx == other.endTxIdx
                && l2Timestamp == other.l2Timestamp;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(blockNumber);
        result = 31 * result + Long.hashCode(endTxIdx);
        result = 31 * result + Long.hashCode(l2Timestamp);
        return result;
    }

    @Override
    public String toString() {
        return "Boundary{blockNumber=" + blockNumber
                + ", endTxIdx=" + endTxIdx
                + ", l2Timestamp=" + l2Timestamp + '}';
    }
}
