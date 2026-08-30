package io.kardamom.sealer;

/**
 * A block boundary stamped at a tick. This is an immutable value class.
 *
 * <p>This class is a port of Rust's {@code BlockBoundaryStart}. The sealer's
 * boundary emitter produces it (see {@code crates/sealer/src/emitter.rs}).</p>
 * <ul>
 *   <li>{@code blockNumber} — the block that this boundary opens.</li>
 *   <li>{@code endTxIdx} — the cumulative count of canonical records published
 *       before this tick (mirrors {@code end_tx_idx = BPosition::from_index(canonical_count)}).</li>
 *   <li>{@code l2Timestamp} — the leader clock, floored to the 250 ms tick
 *       interval and forced strictly greater than the previous boundary's.</li>
 *   <li>{@code l1Origin} — the L1 block number for this block's epoch. The
 *       sealer never reads L1. It echoes the value from the last ordered
 *       origin-advancing record (see
 *       {@code docs/agents/l1-origin-deposit-derivation-spec.md}). The value
 *       is {@code 0} until the first such record is ordered.</li>
 * </ul>
 */
public final class Boundary {
    public final long blockNumber;
    public final long endTxIdx;
    public final long l2Timestamp;
    public final long l1Origin;

    public Boundary(long blockNumber, long endTxIdx, long l2Timestamp, long l1Origin) {
        this.blockNumber = blockNumber;
        this.endTxIdx = endTxIdx;
        this.l2Timestamp = l2Timestamp;
        this.l1Origin = l1Origin;
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
                && l2Timestamp == other.l2Timestamp
                && l1Origin == other.l1Origin;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(blockNumber);
        result = 31 * result + Long.hashCode(endTxIdx);
        result = 31 * result + Long.hashCode(l2Timestamp);
        result = 31 * result + Long.hashCode(l1Origin);
        return result;
    }

    @Override
    public String toString() {
        return "Boundary{blockNumber=" + blockNumber
                + ", endTxIdx=" + endTxIdx
                + ", l2Timestamp=" + l2Timestamp
                + ", l1Origin=" + l1Origin + '}';
    }
}
