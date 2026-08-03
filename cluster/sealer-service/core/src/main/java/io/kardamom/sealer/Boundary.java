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
 *   <li>{@code l2Timestamp} — leader clock floored to the 250 ms tick interval,
 *       forced strictly greater than the previous boundary's.</li>
 *   <li>{@code l1Origin} — the L1 block number whose epoch this block belongs
 *       to. The sealer never reads L1: it echoes the value carried by the last
 *       ordered origin-advancing record (see
 *       {@code docs/agents/l1-origin-deposit-derivation-spec.md}). {@code 0}
 *       until the first such record is ordered.</li>
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
