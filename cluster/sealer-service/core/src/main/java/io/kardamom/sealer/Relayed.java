package io.kardamom.sealer;

import java.util.Arrays;

/**
 * A first-seen application record that the sealer relays onto the canonical
 * stream. Immutable value class.
 *
 * <p>Port of the Rust republish loop's per-survivor output (see
 * {@code crates/sealer/src/bin/kardamom-sealer.rs}): each deduped record gets a
 * 0-based canonical {@code index} (the cumulative count of records republished
 * BEFORE it) and the opaque {@code payload} is relayed verbatim — never parsed.</p>
 */
public final class Relayed {
    /**
     * 0-based canonical index assigned to this record: the value of
     * {@code canonicalCount} immediately before it was counted. After N
     * first-seen records, indices run 0..N-1 and {@code canonicalCount == N}.
     */
    public final long index;

    /** Opaque application payload, relayed byte-for-byte. Never inspected. */
    public final byte[] payload;

    public Relayed(long index, byte[] payload) {
        this.index = index;
        this.payload = payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Relayed)) {
            return false;
        }
        Relayed other = (Relayed) o;
        return index == other.index && Arrays.equals(payload, other.payload);
    }

    @Override
    public int hashCode() {
        return 31 * Long.hashCode(index) + Arrays.hashCode(payload);
    }

    @Override
    public String toString() {
        return "Relayed{index=" + index + ", payloadLen=" + (payload == null ? 0 : payload.length) + '}';
    }
}
