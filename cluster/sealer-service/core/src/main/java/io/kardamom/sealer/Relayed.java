package io.kardamom.sealer;

import java.util.Arrays;

/**
 * A first-seen application record that the sealer relays onto the
 * canonical stream. This is an immutable value class.
 *
 * <p>This class is a port of the Rust republish loop's per-survivor output
 * (see {@code crates/sealer/src/bin/kardamom-sealer.rs}). Each deduped
 * record gets a 0-based canonical {@code index} — the cumulative count of
 * records republished before it — and the opaque {@code payload} is
 * relayed as-is, never parsed.</p>
 */
public final class Relayed {
    /**
     * 0-based canonical index assigned to this record: the value of
     * {@code canonicalCount} right before this record was counted. After N
     * first-seen records, indices run from 0 to N-1, and
     * {@code canonicalCount == N}.
     */
    public final long index;

    /** Opaque application payload. It is relayed byte-for-byte and never inspected. */
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
