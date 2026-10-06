package io.kardamom.sealer;

import java.util.Optional;

/**
 * Outcome of an origin-advancing record ({@link CanonicalSealerState#onOriginRecord}).
 * It is exactly one of:
 * <ul>
 *   <li>dropped duplicate: {@link #advance} is empty, and {@link #gap} is
 *       false;</li>
 *   <li>relayed: {@link #advance} holds the forced boundary (if any) and the
 *       record to relay;</li>
 *   <li>origin gap: {@link #gap} is true, and {@link #expectedOrigin} is the
 *       only L1 origin the state accepts next. The record is not ordered, and
 *       its id does not enter the dedup window, so the same record is
 *       accepted after the missing epochs.</li>
 * </ul>
 */
public final class OriginOutcome {
    public final Optional<OriginAdvance> advance;
    public final boolean gap;
    public final long expectedOrigin;

    private OriginOutcome(Optional<OriginAdvance> advance, boolean gap, long expectedOrigin) {
        this.advance = advance;
        this.gap = gap;
        this.expectedOrigin = expectedOrigin;
    }

    static OriginOutcome duplicate() {
        return new OriginOutcome(Optional.empty(), false, 0L);
    }

    static OriginOutcome relayed(OriginAdvance advance) {
        return new OriginOutcome(Optional.of(advance), false, 0L);
    }

    static OriginOutcome gap(long expectedOrigin) {
        return new OriginOutcome(Optional.empty(), true, expectedOrigin);
    }
}
