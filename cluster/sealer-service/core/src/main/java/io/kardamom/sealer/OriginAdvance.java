package io.kardamom.sealer;

import java.util.Optional;

/**
 * Outcome of an origin-advancing record ({@link CanonicalSealerState#onOriginRecord}).
 * It holds the boundary that closes the outgoing epoch's block (empty if the
 * open block was still empty), plus the record to relay.
 *
 * <p>Order matters: the caller must offer {@link #forcedBoundary} before
 * {@link #relayed}. Otherwise the epoch's deposits land at the tail of the
 * previous block, not at the head of the new one. See
 * {@code docs/agents/l1-origin-deposit-derivation-spec.md}.</p>
 */
public final class OriginAdvance {
    private final Boundary forcedBoundary;
    private final Relayed relayed;

    public OriginAdvance(Boundary forcedBoundary, Relayed relayed) {
        this.forcedBoundary = forcedBoundary;
        this.relayed = relayed;
    }

    /** The boundary that closes the previous epoch's block. Empty if that block was empty. */
    public Optional<Boundary> forcedBoundary() {
        return Optional.ofNullable(forcedBoundary);
    }

    /** The record to relay. It leads the newly opened block. */
    public Relayed relayed() {
        return relayed;
    }
}
