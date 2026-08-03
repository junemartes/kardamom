package io.kardamom.sealer;

import java.util.Optional;

/**
 * Outcome of an origin-advancing record
 * ({@link CanonicalSealerState#onOriginRecord}): the boundary that had to be
 * forced to close the outgoing epoch's block (absent when the open block was
 * still empty), plus the record to relay.
 *
 * <p>The order matters and is the whole point: the caller MUST offer
 * {@link #forcedBoundary} before {@link #relayed}, or the epoch's deposits
 * land at the tail of the previous block instead of leading the new one. See
 * {@code docs/agents/l1-origin-deposit-derivation-spec.md}.</p>
 */
public final class OriginAdvance {
    private final Boundary forcedBoundary;
    private final Relayed relayed;

    public OriginAdvance(Boundary forcedBoundary, Relayed relayed) {
        this.forcedBoundary = forcedBoundary;
        this.relayed = relayed;
    }

    /** Boundary closing the previous epoch's block; empty if it was empty. */
    public Optional<Boundary> forcedBoundary() {
        return Optional.ofNullable(forcedBoundary);
    }

    /** The record to relay, leading the newly opened block. */
    public Relayed relayed() {
        return relayed;
    }
}
