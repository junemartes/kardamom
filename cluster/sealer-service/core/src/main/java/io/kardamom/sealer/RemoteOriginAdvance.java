package io.kardamom.sealer;

import java.util.Optional;

/**
 * Outcome of a remote-origin record
 * ({@link CanonicalSealerState#onRemoteOriginRecord}): the boundary that had to
 * be forced to close the open block (absent when it was still empty), plus the
 * record to relay.
 *
 * <p>The order matters and is the whole point, exactly as for
 * {@link OriginAdvance}: the caller MUST offer {@link #forcedBoundary} before
 * {@link #relayed}, or the batch's messages land at the tail of the previous
 * block instead of leading the new one — which would split the batch's
 * contiguous slot range across two blocks and leave its marker mid-block. See
 * {@code docs/specs/interop-outbox-messaging-spec.md} §7.</p>
 *
 * <p>Deliberately a separate type from {@link OriginAdvance} rather than a
 * shared one: the two advances move DIFFERENT state (one L1 origin that
 * boundaries carry; one per-peer position that they do not), so a caller that
 * mixes them up should not typecheck.</p>
 */
public final class RemoteOriginAdvance {
    private final Boundary forcedBoundary;
    private final Relayed relayed;

    public RemoteOriginAdvance(Boundary forcedBoundary, Relayed relayed) {
        this.forcedBoundary = forcedBoundary;
        this.relayed = relayed;
    }

    /** Boundary closing the block the batch must not join; empty if it was empty. */
    public Optional<Boundary> forcedBoundary() {
        return Optional.ofNullable(forcedBoundary);
    }

    /** The record to relay, leading the newly opened block. */
    public Relayed relayed() {
        return relayed;
    }
}
