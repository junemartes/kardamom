package io.kardamom.sealer.cluster;

import java.util.List;

/**
 * When a member is stable after its start, and its {@link StartHold} ends.
 *
 * <p>A member is stable when it holds a settled role with a closed election,
 * its service applied the start of a leadership term, and every peer reports
 * the same term with a closed election. A service applies the start of a
 * term only when the leader committed it, so the leader committed an entry
 * in that term. A peer that does not answer cannot agree. After a full
 * restart a dead member must not block the chain, so after the stall limit a
 * majority that agrees is enough. A member with no known peers is stable
 * once its own election closes.</p>
 *
 * <p>A peer of an older release states no term. It agrees when its election
 * is closed, so a rolling deploy never holds the first members of the new
 * release: the older release has no hold, and the full rule applies once
 * every member runs this one.</p>
 *
 * @param peers how many peers the probe asks
 * @param stallLimitMs how long the member waits for every peer before a
 *     majority is enough
 */
record StartHoldRule(int peers, long stallLimitMs) {

    /** The answer of one round. */
    enum Verdict {
        /** Not stable yet. */
        HOLD,
        /** The member and every peer agree. */
        ALL,
        /** After the stall limit, a majority agrees. */
        MAJORITY
    }

    /**
     * The member's own view.
     *
     * @param settled whether it holds a settled role with a closed election
     * @param term the leadership term whose start its service applied last,
     *     or -1 before any
     */
    record Own(boolean settled, long term) {
    }

    /** Judge one round: the member's own view, the answers of its peers, and how long it holds. */
    Verdict judge(final Own own, final List<PeerProbe.PeerStatus> answers, final long heldMs) {
        if (!own.settled() || own.term() < 0) {
            return Verdict.HOLD;
        }
        final long agreeing = answers.stream()
            .filter(peer -> peer.electionClosed() && agrees(peer.leadershipTermId(), own.term()))
            .count();
        if (agreeing == peers) {
            return Verdict.ALL;
        }
        final boolean majority = (agreeing + 1) * 2 > peers + 1;
        return heldMs >= stallLimitMs && majority ? Verdict.MAJORITY : Verdict.HOLD;
    }

    /** Whether a peer's term agrees with the member's: the same term, or none stated by an older release. */
    private static boolean agrees(final long peerTerm, final long ownTerm) {
        return peerTerm == ownTerm || peerTerm == PeerProbe.PeerStatus.TERM_NOT_STATED;
    }
}
