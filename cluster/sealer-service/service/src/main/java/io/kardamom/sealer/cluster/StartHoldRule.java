package io.kardamom.sealer.cluster;

import java.util.List;

/**
 * When a member is stable after its start, and its {@link StartHold} ends.
 *
 * <p>A member is stable when it holds a settled role with a closed election,
 * its service restored or applied the current consensus term, and every
 * peer reports the same term with a closed election. The applied term
 * comes from snapshot recovery metadata or a committed term entry. It is
 * reported only while it matches the current consensus term. A peer that
 * does not answer cannot agree. After a full
 * restart a dead member must not block the chain, so after the stall limit a
 * majority that agrees is enough. A member with no known peers is stable
 * once its own election closes.</p>
 *
 * <p>A partial restart needs no wait for a dead member. A majority that
 * agrees is enough at once when both of these are true:</p>
 * <ul>
 *   <li>A peer in the majority is out of its own start hold. That peer did
 *       not restart, or it already found the members stable, so this is not
 *       a full restart.</li>
 *   <li>Every peer that answered in any round since the member started
 *       agrees now. A member that votes in two terms gives a quorum to two
 *       leaders, and the leader of the higher term is a live member with an
 *       open election until it cuts the lower term back. Such a leader
 *       answers its status, so it holds the member. A peer that never
 *       answered was down for the whole hold, and a member that is down
 *       cannot lead a term.</li>
 * </ul>
 * <p>The first condition alone is not enough: two members that restart
 * together while the third runs can still give the running member two
 * votes.</p>
 *
 * <p>A peer of an older release states no term. It agrees when its election
 * is closed, so a rolling deploy never holds the first members of the new
 * release: the older release has no hold, and the full rule applies once
 * every member runs this one. It states no start hold either, so it never
 * opens the release of a partial restart.</p>
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
        MAJORITY,
        /** A majority with a peer out of its hold agrees, and every peer that ever answered agrees. */
        PARTIAL_RESTART
    }

    /**
     * The member's own view.
     *
     * @param settled whether it holds a settled role with a closed election
     * @param term the current consensus term restored or applied by the
     *     service, or -1 before it is known
     */
    record Own(boolean settled, long term) {
    }

    /**
     * One round of the hold.
     *
     * @param answers the status of each peer that answered in this round
     * @param heard how many peers answered in any round since the member
     *     started, this round included
     * @param heldMs how long the member has held its outputs
     */
    record Round(List<PeerProbe.PeerStatus> answers, int heard, long heldMs) {
    }

    /** Judge one round against the member's own view. */
    Verdict judge(final Own own, final Round round) {
        if (!own.settled() || own.term() < 0) {
            return Verdict.HOLD;
        }
        final List<PeerProbe.PeerStatus> agreeing = round.answers().stream()
            .filter(peer -> peer.electionClosed() && agrees(peer.leadershipTermId(), own.term()))
            .toList();
        if (agreeing.size() == peers) {
            return Verdict.ALL;
        }
        if ((agreeing.size() + 1) * 2 <= peers + 1) {
            return Verdict.HOLD;
        }
        if (round.heldMs() >= stallLimitMs) {
            return Verdict.MAJORITY;
        }
        final boolean partial = agreeing.size() == round.heard()
            && agreeing.stream().anyMatch(peer -> !peer.startHold());
        return partial ? Verdict.PARTIAL_RESTART : Verdict.HOLD;
    }

    /** Whether a peer's term agrees with the member's: the same term, or none stated by an older release. */
    private static boolean agrees(final long peerTerm, final long ownTerm) {
        return peerTerm == ownTerm || peerTerm == PeerProbe.PeerStatus.TERM_NOT_STATED;
    }
}
