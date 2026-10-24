package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.kardamom.sealer.cluster.StartHoldRule.Own;
import io.kardamom.sealer.cluster.StartHoldRule.Verdict;
import java.util.List;
import org.junit.jupiter.api.Test;

/** When a member is stable after its start, and its outputs go out. */
class StartHoldRuleTest {

    private static final long STALL_MS = 80_000;
    private static final StartHoldRule TWO_PEERS = new StartHoldRule(2, STALL_MS);
    private static final Own SETTLED_IN_5 = new Own(true, 5);

    private static PeerProbe.PeerStatus peer(final String role, final String election, final long term) {
        return new PeerProbe.PeerStatus(1, role, election, 100L, term);
    }

    private static PeerProbe.PeerStatus closed(final long term) {
        return peer(MemberStatus.ROLE_FOLLOWER, MemberStatus.ELECTION_CLOSED, term);
    }

    @Test
    void everyPeerInTheSameTermWithAClosedElectionEndsTheHold() {
        assertEquals(Verdict.ALL, TWO_PEERS.judge(SETTLED_IN_5, List.of(closed(5), closed(5)), 0));
    }

    @Test
    void aMemberThatIsNotSettledOrKnowsNoTermHolds() {
        final List<PeerProbe.PeerStatus> agree = List.of(closed(5), closed(5));
        assertEquals(Verdict.HOLD, TWO_PEERS.judge(new Own(false, 5), agree, STALL_MS));
        assertEquals(Verdict.HOLD, TWO_PEERS.judge(new Own(true, -1), agree, STALL_MS));
    }

    /**
     * After a full restart, a member that voted in two terms leaves the
     * members in different terms. No member is stable, so no output goes out
     * while the higher term can still cut the lower one back.
     */
    @Test
    void peersInAnotherTermOrInAnElectionHoldTheMember() {
        assertEquals(Verdict.HOLD, TWO_PEERS.judge(SETTLED_IN_5, List.of(closed(5), closed(6)), 0));
        assertEquals(Verdict.HOLD, TWO_PEERS.judge(SETTLED_IN_5,
            List.of(closed(5), peer(MemberStatus.ROLE_FOLLOWER, "CANVASS", 5)), 0));
    }

    /** A dead member must not block the chain: after the stall limit, a majority is enough. */
    @Test
    void afterTheStallLimitAMajorityIsEnough() {
        final List<PeerProbe.PeerStatus> oneAnswers = List.of(closed(5));
        assertEquals(Verdict.HOLD, TWO_PEERS.judge(SETTLED_IN_5, oneAnswers, STALL_MS - 1));
        assertEquals(Verdict.MAJORITY, TWO_PEERS.judge(SETTLED_IN_5, oneAnswers, STALL_MS));
        assertEquals(Verdict.HOLD, TWO_PEERS.judge(SETTLED_IN_5, List.of(), STALL_MS),
            "the member alone is no majority of three");
    }

    /** A rolling deploy never holds the first members of this release. */
    @Test
    void aPeerOfAnOlderReleaseAgreesWhenItsElectionIsClosed() {
        final long older = PeerProbe.PeerStatus.TERM_NOT_STATED;
        assertEquals(Verdict.ALL, TWO_PEERS.judge(SETTLED_IN_5, List.of(closed(older), closed(older)), 0));
        assertEquals(Verdict.HOLD, TWO_PEERS.judge(SETTLED_IN_5,
            List.of(closed(older), peer(MemberStatus.ROLE_FOLLOWER, "CANVASS", older)), 0));
    }

    /** A peer of this release whose service applied no term yet does not agree. */
    @Test
    void aPeerWithNoTermYetDoesNotAgree() {
        assertEquals(Verdict.HOLD, TWO_PEERS.judge(SETTLED_IN_5, List.of(closed(5), closed(-1)), 0));
    }

    @Test
    void aMemberWithNoKnownPeersIsStableOnceItsElectionCloses() {
        final StartHoldRule alone = new StartHoldRule(0, STALL_MS);
        assertEquals(Verdict.ALL, alone.judge(SETTLED_IN_5, List.of(), 0));
        assertEquals(Verdict.HOLD, alone.judge(new Own(false, 5), List.of(), 0));
    }
}
