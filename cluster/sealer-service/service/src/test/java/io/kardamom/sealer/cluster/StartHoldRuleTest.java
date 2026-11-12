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
        return new PeerProbe.PeerStatus(1, role, election, 100L, term, true);
    }

    private static PeerProbe.PeerStatus closed(final long term) {
        return peer(MemberStatus.ROLE_FOLLOWER, MemberStatus.ELECTION_CLOSED, term);
    }

    /** A peer in a closed election of {@code term} that is out of its own start hold: it did not restart. */
    private static PeerProbe.PeerStatus running(final long term) {
        return new PeerProbe.PeerStatus(1, MemberStatus.ROLE_FOLLOWER, MemberStatus.ELECTION_CLOSED, 100L, term, false);
    }

    /** One round where the peers that answer now are all the peers that ever answered. */
    private static Verdict judge(
            final StartHoldRule rule, final Own own, final List<PeerProbe.PeerStatus> answers, final long heldMs) {
        return rule.judge(own, new StartHoldRule.Round(answers, answers.size(), heldMs));
    }

    @Test
    void everyPeerInTheSameTermWithAClosedElectionEndsTheHold() {
        assertEquals(Verdict.ALL, judge(TWO_PEERS, SETTLED_IN_5, List.of(closed(5), closed(5)), 0));
    }

    @Test
    void aMemberThatIsNotSettledOrKnowsNoTermHolds() {
        final List<PeerProbe.PeerStatus> agree = List.of(closed(5), closed(5));
        assertEquals(Verdict.HOLD, judge(TWO_PEERS, new Own(false, 5), agree, STALL_MS));
        assertEquals(Verdict.HOLD, judge(TWO_PEERS, new Own(true, -1), agree, STALL_MS));
    }

    /**
     * After a full restart, a member that voted in two terms leaves the
     * members in different terms. No member is stable, so no output goes out
     * while the higher term can still cut the lower one back.
     */
    @Test
    void peersInAnotherTermOrInAnElectionHoldTheMember() {
        assertEquals(Verdict.HOLD, judge(TWO_PEERS, SETTLED_IN_5, List.of(closed(5), closed(6)), 0));
        assertEquals(Verdict.HOLD, judge(TWO_PEERS, SETTLED_IN_5,
            List.of(closed(5), peer(MemberStatus.ROLE_FOLLOWER, "CANVASS", 5)), 0));
    }

    /** A dead member must not block the chain: after the stall limit, a majority is enough. */
    @Test
    void afterTheStallLimitAMajorityIsEnough() {
        final List<PeerProbe.PeerStatus> oneAnswers = List.of(closed(5));
        assertEquals(Verdict.HOLD, judge(TWO_PEERS, SETTLED_IN_5, oneAnswers, STALL_MS - 1));
        assertEquals(Verdict.MAJORITY, judge(TWO_PEERS, SETTLED_IN_5, oneAnswers, STALL_MS));
        assertEquals(Verdict.HOLD, judge(TWO_PEERS, SETTLED_IN_5, List.of(), STALL_MS),
            "the member alone is no majority of three");
    }

    /**
     * Quorum loss: one member ran on, one restarted, and one stays down. The
     * restarted member needs no wait for the dead one.
     */
    @Test
    void aPartialRestartWithADeadPeerEndsTheHoldAtOnce() {
        assertEquals(Verdict.PARTIAL_RESTART, judge(TWO_PEERS, SETTLED_IN_5, List.of(running(5)), 0));
    }

    /** A full restart with a dead member: every live member restarted, so the member waits for the stall limit. */
    @Test
    void aMajorityOfRestartedMembersWaitsForTheStallLimit() {
        assertEquals(Verdict.HOLD, judge(TWO_PEERS, SETTLED_IN_5, List.of(closed(5)), STALL_MS - 1));
    }

    /**
     * Two members restart while the third runs. The running member can vote
     * for one of them in a higher term, then follow the other in a lower
     * term. The leader of the higher term answers with an open election, so
     * the member holds until the stall limit, even though the running peer
     * agrees.
     */
    @Test
    void aPeerThatAnswersAndDisagreesHoldsAPartialRestart() {
        final PeerProbe.PeerStatus higherLeader = peer(MemberStatus.ROLE_LEADER, "LEADER_REPLAY", 4);
        assertEquals(Verdict.HOLD, judge(TWO_PEERS, SETTLED_IN_5, List.of(running(5), higherLeader), 0));
        assertEquals(Verdict.HOLD,
            TWO_PEERS.judge(SETTLED_IN_5, new StartHoldRule.Round(List.of(running(5)), 2, 0)),
            "a peer that answered once and is silent now may still lead a higher term");
        assertEquals(Verdict.MAJORITY,
            TWO_PEERS.judge(SETTLED_IN_5, new StartHoldRule.Round(List.of(running(5)), 2, STALL_MS)));
    }

    /** A rolling deploy never holds the first members of this release. */
    @Test
    void aPeerOfAnOlderReleaseAgreesWhenItsElectionIsClosed() {
        final long older = PeerProbe.PeerStatus.TERM_NOT_STATED;
        assertEquals(Verdict.ALL, judge(TWO_PEERS, SETTLED_IN_5, List.of(closed(older), closed(older)), 0));
        assertEquals(Verdict.HOLD, judge(TWO_PEERS, SETTLED_IN_5,
            List.of(closed(older), peer(MemberStatus.ROLE_FOLLOWER, "CANVASS", older)), 0));
    }

    /** A peer of an older release states no start hold, so it reads as held and opens no partial release. */
    @Test
    void aPeerOfAnOlderReleaseOpensNoPartialRelease() {
        final String older = "{\"memberId\":1,\"role\":\"FOLLOWER\",\"election\":\"CLOSED\",\"commitPosition\":9}";
        final PeerProbe.PeerStatus parsed = PeerProbe.PeerStatus.parse(older).orElseThrow();
        assertEquals(Verdict.HOLD, judge(TWO_PEERS, SETTLED_IN_5, List.of(parsed), 0));
    }

    /** A peer of this release whose service applied no term yet does not agree. */
    @Test
    void aPeerWithNoTermYetDoesNotAgree() {
        assertEquals(Verdict.HOLD, judge(TWO_PEERS, SETTLED_IN_5, List.of(closed(5), closed(-1)), 0));
        assertEquals(Verdict.HOLD, judge(TWO_PEERS, SETTLED_IN_5, List.of(running(-1)), 0));
    }

    @Test
    void aMemberWithNoKnownPeersIsStableOnceItsElectionCloses() {
        final StartHoldRule alone = new StartHoldRule(0, STALL_MS);
        assertEquals(Verdict.ALL, judge(alone, SETTLED_IN_5, List.of(), 0));
        assertEquals(Verdict.HOLD, judge(alone, new Own(false, 5), List.of(), 0));
    }
}
