package io.kardamom.sealer.cluster;

import io.aeron.cluster.ElectionState;
import io.aeron.cluster.service.Cluster;
import java.util.HashSet;
import java.util.Set;
import java.util.function.LongSupplier;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * Ends the {@link StartHold} of a member once {@link StartHoldRule} finds it
 * stable. The thread asks the peers once each second through their admin
 * status. It ends after the release, at a stop request, or when it is
 * interrupted.
 */
final class StartHoldThread {

    private static final Interval POLL = new Interval(1_000);

    /**
     * What the thread reads: the role and election counters of the member,
     * the leadership term of its service, the probe of its peers, and its
     * stop request.
     */
    record Member(
            int memberId,
            AtomicCounter role,
            AtomicCounter election,
            LongSupplier term,
            PeerProbe peers,
            StopRequest stop) {

        /** Whether the member holds a settled role with a closed election. A closed counter is not settled. */
        boolean settled() {
            if (role.isClosed() || election.isClosed()) {
                return false;
            }
            final Cluster.Role r = Cluster.Role.get(role);
            return (r == Cluster.Role.LEADER || r == Cluster.Role.FOLLOWER)
                && ElectionState.get(election) == ElectionState.CLOSED;
        }
    }

    private final Member member;
    private final StartHold hold;
    private final long stallLimitMs;
    private final long startMs = System.currentTimeMillis();
    /** The ids of the peers that answered in any round; only the hold thread uses it. */
    private final Set<Integer> heard = new HashSet<>();

    StartHoldThread(final Member member, final StartHold hold, final long stallLimitMs) {
        this.member = member;
        this.hold = hold;
        this.stallLimitMs = stallLimitMs;
    }

    void start() {
        final Thread thread = new Thread(this::runUntilReleased, "kardamom-start-hold");
        thread.setDaemon(true);
        thread.start();
        System.out.println("cluster START-HOLD up memberId=" + member.memberId()
            + " stallLimitMs=" + stallLimitMs);
    }

    private void runUntilReleased() {
        while (hold.held() && !member.stop().isRequested() && POLL.await()) {
            step();
        }
    }

    /** One round: judge the member's own view and the answers of its peers, and end the hold when it is stable. */
    private void step() {
        final StartHoldRule.Own own = new StartHoldRule.Own(member.settled(), member.term().getAsLong());
        final PeerProbe.Round round = member.peers().ask();
        final long heldMs = System.currentTimeMillis() - startMs;
        round.answers().forEach(peer -> heard.add(peer.memberId()));
        final StartHoldRule.Verdict verdict = new StartHoldRule(round.peers(), stallLimitMs)
            .judge(own, new StartHoldRule.Round(round.answers(), heard.size(), heldMs));
        if (verdict == StartHoldRule.Verdict.HOLD) {
            return;
        }
        hold.release();
        System.out.println("cluster START-HOLD released memberId=" + member.memberId()
            + " term=" + own.term() + " verdict=" + verdict + " heldMs=" + heldMs
            + " answers=" + round.answers().size() + "/" + round.peers() + " heard=" + heard.size());
    }
}
