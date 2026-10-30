package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * A member that led, stepped down with no restart, and wins a later term
 * sends each kept session a new-leader event and no session OK. A client
 * must therefore treat a new-leader event for a term with no OK as a new
 * start of its session: the sealer client of the Rust services requests
 * a replay on it.
 *
 * <p>The leader loses its quorum by a one-way cut of its frames to its
 * peers, so it stays up in the same process with its sessions. The cut
 * moves to each new leader until a member that led before wins again:
 * with three members, the third cut at the latest.</p>
 */
@ExtendWith(InterruptingTestCallback.class)
class RaftLeaderReturnTest {

    private static final Duration LEADER_BOUND = Duration.ofSeconds(45);
    private static final Duration TRANSFER_BOUND = Duration.ofSeconds(60);
    private static final Duration JOIN_BOUND = Duration.ofSeconds(20);
    private static final int CUTS = 3;

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    Path dir;

    @Test
    // Each cut takes up to a leader bound and a transfer bound.
    @InterruptAfter(value = 300, unit = TimeUnit.SECONDS)
    void a_former_leader_that_wins_again_sends_a_new_leader_event_and_no_session_ok() {
        try (LocalSealers group = new LocalSealers(dir, 3, LocalSealers.Settings.DEFAULT);
             CanonicalClient client = new CanonicalClient(dir.resolve("client"), group::ingressEndpoints, group.hostsFile())) {
            group.startAll(List.of("-Dkardamom.cluster.bootstrap=true"));
            SealerNode.View leader = group.awaitLeader(Duration.ofSeconds(20));
            client.transfer(0, Duration.ofSeconds(10));
            final int sessions = client.reconnects();
            final Map<Integer, Long> ledByPid = new HashMap<>();
            ledByPid.put(leader.memberId(), group.node(leader.memberId()).pid());
            int record = 1;
            for (int cut = 0; cut < CUTS; cut++) {
                group.links(Map.of(leader.memberId(), "cut {src}"));
                final SealerNode.View old = leader;
                client.holdUntil(LEADER_BOUND, "a new leader in a term above " + old.term(), () -> group.leader()
                    .filter(v -> v.memberId() != old.memberId() && v.term() > old.term()).isPresent());
                final SealerNode.View next = group.awaitNewLeader(old.memberId(), old.term(), LEADER_BOUND);
                group.links(Map.of());
                client.holdUntil(JOIN_BOUND, "every member joined", () -> group.views().stream()
                    .filter(v -> v.isLeader() || v.isFollower()).count() == 3);
                // The 1.44 client closes its ingress publication when the
                // egress image of the old leader closes, and opens it again
                // on the event of the new leader. An offer before that event
                // finds the publication closed, so wait for the event first.
                client.holdUntil(JOIN_BOUND, "an event of term " + next.term(), () ->
                    client.egress().newLeaderTerms.contains(next.term())
                        || client.egress().okTerms.contains(next.term()));
                client.transfer(record++, TRANSFER_BOUND);
                System.out.printf("return: member %d leads term %d; oks=%s newLeaders=%s relayed=%s%n",
                    next.memberId(), next.term(), client.egress().okTerms, client.egress().newLeaderTerms,
                    client.egress().relayedIndexes);
                final Long pid = ledByPid.putIfAbsent(next.memberId(), group.node(next.memberId()).pid());
                leader = next;
                if (pid != null) {
                    assertEquals(pid, group.node(next.memberId()).pid(), "the former leader restarted");
                    break;
                }
            }
            assertTrue(ledByPid.size() < CUTS + 1, "no former leader won again in " + CUTS + " cuts");
            assertEquals(sessions, client.reconnects(), "the client opened a new session");
            assertTrue(client.egress().newLeaderTerms.contains(leader.term()),
                "no new-leader event for term " + leader.term());
            assertFalse(client.egress().okTerms.contains(leader.term()),
                "a session OK for term " + leader.term() + " of the former leader");
            group.assertOneLeaderPerTerm();
        }
    }
}
