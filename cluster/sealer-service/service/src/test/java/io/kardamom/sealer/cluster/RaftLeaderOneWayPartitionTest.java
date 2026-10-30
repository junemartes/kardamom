package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.kardamom.sealer.CanonicalSealerState;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * The raft-leader-one-way-partition case on three member processes. The
 * leader stays live, but every frame it sends to its peers is lost; its
 * clients still reach it. The peers must elect a new leader in a higher
 * term within the leader bound, clients must commit through the fault,
 * and the old leader must stay up without a restart. After the cut heals,
 * the old leader joins, each term has one leader, and every member holds
 * the same canonical order.
 */
@ExtendWith(InterruptingTestCallback.class)
class RaftLeaderOneWayPartitionTest {

    /** The leader bound of the chaos suite. */
    private static final Duration LEADER_BOUND = Duration.ofSeconds(45);
    private static final Duration TRANSFER_BOUND = Duration.ofSeconds(60);
    private static final long HOLD_MS = 10_000;

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    Path dir;

    @Test
    // The step bounds fail fast; their sum is above 120 s, so the limit is 120 s.
    @InterruptAfter(value = 120, unit = TimeUnit.SECONDS)
    void the_peers_of_a_leader_that_cannot_send_elect_a_new_leader_and_the_old_one_stays_up() {
        try (LocalSealers group = new LocalSealers(dir, 3, LocalSealers.Settings.DEFAULT);
             CanonicalClient client = new CanonicalClient(dir.resolve("client"), group)) {
            group.startAll(List.of("-Dkardamom.cluster.bootstrap=true"));
            final SealerNode.View before = group.awaitLeader(Duration.ofSeconds(20));
            IntStream.range(0, 3).forEach(n -> client.transfer(n, Duration.ofSeconds(10)));
            final SealerNode old = group.node(before.memberId());
            final long pid = old.pid();

            group.links(Map.of(old.id, "cut {src}"));
            final long faultStart = System.nanoTime();
            final SealerNode.View after = group.awaitNewLeader(old.id, before.term(), LEADER_BOUND);
            System.out.printf("partition: member %d leads term %d after %d ms%n", after.memberId(), after.term(),
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - faultStart));
            assertLive(old, pid);
            client.transfer(3, TRANSFER_BOUND);
            Await.pause(HOLD_MS);
            client.transfer(4, TRANSFER_BOUND);
            assertLive(old, pid);
            peers(group, old).forEach(p -> assertTrue(group.counts(p, old.id).dropped() > 0,
                "member " + p + " dropped no frame of the cut leader"));
            group.assertOneLeaderPerTerm();

            group.links(Map.of());
            group.awaitJoined(List.of(0, 1, 2), Duration.ofSeconds(20));
            assertEquals(pid, old.pid(), "the old leader restarted");
            client.transfer(5, TRANSFER_BOUND);
            group.assertOneLeaderPerTerm();
            MemberOrder.assertSame(group.nodes(), 0, CanonicalSealerState.GENESIS_BLOCK_NUMBER,
                IntStream.range(0, 6).boxed().collect(Collectors.toSet()));
        }
    }

    /** The old leader runs in the same process, and its admin endpoint answers. */
    private static void assertLive(final SealerNode member, final long pid) {
        assertTrue(member.isAlive() && member.pid() == pid, "the selected member restarted during the fault");
        assertTrue(member.view().isPresent(), "the admin endpoint of the selected member does not answer");
    }

    private static List<Integer> peers(final LocalSealers group, final SealerNode member) {
        return group.nodes().stream().map(n -> n.id).filter(id -> id != member.id).toList();
    }
}
