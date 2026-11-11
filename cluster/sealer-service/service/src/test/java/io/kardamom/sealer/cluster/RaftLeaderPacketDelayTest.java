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
 * The raft-leader-packet-delay case on three member processes: every frame
 * from the leader to each peer arrives 250 ms late, in order, while its
 * clients transact. Records must commit within the client bound, the leader
 * must stay up, each term keeps one leader, and both delay queues must
 * deliver frames with no loss. After the delay ends, every member holds the
 * same canonical order.
 */
@ExtendWith(InterruptingTestCallback.class)
class RaftLeaderPacketDelayTest {

    private static final Duration TRANSFER_BOUND = Duration.ofSeconds(60);
    private static final long HOLD_MS = 15_000;

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    Path dir;

    @Test
    @InterruptAfter(value = 60, unit = TimeUnit.SECONDS)
    void a_slow_leader_link_keeps_commits_and_one_leader_per_term() {
        try (LocalSealers group = new LocalSealers(dir, 3, LocalSealers.Settings.DEFAULT);
             CanonicalClient client = new CanonicalClient(dir.resolve("client"), group)) {
            group.startAll(List.of("-Dkardamom.cluster.bootstrap=true"));
            final SealerNode leader = group.node(group.awaitLeader(Duration.ofSeconds(20)).memberId());
            IntStream.range(0, 3).forEach(n -> client.transfer(n, Duration.ofSeconds(10)));
            final long pid = leader.pid();

            group.links(Map.of(leader.id, "delay {src} 250"));
            client.transfer(3, TRANSFER_BOUND);
            final long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(HOLD_MS);
            final int sent = IntStream.iterate(4, n -> n + 1)
                .takeWhile(n -> System.nanoTime() - end < 0)
                .peek(n -> client.transfer(n, TRANSFER_BOUND))
                .reduce(0, (a, b) -> b);
            assertTrue(leader.isAlive() && leader.pid() == pid, "the selected member restarted during the fault");
            assertTrue(leader.view().isPresent(), "the admin endpoint of the selected member does not answer");
            group.assertOneLeaderPerTerm();
            group.nodes().stream().filter(n -> n.id != leader.id).forEach(p -> {
                final LocalSealers.LinkCounts counts = group.counts(p.id, leader.id);
                System.out.printf("delay: member %d saw %s from member %d%n", p.id, counts, leader.id);
                assertTrue(counts.released() > 0 && counts.dropped() == 0,
                    "the delay queue of member " + p.id + " did not deliver frames without loss: " + counts);
            });

            group.links(Map.of());
            group.awaitJoined(List.of(0, 1, 2), Duration.ofSeconds(20));
            assertEquals(pid, leader.pid(), "the selected member restarted");
            client.transfer(sent + 1, TRANSFER_BOUND);
            group.assertOneLeaderPerTerm();
            MemberOrder.assertSame(group.nodes(), 0, CanonicalSealerState.GENESIS_BLOCK_NUMBER,
                IntStream.rangeClosed(0, sent + 1).boxed().collect(Collectors.toSet()));
        }
    }
}
