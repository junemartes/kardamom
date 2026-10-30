package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.kardamom.sealer.CanonicalSealerState;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * The node-replace-sealer case on three member processes. A follower's node
 * is replaced: the member process dies, and the member comes back on a new
 * address with empty volumes and a new shared driver, under the same host
 * name. The peers find it by its name, the quorum commits meanwhile, and
 * the blank member seeds from a peer and replays to the head of the
 * replacement.
 */
@ExtendWith(InterruptingTestCallback.class)
class NodeReplaceSealerTest {

    private static final LocalSealers.Settings SNAPSHOTS = LocalSealers.Settings.SNAPSHOTS;

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    Path dir;

    @Test
    @InterruptAfter(value = 60, unit = TimeUnit.SECONDS)
    void a_follower_on_a_new_node_seeds_from_a_peer_and_catches_up() {
        try (LocalSealers group = new LocalSealers(dir, 3, SNAPSHOTS);
             CanonicalClient client = new CanonicalClient(dir.resolve("client"), group::ingressEndpoints)) {
            group.startAll(List.of("-Dkardamom.cluster.bootstrap=true"));
            final int leader = group.awaitLeader(Duration.ofSeconds(20)).memberId();
            IntStream.range(0, 5).forEach(n -> client.transfer(n, Duration.ofSeconds(10)));
            Await.holds(Duration.ofSeconds(20), "a cluster snapshot",
                () -> group.count(leader, 0, "snapshot TAKEN") > 0);

            final SealerNode follower = group.node((leader + 1) % 3);
            final String oldIp = follower.ip();
            final long head = client.head();
            follower.kill();
            follower.replace(group.address(20));
            group.writeHosts();
            assertNotEquals(oldIp, follower.ip());
            client.transfer(5, Duration.ofSeconds(60));

            final int start = follower.starts();
            follower.start(List.of());
            Purged.awaitCaughtUp(group, follower, start, head);
            group.awaitJoined(List.of(0, 1, 2), Duration.ofSeconds(10));
            group.assertOneLeaderPerTerm();
            client.transfer(6, Duration.ofSeconds(20));
            MemberOrder.assertSame(group.nodes(), 0, CanonicalSealerState.GENESIS_BLOCK_NUMBER,
                IntStream.range(0, 7).boxed().collect(Collectors.toSet()));
        }
    }
}
