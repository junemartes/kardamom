package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.kardamom.sealer.CanonicalSealerState;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * The cluster-member-rejoin case on three member processes. The leader
 * first purges its Raft log, so no member can replay from position 0. Then
 * a follower is killed and loses its cluster and archive directories. The
 * quorum keeps committing. The blank member seeds from a peer's latest
 * snapshot, and its first snapshot after the start reaches the head that
 * the cluster had at the wipe.
 */
@ExtendWith(InterruptingTestCallback.class)
class ClusterMemberRejoinTest {

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    Path dir;

    @Test
    @InterruptAfter(value = 60, unit = TimeUnit.SECONDS)
    void a_wiped_follower_seeds_from_a_peer_after_the_leader_purged_its_log() {
        try (LocalSealers group = new LocalSealers(dir, 3, Purged.SETTINGS);
             CanonicalClient client = new CanonicalClient(dir.resolve("client"), group::ingressEndpoints)) {
            group.startAll(List.of("-Dkardamom.cluster.bootstrap=true"));
            final int leader = group.awaitLeader(Duration.ofSeconds(20)).memberId();
            Purged.fillAndAwaitPurge(group, client, leader);

            final SealerNode follower = group.node((leader + 1) % 3);
            final long head = client.head();
            follower.kill();
            follower.wipe();
            client.transfer(Purged.RECORDS, Duration.ofSeconds(60));

            final int start = follower.starts();
            follower.start(List.of());
            Purged.awaitCaughtUp(group, follower, start, head);
            group.awaitJoined(List.of(0, 1, 2), Duration.ofSeconds(10));
            group.assertOneLeaderPerTerm();
            assertTrue(group.count(follower.id, start, "sealer state FRESH at genesis") == 0,
                "the blank member started at genesis instead of a peer seed");
            client.transfer(Purged.RECORDS + 1, Duration.ofSeconds(20));
            MemberOrder.assertSame(group.nodes(), 0, CanonicalSealerState.GENESIS_BLOCK_NUMBER,
                Set.of(Purged.RECORDS, Purged.RECORDS + 1));
        }
    }
}
