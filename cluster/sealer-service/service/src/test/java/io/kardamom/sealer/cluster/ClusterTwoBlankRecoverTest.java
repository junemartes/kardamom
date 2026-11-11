package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.cluster.ConsensusModule;
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
 * The cluster-two-blank-recover case on three member processes. After the
 * leader purged its log, every member stops. Two members lose all their
 * Raft files; the survivor's files go to one of them as a checked offline
 * copy. All three start again: the copied member resumes from the copy,
 * the two with data elect a leader, and the last blank member seeds from
 * that quorum. Both recovered members reach the head of the stop, and the
 * cluster commits again.
 */
@ExtendWith(InterruptingTestCallback.class)
class ClusterTwoBlankRecoverTest {

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    Path dir;

    @Test
    // The step bounds fail fast; their sum is above 120 s, so the limit is 120 s.
    @InterruptAfter(value = 120, unit = TimeUnit.SECONDS)
    void an_offline_copy_of_the_survivor_restores_a_quorum_that_seeds_the_last_blank_member() {
        try (LocalSealers group = new LocalSealers(dir, 3, Purged.SETTINGS);
             CanonicalClient client = new CanonicalClient(dir.resolve("client"), group)) {
            group.startAll(List.of("-Dkardamom.cluster.bootstrap=true"));
            final int leader = group.awaitLeader(Duration.ofSeconds(20)).memberId();
            Stage.log("two-blank: leader");
            Purged.fillAndAwaitPurge(group, client, leader);
            Stage.log("two-blank: purged");
            final long head = client.head();

            final SealerNode survivor = group.node(leader);
            final SealerNode copied = group.node((leader + 1) % 3);
            final SealerNode blank = group.node((leader + 2) % 3);
            group.stopAll();
            Stage.log("two-blank: stopped");
            blank.wipe();
            copied.copyFrom(survivor);
            final int copiedStart = copied.starts();
            final int blankStart = blank.starts();
            group.startAll(List.of());
            Stage.log("two-blank: restarted");

            // The two members with data canvass for the startup canvass timeout
            // before a quorum may elect without the blank member. The clock
            // passes that timeout once both canvass.
            Await.holds(Duration.ofSeconds(20), "both members with data canvass", () -> List.of(survivor, copied)
                .stream().allMatch(n -> n.view().filter(v -> "CANVASS".equals(v.election())).isPresent()));
            group.advanceClock(Duration.ofNanos(ConsensusModule.Configuration.startupCanvassTimeoutNs()));
            Stage.log("two-blank: clock advanced");
            group.awaitLeader(Duration.ofSeconds(20));
            Stage.log("two-blank: elected");
            assertEquals(1, group.count(copied.id, copiedStart, "cluster START mode=RESUME memberId=" + copied.id + " "),
                "the copied member did not resume from the copy");
            Purged.awaitCaughtUp(group, blank, blankStart, head);
            Stage.log("two-blank: blank caught up");
            assertTrue(group.lastSnapshotBlock(copied.id, copiedStart) >= head,
                "the copied member did not reach the head of the stop");
            group.awaitJoined(List.of(0, 1, 2), Duration.ofSeconds(10));
            group.assertOneLeaderPerTerm();
            client.transfer(Purged.RECORDS, Duration.ofSeconds(20));
            MemberOrder.assertSame(group.nodes(), 0, CanonicalSealerState.GENESIS_BLOCK_NUMBER, Set.of(Purged.RECORDS));
            Stage.log("two-blank: committed");
        }
    }
}
