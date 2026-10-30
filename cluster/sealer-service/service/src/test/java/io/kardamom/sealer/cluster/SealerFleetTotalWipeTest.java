package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * The sealer part of the sealer-fleet-total-wipe-recover case on three
 * member processes. The old chain posts its head H, then seals more
 * blocks. Every member stops and loses its cluster and archive
 * directories. The members start inside the bootstrap from a seed at
 * {@code (H, E_H)} in the production seed format: each one logs SEEDED at
 * H and none starts at genesis. After the seed snapshot they start again
 * without the seed and restore the snapshot. A consumer with a cursor of
 * the old chain past the new head gets REPLAY_AHEAD; a consumer at the
 * seed head resumes at {@code (E_H, H + 1)}, and new records take the
 * indexes from {@code E_H} on every member.
 */
@ExtendWith(InterruptingTestCallback.class)
class SealerFleetTotalWipeTest {

    private static final int SEED_MAGIC = 0x4b534544;
    private static final int SEED_VERSION = 1;
    private static final long CHAIN_ID = 412_346L;
    private static final long L1_ORIGIN = 7L;

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    Path dir;

    @Test
    @InterruptAfter(value = 60, unit = TimeUnit.SECONDS)
    void a_wiped_fleet_restarts_after_the_posted_head_from_a_seed() {
        try (LocalSealers group = new LocalSealers(dir, 3, LocalSealers.Settings.DEFAULT);
             CanonicalClient client = new CanonicalClient(dir.resolve("client"), group)) {
            group.startAll(List.of("-Dkardamom.cluster.bootstrap=true"));
            group.awaitLeader(Duration.ofSeconds(20));
            IntStream.range(0, 5).forEach(n -> client.transfer(n, Duration.ofSeconds(10)));
            final long[] posted = Await.until(Duration.ofSeconds(10), "a boundary after the first records", () -> {
                client.head();
                return client.egress().boundaries.stream().filter(b -> b[1] >= 5).findFirst();
            });
            final long h = posted[0];
            final long eh = posted[1];
            client.post(h);
            IntStream.range(5, 8).forEach(n -> client.transfer(n, Duration.ofSeconds(10)));
            final long oldEnd = Await.until(Duration.ofSeconds(10), "the old chain seals past H", () -> {
                client.head();
                return client.egress().boundaries.stream().filter(b -> b[0] > h && b[1] >= 8)
                    .map(b -> b[1]).findFirst();
            });
            Stage.log("fleet-wipe: old chain past H");

            group.stopAll();
            group.nodes().forEach(SealerNode::wipe);
            final Path seed = writeSeed(h, eh, posted[2]);
            final List<Integer> seededStarts = group.nodes().stream().map(SealerNode::starts).toList();
            group.startAll(List.of("-Dkardamom.cluster.bootstrap=true", "-Dkardamom.cluster.seedSnapshot=" + seed));
            group.awaitLeader(Duration.ofSeconds(20));
            final String atHead = "block=" + h + " endTx=" + eh + " senders=";
            group.nodes().forEach(n -> Await.holds(Duration.ofSeconds(10), "member " + n.id + " seeds at H",
                () -> group.count(n.id, seededStarts.get(n.id), "sealer state SEEDED memberId=" + n.id + " " + atHead) == 1
                    && group.count(n.id, seededStarts.get(n.id), "snapshot TAKEN memberId=" + n.id) > 0));
            group.nodes().forEach(n -> assertEquals(0,
                group.count(n.id, seededStarts.get(n.id), "sealer state FRESH at genesis"), "member " + n.id));
            Stage.log("fleet-wipe: seeded");

            group.stopAll();
            final List<Integer> restarts = group.nodes().stream().map(SealerNode::starts).toList();
            group.startAll(List.of());
            group.awaitLeader(Duration.ofSeconds(20));
            group.nodes().forEach(n -> Await.holds(Duration.ofSeconds(10), "member " + n.id + " restores the seed snapshot",
                () -> group.count(n.id, restarts.get(n.id), "sealer snapshot RESTORED memberId=" + n.id) > 0));
            group.nodes().forEach(n -> assertEquals(0, group.count(n.id, restarts.get(n.id), "sealer state SEEDED"),
                "member " + n.id + " started from the seed after the seed property was cleared"));
            Stage.log("fleet-wipe: restored");

            try (CanonicalClient stale = new CanonicalClient(dir.resolve("stale"), group);
                 CanonicalClient fresh = new CanonicalClient(dir.resolve("fresh"), group)) {
                stale.offer(IngressFrames.replayRequestFrame(oldEnd, h + 2), Duration.ofSeconds(20));
                stale.pollUntil(Duration.ofSeconds(10), "REPLAY_AHEAD for a cursor of the old chain",
                    () -> stale.egress().replayAheadCount > 0);
                assertEquals(0, stale.egress().replayDoneCount, "a cursor past the head was served");

                fresh.offer(IngressFrames.replayRequestFrame(eh, h + 1), Duration.ofSeconds(10));
                fresh.pollUntil(Duration.ofSeconds(10), "REPLAY_DONE at the seed head",
                    () -> fresh.egress().replayDoneCount > 0);
                assertEquals(0, fresh.egress().replayUnavailableCount);
                assertEquals(List.of(), fresh.egress().relayedIndexes, "a record of the old chain after H was served");
                IntStream.range(100, 103).forEach(n -> fresh.transfer(n, Duration.ofSeconds(10)));
                assertEquals(List.of(eh, eh + 1, eh + 2), fresh.egress().relayedIndexes);
                assertTrue(fresh.egress().boundaries.stream().allMatch(b -> b[0] > h && b[1] >= eh),
                    "the new chain has a boundary at or below H, or below E_H");
            }
            group.assertOneLeaderPerTerm(seededStarts);
            MemberOrder.assertSame(group.nodes(), eh, h + 1, Set.of(100, 101, 102));
            Stage.log("fleet-wipe: resumed");
        }
    }

    /** A seed at {@code (h, eh)} in the format that {@code kardamom-reconstruct --sealer-seed} writes. */
    private Path writeSeed(final long h, final long eh, final long l2Timestamp) {
        final ByteBuffer seed = ByteBuffer.allocate(4 + 4 + 5 * Long.BYTES + 32 + 4);
        seed.putInt(SEED_MAGIC).putInt(SEED_VERSION)
            .putLong(CHAIN_ID).putLong(h).putLong(eh).putLong(l2Timestamp).putLong(L1_ORIGIN)
            .put(new byte[32]).putInt(0);
        final Path file = dir.resolve("seed.bin");
        try {
            Files.write(file, seed.array());
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        return file;
    }
}
