package io.kardamom.sealer.cluster;

import java.time.Duration;
import java.util.stream.IntStream;

/**
 * The start of the blank-member cases: a cluster whose leader has purged
 * its Raft log. The records fill more than one archive segment of the log,
 * the members take a snapshot on each block, the batcher side posts the
 * sealed head, and the leader's first purge pass removes the log below a
 * snapshot. A blank member can then only start from a peer's snapshot.
 */
final class Purged {

    /** A snapshot on each block at the deployed tick; the purge keeps the log of the three newest snapshots. */
    static final LocalSealers.Settings SETTINGS = new LocalSealers.Settings(
        LocalSealers.Settings.TICK_MS, LocalSealers.Settings.TICK_MS / 1000, 3);
    /** The records that fill the log: 40 records of 256 KiB, more than the 8 MiB segment. */
    static final int RECORDS = 40;
    private static final int FILL_BYTES = 256 * 1024;
    /** The first purge pass runs 30 s after the start of a member. */
    private static final Duration PURGE_BOUND = Duration.ofSeconds(40);

    private Purged() {
    }

    /** Commit {@link #RECORDS} large records, post the head, and wait for the leader's log purge. */
    static void fillAndAwaitPurge(final LocalSealers group, final CanonicalClient client, final int leader) {
        IntStream.range(0, RECORDS).forEach(n -> client.transfer(n, FILL_BYTES, Duration.ofSeconds(10)));
        final String purged = "cluster LOG PURGED memberId=" + leader + " ";
        Await.holds(PURGE_BOUND, "the leader logs '" + purged.trim() + "'", () -> {
            client.post(client.head());
            Await.pause(500);
            return group.count(leader, 0, purged) > 0;
        });
    }

    /**
     * Wait until {@code member}, started blank at start {@code start}, logs a
     * peer seed and then a snapshot at or past {@code head}.
     */
    static void awaitCaughtUp(final LocalSealers group, final SealerNode member, final int start, final long head) {
        final String seeded = "cluster SEED from-peer memberId=" + member.id + " ";
        Await.holds(Duration.ofSeconds(30), "member " + member.id + " logs '" + seeded.trim()
            + "' and a later snapshot at block " + head + " or more", () -> {
                final String log = member.logSince(start);
                final int at = log.indexOf(seeded);
                return at >= 0 && snapshotBlock(log.substring(at), member.id) >= head;
            });
    }

    private static long snapshotBlock(final String log, final int member) {
        final java.util.regex.Matcher m = java.util.regex.Pattern
            .compile("snapshot TAKEN memberId=" + member + " block=(\\d+)").matcher(log);
        long block = -1;
        while (m.find()) {
            block = Long.parseLong(m.group(1));
        }
        return block;
    }
}
