package io.kardamom.sealer.cluster;

import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusterBackup;
import io.aeron.cluster.ClusterBackup.Configuration.ReplayStart;
import io.aeron.cluster.ClusterBackupMediaDriver;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * Copies the latest snapshot, and the log after it, from a running peer
 * into a blank member's directories. The member then launches as usual:
 * it restores the snapshot, replays its copy of the log, and its election
 * catches up the rest from the leader.
 *
 * <p>Each round clears both directories and runs an Aeron ClusterBackup
 * with {@code LATEST_SNAPSHOT} against the consensus endpoints of the
 * other members. The backup writes into the member's own directories and
 * listens on the member's own consensus endpoint, which is free until the
 * member launches. The recording log of the backup keeps the real log
 * position of each entry, so the member joins the running cluster at the
 * snapshot position, not at 0.</p>
 *
 * <p>The round ends at {@code BACKING_UP}, not at the leader's commit
 * position. At that state the snapshots are complete and the recording log
 * is written. The log after the snapshot needs no copy: the leader keeps
 * its log from the snapshot position on, and the member's election
 * catches up from there. A longer copy only shortens that catch-up.</p>
 *
 * <p>A round in which no peer answers ends after the peer timeout, and the
 * next round starts after a backoff. The seed has no other exit: a blank
 * member never falls back to a start at log position 0.</p>
 */
final class PeerSeed {

    /** How often a round samples the backup state. */
    private static final long POLL_MS = 100;
    /** The wait of the backup after a peer refuses it, before it asks the next peer. */
    private static final long COOL_DOWN_NS = TimeUnit.SECONDS.toNanos(1);

    /** The peer timeout of a round, and the backoff between rounds. */
    record Timing(long peerTimeoutMs, long firstBackoffMs, long maxBackoffMs) {
        static final Timing DEFAULT = new Timing(20_000, 1_000, 30_000);
        private static final int MAX_DOUBLINGS = 31;

        /**
         * The wait after round {@code round}: it doubles each round up to the
         * maximum. The doubling stops after {@link #MAX_DOUBLINGS} rounds, so
         * the shift cannot overflow a first backoff below 2^32 ms.
         */
        long backoffMs(final int round) {
            return Math.min(maxBackoffMs, firstBackoffMs << Math.min(round - 1, MAX_DOUBLINGS));
        }
    }

    private final int memberId;
    /** The consensus endpoints of the other members, comma separated. */
    private final String peers;
    private final MemberContexts contexts;
    private final StateDir clusterDir;
    private final StateDir archiveDir;
    private final Timing timing;

    PeerSeed(final int memberId, final String peers, final MemberContexts contexts, final Timing timing) {
        this.memberId = memberId;
        this.peers = peers;
        this.contexts = contexts;
        this.clusterDir = contexts.clusterState();
        this.archiveDir = contexts.archiveState();
        this.timing = timing;
    }

    /**
     * Seed the member from a peer. Return the log position of the seeded
     * snapshot, or -1 when the cluster has no snapshot yet: the member
     * then holds the whole log.
     */
    long run() {
        System.out.println("cluster SEED start memberId=" + memberId + " peers=" + peers
            + " — blank member without the bootstrap flag; copying the latest snapshot from a peer");
        final int round = IntStream.iterate(1, r -> r + 1).filter(this::seeded).findFirst().orElseThrow();
        final long snapshotPosition = clusterDir.latestSnapshotPosition();
        System.out.println("cluster SEED from-peer memberId=" + memberId
            + " snapshotPosition=" + snapshotPosition + " round=" + round);
        return snapshotPosition;
    }

    /** Run one round. Return whether it seeded the member; wait out the backoff when it did not. */
    private boolean seeded(final int round) {
        clusterDir.clear();
        archiveDir.clear();
        final Round result = backUp();
        if (result.outcome() == SeedWatch.Outcome.SEEDED) {
            clusterDir.dropMarkFiles();
            archiveDir.dropMarkFiles();
            return true;
        }
        final long backoffMs = timing.backoffMs(round);
        System.out.println("cluster SEED waiting-for-peer memberId=" + memberId + " round=" + round
            + " outcome=" + result.outcome() + " backupState=" + result.lastState() + " peers=" + peers
            + " retryInMs=" + backoffMs
            + " — NOT starting at genesis: only the bootstrap of a new cluster starts a blank member there");
        pause(backoffMs);
        return false;
    }

    /** The end of one round: its outcome and the last backup state it sampled. */
    private record Round(SeedWatch.Outcome outcome, ClusterBackup.State lastState) {
    }

    private Round backUp() {
        try (ClusterBackupMediaDriver backup = ClusterBackupMediaDriver.launch(
                contexts.driver(), contexts.archive(), backupContext())) {
            return await(backup.clusterBackup().context().stateCounter());
        }
    }

    private Round await(final AtomicCounter stateCounter) {
        final SeedWatch watch = new SeedWatch(timing.peerTimeoutMs(), System.currentTimeMillis());
        Round round;
        while ((round = sample(watch, stateCounter)).outcome() == SeedWatch.Outcome.PENDING) {
            pause(POLL_MS);
        }
        return round;
    }

    private static Round sample(final SeedWatch watch, final AtomicCounter stateCounter) {
        final ClusterBackup.State state = ClusterBackup.State.get(stateCounter);
        return new Round(watch.observe(state, System.currentTimeMillis()), state);
    }

    private ClusterBackup.Context backupContext() {
        final ClusterBackup.Context ctx = new ClusterBackup.Context();
        final ChannelUri consensus = ChannelUri.parse(ctx.consensusChannel());
        consensus.put(CommonContext.ENDPOINT_PARAM_NAME, contexts.consensusEndpoint());
        final String ephemeral = contexts.host() + ":0";
        return ctx
            .clusterDir(clusterDir.root().toFile())
            .clusterConsensusEndpoints(peers)
            .consensusChannel(consensus.toString())
            .catchupEndpoint(ephemeral)
            // The backup replaces the request endpoint with the archive
            // endpoint of the peer that answers.
            .clusterArchiveContext(new AeronArchive.Context()
                .controlRequestChannel(CommonContext.UDP_CHANNEL)
                .controlResponseChannel("aeron:udp?endpoint=" + ephemeral))
            .initialReplayStart(ReplayStart.LATEST_SNAPSHOT)
            .clusterBackupCoolDownIntervalNs(COOL_DOWN_NS)
            .errorHandler(error -> System.out.println(
                "cluster SEED backup error memberId=" + memberId + ": " + error));
    }

    private static void pause(final long ms) {
        try {
            Thread.sleep(ms);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("peer seed interrupted", e);
        }
    }
}
