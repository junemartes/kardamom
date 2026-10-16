package io.kardamom.sealer.cluster;

import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusterBackup;
import io.aeron.cluster.ClusterBackup.Configuration.ReplayStart;
import io.aeron.cluster.ClusterBackupMediaDriver;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.agrona.concurrent.YieldingIdleStrategy;
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
 * <p>A member with a retained file also copies the segment files from a
 * peer ({@link RetainedCopy}), and checks the copy against the backed-up
 * snapshot with {@link RetainedWalk}. A round in which no peer serves a
 * copy that passes ends with no result, like a round with no backup. After
 * {@link #MAX_COPY_FAILURES} such rounds, the seed stops the start.</p>
 *
 * <p>A round in which no peer answers ends after the peer timeout, and the
 * next round starts after a backoff. The seed has no other exit: a blank
 * member never falls back to a start at log position 0.</p>
 */
final class PeerSeed {

    /** How often a round samples the backup state. */
    private static final long POLL_MS = 100;
    /** The rounds whose retained-file copy fails before the seed stops the start. */
    static final int MAX_COPY_FAILURES = 5;
    private static final String REPLAY_CHANNEL = "aeron:ipc";
    private static final int REPLAY_STREAM_ID = 1_956;
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
    /** The copy of the retained file, or empty when the member keeps the frames on the heap. */
    private final Optional<RetainedCopy> retainedCopy;
    /** The rounds whose retained-file copy failed. */
    private int copyFailures;

    PeerSeed(
            final int memberId,
            final String peers,
            final MemberContexts contexts,
            final Timing timing,
            final Optional<RetainedCopy> retainedCopy) {
        this.memberId = memberId;
        this.peers = peers;
        this.contexts = contexts;
        this.clusterDir = contexts.clusterState();
        this.archiveDir = contexts.archiveState();
        this.timing = timing;
        this.retainedCopy = retainedCopy;
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
        if (result.outcome() == SeedWatch.Outcome.SEEDED && copiedRetained(result)) {
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

    /**
     * Whether the retained file is in place: the member keeps no file, the
     * cluster has no snapshot yet, or a peer served a copy that matches the
     * backed-up snapshot. Stop the start after {@link #MAX_COPY_FAILURES}
     * failed copies.
     */
    private boolean copiedRetained(final Round result) {
        final boolean copied = retainedCopy
            .map(copy -> result.snapshot().map(copy::run).orElse(true))
            .orElse(true);
        if (!copied) {
            copyFailures++;
            System.out.println("cluster SEED retained-copy MISMATCH memberId=" + memberId
                + " failures=" + copyFailures + "/" + MAX_COPY_FAILURES
                + " — no peer served a retained file that matches the backed-up snapshot");
            if (copyFailures >= MAX_COPY_FAILURES) {
                final String why = "no peer serves a retained file that matches its snapshot after "
                    + copyFailures + " rounds";
                System.out.println("cluster SEED retained-copy GAVE-UP memberId=" + memberId + " — " + why);
                throw new IllegalStateException(why);
            }
        }
        return copied;
    }

    /**
     * The end of one round: its outcome, the last backup state it sampled,
     * and the payload of the backed-up sealer snapshot when the member
     * needs it for the retained-file check.
     */
    private record Round(SeedWatch.Outcome outcome, ClusterBackup.State lastState, Optional<byte[]> snapshot) {
    }

    private Round backUp() {
        try (ClusterBackupMediaDriver backup = ClusterBackupMediaDriver.launch(
                contexts.driver(), contexts.archive(), backupContext())) {
            final Round round = await(backup.clusterBackup().context().stateCounter());
            final boolean wanted = round.outcome() == SeedWatch.Outcome.SEEDED && retainedCopy.isPresent();
            return new Round(round.outcome(), round.lastState(),
                wanted ? clusterDir.latestServiceSnapshotRecording().map(this::readSnapshot) : Optional.empty());
        }
    }

    /** The payload of the sealer snapshot in {@code recordingId}, replayed from the archive of the backup. */
    private byte[] readSnapshot(final long recordingId) {
        try (AeronArchive archive = AeronArchive.connect(contexts.localArchiveClient())) {
            final int sessionId = (int) archive.startReplay(
                recordingId, 0L, AeronArchive.NULL_LENGTH, REPLAY_CHANNEL, REPLAY_STREAM_ID);
            try (Subscription subscription = archive.context().aeron().addSubscription(
                    ChannelUri.addSessionId(REPLAY_CHANNEL, sessionId), REPLAY_STREAM_ID)) {
                return SnapshotIo.readServicePayload(awaitImage(subscription, sessionId), YieldingIdleStrategy.INSTANCE);
            }
        }
    }

    private static Image awaitImage(final Subscription subscription, final int sessionId) {
        Image image = subscription.imageBySessionId(sessionId);
        while (image == null) {
            image = nextImage(subscription, sessionId);
        }
        return image;
    }

    private static Image nextImage(final Subscription subscription, final int sessionId) {
        Thread.onSpinWait();
        return subscription.imageBySessionId(sessionId);
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
        return new Round(watch.observe(state, System.currentTimeMillis()), state, Optional.empty());
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
