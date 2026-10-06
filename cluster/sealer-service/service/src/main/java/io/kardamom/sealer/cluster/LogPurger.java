package io.kardamom.sealer.cluster;

import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.cluster.ElectionState;
import java.util.Optional;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * Purges this member's Raft log below the snapshot that {@link PurgePlanner}
 * picks.
 *
 * <p>Each member purges its own log. A purge never removes data that
 * another member needs: an intact member that stopped inside the margin
 * finds its log tail on every peer, and a member outside the margin seeds
 * from a peer's latest snapshot ({@link PeerSeed}). The archive removes
 * whole segment files, so the new log start is the segment base at or
 * below the snapshot position.</p>
 *
 * <p>A pass runs on its own thread, never on the service thread or on the
 * consensus module thread. A pass opens an archive client and waits for
 * the archive, which can take seconds. The service thread must not wait:
 * it applies the log, and a member that stalls there lags its peers. A
 * pass runs only while the election is closed, so it never competes with
 * the catch-up of an election. The service hands its snapshot marks and
 * the posted head to this thread in one immutable {@link PurgeView}.</p>
 */
final class LogPurger {

    /** The pause between two passes. A pass opens the archive only when its plan names a new snapshot. */
    static final Interval PASS = new Interval(30_000);

    private final Member member;
    private final PurgePlanner planner;
    /** The snapshot of the last pass that reached the archive. Only the purge thread uses it. */
    private Optional<PurgeView.Mark> lastTried = Optional.empty();

    LogPurger(final Member member, final PurgePlanner planner) {
        this.member = member;
        this.planner = planner;
    }

    /**
     * The member whose log a purger purges: its id, its directories, and
     * the service that knows the block number of each snapshot.
     */
    record Member(int memberId, MemberContexts contexts, SealerClusteredService service) {
    }

    /** One purge: the new log start, the snapshot that it keeps, and the posted head of the plan. */
    record Purge(long position, PurgeView.Mark mark, long postedHead, long segments) {
    }

    /** Start the purge thread. It ends when the member closes its election state counter. */
    void start(final AtomicCounter electionState) {
        final Thread thread = new Thread(() -> runWhileOpen(electionState), "kardamom-log-purger");
        thread.setDaemon(true);
        thread.start();
        System.out.println("cluster log purge up memberId=" + member.memberId()
            + " keepSnapshots=" + planner.keepSnapshots() + " passS=" + PASS.ms() / 1000);
    }

    /**
     * Run one pass: plan, then purge when the plan names a snapshot that no
     * earlier pass tried. Return the purge, or none when the plan names no
     * snapshot, names the same one again, or leaves no whole segment below
     * it.
     */
    Optional<Purge> runOnce() {
        final PurgeView view = member.service().purgeView();
        final StateDir clusterDir = member.contexts().clusterState();
        return planner.plan(clusterDir.snapshotPositions(), view)
            .filter(mark -> lastTried.filter(mark::equals).isEmpty())
            .flatMap(mark -> purgeTo(clusterDir.logRecordingId(), mark, view.postedHead()));
    }

    private Optional<Purge> purgeTo(final long recordingId, final PurgeView.Mark mark, final long postedHead) {
        try (AeronArchive archive = AeronArchive.connect(member.contexts().localArchiveClient())) {
            final Extent extent = Extent.of(archive, recordingId);
            final long position = AeronArchive.segmentFileBasePosition(
                extent.startPosition, mark.logPosition(), extent.termBufferLength, extent.segmentFileLength);
            final Optional<Purge> purge = Optional.of(position)
                .filter(base -> base > extent.startPosition)
                .map(base -> purged(new Purge(base, mark, postedHead, archive.purgeSegments(recordingId, base))));
            lastTried = Optional.of(mark);
            return purge;
        }
    }

    private Purge purged(final Purge purge) {
        System.out.println("cluster LOG PURGED memberId=" + member.memberId()
            + " position=" + purge.position()
            + " snapshotPosition=" + purge.mark().logPosition()
            + " block=" + purge.mark().blockNumber()
            + " postedHead=" + purge.postedHead()
            + " segments=" + purge.segments());
        return purge;
    }

    private void runWhileOpen(final AtomicCounter electionState) {
        while (PASS.await() && !electionState.isClosed()) {
            passWhenClosed(electionState);
        }
    }

    /** Run a pass when the election is closed. A failed pass logs, and the next pass tries again. */
    private void passWhenClosed(final AtomicCounter electionState) {
        if (ElectionState.get(electionState) != ElectionState.CLOSED) {
            return;
        }
        try {
            runOnce();
        } catch (final RuntimeException e) {
            System.out.println("cluster LOG PURGE failed memberId=" + member.memberId()
                + " (the next pass retries): " + e);
        }
    }

    /** The extent of a recording that sets its segment bases. */
    private static final class Extent implements RecordingDescriptorConsumer {
        private long startPosition;
        private int termBufferLength;
        private int segmentFileLength;

        static Extent of(final AeronArchive archive, final long recordingId) {
            final Extent extent = new Extent();
            if (archive.listRecording(recordingId, extent) == 0) {
                throw new IllegalStateException("the archive has no log recording " + recordingId);
            }
            return extent;
        }

        @Override
        public void onRecordingDescriptor(
                final long controlSessionId,
                final long correlationId,
                final long recordingId,
                final long startTimestamp,
                final long stopTimestamp,
                final long startPosition,
                final long stopPosition,
                final int initialTermId,
                final int segmentFileLength,
                final int termBufferLength,
                final int mtuLength,
                final int sessionId,
                final int streamId,
                final String strippedChannel,
                final String originalChannel,
                final String sourceIdentity) {
            this.startPosition = startPosition;
            this.termBufferLength = termBufferLength;
            this.segmentFileLength = segmentFileLength;
        }
    }
}
