package io.kardamom.sealer.cluster;

import io.aeron.cluster.ClusterBackupEventsListener;
import io.aeron.cluster.ClusterMember;
import io.aeron.cluster.RecordingLog;
import java.util.List;
import java.util.Optional;
import java.util.function.LongSupplier;

/** The backup agent publishes a coherent log requirement and copied position to its seed caller. */
final class SeedProgress implements ClusterBackupEventsListener {
    /** The local recording and the minimum position that its term metadata requires. */
    record Floor(long recordingId, long position) {
        static Optional<Floor> of(final RecordingLog log) {
            return Optional.ofNullable(log.findLastTerm()).map(term -> new Floor(
                term.recordingId,
                Math.max(term.termBaseLogPosition, log.entries().stream()
                    .filter(entry -> entry.isValid && entry.type == RecordingLog.ENTRY_TYPE_SNAPSHOT)
                    .mapToLong(entry -> entry.logPosition).max().orElse(-1L))));
        }
    }

    /** One immutable observation; only the backup agent writes it. */
    private record Copy(Floor floor, long position) {
        boolean covers(final Floor required) {
            return floor.recordingId() == required.recordingId() && position >= required.position();
        }
    }

    private final LongSupplier position;
    /** The seed caller reads the agent's whole observation across threads. */
    private volatile Optional<Copy> latest = Optional.empty();

    SeedProgress(final LongSupplier position) {
        this.position = position;
    }

    SeedWatch.Copy sample() {
        return latest.map(copy -> new SeedWatch.Copy(copy.position(), copy.covers(copy.floor())))
            .orElse(SeedWatch.Copy.MISSING);
    }

    /** The stopped backup must still have the recording and boundary that the copied bytes cover. */
    boolean covers(final Optional<Floor> required) {
        return required.flatMap(floor -> latest.map(copy -> copy.covers(floor))).orElse(false);
    }

    @Override
    public void onUpdatedRecordingLog(final RecordingLog log, final List<RecordingLog.Snapshot> snapshots) {
        latest = Floor.of(log).map(floor -> new Copy(floor, position.getAsLong()));
    }

    @Override
    public void onLiveLogProgress(final long recordingId, final long counterId, final long copiedPosition) {
        latest = latest.filter(copy -> copy.floor().recordingId() == recordingId)
            .map(copy -> new Copy(copy.floor(), copiedPosition));
    }

    @Override
    public void onBackupQuery() { }

    @Override
    public void onPossibleFailure(final Exception error) { }

    @Override
    public void onBackupResponse(
            final ClusterMember[] members,
            final ClusterMember supplier,
            final List<RecordingLog.Snapshot> snapshots) { }
}
