package io.kardamom.sealer.cluster;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Picks the point below which one member purges its Raft log.
 *
 * <p>The point is a snapshot of the member's recording log, so a restart
 * always finds a snapshot with the whole log after it. The planner picks
 * the newest snapshot that obeys two rules:</p>
 * <ul>
 *   <li>The margin: it is older than the {@code keepSnapshots} newest
 *       snapshots. An intact member that stopped inside the margin still
 *       finds its log tail in the archive of the leader, so it rejoins
 *       without a peer seed.</li>
 *   <li>The floor: its block number is at or below the posted head. No
 *       record of a block that the batcher has not confirmed on L1 falls
 *       below the point.</li>
 * </ul>
 * <p>No snapshot obeys both rules until the member knows more snapshots
 * than it keeps. The planner then returns no point.</p>
 *
 * @param keepSnapshots how many of the newest snapshots keep their log; at least 1
 */
record PurgePlanner(int keepSnapshots) {

    static final String SETTING = "kardamom.cluster.logPurgeKeepSnapshots";
    static final int DEFAULT_KEEP_SNAPSHOTS = 3;

    /**
     * The planner of the setting {@code raw}: unset or blank for the
     * default, 0 for no purge. Any other text, or a count that leaves no
     * mark to purge to, is fatal, so a typo cannot purge the log.
     */
    static Optional<PurgePlanner> fromSetting(final String raw) {
        final int keep = raw == null || raw.isBlank() ? DEFAULT_KEEP_SNAPSHOTS : parse(raw.trim());
        return keep == 0 ? Optional.empty() : Optional.of(new PurgePlanner(keep));
    }

    private static int parse(final String value) {
        final int keep;
        try {
            keep = Integer.parseInt(value);
        } catch (final NumberFormatException e) {
            throw new IllegalStateException(SETTING + ": '" + value + "' is not a snapshot count", e);
        }
        if (keep < 0 || keep >= PurgeView.MAX_MARKS) {
            throw new IllegalStateException(
                SETTING + ": " + keep + " is outside [0, " + PurgeView.MAX_MARKS + ")");
        }
        return keep;
    }

    /**
     * The snapshot to purge to, or none.
     *
     * @param snapshotPositions the log positions of the valid snapshots in
     *     the member's recording log, in any order
     * @param view the block numbers of the snapshots that the service knows,
     *     and the posted head
     */
    Optional<PurgeView.Mark> plan(final List<Long> snapshotPositions, final PurgeView view) {
        final List<Long> ascending = snapshotPositions.stream().distinct().sorted().toList();
        final Set<Long> older = Set.copyOf(ascending.subList(0, Math.max(0, ascending.size() - keepSnapshots)));
        return view.marks().stream()
            .filter(mark -> older.contains(mark.logPosition()))
            .filter(mark -> mark.blockNumber() <= view.postedHead())
            .max(Comparator.comparingLong(PurgeView.Mark::logPosition));
    }
}
