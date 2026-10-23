package io.kardamom.sealer.cluster;

import io.aeron.cluster.ClusterBackup;
import io.aeron.cluster.ClusterBackupMediaDriver;
import io.aeron.cluster.client.ClusterException;
import java.io.IOException;
import java.util.Optional;
import org.agrona.CloseHelper;

/**
 * A backup node: it is no member of the cluster and never votes. It runs
 * an Aeron ClusterBackup until a shutdown signal, and keeps the latest
 * snapshots and the log after them in its own cluster and archive
 * directories. After a loss of the members, a member can start from a
 * copy of these directories.
 *
 * <p>The backup starts at the latest snapshot. It then records the live
 * log, and asks the members for new snapshots at each interval. A watch
 * thread samples the backup, logs each change of the state or of the
 * snapshot, and hands the sample to the admin endpoint.</p>
 *
 * <p>The backup syncs its archive at its own level. It is outside the
 * quorum, so a sync never delays a commit.</p>
 */
final class BackupNode {

    /** The property that selects the backup mode of the process. */
    static final String SETTING = "kardamom.cluster.backup";
    /** The pause between two samples of the watch thread. */
    private static final Interval POLL = new Interval(250);

    private final BackupSettings settings;

    BackupNode(final BackupSettings settings) {
        this.settings = settings;
    }

    /** Run the backup until a shutdown signal, or until the backup terminates. */
    void run() {
        final StopRequest stop = new StopRequest();
        try (Running ignored = start(stop)) {
            stop.await();
        }
    }

    /**
     * Launch the backup, its watch thread and its admin endpoint. A
     * termination of the backup signals {@code stop}.
     */
    Running start(final StopRequest stop) {
        final MemberContexts contexts = settings.contexts();
        final ClusterBackupMediaDriver backup = ClusterBackupMediaDriver.launch(
            contexts.driver(), contexts.archive(), settings.backupContext(stop));
        try {
            return new Running(backup, contexts.clusterState(), settings.adminPort());
        } catch (final RuntimeException e) {
            CloseHelper.quietClose(backup);
            throw e;
        }
    }

    /** A launched backup: the backup itself, its watch thread and its admin endpoint. */
    static final class Running implements AutoCloseable {
        private final ClusterBackupMediaDriver backup;
        private final ClusterBackup.Context backupContext;
        private final StateDir clusterDir;
        private final Thread watch;
        private final Optional<AdminServer> admin;
        /**
         * The latest sample. The watch thread writes it. The admin thread and
         * the callers of {@link #status} read it, so it is volatile.
         */
        private volatile BackupStatus latest;

        private Running(final ClusterBackupMediaDriver backup, final StateDir clusterDir, final int adminPort) {
            this.backup = backup;
            this.backupContext = backup.clusterBackup().context();
            this.clusterDir = clusterDir;
            this.latest = sample(BackupStatus.Snapshot.NONE);
            System.out.println("cluster BACKUP up consensus=" + backupContext.consensusChannel()
                + " peers=" + backupContext.clusterConsensusEndpoints()
                + " sourceType=" + backupContext.sourceType()
                + " intervalNs=" + backupContext.clusterBackupIntervalNs());
            System.out.println(latest.logLine());
            this.admin = startAdmin(adminPort);
            this.watch = new Thread(this::watchUntilInterrupted, "kardamom-backup-watch");
            this.watch.setDaemon(true);
            this.watch.start();
        }

        /** The latest sample of the watch thread. */
        BackupStatus status() {
            return latest;
        }

        /** The admin endpoint on {@code port}, or none when the port is 0. */
        private Optional<AdminServer> startAdmin(final int port) {
            if (port == 0) {
                System.out.println("cluster admin endpoint DISABLED backup");
                return Optional.empty();
            }
            try {
                final AdminServer server = AdminServer.serve(port, () -> latest.report(), Optional.empty());
                System.out.println("cluster admin endpoint up backup port=" + server.port());
                return Optional.of(server);
            } catch (final IOException e) {
                throw new IllegalStateException("cluster admin endpoint bind failed on port " + port, e);
            }
        }

        private void watchUntilInterrupted() {
            while (POLL.await()) {
                step();
            }
        }

        /** Take one sample, and log it when the state or the snapshot changed. */
        private void step() {
            final BackupStatus next = sample(latest.snapshot());
            if (!next.sameStage(latest)) {
                System.out.println(next.logLine());
            }
            latest = next;
        }

        /** Sample the counters of the backup and its recording log. */
        private BackupStatus sample(final BackupStatus.Snapshot last) {
            return new BackupStatus(
                ClusterBackup.State.get(backupContext.stateCounter()),
                backupContext.liveLogPositionCounter().get(),
                snapshot(last));
        }

        /**
         * The latest snapshot in the recording log. The backup writes the
         * recording log on its own thread, so a read can meet a missing file
         * or a half-written entry, and the recording log refuses it. That
         * read keeps {@code last}.
         */
        private BackupStatus.Snapshot snapshot(final BackupStatus.Snapshot last) {
            try {
                return clusterDir.latestSnapshot().map(BackupStatus.Snapshot::of).orElse(BackupStatus.Snapshot.NONE);
            } catch (final ClusterException e) {
                return last;
            }
        }

        /**
         * Stop the watch thread first: it reads the counters of the backup,
         * and those counters go when the backup closes.
         */
        @Override
        public void close() {
            watch.interrupt();
            try {
                watch.join();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            admin.ifPresent(AdminServer::close);
            backup.close();
            System.out.println("cluster BACKUP closed");
        }
    }
}
