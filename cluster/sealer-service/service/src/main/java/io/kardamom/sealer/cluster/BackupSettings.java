package io.kardamom.sealer.cluster;

import io.aeron.cluster.ClusterBackup;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * The settings of a backup node, parsed once from the JVM properties.
 *
 * <p>The backup asks the consensus endpoints of every member. It listens
 * on {@link #CONSENSUS_PORT} of its own host for their answers, and its
 * archive takes control requests on {@link #ARCHIVE_PORT}. The members
 * use the same two ports, so a host runs a member or a backup, not
 * both.</p>
 *
 * @param peers      the consensus endpoints of every member, comma separated
 * @param contexts   the directories, endpoints, idle cap and file sync level of the node
 * @param adminPort  the port of the admin endpoint; 0 turns it off
 * @param intervalNs how often the backup asks for new snapshots, above 0
 * @param sourceType the members that the backup copies from
 */
record BackupSettings(
        String peers,
        MemberContexts contexts,
        int adminPort,
        long intervalNs,
        ClusterBackup.SourceType sourceType) {

    /** The port of the consensus endpoint of the backup. */
    static final int CONSENSUS_PORT = 40201;
    /** The port of the archive control endpoint of the backup. */
    static final int ARCHIVE_PORT = 40204;
    /** The property of the file sync level of the archive of the backup and its catalog. */
    static final String FILE_SYNC_LEVEL_SETTING = "kardamom.backup.fileSyncLevel";
    /** The property of the interval between two backup queries, in seconds. */
    static final String INTERVAL_SETTING = "kardamom.backup.intervalS";
    /** The property of the source type: a {@link ClusterBackup.SourceType} name. */
    static final String SOURCE_TYPE_SETTING = "kardamom.backup.sourceType";

    /**
     * The settings from {@code props}, with the idle cap {@code idleCap}.
     *
     * @throws IllegalStateException    when the member list or the node IP is not set
     * @throws IllegalArgumentException when a value does not parse or is out of range
     */
    static BackupSettings parse(final Properties props, final IdleCap idleCap) {
        final ClusterMembers members = new ClusterMembers(required(props, "kardamom.cluster.members"));
        final String nodeIp = required(props, "kardamom.cluster.nodeIp").trim();
        final MemberContexts contexts = new MemberContexts(
            props.getProperty("aeron.dir", "/opt/kardamom/aeron-mount/dir"),
            props.getProperty("kardamom.cluster.dir", "/opt/kardamom/cluster"),
            props.getProperty("kardamom.archive.dir", "/opt/kardamom/archive"),
            new MemberContexts.Endpoints(nodeIp, nodeIp + ":" + CONSENSUS_PORT, nodeIp + ":" + ARCHIVE_PORT),
            null)
            .withIdleCap(idleCap)
            .withFileSyncLevel(ClusterNode.checkedSyncLevel(
                FILE_SYNC_LEVEL_SETTING, Integer.parseInt(props.getProperty(FILE_SYNC_LEVEL_SETTING, "1").trim())));
        return new BackupSettings(
            members.consensusEndpoints(),
            contexts,
            Integer.parseInt(props.getProperty("kardamom.cluster.adminPort", "0").trim()),
            TimeUnit.SECONDS.toNanos(intervalS(props.getProperty(INTERVAL_SETTING, "30"))),
            ClusterBackup.SourceType.valueOf(props.getProperty(SOURCE_TYPE_SETTING, "FOLLOWER").trim()));
    }

    /**
     * The cluster backup context of the node. A termination of the backup
     * requests the stop of the node.
     */
    ClusterBackup.Context backupContext(final StopRequest stop) {
        return contexts.backup(peers)
            .sourceType(sourceType)
            .clusterBackupIntervalNs(intervalNs)
            .errorHandler(error -> System.out.println("cluster BACKUP error: " + error))
            .terminationHook(() -> {
                System.out.println("cluster TERMINATION component=CLUSTER_BACKUP (requested shutdown)");
                stop.signal();
            });
    }

    private static String required(final Properties props, final String key) {
        final String value = props.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(key + " not set");
        }
        return value;
    }

    private static long intervalS(final String raw) {
        final long seconds = Long.parseLong(raw.trim());
        if (seconds <= 0) {
            throw new IllegalArgumentException(INTERVAL_SETTING + " must be above 0, not " + seconds);
        }
        return seconds;
    }
}
