package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.ChannelUri;
import io.aeron.archive.Archive;
import io.aeron.cluster.ClusterBackup;
import io.aeron.cluster.ClusterBackup.Configuration.ReplayStart;
import io.aeron.driver.ThreadingMode;
import java.io.File;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** The settings of a backup node from its properties, and the contexts that it launches with. */
final class BackupSettingsTest {
    private static final String MEMBERS =
        "0,10.0.0.1:40200,10.0.0.1:40201,10.0.0.1:40202,10.0.0.1:40203,10.0.0.1:40204"
            + "|1,10.0.0.2:40200,10.0.0.2:40201,10.0.0.2:40202,10.0.0.2:40203,10.0.0.2:40204"
            + "|2,10.0.0.3:40200,10.0.0.3:40201,10.0.0.3:40202,10.0.0.3:40203,10.0.0.3:40204";
    private static final String NODE_IP = "10.0.0.9";

    private static Properties props(final Map<String, String> extra) {
        final Properties props = new Properties();
        props.setProperty("kardamom.cluster.members", MEMBERS);
        props.setProperty("kardamom.cluster.nodeIp", NODE_IP);
        props.putAll(extra);
        return props;
    }

    private static BackupSettings parse(final Map<String, String> extra) {
        return BackupSettings.parse(props(extra), IdleCap.DEFAULT);
    }

    /**
     * Assert the sync level of the archive and its catalog. The getters of
     * the archive context are not public, so read its text form.
     */
    private static void assertSyncLevel(final int level, final Archive.Context archive) {
        final String text = archive.toString();
        assertTrue(text.contains("\n    fileSyncLevel=" + level + "\n"), text);
        assertTrue(text.contains("\n    catalogFileSyncLevel=" + level + "\n"), text);
    }

    @Test
    void theDefaultsCopyFromAFollowerEvery30SecondsAndSyncAtLevelOne() {
        final BackupSettings settings = parse(Map.of());
        final ClusterBackup.Context backup = settings.backupContext(new StopRequest());
        final Archive.Context archive = settings.contexts().archive();

        assertEquals(ClusterBackup.SourceType.FOLLOWER, backup.sourceType());
        assertEquals(TimeUnit.SECONDS.toNanos(30), backup.clusterBackupIntervalNs());
        assertSyncLevel(1, archive);
        assertEquals(0, settings.adminPort());
        assertEquals(new File("/opt/kardamom/cluster"), backup.clusterDir());
        assertEquals(new File("/opt/kardamom/archive"), archive.archiveDir());
        assertEquals("/opt/kardamom/aeron-mount/dir", settings.contexts().driver().aeronDirectoryName());
    }

    @Test
    void theSettingsOverrideTheDefaults() {
        final BackupSettings settings = parse(Map.of(
            "kardamom.backup.sourceType", "ANY",
            "kardamom.backup.intervalS", "5",
            "kardamom.backup.fileSyncLevel", "2",
            "kardamom.cluster.adminPort", "40205",
            "kardamom.cluster.dir", "/data/cluster",
            "kardamom.archive.dir", "/data/archive",
            "aeron.dir", "/dev/shm/backup"));
        final ClusterBackup.Context backup = settings.backupContext(new StopRequest());
        final Archive.Context archive = settings.contexts().archive();

        assertEquals(ClusterBackup.SourceType.ANY, backup.sourceType());
        assertEquals(TimeUnit.SECONDS.toNanos(5), backup.clusterBackupIntervalNs());
        assertSyncLevel(2, archive);
        assertEquals(40205, settings.adminPort());
        assertEquals(new File("/data/cluster"), backup.clusterDir());
        assertEquals(new File("/data/archive"), archive.archiveDir());
        assertEquals("/dev/shm/backup", settings.contexts().driver().aeronDirectoryName());
    }

    /**
     * The backup asks every member. It takes their answers on port 40201 of
     * its own host, and catch-up and archive responses on ephemeral ports.
     */
    @Test
    void theBackupAsksEveryMemberAndListensOnItsOwnHost() {
        final ClusterBackup.Context backup = parse(Map.of()).backupContext(new StopRequest());

        assertEquals("10.0.0.1:40201,10.0.0.2:40201,10.0.0.3:40201", backup.clusterConsensusEndpoints());
        final ChannelUri consensus = ChannelUri.parse(backup.consensusChannel());
        assertEquals("10.0.0.9:40201", consensus.get("endpoint"));
        assertEquals("64k", consensus.get("term-length"));
        assertEquals("10.0.0.9:0", backup.catchupEndpoint());
        assertEquals("aeron:udp?endpoint=10.0.0.9:0", backup.clusterArchiveContext().controlResponseChannel());
        assertEquals(ReplayStart.LATEST_SNAPSHOT, backup.initialReplayStart());
    }

    @Test
    void theArchiveTakesControlOnPort40204AndTheDriverIsShared() {
        final MemberContexts contexts = parse(Map.of()).contexts();

        assertEquals("aeron:udp?endpoint=10.0.0.9:40204", contexts.archive().controlChannel());
        assertEquals("aeron:udp?endpoint=10.0.0.9:0", contexts.archive().replicationChannel());
        assertEquals(ThreadingMode.SHARED, contexts.driver().threadingMode());
    }

    @Test
    void aMissingMemberListOrNodeIpStopsTheStart() {
        final Properties noMembers = props(Map.of());
        noMembers.remove("kardamom.cluster.members");
        final Properties noNodeIp = props(Map.of());
        noNodeIp.remove("kardamom.cluster.nodeIp");

        assertThrows(IllegalStateException.class, () -> BackupSettings.parse(noMembers, IdleCap.DEFAULT));
        assertThrows(IllegalStateException.class, () -> BackupSettings.parse(noNodeIp, IdleCap.DEFAULT));
    }

    @Test
    void aBadValueStopsTheStart() {
        assertThrows(IllegalArgumentException.class, () -> parse(Map.of("kardamom.backup.fileSyncLevel", "3")));
        assertThrows(IllegalArgumentException.class, () -> parse(Map.of("kardamom.backup.intervalS", "0")));
        assertThrows(IllegalArgumentException.class, () -> parse(Map.of("kardamom.backup.intervalS", "soon")));
        assertThrows(IllegalArgumentException.class, () -> parse(Map.of("kardamom.backup.sourceType", "LEADERS")));
    }
}
