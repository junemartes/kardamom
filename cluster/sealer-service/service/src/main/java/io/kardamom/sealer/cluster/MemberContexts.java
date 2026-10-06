package io.kardamom.sealer.cluster;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.NameResolver;
import io.aeron.driver.ThreadingMode;
import java.io.File;
import java.nio.file.Path;

/**
 * The directories of one member, and the media driver and archive
 * contexts that use them. Aeron contexts are single-use, so each launch
 * and each peer seed round takes fresh ones. The member and its peer seed
 * use the same contexts: the seed writes the archive that the member then
 * opens.
 */
final class MemberContexts {
    private final String aeronDir;
    private final String clusterDir;
    private final String archiveDir;
    /** This member's endpoints: ingress, consensus, log, catch-up, archive. */
    private final String[] endpoints;
    /** The driver's host name resolver. Null selects the Aeron default. */
    private final NameResolver nameResolver;

    MemberContexts(
            final String aeronDir, final String clusterDir, final String archiveDir, final String[] endpoints) {
        this(aeronDir, clusterDir, archiveDir, endpoints, null);
    }

    MemberContexts(
            final String aeronDir,
            final String clusterDir,
            final String archiveDir,
            final String[] endpoints,
            final NameResolver nameResolver) {
        this.aeronDir = aeronDir;
        this.clusterDir = clusterDir;
        this.archiveDir = archiveDir;
        this.endpoints = endpoints;
        this.nameResolver = nameResolver;
    }

    /** The consensus module directory, which the service container shares. */
    StateDir clusterState() {
        return new StateDir(Path.of(clusterDir));
    }

    StateDir archiveState() {
        return new StateDir(Path.of(archiveDir));
    }

    /** This node's host: every endpoint of the member shares the host of the ingress endpoint. */
    String host() {
        return endpoints[0].split(":")[0];
    }

    /** This member's consensus endpoint. */
    String consensusEndpoint() {
        return endpoints[1];
    }

    MediaDriver.Context driver() {
        return new MediaDriver.Context()
            .aeronDirectoryName(aeronDir)
            .threadingMode(ThreadingMode.SHARED)
            .nameResolver(nameResolver)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(false);
    }

    // Aeron 1.44 requires Archive.Context.replicationChannel to be set;
    // it has no default. This is the channel this archive uses to
    // receive replication during cluster catch-up (snapshot and log
    // transfer between members). The standard ClusteredMediaDriver
    // pattern uses this node's IP with an OS-assigned (ephemeral) port.
    Archive.Context archive() {
        return new Archive.Context()
            .aeronDirectoryName(aeronDir)
            .archiveDir(new File(archiveDir))
            .controlChannel("aeron:udp?endpoint=" + endpoints[4])
            .localControlChannel("aeron:ipc?term-length=64k")
            .replicationChannel("aeron:udp?endpoint=" + host() + ":0")
            // The catalog level must be at least the recording level.
            .fileSyncLevel(ClusterNode.fileSyncLevel())
            .catalogFileSyncLevel(ClusterNode.fileSyncLevel())
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED);
    }
}
