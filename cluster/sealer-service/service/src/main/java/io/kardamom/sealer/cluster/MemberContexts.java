package io.kardamom.sealer.cluster;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.NameResolver;
import io.aeron.driver.ThreadingMode;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.Map;

/**
 * The directories of one member, and the media driver and archive
 * contexts that use them. Aeron contexts are single-use, so each launch
 * and each peer seed round takes fresh ones. The member and its peer seed
 * use the same contexts: the seed writes the archive that the member then
 * opens.
 */
final class MemberContexts {
    /** The archive's control channel for clients in this process. */
    private static final String LOCAL_CONTROL_CHANNEL = "aeron:ipc?term-length=64k";
    /**
     * The archive segment file of a new recording: one term of the Raft
     * log (8 MiB, the log channel's term-length). A log purge removes whole
     * segment files only, so a small segment keeps little log below the
     * purge point.
     */
    private static final int SEGMENT_FILE_LENGTH = 8 * 1024 * 1024;

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

    /**
     * These contexts with the member's name resolver: it keeps the last
     * address of each name, and with a node address it resolves the
     * member's own name to that address with no lookup.
     */
    MemberContexts withPeerNames(final int memberId, final String nodeIp) {
        return new MemberContexts(
            aeronDir, clusterDir, archiveDir, endpoints, new PeerNameResolver(memberId, ownAddress(nodeIp)));
    }

    private Map<String, InetAddress> ownAddress(final String nodeIp) {
        if (nodeIp == null || nodeIp.isBlank()) {
            return Map.of();
        }
        try {
            return Map.of(host(), InetAddress.getByName(nodeIp.trim()));
        } catch (final UnknownHostException e) {
            throw new IllegalArgumentException("kardamom.cluster.nodeIp is not an address: " + nodeIp, e);
        }
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
            .localControlChannel(LOCAL_CONTROL_CHANNEL)
            .segmentFileLength(SEGMENT_FILE_LENGTH)
            .replicationChannel("aeron:udp?endpoint=" + host() + ":0")
            // The catalog level must be at least the recording level.
            .fileSyncLevel(ClusterNode.fileSyncLevel())
            .catalogFileSyncLevel(ClusterNode.fileSyncLevel())
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED);
    }

    /**
     * A client context for this member's archive over IPC. The client
     * creates its own Aeron client on this member's media driver, so it
     * shares no state with the consensus module.
     */
    AeronArchive.Context localArchiveClient() {
        return new AeronArchive.Context()
            .aeronDirectoryName(aeronDir)
            .controlRequestChannel(LOCAL_CONTROL_CHANNEL)
            .controlResponseChannel(LOCAL_CONTROL_CHANNEL);
    }
}
