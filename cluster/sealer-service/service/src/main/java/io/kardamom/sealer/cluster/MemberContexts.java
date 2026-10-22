package io.kardamom.sealer.cluster;

import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusterBackup;
import io.aeron.cluster.ClusterBackup.Configuration.ReplayStart;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.NameResolver;
import io.aeron.driver.ThreadingMode;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The directories of one node, a member or a backup, and the media driver,
 * archive and cluster backup contexts that use them. Aeron contexts are
 * single-use, so each launch and each peer seed round takes fresh ones.
 * The member and its peer seed use the same contexts: the seed writes the
 * archive that the member then opens.
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
    /** The wait of a cluster backup after a peer refuses it, before it asks the next peer. */
    private static final long COOL_DOWN_NS = TimeUnit.SECONDS.toNanos(1);

    /**
     * The endpoints of a node that its contexts use.
     *
     * @param host      the host of every endpoint of the node
     * @param consensus the consensus endpoint: a cluster backup on the node takes the answers of its peers there
     * @param archive   the control endpoint of the archive of the node
     */
    record Endpoints(String host, String consensus, String archive) {
        /**
         * The endpoints of a member from its five: ingress, consensus, log,
         * catch-up, archive. Every endpoint of the member shares the host of
         * the ingress endpoint.
         */
        static Endpoints ofMember(final String[] endpoints) {
            return new Endpoints(endpoints[0].split(":")[0], endpoints[1], endpoints[4]);
        }
    }

    private final String aeronDir;
    private final String clusterDir;
    private final String archiveDir;
    private final Endpoints endpoints;
    /** The driver's host name resolver. Null selects the Aeron default. */
    private final NameResolver nameResolver;
    /** The longest idle park of the driver, the archive and a cluster backup. */
    private final IdleCap idleCap;
    /** The file sync level of the archive and its catalog. */
    private final int fileSyncLevel;

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
        this(aeronDir, clusterDir, archiveDir, Endpoints.ofMember(endpoints), nameResolver);
    }

    /** The contexts of a node with {@code endpoints}, the member's file sync level and the default idle cap. */
    MemberContexts(
            final String aeronDir,
            final String clusterDir,
            final String archiveDir,
            final Endpoints endpoints,
            final NameResolver nameResolver) {
        this(aeronDir, clusterDir, archiveDir, endpoints, nameResolver, IdleCap.DEFAULT, ClusterNode.fileSyncLevel());
    }

    private MemberContexts(
            final String aeronDir,
            final String clusterDir,
            final String archiveDir,
            final Endpoints endpoints,
            final NameResolver nameResolver,
            final IdleCap idleCap,
            final int fileSyncLevel) {
        this.aeronDir = aeronDir;
        this.clusterDir = clusterDir;
        this.archiveDir = archiveDir;
        this.endpoints = endpoints;
        this.nameResolver = nameResolver;
        this.idleCap = idleCap;
        this.fileSyncLevel = fileSyncLevel;
    }

    /**
     * These contexts with the member's name resolver: it keeps the last
     * address of each name, and with a node address it resolves the
     * member's own name to that address with no lookup.
     */
    MemberContexts withPeerNames(final int memberId, final String nodeIp) {
        return new MemberContexts(aeronDir, clusterDir, archiveDir, endpoints,
            new PeerNameResolver(memberId, ownAddress(nodeIp)), idleCap, fileSyncLevel);
    }

    /** These contexts with the idle cap {@code cap} for the driver, the archive and a cluster backup. */
    MemberContexts withIdleCap(final IdleCap cap) {
        return new MemberContexts(aeronDir, clusterDir, archiveDir, endpoints, nameResolver, cap, fileSyncLevel);
    }

    /** These contexts with the file sync level {@code level} for the archive and its catalog. */
    MemberContexts withFileSyncLevel(final int level) {
        return new MemberContexts(aeronDir, clusterDir, archiveDir, endpoints, nameResolver, idleCap, level);
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

    /** This node's host. */
    String host() {
        return endpoints.host();
    }

    /** This node's consensus endpoint. */
    String consensusEndpoint() {
        return endpoints.consensus();
    }

    MediaDriver.Context driver() {
        return new MediaDriver.Context()
            .aeronDirectoryName(aeronDir)
            .threadingMode(ThreadingMode.SHARED)
            .sharedIdleStrategy(idleCap.strategy())
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
            .controlChannel("aeron:udp?endpoint=" + endpoints.archive())
            .localControlChannel(LOCAL_CONTROL_CHANNEL)
            .segmentFileLength(SEGMENT_FILE_LENGTH)
            .replicationChannel("aeron:udp?endpoint=" + host() + ":0")
            // The catalog level must be at least the recording level.
            .fileSyncLevel(fileSyncLevel)
            .catalogFileSyncLevel(fileSyncLevel)
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .idleStrategySupplier(idleCap::strategy);
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

    /**
     * A cluster backup context that copies the latest snapshot, and the log
     * after it, from {@code peers} into this node's cluster directory.
     *
     * <p>The backup takes the answers of the peers on this node's consensus
     * endpoint, so the endpoint must be free. Catch-up and archive control
     * responses use ephemeral ports on this node's host. The backup starts
     * at the latest snapshot: the members purge the log below their
     * snapshots, so a backup from the start of the log cannot work.</p>
     *
     * @param peers the consensus endpoints of the members to ask, comma separated
     */
    ClusterBackup.Context backup(final String peers) {
        final ClusterBackup.Context ctx = new ClusterBackup.Context();
        final ChannelUri consensus = ChannelUri.parse(ctx.consensusChannel());
        consensus.put(CommonContext.ENDPOINT_PARAM_NAME, consensusEndpoint());
        final String ephemeral = host() + ":0";
        return ctx
            .clusterDir(new File(clusterDir))
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
            .idleStrategySupplier(idleCap::strategy);
    }
}
