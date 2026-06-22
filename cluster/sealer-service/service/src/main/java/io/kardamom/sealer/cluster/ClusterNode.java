package io.kardamom.sealer.cluster;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.io.File;
import org.agrona.concurrent.ShutdownSignalBarrier;

/** Boots an all-in-one Aeron Cluster member (media driver + archive +
 *  consensus module) running {@link SealerClusteredService}. Config via -D sysprops. */
public final class ClusterNode {
    public static void main(final String[] args) {
        // "0,ingressHost:port,consensusHost:port,logHost:port,catchupHost:port,archiveHost:port|1,...|2,..."
        final String clusterMembers = System.getProperty("kardamom.cluster.members");
        if (clusterMembers == null) throw new IllegalStateException("kardamom.cluster.members not set");
        // memberId: use an explicit -Dkardamom.cluster.memberId when given (>=0, e.g.
        // TestCluster), else derive it from this node's IP. The Nomad deploy passes the
        // node IP (not an explicit id) because the alloc index != the node it lands on.
        final int memberIdProp = Integer.getInteger("kardamom.cluster.memberId", -1);
        final String nodeIp = System.getProperty("kardamom.cluster.nodeIp");
        final int memberId = (memberIdProp >= 0) ? memberIdProp : memberIdForNodeIp(clusterMembers, nodeIp);
        final String aeronDir = System.getProperty("aeron.dir", "/opt/kardamom/aeron-mount/dir");
        final String clusterDir = System.getProperty("kardamom.cluster.dir", "/opt/kardamom/cluster");
        final String archiveDir = System.getProperty("kardamom.archive.dir", "/opt/kardamom/archive");
        final int ingressStreamId = Integer.getInteger("kardamom.cluster.ingressStreamId", 101);
        final long tickMs = Long.getLong("kardamom.cluster.tickMs", 2000L);
        final int dedupCapacity = Integer.getInteger("kardamom.cluster.dedupCapacity", 8192);

        final String[] me = memberEndpoints(clusterMembers, memberId); // [ingress,consensus,log,catchup,archive]

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .aeronDirectoryName(aeronDir)
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(false);

        final Archive.Context archiveCtx = new Archive.Context()
            .aeronDirectoryName(aeronDir)
            .archiveDir(new File(archiveDir))
            .controlChannel("aeron:udp?endpoint=" + me[4])
            .localControlChannel("aeron:ipc?term-length=64k")
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED);

        final ConsensusModule.Context consensusCtx = new ConsensusModule.Context()
            .clusterMemberId(memberId)
            .clusterMembers(clusterMembers)
            .aeronDirectoryName(aeronDir)
            .clusterDir(new File(clusterDir))
            .ingressChannel("aeron:udp")
            .ingressStreamId(ingressStreamId)
            .replicationChannel("aeron:udp?endpoint=" + me[3]);

        final ClusteredServiceContainer.Context serviceCtx = new ClusteredServiceContainer.Context()
            .aeronDirectoryName(aeronDir)
            .clusterDir(new File(clusterDir))
            .clusteredService(new SealerClusteredService(dedupCapacity, tickMs, memberId));

        try (ClusteredMediaDriver ignored = ClusteredMediaDriver.launch(driverCtx, archiveCtx, consensusCtx);
             ClusteredServiceContainer ignored2 = ClusteredServiceContainer.launch(serviceCtx)) {
            System.out.println("cluster node up memberId=" + memberId + " endpoints=" + String.join(",", me));
            new ShutdownSignalBarrier().await();
        }
    }

    /**
     * Extract this member's 5 endpoints from the pipe/comma clusterMembers string.
     * Each member entry is {@code id,ingress,consensus,log,catchup,archive}; entries
     * are pipe-separated. Endpoints are trimmed (stray whitespace in the rendered
     * Nomad template would otherwise flow opaquely into an Aeron channel URI and fail
     * deep inside the driver). Malformed entries fail with a descriptive message so a
     * bad {@code -Dkardamom.cluster.members} is debuggable from the container log.
     */
    static String[] memberEndpoints(final String clusterMembers, final int memberId) {
        for (final String member : clusterMembers.split("\\|")) {
            final String[] f = member.split(",");
            if (f.length < 6) {
                throw new IllegalArgumentException(
                    "cluster member entry needs 6 comma fields (id,ingress,consensus,log,catchup,archive), got "
                        + f.length + ": '" + member + "'");
            }
            final int id;
            try {
                id = Integer.parseInt(f[0].trim());
            } catch (final NumberFormatException e) {
                throw new IllegalArgumentException("non-numeric member id in entry '" + member + "'", e);
            }
            if (id == memberId) {
                return new String[] { f[1].trim(), f[2].trim(), f[3].trim(), f[4].trim(), f[5].trim() };
            }
        }
        throw new IllegalArgumentException("memberId " + memberId + " not in " + clusterMembers);
    }

    /** Resolve this member's id by matching nodeIp against each member's ingress host
     *  in the clusterMembers string. Used when memberId isn't given explicitly (the
     *  Nomad deploy passes the node IP, since alloc index != node). */
    static int memberIdForNodeIp(final String clusterMembers, final String nodeIp) {
        if (nodeIp == null || nodeIp.isEmpty()) {
            throw new IllegalStateException(
                "neither kardamom.cluster.memberId nor kardamom.cluster.nodeIp was set");
        }
        for (final String member : clusterMembers.split("\\|")) {
            final String[] f = member.split(",");
            if (f.length < 6) {
                throw new IllegalArgumentException("malformed cluster member entry: '" + member + "'");
            }
            final String ingressHost = f[1].trim().split(":")[0];
            if (ingressHost.equals(nodeIp.trim())) {
                return Integer.parseInt(f[0].trim());
            }
        }
        throw new IllegalArgumentException("nodeIp " + nodeIp + " matches no member ingress host in " + clusterMembers);
    }
}
