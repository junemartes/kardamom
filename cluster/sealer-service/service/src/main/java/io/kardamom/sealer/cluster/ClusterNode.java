package io.kardamom.sealer.cluster;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.io.File;
import org.agrona.SemanticVersion;
import org.agrona.concurrent.ShutdownSignalBarrier;

/** Boots an all-in-one Aeron Cluster member (media driver + archive +
 *  consensus module) running {@link SealerClusteredService}. Config via -D sysprops. */
public final class ClusterNode {
    // App version shared with the Rust cluster-client (kardamom-cluster-client's
    // SessionConnectRequest sends APP_SEMANTIC_VERSION = 0.3.0). Two Aeron checks
    // constrain this, and BOTH require MAJOR 0:
    //   1. session connect: the client's version major must equal the cluster's
    //      appVersion major;
    //   2. leadership term: a FRESH cluster's log version is 0.0.0, and Aeron
    //      rejects a major mismatch ("incompatible version: X log=0.0.0") which
    //      makes every member self-terminate.
    // Pinned to 0.3.0 (Aeron 1.44's default appVersion) on BOTH the ConsensusModule
    // and the ServiceContainer (they must agree).
    static final int APP_VERSION = SemanticVersion.compose(0, 3, 0);

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

        // Launch with a retry past the mark-file liveness window. A member that
        // was HARD-KILLED (kill -9 / docker kill) cannot clear its archive and
        // cluster mark files; agrona's guard then sees a fresh-enough activity
        // timestamp and ClusteredMediaDriver.launch throws
        // "IllegalStateException: active Mark file detected" until the stale
        // heartbeat ages out (~10s) — the chaos suite's "poison window": a
        // supervisor restarting the task within it burned restart attempts
        // until the member was stranded below quorum. Retrying IN-PROCESS
        // (fresh contexts each attempt — Aeron contexts are single-use) makes a
        // supervised restart deterministic while PRESERVING the double-run
        // guard: a genuinely live sibling on the same dirs keeps heartbeating,
        // so every retry still fails and we exit with the original error.
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        ClusteredMediaDriver driver = null;
        ClusteredServiceContainer container = null;
        for (int attempt = 1; ; attempt++) {
            try {
                driver = ClusteredMediaDriver.launch(
                    driverContext(aeronDir),
                    archiveContext(aeronDir, archiveDir, me),
                    consensusContext(aeronDir, clusterDir, clusterMembers, memberId, ingressStreamId, me, barrier));
                container = ClusteredServiceContainer.launch(
                    serviceContext(aeronDir, clusterDir, dedupCapacity, tickMs, memberId, barrier));
                break;
            } catch (final RuntimeException e) {
                org.agrona.CloseHelper.quietClose(driver);
                driver = null;
                if (!isActiveMarkFile(e) || attempt >= MAX_LAUNCH_ATTEMPTS) {
                    throw e;
                }
                System.out.println("cluster LAUNCH RETRY memberId=" + memberId + " attempt=" + attempt
                    + " — stale mark file from a hard-killed predecessor; waiting out the liveness window");
                try {
                    Thread.sleep(LAUNCH_RETRY_DELAY_MS);
                } catch (final InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }

        try (ClusteredMediaDriver ignored = driver;
             ClusteredServiceContainer ignored2 = container) {
            System.out.println("cluster node up memberId=" + memberId + " endpoints=" + String.join(",", me));
            barrier.await();
        }
    }

    /** Launch retries past the ~10s mark-file liveness window with margin. */
    static final int MAX_LAUNCH_ATTEMPTS = 6;
    static final long LAUNCH_RETRY_DELAY_MS = 5_000;

    /** Whether the launch failure is agrona's "active Mark file detected" guard. */
    static boolean isActiveMarkFile(final Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof IllegalStateException
                && c.getMessage() != null
                && c.getMessage().contains("active Mark file detected")) {
                return true;
            }
        }
        return false;
    }

    private static MediaDriver.Context driverContext(final String aeronDir) {
        return new MediaDriver.Context()
            .aeronDirectoryName(aeronDir)
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(false);
    }

        // Aeron 1.44 requires Archive.Context.replicationChannel to be set (no
        // default). It's the channel this archive receives replication on during
        // cluster catch-up (snapshot/log transfer between members). The standard
        // ClusteredMediaDriver pattern uses this node's IP with an OS-assigned
        // (ephemeral) port. me[*] all share this node's IP (host of the ingress ep).
    private static Archive.Context archiveContext(
            final String aeronDir, final String archiveDir, final String[] me) {
        final String nodeHost = me[0].split(":")[0];
        return new Archive.Context()
            .aeronDirectoryName(aeronDir)
            .archiveDir(new File(archiveDir))
            .controlChannel("aeron:udp?endpoint=" + me[4])
            .localControlChannel("aeron:ipc?term-length=64k")
            .replicationChannel("aeron:udp?endpoint=" + nodeHost + ":0")
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED);
    }

    private static ConsensusModule.Context consensusContext(
            final String aeronDir, final String clusterDir, final String clusterMembers,
            final int memberId, final int ingressStreamId, final String[] me,
            final ShutdownSignalBarrier barrier) {
        final ConsensusModule.Context ctx = new ConsensusModule.Context()
            .clusterMemberId(memberId)
            .clusterMembers(clusterMembers)
            .aeronDirectoryName(aeronDir)
            .clusterDir(new File(clusterDir))
            .ingressChannel("aeron:udp")
            // The cluster LOG rides Aeron's 64MB default terms -> a 192MB log
            // buffer for the log publication, PLUS 192MB per member image,
            // PLUS 192MB per election LogReplay — on the aeron tmpfs. At this
            // deployment's KB/s rates that's ~all of a 1GB tmpfs gone at
            // election time: members then die with 'insufficient usable
            // storage' (log replay / session response pubs) while Raft looks
            // healthy from outside. 8MB terms (24MB logs) leave an order of
            // magnitude of headroom without approaching flow-control limits.
            .logChannel("aeron:udp?term-length=8m")
            .ingressStreamId(ingressStreamId)
            .appVersion(APP_VERSION)
            // Client sessions must survive a full quorum outage END TO END
            // (kill 2/3 -> 15s stall window -> node restart -> member recovery
            // -> re-election: ~40-60s observed on CI) instead of expiring and
            // forcing clients through a reconnect. Without canonical-stream
            // replay on reconnect (a tracked follow-up), a session that dies
            // while commits continue re-attaches with an unrecoverable GAP —
            // the client then fail-stops (observed: the validator halting after
            // the quorum case at 30s). No commits happen during the outage
            // itself, so a SURVIVING session resumes gap-free. Clients
            // keep-alive every 1s; a genuinely dead client holds a session
            // slot for at most 90s, acceptable for this deployment's small,
            // long-lived client set.
            .sessionTimeoutNs(java.util.concurrent.TimeUnit.SECONDS.toNanos(90))
            // leaderHeartbeatTimeoutNs stays at Aeron's 10s default: raising it
            // to 20s was tried and made real failovers ~20s slower (leader-kill
            // recovery blew the 60s pipeline-progress SLO), while the "leader
            // heartbeat timeout" warnings it was meant to silence were benign.
            .replicationChannel("aeron:udp?endpoint=" + me[3]);

        // Announce self-termination on stdout — Aeron's DEFAULT termination
        // hook signals the shutdown barrier and the JVM exits 0 with NOTHING on
        // stderr or in the error log; the line makes the WHEN and the WHICH
        // grep-able next to the role lines the chaos suite already reads.
        ctx.terminationHook(() -> {
            System.out.println("cluster TERMINATION memberId=" + memberId
                + " component=CONSENSUS_MODULE (requested shutdown — e.g. election/state conflict on rejoin)");
            barrier.signal();
        });
        return ctx;
    }

    private static ClusteredServiceContainer.Context serviceContext(
            final String aeronDir, final String clusterDir, final int dedupCapacity,
            final long tickMs, final int memberId, final ShutdownSignalBarrier barrier) {
        final ClusteredServiceContainer.Context ctx = new ClusteredServiceContainer.Context()
            .aeronDirectoryName(aeronDir)
            .clusterDir(new File(clusterDir))
            .appVersion(APP_VERSION)
            .clusteredService(new SealerClusteredService(dedupCapacity, tickMs, memberId));
        // The clustered-service CONTAINER has its OWN termination hook;
        // instrumenting only the consensus module still exits silently when the
        // container is the one that terminates.
        ctx.terminationHook(() -> {
            System.out.println("cluster TERMINATION memberId=" + memberId
                + " component=SERVICE_CONTAINER (requested shutdown)");
            barrier.signal();
        });
        return ctx;
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
