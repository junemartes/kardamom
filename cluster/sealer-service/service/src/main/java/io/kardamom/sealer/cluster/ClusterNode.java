package io.kardamom.sealer.cluster;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.cluster.ClusterTool;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.io.File;
import org.agrona.SemanticVersion;
import org.agrona.concurrent.ShutdownSignalBarrier;

/**
 * Boots an all-in-one Aeron Cluster member: a media driver, an archive, and
 * a consensus module, running {@link SealerClusteredService}.
 * Configure it with -D system properties.
 */
public final class ClusterNode {
    // App version shared with the Rust cluster-client (kardamom-cluster-client's
    // SessionConnectRequest sends APP_SEMANTIC_VERSION = 0.3.0). Two Aeron checks
    // constrain this value, and both require major version 0:
    //   1. Session connect: the client's version major must equal the
    //      cluster's appVersion major.
    //   2. Leadership term: a fresh cluster's log version is 0.0.0, and
    //      Aeron rejects a major mismatch ("incompatible version:
    //      X log=0.0.0"), which makes every member self-terminate.
    // Pinned to 0.3.0 (Aeron 1.44's default appVersion) on both the
    // ConsensusModule and the ServiceContainer. They must agree.
    static final int APP_VERSION = SemanticVersion.compose(0, 3, 0);

    public static void main(final String[] args) {
        // "0,ingressHost:port,consensusHost:port,logHost:port,catchupHost:port,archiveHost:port|1,...|2,..."
        final String clusterMembers = System.getProperty("kardamom.cluster.members");
        if (clusterMembers == null) throw new IllegalStateException("kardamom.cluster.members not set");
        // memberId: use an explicit -Dkardamom.cluster.memberId when given
        // (>=0, for example from TestCluster), else derive it from this
        // node's IP. The Nomad deploy passes the node IP, not an explicit
        // id, because the alloc index does not match the node it lands on.
        final int memberIdProp = Integer.getInteger("kardamom.cluster.memberId", -1);
        final String nodeIp = System.getProperty("kardamom.cluster.nodeIp");
        final int memberId = (memberIdProp >= 0) ? memberIdProp : memberIdForNodeIp(clusterMembers, nodeIp);
        final String aeronDir = System.getProperty("aeron.dir", "/opt/kardamom/aeron-mount/dir");
        final String clusterDir = System.getProperty("kardamom.cluster.dir", "/opt/kardamom/cluster");
        final String archiveDir = System.getProperty("kardamom.archive.dir", "/opt/kardamom/archive");
        final int ingressStreamId = Integer.getInteger("kardamom.cluster.ingressStreamId", 101);
        final long tickMs = Long.getLong("kardamom.cluster.tickMs", 2000L);
        // Dedup window: this must exceed the worst-case racing-replica stall
        // multiplied by the peak unique-record throughput, and every member
        // must use the same value. See SealerWire.DEFAULT_DEDUP_CAPACITY for
        // the sizing math.
        final int dedupCapacity = Integer.getInteger(
            "kardamom.cluster.dedupCapacity", SealerWire.DEFAULT_DEDUP_CAPACITY);

        final String[] me = memberEndpoints(clusterMembers, memberId); // [ingress,consensus,log,catchup,archive]

        // Launch with a retry past the mark-file liveness window. A member
        // that was hard-killed (kill -9 or docker kill) cannot clear its
        // archive and cluster mark files. Agrona's guard then sees a
        // fresh-enough activity timestamp, and ClusteredMediaDriver.launch
        // throws "IllegalStateException: active Mark file detected" until
        // the stale heartbeat ages out, after about 10 seconds. In this
        // window, a supervisor that restarts the task can burn all its
        // restart attempts and strand the member below quorum. Retrying
        // in-process, with fresh contexts each attempt since Aeron contexts
        // are single-use, makes a supervised restart reliable. It also keeps
        // the double-run guard: a genuinely live sibling on the same
        // directories keeps sending heartbeats, so every retry still fails
        // and this exits with the original error.
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
            startSnapshotScheduler(clusterDir, memberId);
            barrier.await();
        }
    }

    /**
     * Periodic cluster-wide snapshot trigger
     * ({@code -Dkardamom.cluster.snapshotIntervalS}, 0 disables it).
     *
     * <p>This runs the same way on every member. Only the leader's control
     * toggle accepts SNAPSHOT. The action is appended to the replicated log,
     * so all members then snapshot at the same log position. On a follower,
     * {@link ClusterTool#snapshot} is a no-op that returns false. So the
     * current leader snapshots on schedule, with no cross-member
     * coordination and no special deploy case. Failures are logged, and the
     * next tick retries. A snapshot trigger must never take a member
     * down.</p>
     */
    private static void startSnapshotScheduler(final String clusterDir, final int memberId) {
        final long intervalS = Long.getLong("kardamom.cluster.snapshotIntervalS", 300L);
        if (intervalS <= 0) {
            System.out.println("cluster snapshot scheduler DISABLED memberId=" + memberId);
            return;
        }
        final Thread t = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(intervalS * 1000L);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                try {
                    if (ClusterTool.snapshot(new File(clusterDir), System.out)) {
                        System.out.println("cluster SNAPSHOT triggered memberId=" + memberId);
                    }
                } catch (final Exception e) {
                    System.out.println("cluster SNAPSHOT attempt failed memberId=" + memberId
                        + " (leader may be mid-election; next tick retries): " + e);
                }
            }
        }, "kardamom-snapshot-scheduler");
        t.setDaemon(true);
        t.start();
        System.out.println("cluster snapshot scheduler up memberId=" + memberId
            + " intervalS=" + intervalS);
    }

    /** Launch retries past the ~10s mark-file liveness window, with margin. */
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

        // Aeron 1.44 requires Archive.Context.replicationChannel to be set;
        // it has no default. This is the channel this archive uses to
        // receive replication during cluster catch-up (snapshot and log
        // transfer between members). The standard ClusteredMediaDriver
        // pattern uses this node's IP with an OS-assigned (ephemeral) port.
        // Every entry in me[*] shares this node's IP, the host of the
        // ingress endpoint.
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
            // The cluster log uses Aeron's 64MB default term length. That
            // gives a 192MB log buffer for the log publication, plus 192MB
            // per member image, plus 192MB per election LogReplay, all on
            // the Aeron tmpfs. At this deployment's KB/s rates, that uses
            // almost all of a 1GB tmpfs at election time. Members then fail
            // with "insufficient usable storage" for log replay or session
            // response publications, while Raft looks healthy from outside.
            // 8MB terms (24MB logs) leave an order of magnitude of headroom
            // without approaching flow-control limits.
            .logChannel("aeron:udp?term-length=8m")
            .ingressStreamId(ingressStreamId)
            .appVersion(APP_VERSION)
            // Client sessions must survive a full quorum outage end to end
            // (kill 2 of 3 members, stall, node restart, member recovery,
            // re-election) instead of expiring and forcing a client
            // reconnect. Without canonical-stream replay on reconnect (a
            // tracked follow-up), a session that dies while commits continue
            // re-attaches with an unrecoverable gap, and the client then
            // fail-stops. No commits happen during the outage itself, so a
            // surviving session resumes with no gap. Clients send a
            // keep-alive every second. A genuinely dead client holds a
            // session slot for at most 90 seconds, which is acceptable for
            // this deployment's small, long-lived client set.
            .sessionTimeoutNs(java.util.concurrent.TimeUnit.SECONDS.toNanos(90))
            // The Aeron default (10) sits at this deployment's normal session
            // count (3 executors, 1 validator, 4 sequencer publishers, plus
            // transient smoke and load clients), so any reconnect churn locks
            // everyone out. A forced re-establishment leaks its old session
            // for up to the 90-second timeout above, and a consumer-side
            // egress-silence storm can exhaust the slots within seconds,
            // and rejected connects then add more load to the module. 256
            // gives the storm three orders of magnitude of headroom, while
            // the 90-second timeout still reaps zombie sessions.
            .maxConcurrentSessions(256)
            // leaderHeartbeatTimeoutNs stays at Aeron's 10-second default.
            // Raising it to 20 seconds was tried and made real failovers
            // about 20 seconds slower, missing the 60-second pipeline
            // progress SLO on leader-kill recovery, while the "leader
            // heartbeat timeout" warnings it aimed to silence were harmless.
            .replicationChannel("aeron:udp?endpoint=" + me[3]);

        // Log self-termination to stdout. Aeron's default termination hook
        // signals the shutdown barrier, and the JVM exits with code 0 and
        // nothing in stderr or the error log. This line makes the when and
        // the which grep-able next to the role lines the chaos suite reads.
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
        // The clustered-service container has its own termination hook.
        // Instrumenting only the consensus module would still exit silently
        // when the container is the one that terminates.
        ctx.terminationHook(() -> {
            System.out.println("cluster TERMINATION memberId=" + memberId
                + " component=SERVICE_CONTAINER (requested shutdown)");
            barrier.signal();
        });
        return ctx;
    }

    /**
     * Extract this member's 5 endpoints from the pipe- and comma-separated
     * clusterMembers string.
     * Each member entry is {@code id,ingress,consensus,log,catchup,archive},
     * and entries are pipe-separated. This trims each endpoint, since stray
     * whitespace from the rendered Nomad template would otherwise flow into
     * an Aeron channel URI and fail deep inside the driver. A malformed
     * entry fails with a clear message, so a bad
     * {@code -Dkardamom.cluster.members} value is easy to debug from the
     * container log.
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

    /**
     * Resolve this member's id by matching nodeIp against each member's
     * ingress host in the clusterMembers string.
     * Used when memberId is not given explicitly. The Nomad deploy passes
     * the node IP, since the alloc index does not match the node.
     */
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
