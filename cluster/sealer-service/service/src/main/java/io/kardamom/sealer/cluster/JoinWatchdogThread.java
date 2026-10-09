package io.kardamom.sealer.cluster;

import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.ElectionState;
import io.aeron.cluster.service.ClusteredServiceContainer;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * Samples the election state and the commit position for the
 * {@link JoinWatchdog}, once a second, and acts on its verdict. It also
 * watches the two components of the member, and ends the process when one
 * of them closes with no stop request.
 *
 * <p>Each action ends the process with {@link Runtime#halt}, not
 * {@link System#exit}. A graceful close joins the stuck agent thread and
 * can hang the same way. The relaunch then goes through the mark-file
 * retry loop of {@link ClusterNode}, which is the expected path after a
 * hard exit. The exit code marks the cause in the alloc's exit event.</p>
 *
 * <ul>
 *   <li>An INIT wedge exits with code {@link #JOIN_WEDGE_EXIT_CODE}. The
 *       relaunch starts from the member's own state.</li>
 *   <li>A catch-up stall first deletes the recording log, then exits with
 *       code {@link #CATCHUP_STALL_EXIT_CODE}. The relaunch finds no
 *       recording log, so it seeds the member from a peer's latest
 *       snapshot. The member loses no data that the cluster needs: it
 *       reached a catch-up state, so a leader holds a longer log, and the
 *       leader's log holds every committed entry.</li>
 *   <li>A closed component exits with code {@link #COMPONENT_CLOSED_EXIT_CODE}.
 *       An agent that throws in its start, for example on an archive
 *       request that times out while it loads its snapshot, records the
 *       error in its error log and closes. Aeron calls no termination hook
 *       for it, and the media driver and the archive keep the process
 *       alive. The member then never joins, and it takes no snapshot. The
 *       relaunch starts from the member's own state.</li>
 * </ul>
 */
final class JoinWatchdogThread {

    /** How often the thread samples the member. */
    static final Interval POLL = new Interval(1_000);
    /** Process exit code when the election wedges in INIT. */
    static final int JOIN_WEDGE_EXIT_CODE = 3;
    /** Process exit code when the catch-up stalls. */
    static final int CATCHUP_STALL_EXIT_CODE = 4;
    /** Process exit code when a component closes with no stop request. */
    static final int COMPONENT_CLOSED_EXIT_CODE = 5;

    /**
     * The member that the thread watches: its id, its two components, and
     * its stop request.
     */
    record Member(
            int memberId,
            ConsensusModule.Context consensus,
            ClusteredServiceContainer.Context service,
            StopRequest stop) {

        /**
         * The name of the first closed component, or empty while both run.
         * A component closes its own Aeron client when its agent closes.
         */
        Optional<String> closedComponent() {
            return Stream.of(
                    Map.entry("CONSENSUS_MODULE", consensus.aeron()),
                    Map.entry("SERVICE_CONTAINER", service.aeron()))
                .filter(component -> component.getValue().isClosed())
                .map(Map.Entry::getKey)
                .findFirst();
        }
    }

    private final Member member;
    private final JoinWatchdog watchdog;
    private final AtomicCounter electionState;
    private final AtomicCounter commitPosition;
    private final StateDir clusterDir;
    /** The INIT window and the stall window, for the log lines. */
    private final long windowS;
    private final long stallWindowS;

    JoinWatchdogThread(
            final Member member,
            final StateDir clusterDir,
            final long windowS,
            final long stallWindowS) {
        this.member = member;
        this.watchdog = new JoinWatchdog(windowS * 1000L, stallWindowS * 1000L);
        this.electionState = member.consensus().electionStateCounter();
        this.commitPosition = member.consensus().commitPositionCounter();
        this.clusterDir = clusterDir;
        this.windowS = windowS;
        this.stallWindowS = stallWindowS;
    }

    void start() {
        final Thread thread = new Thread(this::runUntilStop, "kardamom-join-watchdog");
        thread.setDaemon(true);
        thread.start();
        System.out.println("cluster join watchdog up memberId=" + member.memberId() + " windowS=" + windowS
            + " catchupStallS=" + stallWindowS);
    }

    private void runUntilStop() {
        while (POLL.await() && !member.stop().isRequested()) {
            sample();
        }
    }

    /**
     * One sample. The closed state is read before the stop request: a
     * requested stop sets the request before any component closes, so a
     * closed component with no request is a failure.
     */
    private void sample() {
        member.closedComponent().ifPresentOrElse(this::componentClosed, this::observe);
    }

    private void componentClosed(final String component) {
        if (member.stop().isRequested()) {
            return;
        }
        System.out.println("cluster COMPONENT CLOSED memberId=" + member.memberId()
            + " component=" + component
            + " closed with no stop request (its error log in the cluster dir holds the cause);"
            + " exiting for a clean relaunch");
        halt(COMPONENT_CLOSED_EXIT_CODE);
    }

    private void observe() {
        final long nowMs = System.currentTimeMillis();
        final long commit = commitPosition.get();
        switch (watchdog.observe(ElectionState.get(electionState), commit, nowMs)) {
            case INIT_WEDGE -> initWedge(nowMs);
            case CATCHUP_STALL -> catchupStall(commit, nowMs);
            case NONE -> { }
        }
    }

    private void initWedge(final long nowMs) {
        System.out.println("cluster JOIN WEDGE memberId=" + member.memberId()
            + " election stuck in INIT for " + watchdog.initForMs(nowMs) / 1000L
            + "s (window " + windowS + "s); exiting for a clean relaunch (issue #195)");
        halt(JOIN_WEDGE_EXIT_CODE);
    }

    private void catchupStall(final long commit, final long nowMs) {
        System.out.println("cluster CATCHUP STALL memberId=" + member.memberId()
            + " commitPosition=" + commit
            + " no commit progress for " + watchdog.stallForMs(nowMs) / 1000L
            + "s while the election cycles through catch-up (window " + stallWindowS
            + "s): the leader no longer holds this member's log tail; dropping the recording log"
            + " so the relaunch seeds from a peer");
        clusterDir.dropRecordingLog();
        halt(CATCHUP_STALL_EXIT_CODE);
    }

    private static void halt(final int code) {
        System.out.flush();
        Runtime.getRuntime().halt(code);
    }

}
