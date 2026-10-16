package io.kardamom.sealer.cluster;

import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.ClusterTool;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.ElectionState;
import io.aeron.cluster.service.ClusteredServiceContainer;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;

/**
 * Samples the election state, the commit position, and the progress of a
 * log replication for the {@link JoinWatchdog}, once a second, and acts on
 * its verdict. It also watches the two components of the member, and ends
 * the process when one of them closes with no stop request.
 *
 * <p>Each action ends the process with {@link Runtime#halt}, not
 * {@link System#exit}. A graceful close joins the stuck agent thread and
 * can hang the same way. The relaunch then goes through the mark-file
 * retry loop of {@link ClusterNode}, which is the expected path after a
 * hard exit. The exit code marks the cause in the alloc's exit event.
 * Before the exit, the thread prints the error logs of the consensus
 * module, the media driver, and the service container, so the alloc log
 * holds the Aeron errors that led to the exit.</p>
 *
 * <ul>
 *   <li>An INIT wedge exits with code {@link #JOIN_WEDGE_EXIT_CODE}. The
 *       relaunch starts from the member's own state.</li>
 *   <li>A catch-up stall first deletes the recording log, then exits with
 *       code {@link #CATCHUP_STALL_EXIT_CODE}. The relaunch finds no
 *       recording log, so it seeds the member from a peer's latest
 *       snapshot. The rule does not act when every peer answers with an
 *       open election: then no leader is live, and no peer can seed the
 *       member. A peer with a closed election follows a live leader, and
 *       the live leader's log holds every committed entry.</li>
 *   <li>A leader wedge exits with code {@link #LEADER_WEDGE_EXIT_CODE}.
 *       The completion of a leader's election failed, so the leader has
 *       no ingress. The relaunch starts from the member's own state, and
 *       the members elect again.</li>
 *   <li>A closed component exits with code {@link #COMPONENT_CLOSED_EXIT_CODE}.
 *       An agent that throws in its start, for example on an archive
 *       request that times out while it loads its snapshot, records the
 *       error in its error log and closes. Aeron calls no termination hook
 *       for it, and the media driver and the archive keep the process
 *       alive. The member then never joins, and it takes no snapshot. The
 *       relaunch starts from the member's own state.</li>
 *   <li>A leader stall exits with code {@link #LEADER_STALL_EXIT_CODE}.
 *       The leader waits in LEADER_READY for a member that cannot join.
 *       The relaunch starts from the member's own state, and the members
 *       elect again.</li>
 *   <li>A follower stall exits with code {@link #FOLLOWER_STALL_EXIT_CODE}.
 *       The member found a leader and cannot join it. The relaunch keeps
 *       the recording log: it replays the member's own log, and its
 *       election starts again from that state.</li>
 * </ul>
 */
final class JoinWatchdogThread {

    /** How often the thread samples the member. */
    static final Interval POLL = new Interval(1_000);
    /** How often a leader in LEADER_READY reads the commit positions of its peers. */
    static final long PEER_POLL_MS = 5_000L;
    /** Process exit code when the election wedges in INIT. */
    static final int JOIN_WEDGE_EXIT_CODE = 3;
    /** Process exit code when the catch-up stalls and the member seeds from a peer. */
    static final int CATCHUP_STALL_EXIT_CODE = 4;
    /** Process exit code when a component closes with no stop request. */
    static final int COMPONENT_CLOSED_EXIT_CODE = 5;
    /** Process exit code when the completion of a leader's election fails. */
    static final int LEADER_WEDGE_EXIT_CODE = 6;
    /** Process exit code when a leader waits in LEADER_READY with no progress. */
    static final int LEADER_STALL_EXIT_CODE = 7;
    /** Process exit code when a member that found a leader makes no progress. */
    static final int FOLLOWER_STALL_EXIT_CODE = 8;

    /**
     * The member that the thread watches: its id, its two components, its
     * stop request, and the probe of its peers.
     */
    record Member(
            int memberId,
            ConsensusModule.Context consensus,
            ClusteredServiceContainer.Context service,
            StopRequest stop,
            PeerProbe peers) {

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

        /** The Aeron timeouts of the member's configuration. */
        JoinWatchdog.Timeouts timeouts() {
            return new JoinWatchdog.Timeouts(
                TimeUnit.NANOSECONDS.toMillis(consensus.leaderHeartbeatTimeoutNs()),
                TimeUnit.NANOSECONDS.toMillis(consensus.archiveContext().messageTimeoutNs()),
                consensus.aeron().context().driverTimeoutMs());
        }
    }

    /**
     * The archive recording with the highest position. During log
     * replication the member's archive extends its log recording, and no
     * other recording of the member is active.
     */
    record Recording(long recordingId, long position) {
        static final Recording NONE = new Recording(RecordingPos.NULL_RECORDING_ID, -1L);

        /** The active recording with the highest position, or {@link #NONE}. */
        static Recording highest(final CountersReader counters) {
            return IntStream.rangeClosed(0, counters.maxCounterId())
                .filter(id -> counters.getCounterState(id) == CountersReader.RECORD_ALLOCATED
                    && counters.getCounterTypeId(id) == RecordingPos.RECORDING_POSITION_TYPE_ID)
                .mapToObj(id -> new Recording(RecordingPos.getRecordingId(counters, id), counters.getCounterValue(id)))
                .max(Comparator.comparingLong(Recording::position))
                .orElse(NONE);
        }
    }

    private final Member member;
    private final JoinWatchdog watchdog;
    private final AtomicCounter electionState;
    private final AtomicCounter commitPosition;
    private final AtomicCounter controlToggle;
    private final StateDir clusterDir;
    /** The election state of the last sample; only this thread reads and writes it. */
    private ElectionState lastState;
    /** The log recording during the last log replication sample. */
    private Recording lastRecording = Recording.NONE;
    /** The last peer commit mark, and the time of the next peer read. */
    private long peerCommitMark = 0L;
    private long nextPeerPollMs = 0L;
    /** The windows, for the log lines. */
    private final long windowS;
    private final long stallWindowS;
    private final long stallLimitS;

    JoinWatchdogThread(
            final Member member,
            final StateDir clusterDir,
            final long windowS,
            final long stallWindowS) {
        this.member = member;
        final JoinWatchdog.Timeouts timeouts = member.timeouts();
        this.watchdog = new JoinWatchdog(timeouts.limits(windowS * 1000L, stallWindowS * 1000L));
        this.electionState = member.consensus().electionStateCounter();
        this.commitPosition = member.consensus().commitPositionCounter();
        this.controlToggle = member.consensus().controlToggleCounter();
        this.clusterDir = clusterDir;
        this.windowS = windowS;
        this.stallWindowS = stallWindowS;
        this.stallLimitS = timeouts.stallLimitMs() / 1000L;
    }

    void start() {
        final Thread thread = new Thread(this::runUntilStop, "kardamom-join-watchdog");
        thread.setDaemon(true);
        thread.start();
        System.out.println("cluster join watchdog up memberId=" + member.memberId() + " windowS=" + windowS
            + " catchupStallS=" + stallWindowS + " stallLimitS=" + stallLimitS);
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
        exit(COMPONENT_CLOSED_EXIT_CODE);
    }

    private void observe() {
        final long nowMs = System.currentTimeMillis();
        final long commit = commitPosition.get();
        final ElectionState state = ElectionState.get(electionState);
        logStateChange(state, commit);
        final JoinWatchdog.Sample sample = new JoinWatchdog.Sample(
            state, commit, replicationPosition(state, commit), peerCommitMark(state, nowMs), controlToggle.get(), nowMs);
        act(watchdog.observe(sample), commit, nowMs);
    }

    private void act(final JoinWatchdog.Verdict verdict, final long commit, final long nowMs) {
        switch (verdict) {
            case INIT_WEDGE -> initWedge(nowMs);
            case CATCHUP_STALL -> catchupStall(commit, nowMs);
            case LEADER_WEDGE -> leaderWedge(nowMs);
            case LEADER_STALL -> leaderStall(commit, nowMs);
            case FOLLOWER_STALL -> followerStall(commit, nowMs);
            case PEER_CHECK -> peerCheck(commit, nowMs);
            case NONE -> { }
        }
    }

    /**
     * Log the election state when it differs from the last sample. The
     * samples are one second apart, so a short state can pass unlogged.
     * The lines show where an election waits, and in which term: the
     * candidate term is the highest term that the member voted in or stood
     * for, and the log term is the term of the last term entry of its
     * recording log, which a leader writes when it starts its term and a
     * follower when it joins the leader's log.
     */
    private void logStateChange(final ElectionState state, final long commit) {
        if (state == lastState) {
            return;
        }
        lastState = state;
        System.out.println("cluster ELECTION memberId=" + member.memberId() + " state=" + state
            + " commitPosition=" + commit
            + " candidateTermId=" + member.consensus().nodeStateFile().candidateTerm().candidateTermId()
            + " logLeadershipTermId=" + clusterDir.lastTermId().map(String::valueOf).orElse("unknown"));
    }

    /**
     * The position of the log recording while the member replicates the
     * leader's log, else the last one. Each change is logged, so a
     * replication that stops shows where it stopped.
     */
    private long replicationPosition(final ElectionState state, final long commit) {
        if (state != ElectionState.FOLLOWER_LOG_REPLICATION) {
            return lastRecording.position();
        }
        final Recording recording = Recording.highest(member.consensus().aeron().countersReader());
        if (!recording.equals(lastRecording)) {
            System.out.println("cluster REPLICATION memberId=" + member.memberId()
                + " recordingId=" + recording.recordingId()
                + " position=" + recording.position()
                + " commitPosition=" + commit
                + (recording.equals(Recording.NONE) ? " (no active recording)" : ""));
        }
        lastRecording = recording;
        return recording.position();
    }

    /**
     * The commit mark of the peers while the member waits in LEADER_READY,
     * read every {@link #PEER_POLL_MS}, else the last one.
     */
    private long peerCommitMark(final ElectionState state, final long nowMs) {
        if (state == ElectionState.LEADER_READY && nowMs >= nextPeerPollMs) {
            nextPeerPollMs = nowMs + PEER_POLL_MS;
            peerCommitMark = member.peers().commitMark();
        }
        return peerCommitMark;
    }

    /**
     * Ask the peers whether one of them has a closed election, and act on
     * the verdict that follows. A seed that waits for a live peer logs once.
     */
    private void peerCheck(final long commit, final long nowMs) {
        final PeerProbe.Round round = member.peers().ask();
        final JoinWatchdog.Verdict verdict = watchdog.afterPeerCheck(round.elections(), nowMs);
        round.closedPeer().filter(peer -> verdict == JoinWatchdog.Verdict.NONE).ifPresent(peer ->
            System.out.println("cluster FOLLOWER STALL memberId=" + member.memberId()
                + " deferred: peer memberId=" + peer.memberId() + " has a closed election, so a seed"
                + " can recover this member; the CATCHUP STALL rule decides at " + stallWindowS + "s"));
        act(verdict, commit, nowMs);
    }

    private void initWedge(final long nowMs) {
        System.out.println("cluster JOIN WEDGE memberId=" + member.memberId()
            + " election stuck in INIT for " + watchdog.initForMs(nowMs) / 1000L
            + "s (window " + windowS + "s); exiting for a clean relaunch (issue #195)");
        exit(JOIN_WEDGE_EXIT_CODE);
    }

    private void leaderWedge(final long nowMs) {
        System.out.println("cluster LEADER WEDGE memberId=" + member.memberId()
            + " election stuck in " + lastState + " with an active control toggle for "
            + watchdog.halfElectedForMs(nowMs) / 1000L
            + "s: the completion of the election failed and the leader has no ingress;"
            + " exiting for a clean relaunch so the members elect again");
        exit(LEADER_WEDGE_EXIT_CODE);
    }

    private void leaderStall(final long commit, final long nowMs) {
        System.out.println("cluster LEADER STALL memberId=" + member.memberId()
            + " commitPosition=" + commit
            + " in LEADER_READY with no progress on this member or its peers for "
            + watchdog.sinceProgressMs(nowMs) / 1000L + "s (limit " + stallLimitS + "s):"
            + " a member that voted cannot join this term; exiting for a relaunch from the own state"
            + " so the members elect again");
        exit(LEADER_STALL_EXIT_CODE);
    }

    private void followerStall(final long commit, final long nowMs) {
        System.out.println("cluster FOLLOWER STALL memberId=" + member.memberId()
            + " commitPosition=" + commit
            + " state=" + lastState
            + " no progress for " + watchdog.stallForMs(nowMs) / 1000L + "s after the member found a leader"
            + " (limit " + stallLimitS + "s), and every peer answered with an open election, or the member"
            + " never reached a catch-up; exiting for a relaunch that keeps the recording log");
        exit(FOLLOWER_STALL_EXIT_CODE);
    }

    private void catchupStall(final long commit, final long nowMs) {
        System.out.println("cluster CATCHUP STALL memberId=" + member.memberId()
            + " commitPosition=" + commit
            + " no commit progress for " + watchdog.stallForMs(nowMs) / 1000L
            + "s while the election cycles through catch-up (window " + stallWindowS
            + "s): the leader no longer holds this member's log tail; dropping the recording log"
            + " so the relaunch seeds from a peer");
        clusterDir.dropRecordingLog();
        exit(CATCHUP_STALL_EXIT_CODE);
    }

    /**
     * Print the Aeron error logs of the member, then end the process with
     * {@code code}. Each line carries the {@code cluster ERROR LOG} prefix,
     * so a filter of the consensus lines keeps it. The exit runs even when
     * the print throws.
     */
    private void exit(final int code) {
        try {
            final ByteArrayOutputStream errors = new ByteArrayOutputStream();
            ClusterTool.errors(new PrintStream(errors, true, StandardCharsets.UTF_8), clusterDir.root().toFile());
            final String prefix = "cluster ERROR LOG memberId=" + member.memberId() + " | ";
            errors.toString(StandardCharsets.UTF_8).lines().map(prefix::concat).forEach(System.out::println);
        } finally {
            System.out.flush();
            Runtime.getRuntime().halt(code);
        }
    }
}
