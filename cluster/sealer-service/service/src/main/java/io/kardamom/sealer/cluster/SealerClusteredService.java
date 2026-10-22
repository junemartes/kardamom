package io.kardamom.sealer.cluster;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import io.kardamom.sealer.Boundary;
import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.DecisionRules;
import io.kardamom.sealer.ClusterStatus;
import io.kardamom.sealer.LagBudgets;
import io.kardamom.sealer.OrderingWindow;
import io.kardamom.sealer.OriginOutcome;
import io.kardamom.sealer.Relayed;
import io.kardamom.sealer.VersionSwitch;
import io.kardamom.sealer.RemoteOriginAdvance;
import io.kardamom.sealer.SealerSeed;
import io.kardamom.sealer.TxOffer;
import io.kardamom.sealer.VoidLedger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Thin Aeron Cluster {@link ClusteredService} that sends all deterministic
 * canonical logic to the pure POJO {@link CanonicalSealerState}.
 *
 * <p>This class owns only the cluster plumbing: ingress decoding and dispatch,
 * timer scheduling, and the lifecycle hooks. It holds no canonical state of
 * its own. Keeping the state machine free of Aeron means its unit tests run
 * with no Aeron jars on the classpath (see the {@code core} subproject). The
 * wire layout (envelope offsets, message kinds, and the Java&harr;Rust
 * contract) lives in {@link SealerWire}. The egress framing, offer, and
 * retention logic lives in {@link SealerEgress}. Snapshot stream I/O lives in
 * {@link SnapshotIo}.</p>
 */
public final class SealerClusteredService implements ClusteredService {

    /** Correlation id used when scheduling the repeating boundary timer. */
    public static final long BOUNDARY_TIMER_CORRELATION_ID = 1L;

    /**
     * Correlation id of the ordering window's hold timer. It is armed when a
     * record opens a window and fires {@link OrderingWindow#HOLD_MS} later.
     * The expiry is a log event, so every member flushes at the same log
     * position. A stale expiry on an empty window is a no-op.
     */
    public static final long WINDOW_TIMER_CORRELATION_ID = 2L;

    /**
     * One record held in the ordering window: the parsed guard header, the
     * relayed payload, and the session to answer a reject to.
     */
    private static final class HeldRecord {
        final ClientSession session;
        final byte[] canonicalId;
        final byte[] sender;
        final long nonce;
        final long deadline;
        final byte[] payload;

        HeldRecord(
                final ClientSession session, final byte[] canonicalId, final byte[] sender,
                final long nonce, final long deadline, final byte[] payload) {
            this.session = session;
            this.canonicalId = canonicalId;
            this.sender = sender;
            this.nonce = nonce;
            this.deadline = deadline;
            this.payload = payload;
        }
    }

    /** Tick cadence for the boundary timer, in ms. Matches the 250 ms L2 tick. */
    private final long tickIntervalMs;
    private final int dedupCapacity;
    /** This cluster member's id, logged on role changes for the chaos suite. */
    private final int memberId;
    /**
     * Peer chain ids accepted as remote origins. Empty disables interop.
     * Every member must run the same list: it decides accept-or-reject in
     * the replicated state machine, like the dedup capacity.
     */
    private final Set<Long> remoteOrigins;

    /**
     * The seed this member starts from when the cluster has no snapshot.
     * Empty starts it at genesis. A snapshot, when one exists, wins.
     */
    private Optional<SealerSeed> seed = Optional.empty();

    /**
     * The retained-file settings, or empty to keep the retained frames on
     * the heap. Replicated configuration: the choice decides the layout of
     * the retention section of the snapshot, so every member runs the same.
     */
    private Optional<RetainedFile.Config> retainedFile = Optional.empty();
    /** The member feed settings, or empty for no feed. */
    private Optional<FeedConfig> feedConfig = Optional.empty();
    /** The member feed. The service thread opens it in {@code onStart}. */
    private Feed feed = Feed.Off.INSTANCE;

    private Cluster cluster;
    private CanonicalSealerState state;
    /**
     * The store of the retained frames. The service thread opens it in
     * {@code onStart}. The admin thread reads its published head through
     * {@link #retainedHead()}.
     */
    private volatile RetainedFrames retained;
    private SealerEgress egress;
    private SeedRecord seedRecord;
    private SnapshotRequests snapshots;

    /** The periodic snapshot. Empty leaves the snapshots to an operator and to the seed. */
    private Optional<SnapshotSchedule> snapshotSchedule = Optional.empty();

    /**
     * The log position of the last entry this service applied. The admin
     * endpoint reads it from another thread and compares it with the
     * commit position: the gap is the service's lag behind the log.
     */
    private volatile long servicePosition;

    /**
     * The leadership term whose start this service applied last, or -1
     * before any. The leader commits the start of its term, so a term here
     * is one that a quorum took. The start hold of each member compares it
     * with its peers ({@link StartHoldRule}).
     */
    private volatile long leadershipTermId = -1L;

    /** The hold of this member's outputs after its start; over at once when no member thread watches it. */
    private StartHold startHold = StartHold.released();

    /**
     * The snapshot marks and the posted head, for the log purge thread.
     * The service thread replaces the value after a snapshot and after the
     * posted head moves; the purge thread reads it.
     */
    private volatile PurgeView purgeView = PurgeView.EMPTY;

    /** Malformed ingress frames dropped (logged at power-of-two counts). */
    private long droppedFrameCount = 0;

    /**
     * Ingress frames of a kind this release does not know, dropped (logged
     * at power-of-two counts). The admin thread and the tests read it.
     */
    private volatile long unknownKindCount = 0;

    /**
     * The version of the newest snapshot this member restored or took, or
     * 0 before either. The admin thread reads it: a deploy refuses a
     * release that cannot read this version.
     */
    private volatile int latestSnapshotVersion = 0;

    /**
     * Cluster time of the last boundary tick or timer arm.
     * This is the liveness watermark for the boundary clock. Pending cluster
     * timers live in the leader's wheel only. Rapid election churn (a killed
     * leader restarting and re-contesting) can strand every member without a
     * live timer: a term's re-arm from {@link #onNewLeadershipTermEvent} runs
     * only on a module that is still leader when the call lands, so a re-arm
     * that races a later election is silently dropped. The clock then stops
     * forever, while elections, replay serving, and session traffic all still
     * look healthy. {@link #maybeReviveBoundaryClock()} uses this watermark to
     * revive the clock from log-driven callbacks.
     */
    private long lastBoundaryClockMs = 0L;

    /**
     * Boundary ticks this process applied, for the stdout heartbeat only.
     * It is not part of the replicated state and is never snapshotted.
     */
    private long boundaryTicks = 0L;

    /** One heartbeat line per this many boundary ticks (a minute at a 2 s tick). */
    private static final long BOUNDARY_TICK_LOG_EVERY = 30L;

    /** Contiguity rejects emitted (logged at power-of-two counts). */
    private long rejectedFrameCount = 0;
    private long pastDeadlineCount = 0;
    private long windowFullCount = 0;

    /**
     * The decision rules of the member, for the admin thread. The service
     * thread sets them after the start, at each activation record that
     * applies, and at each switch.
     */
    private volatile DecisionRules.View publishedRules = DecisionRules.START_VIEW;

    /** The feature activation records that the switch follows late or not at all. */
    private long lateActivations;

    /** The activation records of the carry flag that came while version 4 was off: the switch does not follow them. */
    private long earlyActivations;

    /** Version-4 references that came a window behind their image (logged at power-of-two counts). */
    private long expiredRefCount;

    /** The transaction hash of the reference on the version-4 path: one array for every record. */
    private final byte[] refId = new byte[CanonicalSealerState.CANONICAL_ID_LEN];

    /** The view of the full reference on the record path of decision version 5. */
    private final TxRefOffer carried = new TxRefOffer();

    /** {@link #orderCarried}: the frame holds no whole envelope, or its deadline field holds no position. */
    private static final long CARRIED_MALFORMED = Long.MIN_VALUE;

    /** Remote-origin rejects emitted (logged at power-of-two counts). */
    private long remoteRejectedFrameCount = 0;

    /** Origin-gap rejects emitted (logged at power-of-two counts). */
    private long originGapCount = 0;

    private final VoidLedger.Config voidConfig;
    /**
     * The inclusion horizon, in blocks. Replicated configuration: every
     * member must agree on it, because it bounds the dedup window and the
     * deadline the sealer assigns a marker.
     */
    private final long inclusionHorizonBlocks;
    /**
     * The DA-lag and the record-lag budgets. Replicated configuration:
     * they decide accept-or-reject inside the replicated state machine.
     * Zero turns a guard off.
     */
    private final LagBudgets budgets;
    /** DA-lag rejects emitted (logged at power-of-two counts). */
    private long daLagRejectCount = 0;
    /** Record-lag rejects emitted (logged at power-of-two counts). */
    private long recordLagRejectCount = 0;
    /** Moves up of the best recorded cursor (logged at power-of-two counts). */
    private long recordedAdvanceCount = 0;
    /**
     * The priority window in front of the record path. Replicated
     * configuration: its size decides the relay order, so every member runs
     * the same value, and a snapshot restore checks it. Size 0 passes every
     * record through at once.
     */
    private final OrderingWindow<HeldRecord> window;

    /**
     * Scratch buffer for the id of an origin, remote-origin, or void
     * frame.
     * Reused to avoid a per-message allocation on the single cluster
     * service thread. A held record copies its id instead.
     */
    private final byte[] canonicalIdScratch = new byte[CanonicalSealerState.CANONICAL_ID_LEN];

    public SealerClusteredService(
            int dedupCapacity,
            long tickIntervalMs,
            int memberId,
            Set<Long> remoteOrigins,
            VoidLedger.Config voidConfig) {
        this(dedupCapacity, tickIntervalMs, memberId, remoteOrigins, voidConfig,
            CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS);
    }

    /** The constructor with this member's inclusion horizon, no ordering window, and the default DA-lag budget. */
    public SealerClusteredService(
            int dedupCapacity,
            long tickIntervalMs,
            int memberId,
            Set<Long> remoteOrigins,
            VoidLedger.Config voidConfig,
            long inclusionHorizonBlocks) {
        this(dedupCapacity, tickIntervalMs, memberId, remoteOrigins, voidConfig,
            inclusionHorizonBlocks, CanonicalSealerState.DEFAULT_ORDERING_WINDOW);
    }

    /** The constructor with this member's ordering window size (0 for off) and the default DA-lag budget. */
    public SealerClusteredService(
            int dedupCapacity,
            long tickIntervalMs,
            int memberId,
            Set<Long> remoteOrigins,
            VoidLedger.Config voidConfig,
            long inclusionHorizonBlocks,
            int orderingWindow) {
        this(dedupCapacity, tickIntervalMs, memberId, remoteOrigins, voidConfig,
            inclusionHorizonBlocks, orderingWindow, LagBudgets.DEFAULT);
    }

    /** The full constructor, with this member's ordering window size and lag budgets. */
    public SealerClusteredService(
            int dedupCapacity,
            long tickIntervalMs,
            int memberId,
            Set<Long> remoteOrigins,
            VoidLedger.Config voidConfig,
            long inclusionHorizonBlocks,
            int orderingWindow,
            LagBudgets budgets) {
        this.inclusionHorizonBlocks = inclusionHorizonBlocks;
        this.window = new OrderingWindow<>(orderingWindow);
        this.budgets = budgets;
        this.dedupCapacity = dedupCapacity;
        this.tickIntervalMs = tickIntervalMs;
        this.memberId = memberId;
        this.remoteOrigins = Set.copyOf(remoteOrigins);
        this.voidConfig = voidConfig;
    }

    /** A service that refuses every void request (no configured voters). */
    public SealerClusteredService(
            int dedupCapacity, long tickIntervalMs, int memberId, Set<Long> remoteOrigins) {
        this(dedupCapacity, tickIntervalMs, memberId, remoteOrigins, VoidLedger.Config.DISABLED);
    }

    /** A service with interop disabled (an empty remote-origin allowlist). */
    public SealerClusteredService(int dedupCapacity, long tickIntervalMs, int memberId) {
        this(dedupCapacity, tickIntervalMs, memberId, Set.of());
    }

    public SealerClusteredService(int dedupCapacity, long tickIntervalMs) {
        this(dedupCapacity, tickIntervalMs, -1);
    }

    public SealerClusteredService() {
        this(SealerWire.DEFAULT_DEDUP_CAPACITY, CanonicalSealerState.TICK_INTERVAL_MS);
    }

    /**
     * Start from {@code seed} when the cluster has no snapshot, instead of
     * at genesis. A seed carries no peer anchor, so a member with interop
     * on refuses it.
     *
     * @throws IllegalArgumentException if the remote-origin allowlist is
     *         not empty
     */
    SealerClusteredService seededFrom(final SealerSeed seed) {
        if (!remoteOrigins.isEmpty()) {
            throw new IllegalArgumentException("a seed carries no remote-origin anchor, so a seeded cluster"
                + " runs with interop off; the allowlist is " + remoteOrigins);
        }
        this.seed = Optional.of(seed);
        return this;
    }

    /**
     * Keep the retained frames in an append-only file, not on the heap.
     * Every member must run the same choice.
     */
    SealerClusteredService carryingBytes(final RetainedFile.Config config) {
        return carryingBytes(config, Optional.empty());
    }

    /** Keep the retained frames in a file, and send them to the paired executor through {@code feed}. */
    SealerClusteredService carryingBytes(final RetainedFile.Config config, final Optional<FeedConfig> feed) {
        this.retainedFile = Optional.of(config);
        this.feedConfig = feed;
        return this;
    }

    /**
     * The void configuration of the state. A member with the retained file
     * takes no void request, and its snapshot holds no void ledger.
     */
    private VoidLedger.Config voids() {
        return retainedFile.isPresent() ? VoidLedger.Config.DISABLED : voidConfig;
    }

    /** Hold this member's outputs after its start, until {@code hold} ends ({@link StartHold}). */
    SealerClusteredService holdAtStart(final StartHold hold) {
        this.startHold = hold;
        return this;
    }

    /** Ask for a snapshot on each boundary tick that {@code schedule} makes due. */
    SealerClusteredService snapshotEvery(final SnapshotSchedule schedule) {
        this.snapshotSchedule = Optional.of(schedule);
        return this;
    }

    @Override
    public void onStart(Cluster cluster, Image snapshotImage) {
        // An unreadable or empty snapshot image is fatal. Restarting silently
        // at genesis would diverge from the rest of the cluster, which assumes
        // the snapshotted state (and the log replayed after it) is correct.
        start(cluster, Optional.ofNullable(snapshotImage)
            .map(image -> SnapshotIo.readSnapshot(image, cluster.idleStrategy())));
    }

    /** Start from {@code snapshot} when there is one, else from the seed or at genesis. */
    void start(final Cluster cluster, final Optional<byte[]> snapshot) {
        this.cluster = cluster;
        this.seedRecord = new SeedRecord(cluster, memberId);
        this.snapshots = new SnapshotRequests(cluster, memberId);
        final Optional<RetainedFile> file = retainedFile.map(config -> new RetainedFile(config, memberId));
        this.retained = file.<RetainedFrames>map(f -> f).orElseGet(HeapFrames::new);
        this.feed = Feed.open(feedConfig, file, memberId);
        if (snapshot.isPresent()) {
            restore(snapshot.get());
            // The log position of a start from a snapshot is the snapshot's position.
            purgeView = PurgeView.EMPTY
                .withMark(new PurgeView.Mark(cluster.logPosition(), state.blockNumber()))
                .withPostedHead(state.postedHead());
            // Log to stdout so the cluster-member-rejoin chaos case can check
            // that a wiped member came back through a snapshot restore, not
            // silently at genesis.
            System.out.println("sealer snapshot RESTORED memberId=" + memberId
                + " block=" + state.blockNumber() + " canonicalCount=" + state.canonicalCount()
                + " retained=" + egress.retainedCount() + " postedHead=" + state.postedHead());
        } else if (seed.isPresent()) {
            startSeeded(seed.get());
        } else {
            this.state = new CanonicalSealerState(
                dedupCapacity, CanonicalSealerState.GENESIS_BLOCK_NUMBER, remoteOrigins, voids(),
                inclusionHorizonBlocks, window.capacity(), budgets);
            openEgress(Optional.empty());
            System.out.println("sealer state FRESH at genesis memberId=" + memberId);
        }
        publishedRules = rulesView();
        System.out.println("sealer decision rules memberId=" + memberId + " version=" + state.decisionVersion());
        // Do not call scheduleTimer here: Aeron rejects timer scheduling from
        // onStart. The boundary timer is armed from onNewLeadershipTermEvent,
        // which is log-driven.
    }

    /**
     * The rules for the admin endpoint: the version of the open block, and
     * the newest activation of the version switch, else of the rules tail.
     */
    private DecisionRules.View rulesView() {
        final VersionSwitch.View versions = state.versionSwitch();
        final DecisionRules.View tail = state.decisionRules();
        return versions.activationOrigin() != 0L
            ? new DecisionRules.View(versions.version(), versions.activationOrigin(), versions.activationMs())
            : new DecisionRules.View(versions.version(), tail.activationOrigin(), tail.activationMs());
    }

    /**
     * After a log entry: when a block boundary of the entry switched the
     * version from {@code versionBefore}, report the switch and ask for a
     * snapshot, so a replay has a start point on each side of it.
     */
    private void afterEntry(final int versionBefore, final long timestamp) {
        if (state.decisionVersion() == versionBefore) {
            return;
        }
        publishedRules = rulesView();
        System.out.println("cluster DECISION-VERSION memberId=" + memberId
            + " version=" + state.decisionVersion() + " at=" + timestamp + " block=" + state.blockNumber());
        egress.offerSealerInfoToAll(state.decisionVersion());
        requestSnapshot();
    }

    /** Offer the rules frame to a session that opened. */
    private void offerRules(final ClientSession session) {
        egress.offerSealerInfo(session, state.decisionVersion());
    }

    /**
     * Offer the rules frame to every open session again. A leader change
     * can lose a frame that the old leader had not sent yet, and a
     * sequencer reports the version that it got last. A new term is a log
     * entry, so every member offers again at the same point, and a client
     * takes a repeat as the same report.
     */
    private void offerRulesAgain() {
        egress.offerSealerInfoToAll(state.decisionVersion());
    }

    /**
     * The snapshot: the state section, then the egress retention above the
     * posted head. Every member writes the same bytes at the same log
     * position. With the retained file, the snapshot is version
     * {@link CanonicalSealerState#RETAINED_FILE_SNAPSHOT_VERSION}.
     */
    byte[] snapshot() {
        final byte[] stateBytes = retainedFile.isPresent()
            ? state.takeSnapshotWithRetainedFile()
            : state.takeSnapshot();
        final byte[] retention = egress.writeSnapshot();
        final byte[] out = new byte[stateBytes.length + retention.length];
        System.arraycopy(stateBytes, 0, out, 0, stateBytes.length);
        System.arraycopy(retention, 0, out, stateBytes.length, retention.length);
        return out;
    }

    /**
     * Restore the state and the retention from {@code snapshot}. The floors
     * start at the restore point (record index canonicalCount and boundary
     * block blockNumber, the next ones to emit) and move down to the oldest
     * retained frame, so a member restored from a snapshot still serves a
     * replay from the posted head. Floors left at genesis would answer a
     * pre-retention replay request with a false REPLAY_DONE (a silent
     * canonical gap) instead of the correct REPLAY_UNAVAILABLE.
     */
    void restore(final byte[] snapshot) {
        final ByteBuffer buf = ByteBuffer.wrap(snapshot).order(ByteOrder.BIG_ENDIAN);
        this.state = CanonicalSealerState.load(
            buf, dedupCapacity, remoteOrigins, voids(), inclusionHorizonBlocks, window.capacity(),
            budgets);
        latestSnapshotVersion = state.restoredSnapshotVersion();
        requireRetentionLayout(latestSnapshotVersion);
        openEgress(Optional.of(buf));
    }

    /**
     * Stop the member when the snapshot does not hold the retention layout
     * of this member. A member with the retained file restores only a
     * version-12 snapshot: a heap section would start its file at another
     * position than the file of a peer that kept running. A member without
     * the file cannot read a file section. The switch changes only with a
     * new chain or a coordinated cut-over, never on a live chain.
     */
    private void requireRetentionLayout(final int version) {
        final boolean fileSection = version == CanonicalSealerState.RETAINED_FILE_SNAPSHOT_VERSION;
        if (fileSection != retainedFile.isPresent()) {
            final String why = "snapshot version " + version + (fileSection
                ? " holds a retained file, and this member keeps the frames on the heap: start it with "
                : " holds no retained file, and this member keeps the frames in a file: start it without ")
                + RetainedFile.Config.SETTING + "=true, or start a new chain";
            System.out.println("sealer RETAINED-FILE FATAL memberId=" + memberId + " " + why);
            System.out.flush();
            retained.close();
            throw new AgentTerminationException(why);
        }
    }

    /**
     * Start after the seed's head: the state opens block {@code H + 1} at
     * index {@code E_H}, and the egress floors start there, with the
     * retention floor at {@code H}. A consumer at the rebuilt head resumes
     * at {@code (E_H, H + 1)}.
     */
    private void startSeeded(final SealerSeed seed) {
        this.state = CanonicalSealerState.seeded(
            seed, dedupCapacity, voids(), inclusionHorizonBlocks, window.capacity(), budgets);
        openEgress(Optional.empty());
        // The seed head is on L1, so the purge floor starts there. The first
        // snapshot after the seed confirmation is the first purge mark.
        purgeView = PurgeView.EMPTY.withPostedHead(state.postedHead());
        System.out.println("sealer state SEEDED memberId=" + memberId
            + " block=" + seed.head().block() + " endTx=" + seed.head().endTxIdx()
            + " senders=" + state.trackedSenders() + " stateRoot=" + seed.stateRootHex()
            + " digest=" + seed.digestHex());
    }

    /**
     * Open the egress at the state's head, restore the retention section
     * of a snapshot or open an empty retention, and set the retention floor
     * at the posted head.
     */
    private void openEgress(final Optional<ByteBuffer> retention) {
        this.egress = new SealerEgress(
            cluster, memberId, retained, feed, state.canonicalCount(), state.blockNumber(), startHold);
        retention.ifPresentOrElse(egress::restore, egress::openEmpty);
        egress.setPostedHead(state.postedHead());
    }

    /** The chain's status: the posted and sealed heads, both guards, and the retention floors. */
    private ClusterStatus status() {
        return new ClusterStatus(
            state.postedHead(),
            state.sealedHead(),
            state.daLagBudgetBlocks(),
            state.daLagHalted(),
            egress.retainedCount(),
            egress.firstRetainedIndex(),
            egress.firstRetainedBlock(),
            state.bestRecorded(),
            state.recordLagBudget(),
            state.recordLagHalted());
    }

    @Override
    public void onNewLeadershipTermEvent(
            final long leadershipTermId,
            final long logPosition,
            final long timestamp,
            final long termBaseLogPosition,
            final int leaderMemberId,
            final int logSessionId,
            final java.util.concurrent.TimeUnit timeUnit,
            final int appVersion) {
        // This is the first sanctioned point to schedule a timer, since it is
        // log-driven (unlike onStart or doBackgroundWork). Re-arm the repeating
        // boundary timer on every new leadership term, unconditionally.
        // Pending cluster timers live in the leader's timer wheel only.
        // A follower's scheduleTimer call has no effect; only the expiry
        // replicates through the log. So any election can lose the pending
        // tick: if the old leader died, or stepped down during a quorum
        // outage, before appending the expiry, no member holds a live timer
        // afterwards, and the boundary clock stops forever. Records still
        // relay, but blocks never seal.
        // Re-arming with the same correlation id is idempotent: Aeron replaces
        // the pending timer instead of scheduling a second one.
        this.leadershipTermId = leadershipTermId;
        scheduleBoundaryTimer();
        offerRulesAgain();
        if (state.seedStatus() == CanonicalSealerState.SeedStatus.PENDING) {
            seedRecord.offer(state.seedDigest());
        }
        // Log-driven, so every member prints it, also on replay. With the
        // role lines it gives the order of the elections: which member led
        // which term, and where in the log the term began.
        System.out.println("cluster TERM memberId=" + memberId
            + " leadershipTermId=" + leadershipTermId
            + " leaderMemberId=" + leaderMemberId
            + " logPosition=" + logPosition
            + " role=" + cluster.role()
            + " block=" + state.blockNumber());
        recordServicePosition();
    }

    @Override
    public void onSessionOpen(ClientSession session, long timestamp) {
        egress.sessionsChanged();
        // Log every open and close. A client whose session the cluster
        // closed without its knowledge offers into nothing, and these lines
        // are the only record of when and why the cluster dropped it. Both
        // callbacks are log-driven, so every member prints them, also on
        // replay.
        System.out.println("cluster SESSION open memberId=" + memberId
            + " session=" + session.id());
        // Nothing session-specific to track: canonical state is global. But a
        // session opening is a log-driven moment where timer scheduling is
        // allowed, so use it to revive a dead boundary clock (see helper).
        maybeReviveBoundaryClock();
        offerRules(session);
    }

    @Override
    public void onSessionClose(ClientSession session, long timestamp, CloseReason closeReason) {
        egress.sessionsChanged();
        System.out.println("cluster SESSION close memberId=" + memberId
            + " session=" + session.id() + " reason=" + closeReason);
        egress.removeConsumer(session.id());
        egress.dropBacklog(session, closeReason);
    }

    @Override
    public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {
        // Every log-driven callback first sends what the egress backlogs
        // hold, so a back-pressured session catches up between frames, and
        // the stall deadline is checked at least once per tick.
        egress.drainBacklogs();
        final int versionBefore = state.decisionVersion();
        dispatchSessionMessage(session, timestamp, buffer, offset, length);
        afterEntry(versionBefore, timestamp);
        recordServicePosition();
    }

    /** Decode the kind tag and dispatch one ingress frame. */
    private void dispatchSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length) {
        if (length <= SealerWire.KIND_OFFSET) {
            // Malformed or too-short envelope: it cannot carry the kind tag.
            onMalformedFrame("ingress-envelope", length);
            return;
        }
        final byte kind = buffer.getByte(offset + SealerWire.KIND_OFFSET);
        switch (kind) {
            case SealerWire.KIND_SUBSCRIBE:
                egress.addConsumer(session.id());
                // A consumer that announces itself learns the current
                // status at once, instead of at the next tick.
                egress.offerStatus(session, status());
                return;
            case SealerWire.KIND_POSTED_CURSOR:
                onPostedCursor(buffer, offset, length);
                return;
            case SealerWire.KIND_RECORDED_CURSOR:
                if (takesVoteKinds()) {
                    onRecordedCursor(buffer, offset, length);
                } else {
                    onUnknownKind(kind, length);
                }
                return;
            case SealerWire.KIND_FEATURE_ACTIVATION:
                onFeatureActivation(buffer, offset, length, timestamp);
                return;
            case SealerWire.KIND_SEED_EPOCH:
                onSeedEpoch(session, buffer, offset, length);
                return;
            case SealerWire.KIND_REPLAY_REQUEST: {
                if (length < SealerWire.MIN_REPLAY_REQUEST_LEN) {
                    onMalformedFrame("replay-request", length);
                    return;
                }
                final long fromIndex =
                    buffer.getLong(offset + 1, ByteOrder.LITTLE_ENDIAN);
                final long fromBlock =
                    buffer.getLong(offset + 1 + Long.BYTES, ByteOrder.LITTLE_ENDIAN);
                // A replay request announces a consumer just as a SUBSCRIBE
                // frame does.
                egress.addConsumer(session.id());
                egress.offerStatus(session, status());
                egress.handleReplayRequest(
                    session, fromIndex, fromBlock, state.canonicalCount(), state.blockNumber());
                // A consumer sends a replay request when it sees no egress.
                // If this member's boundary clock died (it lost its pending
                // timer after election churn), revive it now.
                maybeReviveBoundaryClock();
                return;
            }
            case SealerWire.KIND_VOID_REQUEST:
                if (takesVoteKinds()) {
                    onVoidRequest(buffer, offset, length);
                } else {
                    onUnknownKind(kind, length);
                }
                return;
            case SealerWire.KIND_ORIGIN_RECORD:
                onOriginRecord(session, buffer, offset, length);
                maybeReviveBoundaryClock();
                return;
            case SealerWire.KIND_REMOTE_ORIGIN_RECORD:
                // Its OWN kind, so the branch is taken on the tag alone: the
                // service never peeks into the payload to tell a peer's message
                // batch from an L1 epoch.
                onRemoteOriginRecord(session, buffer, offset, length);
                maybeReviveBoundaryClock();
                return;
            case SealerWire.KIND_BATCH:
                onBatch(session, buffer, offset, length);
                return;
            case SealerWire.KIND_INGRESS_RECORD:
                onIngressRecord(session, buffer, offset, length);
                return;
            default:
                // A kind this release does not know. Only a known kind
                // reaches the replicated state, so members of two releases
                // decide alike: every member drops the frame and counts it.
                onUnknownKind(kind, length);
        }
    }

    /**
     * Whether the member takes the void requests and the recorded cursors
     * (kinds 6 and 9). A member with the retained file, or on decision
     * version 5, drops them as unknown kinds: it takes no void and runs no
     * record-lag guard.
     */
    private boolean takesVoteKinds() {
        return retainedFile.isEmpty() && state.decisionVersion() < VersionSwitch.CARRY_VERSION;
    }

    /**
     * Handle a {@link SealerWire#KIND_INGRESS_RECORD} frame: the length
     * check is the only envelope guard on this path.
     */
    private void onIngressRecord(
            final ClientSession session, final DirectBuffer buffer, final int offset, final int length) {
        if (length < RelayShape.INGRESS_RECORD.minLength()) {
            // Malformed or too-short envelope: it cannot hold the guard
            // header, the id and the record type.
            onMalformedFrame("ingress-envelope", length);
            return;
        }
        processRecord(session, buffer, offset, length);
        // Records relaying while blocks never seal is the sign of a
        // dead clock: the canonical stream advances, but the boundary
        // cadence is gone. Revive here so sustained ingress load heals
        // the clock without waiting for a consumer reconnect.
        maybeReviveBoundaryClock();
    }

    /**
     * Process a {@link SealerWire#KIND_BATCH} frame entry by entry.
     * Each entry is processed exactly like an individually-offered record,
     * with the same dedup and the same per-record relay. A malformed entry
     * drops the rest of the batch and is counted. As on the single-record
     * path, the boundary clock is revived only after a fully-parsed batch.
     */
    private void onBatch(
            final ClientSession session, final DirectBuffer buffer, final int offset, final int length) {
        if (length < 3) {
            onMalformedFrame("batch-envelope", length);
            return;
        }
        final int count = buffer.getShort(offset + 1, ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
        int pos = offset + 3;
        final int limit = offset + length;
        for (int i = 0; i < count; i++) {
            if (pos + 4 > limit) {
                onMalformedFrame("batch-entry-header", length);
                return;
            }
            final int entryLen = buffer.getInt(pos, ByteOrder.LITTLE_ENDIAN);
            pos += 4;
            if (entryLen < RelayShape.INGRESS_RECORD.minLength() || pos + entryLen > limit) {
                onMalformedFrame("batch-entry", entryLen);
                return;
            }
            processRecord(session, buffer, pos, entryLen);
            pos += entryLen;
        }
        maybeReviveBoundaryClock();
    }

    /**
     * Handle a {@link SealerWire#KIND_ORIGIN_RECORD} frame.
     * Strip the origin and slot count, relay the remaining payload as is, and
     * offer the forced boundary first, so the record leads the block it opens
     * instead of trailing the block it closes.
     *
     * <p>A record that skips an L1 block is answered with an
     * {@link SealerWire#EGRESS_KIND_ORIGIN_GAP} frame to the offering
     * session, and is never relayed. Every member refuses it the same way,
     * because the check reads only replicated state.</p>
     */
    private void onOriginRecord(
            final ClientSession session, final DirectBuffer buffer, final int offset, final int length) {
        if (length < RelayShape.ORIGIN_RECORD.minLength()) {
            onMalformedFrame("origin-record", length);
            return;
        }
        buffer.getBytes(offset + SealerWire.ORIGIN_ID_OFFSET, canonicalIdScratch);
        final long l1Origin = buffer.getLong(offset + SealerWire.ORIGIN_OFFSET, ByteOrder.LITTLE_ENDIAN);
        final long slotCount =
                buffer.getInt(offset + SealerWire.SLOT_COUNT_OFFSET, ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL;

        // The relayed payload keeps the same shape as an ordinary record:
        // [canonical_id:32][record_type][fields…].
        final byte[] payload = RelayShape.ORIGIN_RECORD.payload(buffer, offset, length);

        // The record closes the open block, so the window closes with it:
        // a reorder never crosses a block.
        flushWindow();
        final OriginOutcome outcome;
        try {
            outcome =
                state.onOriginRecord(canonicalIdScratch, l1Origin, slotCount, payload, cluster.time());
        } catch (final IllegalArgumentException ex) {
            // A non-advancing origin is a producer bug. Every member rejects
            // it the same way, because the check reads only replicated state,
            // so dropping it is deterministic. This is far better than
            // throwing out of the clustered service, which would take the
            // cluster down.
            onMalformedFrame("origin-record-regression", length);
            return;
        }
        if (outcome.gap) {
            onOriginGap(session, l1Origin, outcome.expectedOrigin);
            return;
        }
        if (outcome.advance.isEmpty()) {
            return; // Duplicate epoch from a racing sequencer.
        }
        offerAdvance(outcome.advance.get().forcedBoundary(), outcome.advance.get().relayed());
    }

    /**
     * Answer an origin-gap reject to the offering session. Member-local
     * egress IO, exactly like {@link #onRemoteOriginReject}: the refusal
     * moved no replicated state, and every member computed it the same way.
     */
    private void onOriginGap(final ClientSession session, final long offeredOrigin, final long expectedOrigin) {
        originGapCount++;
        if (Long.bitCount(originGapCount) == 1) {
            // Log to stdout like the other operational signals, so the e2e
            // and chaos suites can grep it. Count at powers of two so a
            // storm cannot flood the log.
            System.out.println("cluster ORIGIN-GAP memberId=" + memberId
                + " offered=" + offeredOrigin
                + " expected=" + expectedOrigin
                + " totalOriginGaps=" + originGapCount);
        }
        egress.offerOriginGap(session, offeredOrigin, expectedOrigin);
    }

    /**
     * Handle a {@link SealerWire#KIND_REMOTE_ORIGIN_RECORD} frame.
     * Strip the peer identity ({@code origin_chain_id}), that peer's anchor
     * position, the slot count, and the seq range. Relay the remaining
     * payload as is, and offer the forced boundary first, so the batch leads
     * the block it opens instead of trailing the block it closes.
     *
     * <p>A record the state machine rejects (unknown origin, bad range, slot
     * count mismatch, lane cursor mismatch, or anchor regression) is
     * answered with an {@link SealerWire#EGRESS_KIND_REMOTE_ORIGIN_REJECT}
     * frame to the offering session, and is never relayed. Every member
     * rejects it the same way, because the checks read only replicated
     * state and shared configuration.</p>
     */
    private void onRemoteOriginRecord(
            final ClientSession session, final DirectBuffer buffer, final int offset, final int length) {
        if (length < RelayShape.REMOTE_ORIGIN_RECORD.minLength()) {
            onMalformedFrame("remote-origin-record", length);
            return;
        }
        buffer.getBytes(offset + SealerWire.REMOTE_ID_OFFSET, canonicalIdScratch);
        final long originChainId =
                buffer.getLong(offset + SealerWire.REMOTE_CHAIN_ID_OFFSET, ByteOrder.LITTLE_ENDIAN);
        final long anchorNumber =
                buffer.getLong(offset + SealerWire.REMOTE_ANCHOR_OFFSET, ByteOrder.LITTLE_ENDIAN);
        final long slotCount =
                buffer.getInt(offset + SealerWire.REMOTE_SLOT_COUNT_OFFSET, ByteOrder.LITTLE_ENDIAN)
                        & 0xFFFF_FFFFL;
        final long firstSeq =
                buffer.getLong(offset + SealerWire.REMOTE_FIRST_SEQ_OFFSET, ByteOrder.LITTLE_ENDIAN);
        final long lastSeq =
                buffer.getLong(offset + SealerWire.REMOTE_LAST_SEQ_OFFSET, ByteOrder.LITTLE_ENDIAN);

        // Same relay shape as an epoch: [canonical_id:32][record_type][fields…].
        final byte[] payload = RelayShape.REMOTE_ORIGIN_RECORD.payload(buffer, offset, length);

        // Same as an epoch: the window closes with the block.
        flushWindow();
        final CanonicalSealerState.RemoteOriginOutcome outcome = state.onRemoteOriginRecord(
            canonicalIdScratch, originChainId, anchorNumber, slotCount, firstSeq, lastSeq,
            payload, cluster.time());
        if (outcome.rejected) {
            onRemoteOriginReject(session, originChainId, firstSeq, outcome.expectedNextSeq, outcome.reason);
            return;
        }
        if (outcome.advance.isEmpty()) {
            return; // duplicate batch from a racing watcher
        }
        offerAdvance(outcome.advance.get().forcedBoundary(), outcome.advance.get().relayed());
    }

    /**
     * Answer a remote-origin reject to the offering session. Member-local
     * egress IO, exactly like {@link #onContiguityReject}: the rejection
     * itself moved no replicated state, and every member computed it the
     * same way.
     */
    private void onRemoteOriginReject(
            final ClientSession session,
            final long originChainId,
            final long firstSeq,
            final long expectedNextSeq,
            final byte reason) {
        remoteRejectedFrameCount++;
        if (Long.bitCount(remoteRejectedFrameCount) == 1) {
            // Log to stdout like the other operational signals, so the e2e
            // and chaos suites can grep it. Count at powers of two so a
            // storm cannot flood the log.
            System.out.println("cluster REMOTE-ORIGIN-REJECT memberId=" + memberId
                + " origin=" + Long.toUnsignedString(originChainId)
                + " firstSeq=" + Long.toUnsignedString(firstSeq)
                + " expectedNextSeq=" + Long.toUnsignedString(expectedNextSeq)
                + " reason=" + reason
                + " totalRejected=" + remoteRejectedFrameCount);
        }
        egress.offerRemoteOriginReject(session, originChainId, firstSeq, expectedNextSeq, reason);
    }

    /**
     * Hold one single-record ingress frame at {@code offset} in the
     * ordering window. Parse the guard header (sender, nonce, deadline, tip)
     * and the 32-byte canonical id at their fixed offsets. The payload is
     * relayed as is and never inspected. The window flushes when it is
     * full; a record that opens a window arms the hold timer. Shared by the
     * direct path and each {@link SealerWire#KIND_BATCH} entry.
     */
    private void processRecord(
            final ClientSession session, final DirectBuffer buffer, final int offset, final int length) {
        final boolean direct = window.capacity() == 0 && retainedFile.isEmpty();
        if (direct && isPositionedRef(buffer, offset, length)) {
            onPositionedRef(session, buffer, offset, length);
            return;
        }
        if (direct && isCarriedRef(buffer.getByte(offset + SealerWire.RECORD_TYPE_OFFSET))) {
            onCarriedRef(session, buffer, offset, length);
            return;
        }
        final byte[] canonicalId = new byte[CanonicalSealerState.CANONICAL_ID_LEN];
        buffer.getBytes(offset + SealerWire.CANONICAL_ID_OFFSET, canonicalId);
        final byte[] sender = new byte[CanonicalSealerState.SENDER_LEN];
        buffer.getBytes(offset + SealerWire.SENDER_OFFSET, sender);
        final long nonce = buffer.getLong(offset + SealerWire.NONCE_OFFSET, ByteOrder.LITTLE_ENDIAN);
        final long deadline =
            buffer.getLong(offset + SealerWire.DEADLINE_OFFSET, ByteOrder.LITTLE_ENDIAN);
        final long tipLo = buffer.getLong(offset + SealerWire.TIP_OFFSET, ByteOrder.LITTLE_ENDIAN);
        final long tipHi =
            buffer.getLong(offset + SealerWire.TIP_OFFSET + Long.BYTES, ByteOrder.LITTLE_ENDIAN);
        final Optional<byte[]> relay = relayOf(RelayShape.INGRESS_RECORD.payload(buffer, offset, length));
        if (relay.isEmpty()) {
            onMalformedFrame("canonical-entry-on-ingress", length);
            return;
        }
        final byte[] payload = relay.get();
        final boolean opened = window.isEmpty();
        final boolean full = window.add(
            sender, tipHi, tipLo, new HeldRecord(session, canonicalId, sender, nonce, deadline, payload));
        if (full) {
            flushWindow();
            return;
        }
        if (opened) {
            scheduleWindowTimer();
        }
    }

    /**
     * The relayed payload of a kind-0 record. A member with the retained
     * file relays a full entry as a canonical entry
     * ({@link SealerWire#RT_TX}), and takes no canonical entry from a
     * session: only a full entry becomes one, so each canonical entry in
     * the file holds a whole envelope. Every other payload relays as is.
     */
    private Optional<byte[]> relayOf(final byte[] payload) {
        if (retainedFile.isEmpty()) {
            return Optional.of(payload);
        }
        if (TxOffer.isCanonical(payload)) {
            return Optional.empty();
        }
        return Optional.of(TxOffer.parse(payload).map(TxOffer::canonicalPayload).orElse(payload));
    }

    /**
     * Close the ordering window: run the record path on every held record
     * in {@code (tip descending, arrival ascending)} order, one sender's
     * records in their arrival order. Dedup, the deadline, the window-full
     * check, the contiguity guard, and the index assignment all run here,
     * in the flush order, so the egress relays in that order.
     */
    private void flushWindow() {
        final List<HeldRecord> held = window.flush();
        for (HeldRecord record : held) {
            admitRecord(record);
        }
    }

    /**
     * The record path for one held record: dedup the record, check its
     * deadline and contiguity, and relay it if accepted. A reject answers
     * the offering session with the matching egress frame.
     */
    private void admitRecord(final HeldRecord r) {
        if (isPositionedRef(r)) {
            admitPositionedRef(r);
            return;
        }
        if (isCarriedRef(r.payload[CanonicalSealerState.CANONICAL_ID_LEN])) {
            admitCarriedRef(r);
            return;
        }
        final CanonicalSealerState.RecordOutcome outcome =
            state.onRecord(r.canonicalId, r.sender, r.nonce, r.deadline, r.payload);
        switch (outcome.kind) {
            case CONTIGUITY_REJECT -> onContiguityReject(r, outcome.expectedNonce);
            case PAST_DEADLINE -> onPastDeadline(r, outcome.maxInclusionBlock);
            case WINDOW_FULL -> onWindowFull(r);
            case DA_LAG_REJECT -> onDaLagReject(r);
            case RECORD_LAG_REJECT -> onRecordLagReject(r);
            case RELAYED -> outcome.relayed.ifPresent(egress::offerRelayed);
            case DUPLICATE -> { }
        }
    }

    /**
     * Whether a kind-0 frame takes the record path of decision version 4:
     * the open block runs version 4 or 5, and the frame is a transaction
     * reference whose deadline field carries its position.
     */
    private boolean isPositionedRef(final DirectBuffer buffer, final int offset, final int length) {
        return state.decisionVersion() >= VersionSwitch.ON_VERSION
            && length == SealerWire.TXREF_RECORD_LEN
            && buffer.getByte(offset + SealerWire.RECORD_TYPE_OFFSET) == SealerWire.RT_TXREF
            && SealerWire.carriesPosition(buffer.getLong(offset + SealerWire.DEADLINE_OFFSET, ByteOrder.LITTLE_ENDIAN));
    }

    /** {@link #isPositionedRef(DirectBuffer, int, int)} for a record that the ordering window held. */
    private boolean isPositionedRef(final HeldRecord r) {
        return state.decisionVersion() >= VersionSwitch.ON_VERSION
            && r.payload.length == SealerWire.TXREF_PAYLOAD_LEN
            && r.payload[CanonicalSealerState.CANONICAL_ID_LEN] == SealerWire.RT_TXREF
            && SealerWire.carriesPosition(r.deadline);
    }

    /**
     * The record path of decision version 4 with no ordering window: the
     * replay window of the ingress image decides, and the relay copies the
     * payload from the log buffer.
     */
    private void onPositionedRef(
            final ClientSession session, final DirectBuffer buffer, final int offset, final int length) {
        buffer.getBytes(offset + SealerWire.CANONICAL_ID_OFFSET, refId);
        final long index = state.onRef(
            buffer.getByte(offset + SealerWire.REF_LANE_OFFSET) & 0xFF,
            buffer.getInt(offset + SealerWire.REF_SESSION_OFFSET, ByteOrder.LITTLE_ENDIAN),
            buffer.getLong(offset + SealerWire.DEADLINE_OFFSET, ByteOrder.LITTLE_ENDIAN) & SealerWire.POSITION_MASK,
            refId);
        if (index >= 0) {
            egress.offerRelayed(index, buffer, offset + SealerWire.CANONICAL_ID_OFFSET, SealerWire.TXREF_PAYLOAD_LEN);
            return;
        }
        onDirectRefCode(index, session, buffer, offset, length);
    }

    /**
     * Answer the code of a reference that came on the direct path. Only a
     * refusal that answers the session builds the held form of the frame,
     * so a copy and an expired reference allocate nothing.
     */
    private void onDirectRefCode(
            final long code, final ClientSession session, final DirectBuffer buffer, final int offset, final int length) {
        if (code == CanonicalSealerState.REF_DA_LAG || code == CanonicalSealerState.REF_RECORD_LAG) {
            onRefRefused(code, heldOf(session, buffer, offset));
        } else if (code == CanonicalSealerState.REF_EXPIRED) {
            onRefExpired();
        } else if (code == CARRIED_MALFORMED) {
            onMalformedFrame("full-reference", length);
        }
    }

    /**
     * Whether a kind-0 record of {@code recordType} takes the record path
     * of decision version 5: the open block runs version 5, and the record
     * is a full reference. Below version 5, a full reference takes the
     * path of every other payload.
     */
    private boolean isCarriedRef(final byte recordType) {
        return state.decisionVersion() == VersionSwitch.CARRY_VERSION && recordType == SealerWire.RT_TXREF_OFFER;
    }

    /**
     * The record path of decision version 5 with no ordering window: the
     * replay window of the ingress image decides, and the relay copies the
     * id and the envelope from the log buffer. A copy reads no byte of the
     * envelope.
     */
    private void onCarriedRef(
            final ClientSession session, final DirectBuffer buffer, final int offset, final int length) {
        final long code = orderCarried(
            buffer.getLong(offset + SealerWire.DEADLINE_OFFSET, ByteOrder.LITTLE_ENDIAN),
            carried.wrap(buffer, offset + SealerWire.CANONICAL_ID_OFFSET, length - SealerWire.CANONICAL_ID_OFFSET));
        onDirectRefCode(code, session, buffer, offset, length);
    }

    /** The record path of decision version 5 for a record that the ordering window held. */
    private void admitCarriedRef(final HeldRecord r) {
        final long code = orderCarried(r.deadline, carried.wrap(new UnsafeBuffer(r.payload), 0, r.payload.length));
        if (code == CARRIED_MALFORMED) {
            onMalformedFrame("full-reference", SealerWire.CANONICAL_ID_OFFSET + r.payload.length);
            return;
        }
        onRefRefused(code, r);
    }

    /**
     * Order the full reference that {@code offer} views, with the deadline
     * field {@code deadline} of its frame. A fresh reference goes out as
     * its canonical entry ({@link SealerWire#RT_TX}).
     *
     * @return the canonical index, a {@code REF_*} code of the state, or
     *         {@link #CARRIED_MALFORMED} for a frame that holds no whole
     *         envelope or no position
     */
    private long orderCarried(final long deadline, final TxRefOffer offer) {
        if (!offer.isWhole() || !SealerWire.carriesPosition(deadline)) {
            return CARRIED_MALFORMED;
        }
        final long index = state.onCarriedRef(offer.lane(), offer.session(), deadline & SealerWire.POSITION_MASK);
        if (index >= 0) {
            egress.offerCarried(index, offer);
        }
        return index;
    }

    /** The record path of decision version 4 for a record that the ordering window held. */
    private void admitPositionedRef(final HeldRecord r) {
        final ByteBuffer fields = ByteBuffer.wrap(r.payload).order(ByteOrder.LITTLE_ENDIAN);
        final int lane = fields.get(CanonicalSealerState.CANONICAL_ID_LEN + 1) & 0xFF;
        final int imageSession = fields.getInt(SealerWire.TXREF_PAYLOAD_LEN - Integer.BYTES);
        final long index = state.onRef(lane, imageSession, r.deadline & SealerWire.POSITION_MASK, r.canonicalId);
        if (index >= 0) {
            egress.offerRelayed(new Relayed(index, r.payload));
            return;
        }
        onRefRefused(index, r);
    }

    /** Answer a reference that decision version 4 refused. A copy needs no answer. */
    private void onRefRefused(final long code, final HeldRecord record) {
        if (code == CanonicalSealerState.REF_DA_LAG) {
            onDaLagReject(record);
        } else if (code == CanonicalSealerState.REF_RECORD_LAG) {
            onRecordLagReject(record);
        } else if (code == CanonicalSealerState.REF_EXPIRED) {
            onRefExpired();
        }
    }

    /** Count a reference that came a window behind its image: a late copy, or a reference held too long. */
    private void onRefExpired() {
        expiredRefCount++;
        if (Long.bitCount(expiredRefCount) == 1) {
            System.out.println("cluster REF-EXPIRED memberId=" + memberId + " total=" + expiredRefCount);
        }
    }

    /** The held form of a kind-0 frame, for the reject answers. */
    private static HeldRecord heldOf(final ClientSession session, final DirectBuffer buffer, final int offset) {
        final byte[] id = new byte[CanonicalSealerState.CANONICAL_ID_LEN];
        buffer.getBytes(offset + SealerWire.CANONICAL_ID_OFFSET, id);
        final byte[] sender = new byte[CanonicalSealerState.SENDER_LEN];
        buffer.getBytes(offset + SealerWire.SENDER_OFFSET, sender);
        final byte[] payload = new byte[SealerWire.TXREF_PAYLOAD_LEN];
        buffer.getBytes(offset + SealerWire.CANONICAL_ID_OFFSET, payload);
        return new HeldRecord(session, id, sender,
            buffer.getLong(offset + SealerWire.NONCE_OFFSET, ByteOrder.LITTLE_ENDIAN),
            buffer.getLong(offset + SealerWire.DEADLINE_OFFSET, ByteOrder.LITTLE_ENDIAN), payload);
    }

    /**
     * Handle a {@link SealerWire#KIND_POSTED_CURSOR} frame: the batcher's
     * confirmed cursor. The state adopts it as the DA-lag floor, the egress
     * as the retention floor, and every session learns the new status. A
     * cursor past the sealed head is a batcher bug; every member drops it
     * the same way, because the check reads only replicated state.
     */
    private void onPostedCursor(final DirectBuffer buffer, final int offset, final int length) {
        if (length < SealerWire.MIN_POSTED_CURSOR_LEN) {
            onMalformedFrame("posted-cursor", length);
            return;
        }
        final long postedHead =
            buffer.getLong(offset + SealerWire.POSTED_HEAD_OFFSET, ByteOrder.LITTLE_ENDIAN);
        final boolean advanced;
        try {
            advanced = state.onPostedCursor(postedHead);
        } catch (final IllegalArgumentException ex) {
            onMalformedFrame("posted-cursor-ahead", length);
            return;
        }
        if (!advanced) {
            return;
        }
        egress.setPostedHead(postedHead);
        purgeView = purgeView.withPostedHead(postedHead);
        System.out.println("cluster POSTED-CURSOR memberId=" + memberId
            + " postedHead=" + postedHead + " sealedHead=" + state.sealedHead()
            + " retained=" + egress.retainedCount()
            + " halted=" + state.daLagHalted());
        egress.offerStatus(status());
    }

    /**
     * Handle a {@link SealerWire#KIND_RECORDED_CURSOR} frame: one
     * executor's recorded cursor. The state keeps the best cursor as the
     * floor of the record-lag guard. When the best cursor moves up, every
     * session learns the new status. A frame of another length, from an
     * executor id that is not a configured voter, or with a cursor at or
     * past the canonical count, drops as malformed. Every member drops it
     * the same way, because the checks read only replicated state. The
     * best cursor moves up at the cadence of the executors, so the log line
     * prints at power-of-two counts of the moves.
     */
    private void onRecordedCursor(final DirectBuffer buffer, final int offset, final int length) {
        if (length != SealerWire.RECORDED_CURSOR_LEN) {
            onMalformedFrame("recorded-cursor", length);
            return;
        }
        final int executorId = buffer.getByte(offset + SealerWire.RECORDED_EXECUTOR_OFFSET) & 0xFF;
        final long recordedThrough =
            buffer.getLong(offset + SealerWire.RECORDED_THROUGH_OFFSET, ByteOrder.LITTLE_ENDIAN);
        final boolean advanced;
        try {
            advanced = state.onRecordedCursor(executorId, recordedThrough);
        } catch (final IllegalArgumentException ex) {
            onMalformedFrame("recorded-cursor-refused", length);
            return;
        }
        if (!advanced) {
            return;
        }
        recordedAdvanceCount++;
        if (Long.bitCount(recordedAdvanceCount) == 1) {
            System.out.println("cluster RECORDED-CURSOR memberId=" + memberId
                + " executorId=" + executorId
                + " through=" + Long.toUnsignedString(recordedThrough)
                + " best=" + state.bestRecorded()
                + " halted=" + state.recordLagHalted()
                + " totalAdvances=" + recordedAdvanceCount);
        }
        egress.offerStatus(status());
    }

    /**
     * Handle a {@link SealerWire#KIND_FEATURE_ACTIVATION} frame at
     * {@code timestamp}, the cluster time of the record. A record of
     * {@link VersionSwitch#FEATURE_SEALER_V4} or
     * {@link VersionSwitch#FEATURE_SEALER_V5} stores its schedule in the
     * version switch, and a record of another feature changes no state.
     * A frame of another length drops as malformed. Every frame
     * prints a line: an upgrade is rare, and the line shows that the record
     * changed nothing.
     */
    private void onFeatureActivation(
            final DirectBuffer buffer, final int offset, final int length, final long timestamp) {
        if (length != SealerWire.FEATURE_ACTIVATION_LEN) {
            onMalformedFrame("feature-activation", length);
            return;
        }
        final VersionSwitch.FeatureActivation activation = new VersionSwitch.FeatureActivation(
            buffer.getLong(offset + SealerWire.ACTIVATION_ORIGIN_OFFSET, ByteOrder.LITTLE_ENDIAN),
            buffer.getLong(offset + SealerWire.ACTIVATION_FEATURE_OFFSET, ByteOrder.LITTLE_ENDIAN),
            buffer.getLong(offset + SealerWire.ACTIVATION_MS_OFFSET, ByteOrder.LITTLE_ENDIAN));
        final VersionSwitch.Outcome outcome = state.onFeatureActivation(activation);
        System.out.println("cluster FEATURE-ACTIVATION memberId=" + memberId
            + " l1Origin=" + Long.toUnsignedString(activation.l1Origin()) + " feature=" + activation.featureId()
            + " activationMs=" + Long.toUnsignedString(activation.activationMs())
            + " at=" + timestamp + " outcome=" + outcome + " version=" + state.decisionVersion());
        if (outcome == VersionSwitch.Outcome.STORED || outcome == VersionSwitch.Outcome.STORED_LATE) {
            publishedRules = rulesView();
            requestSnapshot();
        }
        if (outcome == VersionSwitch.Outcome.EARLY) {
            earlyActivations++;
            System.out.println("cluster FEATURE-ACTIVATION-EARLY memberId=" + memberId
                + " l1Origin=" + Long.toUnsignedString(activation.l1Origin()) + " feature=" + activation.featureId()
                + " version=" + state.decisionVersion() + " total=" + earlyActivations);
            return;
        }
        if (outcome != VersionSwitch.Outcome.IGNORED && outcome != VersionSwitch.Outcome.STORED) {
            lateActivations++;
            System.out.println("cluster FEATURE-ACTIVATION-LATE memberId=" + memberId
                + " l1Origin=" + Long.toUnsignedString(activation.l1Origin())
                + " stateOrigin=" + Long.toUnsignedString(state.l1Origin())
                + " outcome=" + outcome + " block=" + state.blockNumber() + " total=" + lateActivations);
        }
    }

    /**
     * Handle a {@link SealerWire#KIND_SEED_EPOCH} record. Only the service
     * writes it, so a frame from a client session is malformed. The first
     * record confirms the seed, and the leader then asks for a snapshot,
     * so a member that joins later restores the seeded state from it.
     *
     * <p>A member that started at genesis, or from another seed, holds
     * another history than the cluster. It stops here, before it relays one
     * record: the throw ends the service agent, and the container's
     * termination hook stops the node.</p>
     */
    private void onSeedEpoch(
            final ClientSession session, final DirectBuffer buffer, final int offset, final int length) {
        if (session != null || length != SealerWire.SEED_EPOCH_LEN) {
            onMalformedFrame("seed-epoch", length);
            return;
        }
        final byte[] digest = new byte[SealerSeed.HASH_LEN];
        buffer.getBytes(offset + SealerWire.SEED_DIGEST_OFFSET, digest);
        final boolean confirmed;
        try {
            confirmed = state.onSeedEpoch(digest);
        } catch (final IllegalStateException ex) {
            System.out.println("sealer SEED-EPOCH FATAL memberId=" + memberId + ": " + ex.getMessage());
            System.out.flush();
            retained.close();
            throw new AgentTerminationException(ex);
        }
        if (!confirmed) {
            return;
        }
        System.out.println("sealer seed CONFIRMED memberId=" + memberId + " block=" + state.blockNumber()
            + " canonicalCount=" + state.canonicalCount());
        final SnapshotRequests.Answer answer = requestSnapshot();
        if (answer != SnapshotRequests.Answer.FOLLOWER) {
            System.out.println("sealer seed SNAPSHOT memberId=" + memberId
                + " requested=" + (answer == SnapshotRequests.Answer.REQUESTED));
        }
    }

    /**
     * Answer one offer the DA-lag guard refused. The record is not ordered
     * until the batcher posts again, so the sequencer reports it to the
     * client instead of republishing.
     */
    private void onDaLagReject(final HeldRecord r) {
        final long nonce = r.nonce;
        daLagRejectCount++;
        if (Long.bitCount(daLagRejectCount) == 1) {
            System.out.println("cluster DA-LAG-REJECT memberId=" + memberId
                + " nonce=" + nonce + " sealedHead=" + state.sealedHead()
                + " postedHead=" + state.postedHead()
                + " budget=" + state.daLagBudgetBlocks()
                + " totalDaLagRejected=" + daLagRejectCount);
        }
        egress.offerDaLagReject(r.session, r.sender, nonce, status());
    }

    /**
     * Answer one offer the record-lag guard refused. The record is not
     * ordered until an executor records more, so the sequencer reports it
     * to the client instead of republishing.
     */
    private void onRecordLagReject(final HeldRecord r) {
        recordLagRejectCount++;
        if (Long.bitCount(recordLagRejectCount) == 1) {
            System.out.println("cluster RECORD-LAG-REJECT memberId=" + memberId
                + " nonce=" + r.nonce + " sealedIndex=" + state.sealedIndex()
                + " bestRecorded=" + state.bestRecorded()
                + " budget=" + state.recordLagBudget()
                + " totalRecordLagRejected=" + recordLagRejectCount);
        }
        egress.offerRecordLagReject(r.session, r.sender, r.nonce, state.sealedIndex(), status());
    }

    /**
     * Count one consumer's vote to remove an entry that it cannot execute.
     * The state decides. This method only relays the void record and logs
     * each change of the count, so the failure dump shows which voter the
     * void waits for. A repeated vote prints nothing: consumers send the
     * vote again at an interval.
     */
    private void onVoidRequest(final DirectBuffer buffer, final int offset, final int length) {
        if (length < SealerWire.MIN_VOID_REQUEST_LEN) {
            onMalformedFrame("void-request", length);
            return;
        }
        final int voterId = buffer.getByte(offset + SealerWire.VOID_VOTER_OFFSET) & 0xFF;
        final long index = buffer.getLong(offset + SealerWire.VOID_INDEX_OFFSET, ByteOrder.LITTLE_ENDIAN);
        buffer.getBytes(offset + SealerWire.VOID_HASH_OFFSET, canonicalIdScratch);
        final CanonicalSealerState.VoidOutcome outcome =
            state.onVoidRequest(voterId, index, canonicalIdScratch);
        if (outcome.vote == VoidLedger.Vote.REPEATED) {
            return;
        }
        System.out.println("cluster VOID-VOTE memberId=" + memberId
            + " voter=" + voterId + " index=" + index + " result=" + outcome.vote
            + " votes=" + state.voids().votesFor(index) + "/" + state.voids().voterCount());
        outcome.relayed.ifPresent(egress::offerRelayed);
    }

    /**
     * Answer a contiguity reject to the offering session. Sending it is
     * member-local egress IO, since only the leader's offer reaches the
     * client, exactly like record relaying. The rejection itself has no
     * dedup insert and no count. It is part of the deterministic state
     * machine and is identical on every member.
     */
    private void onContiguityReject(final HeldRecord r, final long expected) {
        final long nonce = r.nonce;
        rejectedFrameCount++;
        if (Long.bitCount(rejectedFrameCount) == 1) {
            // Log to stdout like the other operational signals, so the chaos
            // suite can grep it. Count at powers of two so a gap storm cannot
            // flood the log.
            System.out.println("cluster CONTIGUITY-REJECT memberId=" + memberId
                + " nonce=" + nonce + " expected=" + expected
                + " totalRejected=" + rejectedFrameCount);
        }
        egress.offerContiguityReject(r.session, r.sender, nonce, expected);
    }

    /**
     * Answer one offer whose inclusion deadline the open block has passed.
     * The record is not ordered, and no copy of it can be ordered later, so
     * the sequencer reports it to the client instead of republishing.
     */
    private void onPastDeadline(final HeldRecord r, final long maxInclusionBlock) {
        final long nonce = r.nonce;
        pastDeadlineCount++;
        if (Long.bitCount(pastDeadlineCount) == 1) {
            System.out.println("cluster PAST-DEADLINE memberId=" + memberId
                + " nonce=" + nonce + " maxInclusionBlock=" + maxInclusionBlock
                + " atBlock=" + state.blockNumber()
                + " totalPastDeadline=" + pastDeadlineCount);
        }
        egress.offerPastDeadline(
            r.session, r.sender, nonce, maxInclusionBlock, state.blockNumber());
    }

    /**
     * Answer one offer the dedup window had no room for. Nothing was
     * forgotten to make room, so this is back-pressure: the next tick that
     * passes a deadline frees space, and the sequencer republishes.
     */
    private void onWindowFull(final HeldRecord r) {
        final long nonce = r.nonce;
        windowFullCount++;
        if (Long.bitCount(windowFullCount) == 1) {
            System.out.println("cluster WINDOW-FULL memberId=" + memberId
                + " nonce=" + nonce + " windowSize=" + state.dedupSize()
                + " capacity=" + state.dedupCapacity()
                + " totalWindowFull=" + windowFullCount);
        }
        egress.offerWindowFull(r.session, r.sender, nonce);
    }

    @Override
    public void onTimerEvent(long correlationId, long timestamp) {
        final int versionBefore = state.decisionVersion();
        onTimer(correlationId);
        afterEntry(versionBefore, timestamp);
    }

    /** One timer entry: the ordering window, or the boundary tick. */
    private void onTimer(final long correlationId) {
        recordServicePosition();
        snapshots.onTimer(cluster.time(), state.blockNumber());
        egress.drainBacklogs();
        if (correlationId == WINDOW_TIMER_CORRELATION_ID) {
            flushWindow();
            return;
        }
        if (correlationId != BOUNDARY_TIMER_CORRELATION_ID) {
            return;
        }
        // The window closes at a boundary: a reorder never crosses a block.
        flushWindow();
        final Boundary boundary = state.onTick(cluster.time());
        egress.offerBoundary(boundary);
        retained.checkHealth();
        // The status rides every tick, so an observer sees the guard flip
        // and the retention stretch without waiting for a cursor record.
        egress.offerStatus(status());
        requestSnapshotIfDue();
        boundaryTicks++;
        if (boundaryTicks % BOUNDARY_TICK_LOG_EVERY == 0) {
            // The proof that the boundary clock runs. A stall with a leader
            // and no later TICK line is a dead clock; a stall with TICK
            // lines is a block that does not reach the consumers.
            System.out.println("cluster boundary-clock TICK memberId=" + memberId
                + " block=" + state.blockNumber()
                + " role=" + cluster.role());
        }
        // Cluster timers are one-shot, so re-arm for the next tick.
        scheduleBoundaryTimer();
    }

    /**
     * Offer the boundary that an origin record forces, if any, then the
     * record. A forced boundary opens a block as a tick does, so it counts
     * for the snapshot schedule. The request comes after the record, so the
     * notice is the last status before the stall.
     */
    private void offerAdvance(final Optional<Boundary> forcedBoundary, final Relayed relayed) {
        forcedBoundary.ifPresent(egress::offerBoundary);
        egress.offerRelayed(relayed);
        forcedBoundary.ifPresent(boundary -> requestSnapshotIfDue());
    }

    /**
     * Ask for a snapshot when the block that the last boundary opened is
     * due. Call this after each boundary, of the timer or forced.
     */
    private void requestSnapshotIfDue() {
        if (snapshotSchedule.filter(schedule -> schedule.due(state.blockNumber())).isEmpty()) {
            return;
        }
        final SnapshotRequests.Answer answer = requestSnapshot();
        if (answer != SnapshotRequests.Answer.FOLLOWER) {
            System.out.println((answer == SnapshotRequests.Answer.REQUESTED
                    ? "cluster SNAPSHOT requested" : "cluster SNAPSHOT attempt failed")
                + " memberId=" + memberId + " block=" + state.blockNumber());
        }
    }

    /**
     * Send the status with the snapshot notice to every session, then ask
     * for a snapshot. The notice leaves before the consensus module stops,
     * so a client waits for the snapshot and does not call the sealer lost.
     */
    private SnapshotRequests.Answer requestSnapshot() {
        egress.offerSnapshotNotice(status());
        return snapshots.request();
    }

    @Override
    public void onTakeSnapshot(ExclusivePublication snapshotPublication) {
        System.out.println("sealer snapshot START memberId=" + memberId + " block=" + state.blockNumber());
        snapshots.onSnapshot(lastBoundaryClockMs);
        final long startNs = System.nanoTime();
        // The snapshot action is a log event, so every member closes the
        // window here, and the snapshot never has to carry held records.
        flushWindow();
        SnapshotIo.writeSnapshot(snapshotPublication, snapshot(), cluster.idleStrategy());
        latestSnapshotVersion = retainedFile.isPresent()
            ? CanonicalSealerState.RETAINED_FILE_SNAPSHOT_VERSION
            : state.snapshotVersion();
        purgeView = purgeView.withMark(new PurgeView.Mark(cluster.logPosition(), state.blockNumber()));
        // Log to stdout, like the role line below. The block= value is the
        // proof of catch-up. The SNAPSHOT action is itself a replicated-log
        // entry, so a blank member re-executes historical snapshots (and logs
        // TAKEN) during replay. Only the block position in this line proves
        // progress; the number of log lines does not. The
        // cluster-member-rejoin chaos case checks that a wiped member's
        // latest post-wipe TAKEN block reaches the head seen at wipe time.
        // A follower that starts as a follower never gets an onRoleChange
        // call, so role lines cannot prove rejoin.
        // The ms= value is the wall time of this method: the window flush
        // and the snapshot write.
        final long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
        System.out.println("sealer snapshot TAKEN memberId=" + memberId
            + " block=" + state.blockNumber() + " canonicalCount=" + state.canonicalCount()
            + " ms=" + ms);
    }

    @Override
    public void onRoleChange(Cluster.Role newRole) {
        // No role-specific state: the cluster log is replicated, so every
        // member runs the same deterministic state machine. Only the
        // leader's egress offers reach external clients. Log the role change
        // so the chaos suite (deploy/cluster/scripts/chaos.sh) can grep the
        // alloc log for leadership changes. This uses stdout, not a logger,
        // on purpose. Do not switch it to slf4j without also updating the
        // chaos suite's leader detection.
        System.out.println("cluster role=" + newRole + " memberId=" + memberId);
        // The egress backlogs belong to the clients of this member's
        // leadership. Those clients reconnect to the next leader and replay
        // from there, and a follower's offers never reach a client.
        egress.dropBacklogs();
    }

    @Override
    public void onTerminate(Cluster cluster) {
        feed.close();
        retained.close();
    }

    /** Move the member feed on. It never waits, and it changes no replicated state. */
    @Override
    public int doBackgroundWork(final long nowNs) {
        // While the start hold lasts, the member feed reads no request of
        // its executor, so it sends no frame.
        return startHold.held() ? 0 : feed.doWork(egress, state.canonicalCount(), state.blockNumber());
    }

    /** The leadership term whose start this service applied last, or -1 before any. */
    long leadershipTermId() {
        return leadershipTermId;
    }

    /** Whether this member still holds its outputs after its start. */
    boolean holdingAtStart() {
        return startHold.held();
    }

    /** The log position of the last applied entry; 0 before the first. */
    long servicePosition() {
        return servicePosition;
    }

    /** The version of the newest snapshot this member restored or took; 0 before either. */
    int latestSnapshotVersion() {
        return latestSnapshotVersion;
    }

    /** The decision rules of the member, for the admin thread. */
    DecisionRules.View decisionRules() {
        return publishedRules;
    }

    /** Ingress frames of an unknown kind this member dropped. */
    long droppedUnknownKindCount() {
        return unknownKindCount;
    }

    /** The store of the retained frames. */
    RetainedFrames retained() {
        return retained;
    }

    /**
     * The position below which the retained file holds whole records, for
     * the admin thread that serves the file. 0 before the start.
     */
    long retainedHead() {
        return Optional.ofNullable(retained).map(RetainedFrames::publishedHead).orElse(0L);
    }

    /** The snapshot marks and the posted head that the log purge plans with. */
    PurgeView purgeView() {
        return purgeView;
    }

    // --- helpers ------------------------------------------------------------

    private void recordServicePosition() {
        servicePosition = cluster.logPosition();
    }

    /**
     * Arm the ordering window's hold timer. Only the leader's call takes
     * effect; the expiry replicates through the log, so every member
     * flushes at the same position. Re-arming with the same correlation id
     * replaces a stale pending expiry from an earlier window.
     */
    private void scheduleWindowTimer() {
        final long deadline = cluster.time() + OrderingWindow.HOLD_MS;
        while (!cluster.scheduleTimer(WINDOW_TIMER_CORRELATION_ID, deadline)) {
            cluster.idleStrategy().idle();
        }
    }

    private void scheduleBoundaryTimer() {
        final long deadline = cluster.time() + tickIntervalMs;
        // scheduleTimer can fail for a moment under back-pressure on the log.
        // Loop briefly here so the boundary cadence is never silently dropped.
        while (!cluster.scheduleTimer(BOUNDARY_TIMER_CORRELATION_ID, deadline)) {
            cluster.idleStrategy().idle();
        }
        lastBoundaryClockMs = cluster.time();
    }

    /**
     * Revive a dead boundary clock.
     *
     * <p>Called from the log-driven callbacks that keep firing in the wedged
     * state ({@link #onSessionMessage} for ingress records and reconnect-storm
     * replay requests, and {@link #onSessionOpen} for every reconnect), where
     * scheduling timers is allowed. If this member is the leader and no tick
     * or arm has happened for three tick intervals, the pending timer was
     * lost, so re-arm it.</p>
     *
     * <p>This method is idempotent: the shared correlation id replaces the
     * timer instead of scheduling a second one. The watermark reset in
     * {@link #scheduleBoundaryTimer()} stops this method from re-arming on
     * every message while the fresh expiry is still in flight. A fully idle
     * cluster, with no clients at all, cannot revive itself this way. But
     * with no consumers there is nobody to observe boundaries either, and
     * the first reconnect heals it here.</p>
     */
    private void maybeReviveBoundaryClock() {
        if (cluster.role() != Cluster.Role.LEADER) {
            return;
        }
        final long now = cluster.time();
        if (lastBoundaryClockMs != 0L && now - lastBoundaryClockMs <= 3 * tickIntervalMs) {
            return;
        }
        System.out.println("cluster boundary-clock REVIVE memberId=" + memberId
            + " idleMs=" + (lastBoundaryClockMs == 0L ? -1 : now - lastBoundaryClockMs));
        scheduleBoundaryTimer();
    }

    /**
     * Count and log a dropped malformed frame.
     * These paths should never run on an authoritative stream. If the
     * hand-synced Java and Rust envelopes ever drift (see the
     * TODO(envelope) on {@link SealerWire}), the symptom must be a visible
     * counter, not silent record loss. This logs at powers of two so a
     * framing-mismatch flood cannot drown stdout, which the chaos suite
     * greps.
     */
    private void onMalformedFrame(final String what, final int length) {
        droppedFrameCount++;
        logDrop(droppedFrameCount, "malformed " + what, length);
    }

    /**
     * Count and log a dropped frame of an unknown kind. The drop is part
     * of the replicated state machine: every member drops the same frame,
     * so the counters of the members agree.
     */
    private void onUnknownKind(final byte kind, final int length) {
        unknownKindCount++;
        logDrop(unknownKindCount, "unknown-kind kind=" + kind, length);
    }

    /** One log line per power of two of {@code total}, so a flood cannot drown stdout. */
    private void logDrop(final long total, final String what, final int length) {
        if (Long.bitCount(total) == 1) {
            System.out.println("cluster DROPPED " + what + " memberId=" + memberId
                + " length=" + length + " totalDropped=" + total);
        }
    }
}
