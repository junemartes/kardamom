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
import io.kardamom.sealer.ClusterStatus;
import io.kardamom.sealer.OrderingWindow;
import io.kardamom.sealer.OriginAdvance;
import io.kardamom.sealer.RemoteOriginAdvance;
import io.kardamom.sealer.VoidLedger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.agrona.DirectBuffer;

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

    private Cluster cluster;
    private CanonicalSealerState state;
    private SealerEgress egress;

    /**
     * The log position of the last entry this service applied. The admin
     * endpoint reads it from another thread and compares it with the
     * commit position: the gap is the service's lag behind the log.
     */
    private volatile long servicePosition;

    /**
     * The snapshot marks and the posted head, for the log purge thread.
     * The service thread replaces the value after a snapshot and after the
     * posted head moves; the purge thread reads it.
     */
    private volatile PurgeView purgeView = PurgeView.EMPTY;

    /** Malformed ingress frames dropped (logged at power-of-two counts). */
    private long droppedFrameCount = 0;

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

    /** Remote-origin rejects emitted (logged at power-of-two counts). */
    private long remoteRejectedFrameCount = 0;

    private final VoidLedger.Config voidConfig;
    /**
     * The inclusion horizon, in blocks. Replicated configuration: every
     * member must agree on it, because it bounds the dedup window and the
     * deadline the sealer assigns a marker.
     */
    private final long inclusionHorizonBlocks;
    /**
     * The DA-lag budget, in blocks. Replicated configuration: it decides
     * accept-or-reject inside the replicated state machine. Zero turns the
     * guard off.
     */
    private final long daLagBudgetBlocks;
    /** DA-lag rejects emitted (logged at power-of-two counts). */
    private long daLagRejectCount = 0;
    /**
     * The priority window in front of the record path. Replicated
     * configuration: its size decides the relay order, so every member runs
     * the same value, and a snapshot restore checks it. Size 0 passes every
     * record through at once.
     */
    private final OrderingWindow<HeldRecord> window;

    /**
     * Scratch buffer for the id of an origin, remote-origin, or void frame.
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
            inclusionHorizonBlocks, orderingWindow, CanonicalSealerState.DEFAULT_DA_LAG_BUDGET_BLOCKS);
    }

    /** The full constructor, with this member's ordering window size and DA-lag budget. */
    public SealerClusteredService(
            int dedupCapacity,
            long tickIntervalMs,
            int memberId,
            Set<Long> remoteOrigins,
            VoidLedger.Config voidConfig,
            long inclusionHorizonBlocks,
            int orderingWindow,
            long daLagBudgetBlocks) {
        this.inclusionHorizonBlocks = inclusionHorizonBlocks;
        this.window = new OrderingWindow<>(orderingWindow);
        this.daLagBudgetBlocks = daLagBudgetBlocks;
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

    @Override
    public void onStart(Cluster cluster, Image snapshotImage) {
        this.cluster = cluster;
        if (snapshotImage != null) {
            // Restore canonical state from the cluster snapshot. An unreadable
            // or empty snapshot image is fatal. Restarting silently at genesis
            // would diverge from the rest of the cluster, which assumes the
            // snapshotted state (and the log replayed after it) is correct.
            restore(SnapshotIo.readSnapshot(snapshotImage, cluster.idleStrategy()));
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
        } else {
            this.state = new CanonicalSealerState(
                dedupCapacity, CanonicalSealerState.GENESIS_BLOCK_NUMBER, remoteOrigins, voidConfig,
                inclusionHorizonBlocks, window.capacity(), daLagBudgetBlocks);
            this.egress = new SealerEgress(
                cluster, memberId, 0L, CanonicalSealerState.GENESIS_BLOCK_NUMBER);
            System.out.println("sealer state FRESH at genesis memberId=" + memberId);
        }
        // Do not call scheduleTimer here: Aeron rejects timer scheduling from
        // onStart. The boundary timer is armed from onNewLeadershipTermEvent,
        // which is log-driven.
    }

    /**
     * The snapshot: the state section, then the egress retention above the
     * posted head. Every member writes the same bytes at the same log
     * position.
     */
    byte[] snapshot() {
        final byte[] stateBytes = state.takeSnapshot();
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
            buf, dedupCapacity, remoteOrigins, voidConfig, inclusionHorizonBlocks, window.capacity(),
            daLagBudgetBlocks);
        this.egress = new SealerEgress(
            cluster, memberId, state.canonicalCount(), state.blockNumber());
        egress.readSnapshot(buf);
        egress.setPostedHead(state.postedHead());
    }

    /** The chain's status: the posted and sealed heads, the guard, and the retention floors. */
    private ClusterStatus status() {
        return new ClusterStatus(
            state.postedHead(),
            state.sealedHead(),
            state.daLagBudgetBlocks(),
            state.daLagHalted(),
            egress.retainedCount(),
            egress.firstRetainedIndex(),
            egress.firstRetainedBlock());
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
        scheduleBoundaryTimer();
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
    }

    @Override
    public void onSessionClose(ClientSession session, long timestamp, CloseReason closeReason) {
        System.out.println("cluster SESSION close memberId=" + memberId
            + " session=" + session.id() + " reason=" + closeReason);
        egress.removeConsumer(session.id());
    }

    @Override
    public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {
        dispatchSessionMessage(session, buffer, offset, length);
        recordServicePosition();
    }

    /** Decode the kind tag and dispatch one ingress frame. */
    private void dispatchSessionMessage(
            final ClientSession session,
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
                onVoidRequest(buffer, offset, length);
                return;
            case SealerWire.KIND_ORIGIN_RECORD:
                onOriginRecord(buffer, offset, length);
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
            default:
                // KIND_INGRESS_RECORD, and any unrecognized kind: the length
                // check is the only envelope guard on this path.
                if (length < SealerWire.MIN_INGRESS_LEN) {
                    // Malformed or too-short envelope: it cannot hold kind
                    // plus a 32-byte id.
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
            if (entryLen < SealerWire.MIN_INGRESS_LEN || pos + entryLen > limit) {
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
     */
    private void onOriginRecord(final DirectBuffer buffer, final int offset, final int length) {
        if (length < SealerWire.MIN_ORIGIN_RECORD_LEN) {
            onMalformedFrame("origin-record", length);
            return;
        }
        buffer.getBytes(offset + SealerWire.ORIGIN_ID_OFFSET, canonicalIdScratch);
        final long l1Origin = buffer.getLong(offset + SealerWire.ORIGIN_OFFSET, ByteOrder.LITTLE_ENDIAN);
        final long slotCount =
                buffer.getInt(offset + SealerWire.SLOT_COUNT_OFFSET, ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL;

        // The relayed payload keeps the same shape as an ordinary record:
        // [canonical_id:32][record_type][fields…]. Everything after the slot
        // count is the tail. Measuring from the id offset would count the
        // 32 id bytes twice, and the extra bytes would land where rkyv looks
        // for its root.
        final int tailLength = length - (SealerWire.SLOT_COUNT_OFFSET + Integer.BYTES);
        final byte[] payload = new byte[CanonicalSealerState.CANONICAL_ID_LEN + tailLength];
        buffer.getBytes(
                offset + SealerWire.ORIGIN_ID_OFFSET, payload, 0, CanonicalSealerState.CANONICAL_ID_LEN);
        if (tailLength > 0) {
            buffer.getBytes(
                    offset + SealerWire.SLOT_COUNT_OFFSET + Integer.BYTES,
                    payload,
                    CanonicalSealerState.CANONICAL_ID_LEN,
                    tailLength);
        }

        // The record closes the open block, so the window closes with it:
        // a reorder never crosses a block.
        flushWindow();
        final Optional<OriginAdvance> advance;
        try {
            advance =
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
        if (advance.isEmpty()) {
            return; // Duplicate epoch from a racing sequencer.
        }
        advance.get().forcedBoundary().ifPresent(egress::offerBoundary);
        egress.offerRelayed(advance.get().relayed());
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
        if (length < SealerWire.MIN_REMOTE_ORIGIN_RECORD_LEN) {
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

        // Same relay shape as an epoch: [canonical_id:32][record_type][fields…],
        // measured from the END of the header so no slack lands where rkyv
        // looks for its root.
        final int tailLength = length - SealerWire.MIN_REMOTE_ORIGIN_RECORD_LEN;
        final byte[] payload = new byte[CanonicalSealerState.CANONICAL_ID_LEN + tailLength];
        buffer.getBytes(
                offset + SealerWire.REMOTE_ID_OFFSET, payload, 0, CanonicalSealerState.CANONICAL_ID_LEN);
        if (tailLength > 0) {
            buffer.getBytes(
                    offset + SealerWire.MIN_REMOTE_ORIGIN_RECORD_LEN,
                    payload,
                    CanonicalSealerState.CANONICAL_ID_LEN,
                    tailLength);
        }

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
        outcome.advance.get().forcedBoundary().ifPresent(egress::offerBoundary);
        egress.offerRelayed(outcome.advance.get().relayed());
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
        final int payloadOffset = offset + SealerWire.RELAY_OFFSET;
        final int payloadLength = length - SealerWire.RELAY_OFFSET;
        final byte[] payload = new byte[payloadLength];
        if (payloadLength > 0) {
            buffer.getBytes(payloadOffset, payload);
        }
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
        final CanonicalSealerState.RecordOutcome outcome =
            state.onRecord(r.canonicalId, r.sender, r.nonce, r.deadline, r.payload);
        switch (outcome.kind) {
            case CONTIGUITY_REJECT -> onContiguityReject(r, outcome.expectedNonce);
            case PAST_DEADLINE -> onPastDeadline(r, outcome.maxInclusionBlock);
            case WINDOW_FULL -> onWindowFull(r);
            case DA_LAG_REJECT -> onDaLagReject(r);
            case RELAYED -> outcome.relayed.ifPresent(egress::offerRelayed);
            case DUPLICATE -> { }
        }
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
        recordServicePosition();
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
        // The status rides every tick, so an observer sees the guard flip
        // and the retention stretch without waiting for a cursor record.
        egress.offerStatus(status());
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

    @Override
    public void onTakeSnapshot(ExclusivePublication snapshotPublication) {
        // The snapshot action is a log event, so every member closes the
        // window here, and the snapshot never has to carry held records.
        flushWindow();
        SnapshotIo.writeSnapshot(snapshotPublication, snapshot(), cluster.idleStrategy());
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
        System.out.println("sealer snapshot TAKEN memberId=" + memberId
            + " block=" + state.blockNumber() + " canonicalCount=" + state.canonicalCount());
    }

    @Override
    public void onRoleChange(Cluster.Role newRole) {
        // No role-specific behavior: the cluster log is replicated, so every
        // member runs the same deterministic state machine. Only the
        // leader's egress offers reach external clients. Log the role change
        // so the chaos suite (deploy/cluster/scripts/chaos.sh) can grep the
        // alloc log for leadership changes. This uses stdout, not a logger,
        // on purpose. Do not switch it to slf4j without also updating the
        // chaos suite's leader detection.
        System.out.println("cluster role=" + newRole + " memberId=" + memberId);
    }

    @Override
    public void onTerminate(Cluster cluster) {
        // No external resources to release.
    }

    /** The log position of the last applied entry; 0 before the first. */
    long servicePosition() {
        return servicePosition;
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
        if (Long.bitCount(droppedFrameCount) == 1) {
            System.out.println("cluster DROPPED malformed " + what + " memberId=" + memberId
                + " length=" + length + " totalDropped=" + droppedFrameCount);
        }
    }
}
