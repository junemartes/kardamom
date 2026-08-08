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
import io.kardamom.sealer.OriginAdvance;
import java.nio.ByteOrder;
import java.util.Optional;
import org.agrona.DirectBuffer;

/**
 * Thin Aeron Cluster {@link ClusteredService} that delegates ALL deterministic
 * canonical logic to the pure POJO {@link CanonicalSealerState}.
 *
 * <p>This class owns only the cluster plumbing (ingress decoding and dispatch,
 * timer scheduling and the lifecycle hooks); it holds no canonical state of
 * its own. Keeping the state machine Aeron-free means its unit tests run with
 * no Aeron jars on the classpath (see the {@code core} subproject). The wire
 * layout (envelope offsets, message kinds — and the Java&harr;Rust lockstep
 * contract) lives in {@link SealerWire}, the egress framing/offer/retention
 * machinery in {@link SealerEgress}, and snapshot stream I/O in
 * {@link SnapshotIo}.</p>
 */
public final class SealerClusteredService implements ClusteredService {

    /** Correlation id used when scheduling the repeating boundary timer. */
    public static final long BOUNDARY_TIMER_CORRELATION_ID = 1L;

    /** Tick cadence for the boundary timer (ms). Matches the 250 ms L2 tick. */
    private final long tickIntervalMs;
    private final int dedupCapacity;
    /** This cluster member's id, logged on role changes for the chaos suite. */
    private final int memberId;

    private Cluster cluster;
    private CanonicalSealerState state;
    private SealerEgress egress;

    /** Malformed ingress frames dropped (logged at power-of-two counts). */
    private long droppedFrameCount = 0;

    /**
     * Cluster time of the last boundary tick (or timer arm) — the
     * boundary-clock LIVENESS watermark. Pending cluster timers live in the
     * leader's wheel only, and rapid election churn (a killed leader
     * restarting and re-contesting) can strand EVERY member without a live
     * timer: a term's re-arm from {@link #onNewLeadershipTermEvent} is only
     * actioned by a module that is (still) leader when the call lands, so a
     * re-arm racing a subsequent election is silently dropped — the clock
     * then stops forever while elections, replay serving and session traffic
     * all look healthy ("leader elected but nothing commits", observed live:
     * every executor frozen while ~400 sessions churned through the
     * egress-silence watchdog). {@link #maybeReviveBoundaryClock()} uses this
     * watermark to revive the clock from log-driven callbacks.
     */
    private long lastBoundaryClockMs = 0L;

    /** Contiguity rejects emitted (logged at power-of-two counts). */
    private long rejectedFrameCount = 0;

    // Scratch buffers for ingress id/sender extraction. Reused to avoid
    // per-message allocation on the single cluster service thread.
    private final byte[] canonicalIdScratch = new byte[CanonicalSealerState.CANONICAL_ID_LEN];
    private final byte[] senderScratch = new byte[CanonicalSealerState.SENDER_LEN];

    public SealerClusteredService(int dedupCapacity, long tickIntervalMs, int memberId) {
        this.dedupCapacity = dedupCapacity;
        this.tickIntervalMs = tickIntervalMs;
        this.memberId = memberId;
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
            // or empty snapshot image is FATAL: silently restarting at genesis
            // while the rest of the cluster (and the log replayed after the
            // snapshot point) assumes the snapshotted state is deterministic
            // state-machine divergence.
            final byte[] snapshot = SnapshotIo.readSnapshot(snapshotImage, cluster.idleStrategy());
            this.state = CanonicalSealerState.load(snapshot, dedupCapacity);
            // The retained deque is NOT snapshotted (v1): nothing before the
            // restore point can ever be served, so the retention floors start
            // at the first frame this member CAN retain — record index
            // canonicalCount and boundary block blockNumber (the next ones to
            // be emitted). Leaving the floors at genesis would answer a
            // pre-snapshot replay request with a bogus REPLAY_DONE (a silent
            // canonical gap) instead of the honest REPLAY_UNAVAILABLE.
            this.egress = new SealerEgress(
                cluster, memberId, state.canonicalCount(), state.blockNumber());
            // stdout for grep-ability (same contract as the role line below):
            // the cluster-member-rejoin chaos case asserts a wiped member came
            // back via SNAPSHOT restore, not silently at genesis.
            System.out.println("sealer snapshot RESTORED memberId=" + memberId
                + " block=" + state.blockNumber() + " canonicalCount=" + state.canonicalCount());
        } else {
            this.state = new CanonicalSealerState(dedupCapacity, CanonicalSealerState.GENESIS_BLOCK_NUMBER);
            this.egress = new SealerEgress(
                cluster, memberId, 0L, CanonicalSealerState.GENESIS_BLOCK_NUMBER);
            System.out.println("sealer state FRESH at genesis memberId=" + memberId);
        }
        // Do NOT scheduleTimer here: Aeron rejects it from onStart ("sending
        // messages or scheduling timers is not allowed from onStart"); the
        // boundary timer is armed from onNewLeadershipTermEvent (log-driven).
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
        // First sanctioned point to schedule a timer (this is log-driven, unlike
        // onStart/doBackgroundWork). Re-arm the repeating boundary timer on EVERY
        // new leadership term — unconditionally. Pending cluster timers live in
        // the LEADER's timer wheel only (scheduleTimer from a follower is not
        // actioned; only the expiry is replicated through the log), so any
        // election can lose the pending tick: if the old leader died — or stepped
        // down during a quorum outage — before appending the expiry, NO member
        // holds a live timer afterwards and the boundary clock stops forever
        // (records still relay, but blocks never seal — the executor gauge
        // freezes; observed as the chaos suite's post-quorum-recovery stall).
        // Re-arming with the SAME correlation id is idempotent: Aeron replaces
        // the pending timer rather than double-scheduling.
        scheduleBoundaryTimer();
    }

    @Override
    public void onSessionOpen(ClientSession session, long timestamp) {
        // Nothing session-specific to track; canonical state is global. But a
        // session opening is a log-driven moment where timer scheduling is
        // sanctioned — use it to revive a dead boundary clock (see helper).
        maybeReviveBoundaryClock();
    }

    @Override
    public void onSessionClose(ClientSession session, long timestamp, CloseReason closeReason) {
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
        if (length <= SealerWire.KIND_OFFSET) {
            // Malformed / too-short envelope: cannot even carry the kind tag.
            onMalformedFrame("ingress-envelope", length);
            return;
        }
        final byte kind = buffer.getByte(offset + SealerWire.KIND_OFFSET);
        switch (kind) {
            case SealerWire.KIND_SUBSCRIBE:
                egress.addConsumer(session.id());
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
                // A replay request is a consumer announcing itself as surely as a
                // SUBSCRIBE frame does.
                egress.addConsumer(session.id());
                egress.handleReplayRequest(
                    session, fromIndex, fromBlock, state.canonicalCount(), state.blockNumber());
                // A replay request is exactly what a consumer sends when it sees
                // no egress — if that's because THIS member's boundary clock died
                // (lost pending timer after election churn), revive it now.
                maybeReviveBoundaryClock();
                return;
            }
            case SealerWire.KIND_ORIGIN_RECORD:
                onOriginRecord(buffer, offset, length);
                maybeReviveBoundaryClock();
                return;
            case SealerWire.KIND_BATCH:
                onBatch(session, buffer, offset, length);
                return;
            default:
                // KIND_INGRESS_RECORD — and, exactly as before the kind byte was
                // switched on, any unrecognized kind: the length check is the
                // only envelope guard on this path.
                if (length < SealerWire.MIN_INGRESS_LEN) {
                    // Malformed / too-short envelope: cannot contain kind + 32-byte id.
                    onMalformedFrame("ingress-envelope", length);
                    return;
                }
                processRecord(session, buffer, offset, length);
                // Records relaying while blocks never seal is the dead-clock signature
                // (canonical stream advances, boundary cadence gone) — revive here so
                // sustained ingress load heals the clock without waiting for a
                // consumer reconnect.
                maybeReviveBoundaryClock();
        }
    }

    /**
     * Iterate a {@link SealerWire#KIND_BATCH} frame, processing each embedded
     * entry EXACTLY like an individually-offered record (same dedup, same
     * per-record relay). A malformed entry drops the REST of the batch and is
     * counted; the boundary clock is only revived after a fully-parsed batch,
     * as on the single-record path.
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
     * Handle a {@link SealerWire#KIND_ORIGIN_RECORD} frame: strip the origin
     * and slot count, relay the remaining payload verbatim, and offer the
     * forced boundary FIRST so the record leads the block it opens rather than
     * trailing the one it closes.
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

        // The relayed payload keeps the SAME shape as an ordinary record —
        // [canonical_id:32][record_type][fields…]. Everything after the slot
        // count is the tail; measuring from the id offset would double-count
        // the 32 bytes copied separately, and the slack would land where rkyv
        // looks for its root.
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

        final Optional<OriginAdvance> advance;
        try {
            advance =
                state.onOriginRecord(canonicalIdScratch, l1Origin, slotCount, payload, cluster.time());
        } catch (final IllegalArgumentException ex) {
            // A non-advancing origin is a producer bug. Every member rejects it
            // identically (the check reads only replicated state), so dropping
            // is deterministic — and far better than throwing out of the
            // clustered service, which would take the cluster down.
            onMalformedFrame("origin-record-regression", length);
            return;
        }
        if (advance.isEmpty()) {
            return; // duplicate epoch from a racing sequencer
        }
        advance.get().forcedBoundary().ifPresent(egress::offerBoundary);
        egress.offerRelayed(advance.get().relayed());
    }

    /**
     * Process one single-record ingress frame at {@code offset}: parse the
     * guard header (sender + nonce, #85 fix B) and the 32-byte canonical id
     * at their fixed offsets (the payload is relayed verbatim and never
     * inspected), dedup, contiguity-check, and relay if accepted. A
     * contiguity reject answers the OFFERING session with an
     * {@link SealerWire#EGRESS_KIND_CONTIGUITY_REJECT} frame. Shared by the
     * direct path and each {@link SealerWire#KIND_BATCH} entry.
     */
    private void processRecord(
            final ClientSession session, final DirectBuffer buffer, final int offset, final int length) {
        buffer.getBytes(offset + SealerWire.CANONICAL_ID_OFFSET, canonicalIdScratch);
        buffer.getBytes(offset + SealerWire.SENDER_OFFSET, senderScratch);
        final long nonce = buffer.getLong(offset + SealerWire.NONCE_OFFSET, ByteOrder.LITTLE_ENDIAN);
        final int payloadOffset = offset + SealerWire.RELAY_OFFSET;
        final int payloadLength = length - SealerWire.RELAY_OFFSET;
        final byte[] payload = new byte[payloadLength];
        if (payloadLength > 0) {
            buffer.getBytes(payloadOffset, payload);
        }
        final CanonicalSealerState.RecordOutcome outcome =
            state.onRecord(canonicalIdScratch, senderScratch, nonce, payload);
        if (outcome.rejected) {
            onContiguityReject(session, nonce, outcome.expectedNonce);
            return;
        }
        outcome.relayed.ifPresent(egress::offerRelayed);
    }

    /**
     * Answer a contiguity reject to the offering session. Emitting is
     * member-local egress IO (only the leader's offer reaches the client),
     * exactly like record relaying; the REJECTION itself — no dedup insert,
     * no count — is part of the deterministic state machine and identical on
     * every member.
     */
    private void onContiguityReject(final ClientSession session, final long nonce, final long expected) {
        rejectedFrameCount++;
        if (Long.bitCount(rejectedFrameCount) == 1) {
            // stdout like the other operational signals — grep-able by the
            // chaos suite; power-of-two counted so a gap storm cannot flood.
            System.out.println("cluster CONTIGUITY-REJECT memberId=" + memberId
                + " nonce=" + nonce + " expected=" + expected
                + " totalRejected=" + rejectedFrameCount);
        }
        egress.offerContiguityReject(session, senderScratch, nonce, expected);
    }

    @Override
    public void onTimerEvent(long correlationId, long timestamp) {
        if (correlationId != BOUNDARY_TIMER_CORRELATION_ID) {
            return;
        }
        final Boundary boundary = state.onTick(cluster.time());
        egress.offerBoundary(boundary);
        // Reschedule: cluster timers are one-shot, so re-arm for the next tick.
        scheduleBoundaryTimer();
    }

    @Override
    public void onTakeSnapshot(ExclusivePublication snapshotPublication) {
        SnapshotIo.writeSnapshot(snapshotPublication, state.takeSnapshot(), cluster.idleStrategy());
        // stdout for grep-ability (same contract as the role line below). The
        // block= position is the CATCH-UP PROOF: the SNAPSHOT action is itself
        // a replicated-log entry, so a blank member re-executes historical
        // snapshots (and logs TAKEN) all through replay — only the POSITION in
        // this line proves progress, never the line count. The
        // cluster-member-rejoin chaos case asserts a wiped member's latest
        // post-wipe TAKEN block reaches the head observed at wipe time. (A
        // follower that starts as follower never gets an onRoleChange, so role
        // lines cannot prove rejoin.)
        System.out.println("sealer snapshot TAKEN memberId=" + memberId
            + " block=" + state.blockNumber() + " canonicalCount=" + state.canonicalCount());
    }

    @Override
    public void onRoleChange(Cluster.Role newRole) {
        // No role-specific behaviour: the cluster log is replicated, so every
        // member runs the same deterministic state machine. Only the leader's
        // egress offers reach external clients. We log the role transition so the
        // chaos suite (deploy/cluster/scripts/chaos.sh) can grep the alloc log for
        // leadership changes. Intentionally stdout (NOT a logger) for grep-ability —
        // do not "clean up" into slf4j without updating the chaos leader detection.
        System.out.println("cluster role=" + newRole + " memberId=" + memberId);
    }

    @Override
    public void onTerminate(Cluster cluster) {
        // No external resources to release.
    }

    // --- helpers ------------------------------------------------------------

    private void scheduleBoundaryTimer() {
        final long deadline = cluster.time() + tickIntervalMs;
        // scheduleTimer can transiently fail (back-pressure on the log); retry
        // on the next background tick is acceptable, but we loop briefly here so
        // the boundary cadence is not silently dropped.
        while (!cluster.scheduleTimer(BOUNDARY_TIMER_CORRELATION_ID, deadline)) {
            cluster.idleStrategy().idle();
        }
        lastBoundaryClockMs = cluster.time();
    }

    /**
     * Revive a dead boundary clock. Called from the log-driven callbacks that
     * KEEP firing in the wedged state ({@link #onSessionMessage} — ingress
     * records and the reconnect storm's replay requests; {@link #onSessionOpen}
     * — every reconnect), where scheduling timers is sanctioned. If this
     * member is the leader and no tick (or arm) has been observed for three
     * tick intervals, the pending timer was lost — re-arm it. Idempotent (the
     * shared correlation id replaces rather than double-schedules), and the
     * watermark reset in {@link #scheduleBoundaryTimer()} keeps this from
     * re-arming on every message while the fresh expiry is still in flight.
     * A fully idle cluster (no clients at all) cannot self-revive — but with
     * no consumers there is nobody to observe boundaries either, and the
     * first (re)connect heals it here.
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
     * Count + log a dropped malformed frame. These are "should never happen"
     * paths on an authoritative stream: if the hand-synced Java/Rust envelope
     * ever drifts (see the TODO(envelope) on {@link SealerWire}) the symptom
     * must be a visible counter, not silent record loss. Logged at
     * power-of-two counts so a framing-mismatch flood cannot drown stdout
     * (which the chaos suite greps).
     */
    private void onMalformedFrame(final String what, final int length) {
        droppedFrameCount++;
        if (Long.bitCount(droppedFrameCount) == 1) {
            System.out.println("cluster DROPPED malformed " + what + " memberId=" + memberId
                + " length=" + length + " totalDropped=" + droppedFrameCount);
        }
    }
}
