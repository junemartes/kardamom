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

    /** Tick cadence for the boundary timer, in ms. Matches the 250 ms L2 tick. */
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

    /** Contiguity rejects emitted (logged at power-of-two counts). */
    private long rejectedFrameCount = 0;

    // Scratch buffers for ingress id and sender extraction. Reuse them to
    // avoid a per-message allocation on the single cluster service thread.
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
            // or empty snapshot image is fatal. Restarting silently at genesis
            // would diverge from the rest of the cluster, which assumes the
            // snapshotted state (and the log replayed after it) is correct.
            final byte[] snapshot = SnapshotIo.readSnapshot(snapshotImage, cluster.idleStrategy());
            this.state = CanonicalSealerState.load(snapshot, dedupCapacity);
            // The retained deque is not snapshotted (v1). Nothing before the
            // restore point can ever be served, so the retention floors start
            // at the first frame this member can retain: record index
            // canonicalCount and boundary block blockNumber (the next ones to
            // emit). Floors left at genesis would answer a pre-snapshot replay
            // request with a false REPLAY_DONE (a silent canonical gap)
            // instead of the correct REPLAY_UNAVAILABLE.
            this.egress = new SealerEgress(
                cluster, memberId, state.canonicalCount(), state.blockNumber());
            // Log to stdout so the cluster-member-rejoin chaos case can check
            // that a wiped member came back through a snapshot restore, not
            // silently at genesis.
            System.out.println("sealer snapshot RESTORED memberId=" + memberId
                + " block=" + state.blockNumber() + " canonicalCount=" + state.canonicalCount());
        } else {
            this.state = new CanonicalSealerState(dedupCapacity, CanonicalSealerState.GENESIS_BLOCK_NUMBER);
            this.egress = new SealerEgress(
                cluster, memberId, 0L, CanonicalSealerState.GENESIS_BLOCK_NUMBER);
            System.out.println("sealer state FRESH at genesis memberId=" + memberId);
        }
        // Do not call scheduleTimer here: Aeron rejects timer scheduling from
        // onStart. The boundary timer is armed from onNewLeadershipTermEvent,
        // which is log-driven.
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
    }

    @Override
    public void onSessionOpen(ClientSession session, long timestamp) {
        // Nothing session-specific to track: canonical state is global. But a
        // session opening is a log-driven moment where timer scheduling is
        // allowed, so use it to revive a dead boundary clock (see helper).
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
            // Malformed or too-short envelope: it cannot carry the kind tag.
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
                // A replay request announces a consumer just as a SUBSCRIBE
                // frame does.
                egress.addConsumer(session.id());
                egress.handleReplayRequest(
                    session, fromIndex, fromBlock, state.canonicalCount(), state.blockNumber());
                // A consumer sends a replay request when it sees no egress.
                // If this member's boundary clock died (it lost its pending
                // timer after election churn), revive it now.
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
     * Process one single-record ingress frame at {@code offset}.
     * Parse the guard header (sender and nonce) and the 32-byte canonical id
     * at their fixed offsets. The payload is relayed as is and never
     * inspected. Then dedup the record, check contiguity, and relay it if
     * accepted. A contiguity reject answers the offering session with an
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
     * Answer a contiguity reject to the offering session. Sending it is
     * member-local egress IO, since only the leader's offer reaches the
     * client, exactly like record relaying. The rejection itself has no
     * dedup insert and no count. It is part of the deterministic state
     * machine and is identical on every member.
     */
    private void onContiguityReject(final ClientSession session, final long nonce, final long expected) {
        rejectedFrameCount++;
        if (Long.bitCount(rejectedFrameCount) == 1) {
            // Log to stdout like the other operational signals, so the chaos
            // suite can grep it. Count at powers of two so a gap storm cannot
            // flood the log.
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
        // Cluster timers are one-shot, so re-arm for the next tick.
        scheduleBoundaryTimer();
    }

    @Override
    public void onTakeSnapshot(ExclusivePublication snapshotPublication) {
        SnapshotIo.writeSnapshot(snapshotPublication, state.takeSnapshot(), cluster.idleStrategy());
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

    // --- helpers ------------------------------------------------------------

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
