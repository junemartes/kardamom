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
import io.kardamom.sealer.Relayed;
import java.nio.ByteOrder;
import java.util.Optional;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Thin Aeron Cluster {@link ClusteredService} that delegates ALL deterministic
 * canonical logic to the pure POJO {@link CanonicalSealerState}.
 *
 * <p>This class owns only the cluster plumbing (ingress decoding, egress
 * framing, timer scheduling and snapshot I/O); it holds no canonical state of
 * its own. Keeping the state machine Aeron-free means its unit tests run with
 * no Aeron jars on the classpath (see the {@code core} subproject).</p>
 *
 * <p><b>App envelope framing.</b> The Rust side defines the application envelope
 * as {@code { kind: u8, canonical_id: 32B, payload }} — the 32-byte canonical id
 * sits at a FIXED offset immediately after the 1-byte {@code kind} tag, and the
 * opaque {@code payload} follows. We match that exact layout: id at offset
 * {@link #CANONICAL_ID_OFFSET}, payload at {@link #PAYLOAD_OFFSET}.</p>
 *
 * <p>TODO(envelope): keep this byte framing in lockstep with the Rust app
 * envelope {@code { kind:u8, canonical_id:32B, payload }} (the canonical id is at
 * a fixed offset; do not invent a different layout). If the Rust {@code kind}
 * discriminant gains variants, branch on {@code buffer.getByte(offset + KIND_OFFSET)}
 * here.</p>
 */
public final class SealerClusteredService implements ClusteredService {

    /** Offset of the 1-byte {@code kind} tag within the app envelope. */
    public static final int KIND_OFFSET = 0;
    /** Offset of the 32-byte canonical id within the app envelope. */
    public static final int CANONICAL_ID_OFFSET = KIND_OFFSET + Byte.BYTES;
    /**
     * Offset from which the relayed payload is forwarded to egress. It starts at
     * the canonical id (NOT after it) so the relayed payload is
     * {@code [canonical_id:32][record_type][fields…]} — the executor needs the
     * canonical id (the tx/source hash) to reconstruct the record and to dedup.
     * The id is still parsed for dedup from {@link #CANONICAL_ID_OFFSET}.
     */
    public static final int RELAY_OFFSET = CANONICAL_ID_OFFSET;

    /** Minimum valid ingress length: kind + canonical id (payload may be empty). */
    private static final int MIN_INGRESS_LEN = CANONICAL_ID_OFFSET + CanonicalSealerState.CANONICAL_ID_LEN;

    /** Egress message kinds (first byte of every egress frame). */
    public static final byte EGRESS_KIND_RELAYED = 1;
    public static final byte EGRESS_KIND_BOUNDARY = 2;

    /** Correlation id used when scheduling the repeating boundary timer. */
    public static final long BOUNDARY_TIMER_CORRELATION_ID = 1L;

    /** Tick cadence for the boundary timer (ms). Matches the 250 ms L2 tick. */
    private final long tickIntervalMs;
    private final int dedupCapacity;
    /** This cluster member's id, logged on role changes for the chaos suite. */
    private final int memberId;

    private Cluster cluster;
    private CanonicalSealerState state;

    // Scratch buffers for ingress id extraction and egress framing. Reused to
    // avoid per-message allocation on the single cluster service thread.
    private final byte[] canonicalIdScratch = new byte[CanonicalSealerState.CANONICAL_ID_LEN];
    private final ExpandableArrayBuffer egressBuffer = new ExpandableArrayBuffer();

    public SealerClusteredService(int dedupCapacity, long tickIntervalMs, int memberId) {
        this.dedupCapacity = dedupCapacity;
        this.tickIntervalMs = tickIntervalMs;
        this.memberId = memberId;
    }

    public SealerClusteredService(int dedupCapacity, long tickIntervalMs) {
        this(dedupCapacity, tickIntervalMs, -1);
    }

    public SealerClusteredService() {
        this(8192, CanonicalSealerState.TICK_INTERVAL_MS);
    }

    @Override
    public void onStart(Cluster cluster, Image snapshotImage) {
        this.cluster = cluster;
        if (snapshotImage != null) {
            // Restore canonical state from the cluster snapshot. The whole
            // snapshot is a single message poll'd off the snapshot image.
            this.state = loadFromSnapshot(snapshotImage);
        } else {
            this.state = new CanonicalSealerState(dedupCapacity, CanonicalSealerState.GENESIS_BLOCK_NUMBER);
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
        // Nothing session-specific to track; canonical state is global.
    }

    @Override
    public void onSessionClose(ClientSession session, long timestamp, CloseReason closeReason) {
        // No-op.
    }

    @Override
    public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {
        if (length < MIN_INGRESS_LEN) {
            // Malformed / too-short envelope: cannot contain kind + 32-byte id.
            return;
        }
        // Parse ONLY the 32-byte canonical id at its fixed offset; the payload
        // is relayed verbatim and never inspected here.
        buffer.getBytes(offset + CANONICAL_ID_OFFSET, canonicalIdScratch);

        final int payloadOffset = offset + RELAY_OFFSET;
        final int payloadLength = length - RELAY_OFFSET;
        final byte[] payload = new byte[payloadLength];
        if (payloadLength > 0) {
            buffer.getBytes(payloadOffset, payload);
        }

        final Optional<Relayed> relayed = state.onRecord(canonicalIdScratch, payload);
        relayed.ifPresent(this::offerRelayed);
    }

    @Override
    public void onTimerEvent(long correlationId, long timestamp) {
        if (correlationId != BOUNDARY_TIMER_CORRELATION_ID) {
            return;
        }
        final Boundary boundary = state.onTick(cluster.time());
        offerBoundary(boundary);
        // Reschedule: cluster timers are one-shot, so re-arm for the next tick.
        scheduleBoundaryTimer();
    }

    @Override
    public void onTakeSnapshot(ExclusivePublication snapshotPublication) {
        final byte[] snapshot = state.takeSnapshot();
        final UnsafeBuffer buf = new UnsafeBuffer(snapshot);
        long result;
        do {
            result = snapshotPublication.offer(buf, 0, snapshot.length);
        } while (result < 0 && retryable(result));
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

    private CanonicalSealerState loadFromSnapshot(final Image snapshotImage) {
        final byte[][] holder = new byte[1][];
        while (holder[0] == null) {
            final int fragments = snapshotImage.poll(
                    (buffer, offset, length, header) -> {
                        final byte[] snapshot = new byte[length];
                        buffer.getBytes(offset, snapshot);
                        holder[0] = snapshot;
                    },
                    1);
            if (fragments == 0) {
                if (snapshotImage.isClosed() || snapshotImage.isEndOfStream()) {
                    break;
                }
                cluster.idleStrategy().idle();
            }
        }
        if (holder[0] == null) {
            // Empty snapshot image (no data): start fresh at genesis.
            return new CanonicalSealerState(dedupCapacity, CanonicalSealerState.GENESIS_BLOCK_NUMBER);
        }
        return CanonicalSealerState.load(holder[0], dedupCapacity);
    }

    private void scheduleBoundaryTimer() {
        final long deadline = cluster.time() + tickIntervalMs;
        // scheduleTimer can transiently fail (back-pressure on the log); retry
        // on the next background tick is acceptable, but we loop briefly here so
        // the boundary cadence is not silently dropped.
        while (!cluster.scheduleTimer(BOUNDARY_TIMER_CORRELATION_ID, deadline)) {
            cluster.idleStrategy().idle();
        }
    }

    private void offerRelayed(final Relayed relayed) {
        final int len = frameRelayed(relayed);
        // Broadcast the relayed canonical record to EVERY client session, not just
        // the sender. The executor replicas consume the canonical tx_ordering stream
        // from egress on their OWN sessions (they never publish ingress), so offering
        // only to the sending session (the sequencer) starved them: they received the
        // broadcast boundaries but no records, tripping the executor's
        // BoundaryMisaligned check (want_count>have_count). Mirrors offerBoundary.
        for (final ClientSession session : cluster.clientSessions()) {
            offerToSession(session, len);
        }
    }

    /**
     * Frame a {@link Relayed} into {@link #egressBuffer}:
     * {@code kind(1) | index(8) | payloadLen(4) | payload[]}. The payload is
     * copied through verbatim.
     */
    private int frameRelayed(final Relayed relayed) {
        final MutableDirectBuffer buf = egressBuffer;
        int pos = 0;
        buf.putByte(pos, EGRESS_KIND_RELAYED);
        pos += Byte.BYTES;
        buf.putLong(pos, relayed.index, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putInt(pos, relayed.payload.length, ByteOrder.LITTLE_ENDIAN);
        pos += Integer.BYTES;
        if (relayed.payload.length > 0) {
            buf.putBytes(pos, relayed.payload);
            pos += relayed.payload.length;
        }
        return pos;
    }

    private void offerBoundary(final Boundary boundary) {
        final int len = frameBoundary(boundary);
        // Boundaries are broadcast to every open client session.
        for (final ClientSession session : cluster.clientSessions()) {
            offerToSession(session, len);
        }
    }

    /**
     * Frame a {@link Boundary} into {@link #egressBuffer}:
     * {@code kind(1) | blockNumber(8) | endTxIdx(8) | l2Timestamp(8)}.
     */
    private int frameBoundary(final Boundary boundary) {
        final MutableDirectBuffer buf = egressBuffer;
        int pos = 0;
        buf.putByte(pos, EGRESS_KIND_BOUNDARY);
        pos += Byte.BYTES;
        buf.putLong(pos, boundary.blockNumber, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, boundary.endTxIdx, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, boundary.l2Timestamp, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        return pos;
    }

    /**
     * Bounded egress-offer retries per session per frame. The offers run on the
     * SINGLE clustered-service thread: an UNBOUNDED retry against one wedged or
     * slow client session (its egress image full because the subscriber
     * stopped draining) blocks record relaying and the boundary tick for the
     * WHOLE cluster — one sick client must never stall the pipeline. After the
     * bound, the frame is dropped for THAT session only; the client sees a gap
     * on its egress, which its consumer detects (the executor's boundary
     * alignment) — the correct failure locality.
     */
    private static final int MAX_OFFER_RETRIES = 8;

    private void offerToSession(final ClientSession session, final int length) {
        long result;
        int retries = 0;
        do {
            result = session.offer(egressBuffer, 0, length);
        } while (result < 0 && retryable(result) && ++retries < MAX_OFFER_RETRIES);
    }

    /** Whether a negative offer result is a retryable back-pressure condition. */
    private boolean retryable(final long offerResult) {
        // ADMIN_ACTION / BACK_PRESSURED are retryable; CLOSED / MAX_POSITION_EXCEEDED
        // / NOT_CONNECTED are not. Treat the back-pressure cases as retryable and
        // idle, anything else as terminal for this offer.
        if (offerResult == io.aeron.Publication.BACK_PRESSURED
                || offerResult == io.aeron.Publication.ADMIN_ACTION) {
            cluster.idleStrategy().idle();
            return true;
        }
        return false;
    }
}
