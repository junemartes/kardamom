package io.kardamom.sealer.cluster;

import io.aeron.Publication;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.kardamom.sealer.Boundary;
import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.Relayed;
import java.nio.ByteOrder;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * The egress layer of {@link SealerClusteredService}: frames relayed records,
 * boundaries and control messages, offers them with the deadline-then-close
 * semantics documented on {@link #OFFER_DEADLINE_NS}, targets the record
 * fan-out at announced consumers, and retains framed egress (bounded) to serve
 * client replay requests.
 *
 * <p>SINGLE-THREADED BY DESIGN: every method here is invoked on the one
 * clustered-service thread (Aeron {@code ClusteredService} callbacks), exactly
 * as when this logic lived inside the service — the shared staging buffer, the
 * retained deque and the consumer set are therefore unsynchronized on purpose.
 * Do not call into this class from any other thread.</p>
 */
final class SealerEgress {

    /**
     * Per-frame egress-offer deadline per session. The offers run on the SINGLE
     * clustered-service thread: an UNBOUNDED retry against one wedged client
     * session (its egress image full because the subscriber stopped draining)
     * blocks record relaying and the boundary tick for the WHOLE cluster. But
     * silently DROPPING the frame is worse: a client that misses a boundary
     * seals two blocks as one and provably diverges (observed as a validator
     * BAL divergence under chaos load — a count-bounded retry of a few idle
     * cycles expired in sub-millisecond bursts of ordinary back-pressure). So:
     * retry up to a real deadline, and on exhaustion CLOSE the session — an
     * explicit signal the client can act on (reconnect; the executor's
     * boundary-alignment fail-stop + archive crash recovery self-heal), never
     * a silent gap. Wall-clock is safe here: egress offers are member-local IO
     * (only the leader's reach clients), not replicated state. 1s of grace: a
     * DEAD session still costs only one deadline before it is closed, while a
     * live client riding out a CI CPU spike (e.g. during a chaos node kill +
     * reschedule) is not spuriously executed — a close forces that client
     * through reconnect + fail-stop + crash recovery, so false positives
     * cascade (observed: all three executors restarting at once).
     */
    private static final long OFFER_DEADLINE_NS = java.util.concurrent.TimeUnit.SECONDS.toNanos(1);

    /** One retained, already-framed egress frame (record or boundary). */
    private static final class RetainedFrame {
        final byte[] frame;
        final boolean boundary;
        final long key; // record index, or boundary block number

        RetainedFrame(final byte[] frame, final boolean boundary, final long key) {
            this.frame = frame;
            this.boundary = boundary;
            this.key = key;
        }
    }

    private final Cluster cluster;
    /** This cluster member's id, logged on egress operational signals. */
    private final int memberId;

    /**
     * Session ids that announced themselves as canonical-stream consumers
     * (a {@link SealerWire#KIND_SUBSCRIBE} frame, or any replay request).
     * DETERMINISM: mutated only from logged session messages and
     * {@code onSessionClose} (both log-driven), so every member holds the
     * identical set and a new leader fans out to the same sessions.
     * Deliberately NOT snapshotted: a restart-from-snapshot severs every
     * client connection, each client re-announces on its next session
     * establishment, and until the first announcement arrives
     * {@link #offerToConsumers} falls back to broadcast-to-all so nothing can
     * starve.
     */
    private final LongHashSet consumerSessions = new LongHashSet();

    /**
     * Retained egress frames, in EMISSION ORDER, for `REPLAY_FROM` requests
     * from (re)connecting clients — without replay, frames committed while a
     * client had no session are missed forever and its canonical stream has an
     * unrecoverable gap. Deterministic across members (derived from the
     * replicated log); NOT snapshotted (v1): a member restarted from snapshot
     * initializes the retention floors from the restored state (see
     * {@link SealerClusteredService#onStart}) and serves REPLAY_UNAVAILABLE
     * for pre-restart ranges, an honest degradation.
     */
    private final java.util.ArrayDeque<RetainedFrame> retained = new java.util.ArrayDeque<>();
    private final int retentionCap =
        Integer.getInteger("kardamom.cluster.retention", SealerWire.DEFAULT_RETENTION);
    /** First record index / boundary block still guaranteed retained. */
    private long firstRetainedIndex;
    private long firstRetainedBlock;

    // Staging buffer for egress framing. Reused to avoid per-message
    // allocation on the single cluster service thread.
    private final ExpandableArrayBuffer egressBuffer = new ExpandableArrayBuffer();

    SealerEgress(
            final Cluster cluster,
            final int memberId,
            final long firstRetainedIndex,
            final long firstRetainedBlock) {
        this.cluster = cluster;
        this.memberId = memberId;
        this.firstRetainedIndex = firstRetainedIndex;
        this.firstRetainedBlock = firstRetainedBlock;
    }

    /** Announce a session as a canonical-stream consumer. */
    void addConsumer(final long sessionId) {
        consumerSessions.add(sessionId);
    }

    /** Forget a closed session's consumer announcement. */
    void removeConsumer(final long sessionId) {
        consumerSessions.remove(sessionId);
    }

    /**
     * Serve a client replay request: re-offer every retained frame at/after
     * the requested cursor to the REQUESTING session only, then a REPLAY_DONE
     * marker (or REPLAY_UNAVAILABLE when eviction has outrun the request).
     * Runs identically on every member from the replicated log; only the
     * leader's session offers reach the client. {@code upToIndex}/{@code
     * upToBlock} are the state machine's current canonical count and block
     * number, stamped into the REPLAY_DONE marker.
     *
     * <p>Served SYNCHRONOUSLY, exactly as on main (validated green in the
     * cluster e2e CI): the F07.3 timer-driven chunked drain — which also made
     * live broadcasts SKIP mid-replay sessions — changed the steady-state
     * egress flow and correlates with the all-shards consumer freeze at
     * first-record time; correctness beats the leader-stall optimization it
     * was after (see docs/reviews/2026-07-17-30-commit-review/
     * fixes-CI-replay-loop.md). The stall is bounded in practice: a WEDGED
     * consumer costs one {@link #OFFER_DEADLINE_NS} on its first frame and is
     * then CLOSED (subsequent offers return CLOSED and are skipped
     * immediately), and healthy consumers drain retained frames at line
     * rate.</p>
     */
    void handleReplayRequest(
            final ClientSession session,
            final long fromIndex,
            final long fromBlock,
            final long upToIndex,
            final long upToBlock) {
        if (fromIndex < firstRetainedIndex || fromBlock < firstRetainedBlock) {
            // stdout, like the role lines: grep-able next to the chaos suite's
            // other signals (the service has no other logger).
            System.out.println("cluster REPLAY memberId=" + memberId
                + " session=" + session.id() + " from=(" + fromIndex + "," + fromBlock
                + ") UNAVAILABLE floor=(" + firstRetainedIndex + "," + firstRetainedBlock + ")");
            offerControl(session, SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE, firstRetainedIndex, firstRetainedBlock);
            return;
        }
        long served = 0;
        long dropped = 0;
        for (final RetainedFrame f : retained) {
            final boolean wanted = f.boundary ? f.key >= fromBlock : f.key >= fromIndex;
            if (wanted) {
                // Count DELIVERED frames, not attempted ones: an offer into a
                // closed/back-pressured session returns false, and pretending
                // it was "served" made a wholesale-dropped replay look
                // identical to a successful one in these logs (issue #141).
                if (offerBytesToSession(session, f.frame)) {
                    served++;
                } else {
                    dropped++;
                }
            }
        }
        System.out.println("cluster REPLAY memberId=" + memberId
            + " session=" + session.id() + " from=(" + fromIndex + "," + fromBlock
            + ") served=" + served + " dropped=" + dropped
            + " retained=" + retained.size());
        offerControl(session, SealerWire.EGRESS_KIND_REPLAY_DONE, upToIndex, upToBlock);
    }

    /** Frame + offer a control message {@code kind(1) | a(8) | b(8)}. */
    private void offerControl(final ClientSession session, final byte kind, final long a, final long b) {
        final MutableDirectBuffer buf = egressBuffer;
        int pos = 0;
        buf.putByte(pos, kind);
        pos += Byte.BYTES;
        buf.putLong(pos, a, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, b, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        offerToSession(session, pos);
    }

    /**
     * Frame + offer an {@link SealerWire#EGRESS_KIND_CONTIGUITY_REJECT}
     * ({@code kind(1) | sender(20) | nonce(8) | expected(8)}) to the offering
     * session.
     */
    void offerContiguityReject(
            final ClientSession session, final byte[] sender20, final long nonce, final long expected) {
        final MutableDirectBuffer buf = egressBuffer;
        int pos = 0;
        buf.putByte(pos, SealerWire.EGRESS_KIND_CONTIGUITY_REJECT);
        pos += Byte.BYTES;
        buf.putBytes(pos, sender20);
        pos += CanonicalSealerState.SENDER_LEN;
        buf.putLong(pos, nonce, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, expected, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        offerToSession(session, pos);
    }

    /** Retain an already-framed egress frame for future replays (bounded). */
    private void retain(final int length, final boolean boundary, final long key) {
        final byte[] copy = new byte[length];
        egressBuffer.getBytes(0, copy);
        retained.addLast(new RetainedFrame(copy, boundary, key));
        while (retained.size() > retentionCap) {
            final RetainedFrame evicted = retained.removeFirst();
            if (evicted.boundary) {
                firstRetainedBlock = evicted.key + 1;
            } else {
                firstRetainedIndex = evicted.key + 1;
            }
        }
    }

    void offerRelayed(final Relayed relayed) {
        final int len = frameRelayed(relayed);
        retain(len, false, relayed.index);
        offerToConsumers(len);
    }

    /**
     * Offer the frame staged in {@link #egressBuffer} to every session that
     * announced itself as a canonical-stream consumer — the executors,
     * validator, and ingress observers, but NOT the publisher-only sequencer
     * sessions that used to receive (and client-side drop) every record: on a
     * saturated leader the per-session unicast offer is the dominant cost, so
     * halving the session list directly buys ceiling. Falls back to
     * broadcast-to-all while no consumer has announced itself (pre-subscribe
     * window right after a restart, or a mixed-version deploy whose clients
     * never send SUBSCRIBE) so nothing can starve; the executor replicas
     * consume the canonical stream on their OWN sessions, which is why the
     * fan-out must reach beyond the sending session at all (the original
     * starvation bug: boundaries arrived, records didn't, tripping
     * BoundaryMisaligned want_count&gt;have_count).
     */
    private void offerToConsumers(final int len) {
        if (consumerSessions.isEmpty()) {
            for (final ClientSession session : cluster.clientSessions()) {
                offerToSession(session, len);
            }
            return;
        }
        for (final ClientSession session : cluster.clientSessions()) {
            if (consumerSessions.contains(session.id())) {
                offerToSession(session, len);
            }
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
        buf.putByte(pos, SealerWire.EGRESS_KIND_RELAYED);
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

    void offerBoundary(final Boundary boundary) {
        final int len = frameBoundary(boundary);
        retain(len, true, boundary.blockNumber);
        // Boundaries stay broadcast to EVERY session (unlike relayed records):
        // they are <=1 per tick and the sequencer's boundary-only lag feed
        // (connect_with_egress_kind_filter, #93) consumes them WITHOUT a
        // SUBSCRIBE announcement — consumer-filtering them would starve it.
        for (final ClientSession session : cluster.clientSessions()) {
            offerToSession(session, len);
        }
    }

    /**
     * Frame a {@link Boundary} into {@link #egressBuffer}:
     * {@code kind(1) | blockNumber(8) | endTxIdx(8) | l2Timestamp(8) | l1Origin(8)}.
     */
    private int frameBoundary(final Boundary boundary) {
        final MutableDirectBuffer buf = egressBuffer;
        int pos = 0;
        buf.putByte(pos, SealerWire.EGRESS_KIND_BOUNDARY);
        pos += Byte.BYTES;
        buf.putLong(pos, boundary.blockNumber, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, boundary.endTxIdx, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, boundary.l2Timestamp, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, boundary.l1Origin, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        return pos;
    }

    /**
     * Offer one frame to one session with the deadline-then-close semantics
     * described on {@link #OFFER_DEADLINE_NS} — THE single offer loop; both
     * egress paths (staged buffer and retained raw frame) go through it, so
     * the F07.5 close semantics cannot drift between them.
     *
     * <p>Terminal results: CLOSED means the session is already gone; any
     * other terminal result (MAX_POSITION_EXCEEDED: the egress publication
     * hit its position limit and is permanently dead) must CLOSE the session
     * — returning silently would leave a zombie kept alive by ingress
     * keep-alives while every frame for it is dropped.
     *
     * <p>Deadline exhausted on persistent back-pressure: this session's
     * subscriber has stopped draining. Close it rather than drop frames — a
     * gap is silent corruption. NOTE the close EVENT may never reach the
     * client (it rides the same wedged egress); the client's delivered-frame
     * liveness watchdog is the recovery path.
     */
    private boolean offerWithDeadline(
        final ClientSession session, final DirectBuffer buffer, final int length) {
        final long deadline = System.nanoTime() + OFFER_DEADLINE_NS;
        long result;
        do {
            result = session.offer(buffer, 0, length);
            if (result >= 0) {
                return true;
            }
            if (!retryable(result)) {
                if (result != Publication.CLOSED) {
                    closeSessionLoudly(session, "terminal offer result " + result);
                }
                return false;
            }
        } while (System.nanoTime() < deadline);
        closeSessionLoudly(session, "offer deadline exhausted (back-pressure)");
        return false;
    }

    /** A retained raw frame through {@link #offerWithDeadline}. */
    private boolean offerBytesToSession(final ClientSession session, final byte[] frame) {
        return offerWithDeadline(session, new UnsafeBuffer(frame), frame.length);
    }

    /**
     * Close a session AND say so on stdout. The SessionEvent(CLOSED) the
     * consensus module emits for the client travels over the very egress
     * publication that just failed, so the client plausibly never hears it
     * (its liveness watchdog is what actually recovers it) — this line is
     * then the only durable record of WHY the session died (issue #141).
     */
    private void closeSessionLoudly(final ClientSession session, final String reason) {
        System.out.println("cluster EGRESS-CLOSE memberId=" + memberId
            + " session=" + session.id() + " reason=" + reason);
        session.close();
    }

    /** The staged {@code egressBuffer} head through {@link #offerWithDeadline}. */
    private void offerToSession(final ClientSession session, final int length) {
        offerWithDeadline(session, egressBuffer, length);
    }

    /** Whether a negative offer result is retryable within the deadline. */
    private boolean retryable(final long offerResult) {
        // BACK_PRESSURED / ADMIN_ACTION: transient flow control. NOT_CONNECTED:
        // ALSO retryable-within-deadline — an egress publication is legitimately
        // unconnected for a moment at session open AND after a leader failover
        // (the new leader re-creates it); but a session whose egress NEVER
        // (re)connects within the deadline must be CLOSED by the caller, not
        // skipped: its keep-alives still flow via ingress, so the consensus
        // module keeps it alive while the service silently drops every frame —
        // a ZOMBIE the client cannot detect (observed: a validator starved for
        // 30+ minutes on an intact session after a leader kill). CLOSED /
        // MAX_POSITION_EXCEEDED stay terminal.
        if (offerResult == Publication.BACK_PRESSURED
                || offerResult == Publication.ADMIN_ACTION
                || offerResult == Publication.NOT_CONNECTED) {
            cluster.idleStrategy().idle();
            return true;
        }
        return false;
    }
}
