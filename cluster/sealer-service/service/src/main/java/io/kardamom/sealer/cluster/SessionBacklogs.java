package io.kardamom.sealer.cluster;

import io.aeron.Publication;
import io.aeron.cluster.service.ClientSession;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * The egress offer path of {@link SealerEgress}: one offer attempt per frame,
 * and a bounded backlog for each session that cannot take a frame now.
 *
 * <p>Offers run on the single clustered-service thread. That thread also
 * relays records and runs the boundary tick for every session, so an offer
 * never waits. A frame that meets back-pressure goes to the backlog of its
 * session. While a session has a backlog, every new frame for that session
 * goes to the backlog too, so the session gets its frames in emission order.
 * {@link #drain} sends backlog frames. The service calls it from its
 * log-driven callbacks, because Aeron rejects an egress offer and a session
 * close from {@code doBackgroundWork} and from {@code onRoleChange}.</p>
 *
 * <p>A live session never loses a frame: a consumer that misses a boundary
 * seals two blocks as one, and diverges. So a session that cannot take its
 * backlog is closed instead. The close is a signal the client can act on: it
 * reconnects, replays, and recovers. A session is closed once, when one of
 * these limits is reached:</p>
 * <ul>
 *   <li>No frame left its backlog for {@link #STALL_DEADLINE_NS}. The clock
 *       restarts at each frame that leaves, so a large replay to a client
 *       that drains at line rate stays open.</li>
 *   <li>The backlog holds more than its byte limit.</li>
 * </ul>
 *
 * <p>The backlogs are member-local IO state. They never change the
 * replicated state, what the service writes to the log, or the snapshot.
 * The wall clock is safe here for the same reason. Only the leader's offers
 * reach clients: a follower's offer is a mock that always succeeds, so a
 * follower holds no backlog. A role change drops every backlog, because the
 * clients reconnect to the new leader and replay from there. A session close
 * drops the backlog of that session.</p>
 *
 * <p>While the member holds its outputs after its start ({@link StartHold}),
 * every offered frame goes to the backlog of its session, and a drain sends
 * nothing and closes nothing. The first drain after the hold restarts the
 * clock of each backlog, so a session that waited through the hold is not
 * closed for it. A role change still drops every backlog: the frames of a
 * term that the next election cuts back reach no client.</p>
 *
 * <p>This class is single-threaded, like {@link SealerEgress}.</p>
 */
final class SessionBacklogs {

    /**
     * How long a backlog may make no progress before its session is closed.
     *
     * <p>One second keeps the cost of a dead session to one second of
     * queued frames, and does not close a live client that rides out a
     * brief CPU spike. A close forces the client through reconnect,
     * fail-stop, and crash recovery, so a false close is costly.</p>
     */
    static final long STALL_DEADLINE_NS = TimeUnit.SECONDS.toNanos(1);

    /**
     * The most bytes one backlog may hold. A full replay of the default
     * retention (65536 frames) fits well below it. Sessions share the frame
     * arrays of one emission, so this bounds how far one session may lag,
     * not the heap per session.
     */
    static final long BYTE_LIMIT = 64L * 1024 * 1024;

    /**
     * The offer result for a frame above the maximum message length of the
     * session's egress publication. That length is the term length of the
     * client's response channel divided by 8. No retry can succeed.
     */
    static final long FRAME_TOO_LONG = Long.MIN_VALUE;

    /** What happened to one offered frame. */
    enum Outcome {
        /** The session's egress publication took the frame. */
        SENT,
        /** The frame waits in the session's backlog. */
        QUEUED,
        /** The session is closed or closing, so the frame goes nowhere. */
        DROPPED
    }

    /** The frames one session could not take yet, oldest first. */
    private final class Backlog {
        final ClientSession session;
        final ArrayDeque<byte[]> frames = new ArrayDeque<>();
        long bytes;
        /** When the backlog opened, or when a frame last left it. */
        long progressNs;

        Backlog(final ClientSession session, final long nowNs) {
            this.session = session;
            this.progressNs = nowNs;
        }

        void add(final byte[] frame) {
            frames.addLast(frame);
            bytes += frame.length;
        }

        /**
         * Send frames until the backlog is empty or the session pushes back,
         * then close the session if it is past a limit.
         *
         * @return whether this backlog is done: empty, or its session closed
         */
        boolean drain(final long nowNs) {
            final long result = flush(nowNs);
            if (frames.isEmpty()) {
                return true;
            }
            if (!retryable(result)) {
                closeOnTerminal(session, result, frames.peekFirst());
                return true;
            }
            if (nowNs - progressNs > stallDeadlineNs) {
                closeLoudly(session, "offer deadline exhausted (back-pressure)");
                return true;
            }
            return false;
        }

        /** Send head frames while the session takes them; return the last offer result. */
        private long flush(final long nowNs) {
            long result = 0;
            while (!frames.isEmpty() && result >= 0) {
                result = sendHead(nowNs);
            }
            return result;
        }

        private long sendHead(final long nowNs) {
            final byte[] head = frames.peekFirst();
            final long result = attempt(session, head);
            if (result >= 0) {
                frames.removeFirst();
                bytes -= head.length;
                progressNs = nowNs;
            }
            return result;
        }
    }

    /**
     * The frame that closes a session as too long: its kind, and its
     * canonical index, or -1 for a kind that carries no index.
     */
    private record FrameKey(byte kind, long index) {
        static FrameKey of(final byte[] frame) {
            final boolean indexed = frame[0] == SealerWire.EGRESS_KIND_RELAYED
                || frame[0] == SealerWire.EGRESS_KIND_RELAYED_REF;
            return new FrameKey(frame[0], indexed
                ? new UnsafeBuffer(frame).getLong(Byte.BYTES, ByteOrder.LITTLE_ENDIAN)
                : -1L);
        }
    }

    /**
     * The frame that last closed a session of a response channel as too
     * long, and how many closes in a row it caused after the first.
     */
    private record TooLong(FrameKey frame, long repeats) {
    }

    /**
     * The last {@link TooLong} close of each response channel. A client
     * keeps its response channel across reconnects, so a client whose
     * egress term cannot take a frame shows here as a run of repeats on one
     * frame. This is member-local IO state, like the backlogs. Only the
     * leader closes sessions, so a follower keeps no entry: a role change
     * clears the map. A session close that the service did not ask for
     * removes the entry of its channel ({@link #forgetChannel}), so the map
     * holds only the channels of sessions that the service closed.
     */
    private final Map<String, TooLong> tooLongByChannel = new HashMap<>();

    /** This cluster member's id, logged on each close. */
    private final int memberId;
    private final long stallDeadlineNs;
    private final long byteLimit;
    /** The open backlogs by session id. A session with no backlog has no entry. */
    private final Long2ObjectHashMap<Backlog> bySession = new Long2ObjectHashMap<>();
    /** A view that wraps each frame for the offer, with no allocation. */
    private final UnsafeBuffer view = new UnsafeBuffer(0, 0);
    private final StartHold hold;
    /** Whether a drain saw the hold over; after that, the hold is not read again. */
    private boolean released;

    SessionBacklogs(final int memberId, final long stallDeadlineNs, final long byteLimit) {
        this(memberId, stallDeadlineNs, byteLimit, StartHold.released());
    }

    SessionBacklogs(final int memberId, final long stallDeadlineNs, final long byteLimit, final StartHold hold) {
        this.memberId = memberId;
        this.stallDeadlineNs = stallDeadlineNs;
        this.byteLimit = byteLimit;
        this.hold = hold;
    }

    /** Whether the member still holds its outputs. Once a drain sees the hold over, this is false for good. */
    private boolean holding() {
        return !released && hold.held();
    }

    /**
     * Offer the first {@code length} bytes of {@code frame} to one session.
     * The offer reads the buffer in place, so a sent frame costs no copy.
     * Only a frame that waits in a backlog is copied to an array.
     */
    Outcome offer(final ClientSession session, final DirectBuffer frame, final int length) {
        final Backlog backlog = bySession.get(session.id());
        if (backlog != null) {
            return append(backlog, copy(frame, length));
        }
        if (session.isClosing()) {
            return Outcome.DROPPED;
        }
        if (holding()) {
            return append(open(session), copy(frame, length));
        }
        final long result = attemptInPlace(session, frame, length);
        if (result >= 0) {
            return Outcome.SENT;
        }
        if (retryable(result)) {
            return append(open(session), copy(frame, length));
        }
        closeOnTerminal(session, result, copy(frame, length));
        return Outcome.DROPPED;
    }

    private static byte[] copy(final DirectBuffer frame, final int length) {
        final byte[] out = new byte[length];
        frame.getBytes(0, out);
        return out;
    }

    /** {@link #attempt} on the first {@code length} bytes of a buffer. */
    private static long attemptInPlace(final ClientSession session, final DirectBuffer frame, final int length) {
        try {
            final long result = session.offer(frame, 0, length);
            return result == Publication.ADMIN_ACTION ? session.offer(frame, 0, length) : result;
        } catch (final IllegalArgumentException aboveMaxMessageLength) {
            return FRAME_TOO_LONG;
        }
    }

    /**
     * Offer one frame to one session, or queue it behind the session's
     * backlog. The caller must not change {@code frame} later: a backlog
     * keeps the array, not a copy.
     */
    Outcome offer(final ClientSession session, final byte[] frame) {
        final Backlog backlog = bySession.get(session.id());
        if (backlog != null) {
            return append(backlog, frame);
        }
        // A close only asks the consensus module to close the session. The
        // session stays in the cluster's session set until the close comes
        // back through the log, and its publication is still the wedged one.
        if (session.isClosing()) {
            return Outcome.DROPPED;
        }
        if (holding()) {
            return append(open(session), frame);
        }
        final long result = attempt(session, frame);
        if (result >= 0) {
            return Outcome.SENT;
        }
        if (retryable(result)) {
            return append(open(session), frame);
        }
        closeOnTerminal(session, result, frame);
        return Outcome.DROPPED;
    }

    /** Send what each backlog's session takes now, and close each session past a limit. */
    void drain() {
        if (holding()) {
            return;
        }
        if (!released) {
            released = true;
            restartClocks();
        }
        if (bySession.isEmpty()) {
            // The common case on the record path: no session is back-pressured,
            // and the drain allocates no iterator and no lambda.
            return;
        }
        final long nowNs = System.nanoTime();
        bySession.values().removeIf(backlog -> backlog.drain(nowNs));
    }

    /** Restart the stall clock of each backlog: no backlog made progress while the member held its outputs. */
    private void restartClocks() {
        final long nowNs = System.nanoTime();
        bySession.values().forEach(backlog -> backlog.progressNs = nowNs);
    }

    /** Drop the backlog of a closed session. */
    void drop(final long sessionId) {
        bySession.remove(sessionId);
    }

    /** Drop every backlog and every close count, because this member's role changed. */
    void dropAll() {
        bySession.clear();
        tooLongByChannel.clear();
    }

    /** Forget the close count of a response channel whose session ended for good. */
    void forgetChannel(final String responseChannel) {
        tooLongByChannel.remove(responseChannel);
    }

    private Backlog open(final ClientSession session) {
        final Backlog backlog = new Backlog(session, System.nanoTime());
        bySession.put(session.id(), backlog);
        return backlog;
    }

    private Outcome append(final Backlog backlog, final byte[] frame) {
        backlog.add(frame);
        if (backlog.bytes <= byteLimit) {
            return Outcome.QUEUED;
        }
        bySession.remove(backlog.session.id());
        closeLoudly(backlog.session, "egress backlog over " + byteLimit + " bytes");
        return Outcome.DROPPED;
    }

    /**
     * One offer of {@code frame}. ADMIN_ACTION means the publication rotated
     * its term, and Aeron asks for a retry at once, so this retries once.
     *
     * <p>Aeron throws for a frame above the maximum message length of the
     * publication. It checks the length before it writes, so the
     * publication stays intact. The throw must not leave the log callback:
     * the leader then handles the log entry differently from the
     * followers, whose offers always succeed. So the throw becomes
     * {@link #FRAME_TOO_LONG}, a terminal result, and the session
     * closes.</p>
     */
    private long attempt(final ClientSession session, final byte[] frame) {
        view.wrap(frame);
        try {
            final long result = session.offer(view, 0, frame.length);
            return result == Publication.ADMIN_ACTION ? session.offer(view, 0, frame.length) : result;
        } catch (final IllegalArgumentException aboveMaxMessageLength) {
            return FRAME_TOO_LONG;
        }
    }

    /**
     * Whether a negative offer result can clear later.
     *
     * <p>BACK_PRESSURED and ADMIN_ACTION are transient flow control.
     * NOT_CONNECTED can clear too: an egress publication is unconnected for
     * a moment at session open, and after a failover while the new leader
     * re-creates it. A session whose egress never connects reaches the stall
     * deadline and is closed, not skipped. Its keep-alives still flow through
     * ingress, so a skipped session stays open while it gets no frames, and
     * the client cannot detect that. CLOSED and MAX_POSITION_EXCEEDED are
     * terminal.</p>
     */
    private static boolean retryable(final long offerResult) {
        return offerResult == Publication.BACK_PRESSURED
            || offerResult == Publication.ADMIN_ACTION
            || offerResult == Publication.NOT_CONNECTED;
    }

    /**
     * Close a session after a terminal offer result. CLOSED means the
     * session is already gone. Any other terminal result, such as
     * MAX_POSITION_EXCEEDED, means the egress publication is dead for good,
     * so the session must close. Otherwise ingress keep-alives keep it open
     * while it gets no frames.
     */
    private void closeOnTerminal(final ClientSession session, final long result, final byte[] frame) {
        if (result == FRAME_TOO_LONG) {
            closeTooLong(session, frame);
        } else if (result != Publication.CLOSED) {
            closeLoudly(session, "terminal offer result " + result);
        }
    }

    /**
     * Close a session whose egress publication cannot take {@code frame}.
     * A client that reconnects with the same small term replays the same
     * frame and is closed again. The log line counts these repeats for the
     * response channel, so the loop is visible.
     */
    private void closeTooLong(final ClientSession session, final byte[] frame) {
        final FrameKey key = FrameKey.of(frame);
        final TooLong last = tooLongByChannel.get(session.responseChannel());
        final long repeats = last != null && last.frame().equals(key) ? last.repeats() + 1 : 0;
        tooLongByChannel.put(session.responseChannel(), new TooLong(key, repeats));
        closeLoudly(session, "frame above the max message length of the egress publication"
            + " kind=" + key.kind() + " index=" + key.index() + " length=" + frame.length
            + " repeats=" + repeats + " channel=" + session.responseChannel());
    }

    /**
     * How many closes in a row the frame of {@code kind} at {@code index}
     * caused for {@code channel}, after the first, or -1 when it is not the
     * last such frame.
     */
    long tooLongRepeats(final String channel, final byte kind, final long index) {
        final TooLong last = tooLongByChannel.get(channel);
        return last != null && last.frame().equals(new FrameKey(kind, index)) ? last.repeats() : -1L;
    }

    /**
     * Close a session, and log the reason on stdout. The close event for the
     * client goes over the same egress publication that failed, so the
     * client may never see it. The client's liveness watchdog recovers it.
     * This log line is then the only record of why the session closed.
     */
    private void closeLoudly(final ClientSession session, final String reason) {
        System.out.println("cluster EGRESS-CLOSE memberId=" + memberId
            + " session=" + session.id() + " reason=" + reason);
        session.close();
    }
}
