package io.kardamom.sealer.cluster;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.ImageFragmentAssembler;
import io.aeron.Publication;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import io.kardamom.sealer.Boundary;
import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.Relayed;
import java.nio.ByteOrder;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
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

    /** Ingress message kinds (first byte of every ingress app message). */
    public static final byte KIND_INGRESS_RECORD = 0;
    /** Replay request: {@code [kind:1][from_index:u64 LE][from_block:u64 LE]}. */
    public static final byte KIND_REPLAY_REQUEST = 1;

    /** Minimum valid replay-request length: kind + from_index + from_block. */
    private static final int MIN_REPLAY_REQUEST_LEN = Byte.BYTES + Long.BYTES + Long.BYTES;

    /** Egress message kinds (first byte of every egress frame). */
    public static final byte EGRESS_KIND_RELAYED = 1;
    public static final byte EGRESS_KIND_BOUNDARY = 2;
    /** Replay refused: {@code [kind:3][oldest_index:u64][oldest_block:u64]}. */
    public static final byte EGRESS_KIND_REPLAY_UNAVAILABLE = 3;
    /** Replay complete: {@code [kind:4][up_to_index:u64][up_to_block:u64]}. */
    public static final byte EGRESS_KIND_REPLAY_DONE = 4;

    /** Bounded in-memory retention of framed egress bytes for client replay. */
    private static final int DEFAULT_RETENTION = 65536;

    /**
     * Default first-seen dedup window. SAFETY INVARIANT: the window must be
     * larger than (worst-case racing-replica stall × peak unique-record
     * throughput), or a replica that resumes after its ids were FIFO-evicted
     * gets its re-offers accepted as fresh — the same tx ordered TWICE in the
     * canonical log. At 10k unique tx/s the previous default of 8192 tolerated
     * a stall of only ~0.8s (a single GC pause / cgroup throttle); 1&lt;&lt;17
     * tolerates ~13s for ~20MB of heap and a ~4MB snapshot (snapshot I/O is
     * chunked, see {@link #writeSnapshot}). All members MUST agree on the
     * window ({@code -Dkardamom.cluster.dedupCapacity}) — it is part of the
     * deterministic state machine.
     */
    public static final int DEFAULT_DEDUP_CAPACITY = 1 << 17;

    /** Correlation id used when scheduling the repeating boundary timer. */
    public static final long BOUNDARY_TIMER_CORRELATION_ID = 1L;

    /**
     * Correlation id of the fast self-rescheduling timer that drains pending
     * replays in bounded chunks. A cluster TIMER (not
     * {@code doBackgroundWork}) because Aeron forbids session offers outside
     * log-driven callbacks ("sending messages or scheduling timers is not
     * allowed from doBackgroundWork"); it only exists while a replay is
     * pending, so the extra log traffic is bounded to catch-up windows.
     */
    public static final long REPLAY_TIMER_CORRELATION_ID = 2L;

    /** Tick cadence for the boundary timer (ms). Matches the 250 ms L2 tick. */
    private final long tickIntervalMs;
    private final int dedupCapacity;
    /** This cluster member's id, logged on role changes for the chaos suite. */
    private final int memberId;

    private Cluster cluster;
    private CanonicalSealerState state;

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

    /**
     * Retained egress frames, in EMISSION ORDER, for `REPLAY_FROM` requests
     * from (re)connecting clients — without replay, frames committed while a
     * client had no session are missed forever and its canonical stream has an
     * unrecoverable gap. Deterministic across members (derived from the
     * replicated log); NOT snapshotted (v1): a member restarted from snapshot
     * initializes the retention floors from the restored state (see
     * {@link #onStart}) and serves REPLAY_UNAVAILABLE for pre-restart ranges,
     * an honest degradation.
     */
    private final java.util.ArrayDeque<RetainedFrame> retained = new java.util.ArrayDeque<>();
    private final int retentionCap =
        Integer.getInteger("kardamom.cluster.retention", DEFAULT_RETENTION);
    /** First record index / boundary block still guaranteed retained. */
    private long firstRetainedIndex = 0L;
    private long firstRetainedBlock = CanonicalSealerState.GENESIS_BLOCK_NUMBER;

    /**
     * An in-progress replay for one client session, drained INCREMENTALLY in
     * bounded, non-blocking chunks from the {@link #REPLAY_TIMER_CORRELATION_ID}
     * timer — never in one synchronous burst on the request. Serving up to
     * {@code retentionCap} frames inline in the replay-request handler, with a
     * 1s offer deadline PER FRAME, could stall the single clustered-service
     * thread (and with it every session's egress and all log processing) for
     * minutes on one slow consumer.
     */
    private static final class PendingReplay {
        final long fromIndex;
        final long fromBlock;
        /** Next wanted record index / boundary block (advance as frames send). */
        long nextIndex;
        long nextBlock;
        long served;
        /** Terminal control frame owed to the session, when non-zero. */
        byte controlKind;
        /** Last time (ns) an offer to this session succeeded; -1 = not started. */
        long lastProgressNs = -1;

        PendingReplay(final long fromIndex, final long fromBlock) {
            this.fromIndex = fromIndex;
            this.fromBlock = fromBlock;
            this.nextIndex = fromIndex;
            this.nextBlock = fromBlock;
        }
    }

    /** Pending replays keyed by cluster session id (drained in request order). */
    private final java.util.LinkedHashMap<Long, PendingReplay> pendingReplays =
        new java.util.LinkedHashMap<>();

    /** Max frames offered per session per replay-drain timer event. */
    private static final int REPLAY_CHUNK_LIMIT = 256;

    /**
     * A replaying session that accepts NO frame for this long is closed (the
     * background drain is non-blocking, so unlike the live path's 1s
     * {@link #OFFER_DEADLINE_NS} a wedged replay consumer would otherwise
     * never be reaped — live broadcasts skip sessions mid-replay). 5s: a
     * client actively draining a replay is never back-pressured that long,
     * but a CI CPU spike or GC pause is ridden out.
     */
    private static final long REPLAY_STALL_TIMEOUT_NS =
        java.util.concurrent.TimeUnit.SECONDS.toNanos(5);

    /** Malformed ingress frames dropped (logged at power-of-two counts). */
    private long droppedFrameCount = 0;

    // Scratch buffers for ingress id extraction and egress framing. Reused to
    // avoid per-message allocation on the single cluster service thread.
    private final byte[] canonicalIdScratch = new byte[CanonicalSealerState.CANONICAL_ID_LEN];
    private final ExpandableArrayBuffer egressBuffer = new ExpandableArrayBuffer();
    /** Scratch for control frames offered from the background replay drain. */
    private final ExpandableArrayBuffer controlBuffer = new ExpandableArrayBuffer();
    /** Reusable wrapper for offering retained frames without re-allocating. */
    private final UnsafeBuffer retainedWrapBuffer = new UnsafeBuffer();

    public SealerClusteredService(int dedupCapacity, long tickIntervalMs, int memberId) {
        this.dedupCapacity = dedupCapacity;
        this.tickIntervalMs = tickIntervalMs;
        this.memberId = memberId;
    }

    public SealerClusteredService(int dedupCapacity, long tickIntervalMs) {
        this(dedupCapacity, tickIntervalMs, -1);
    }

    public SealerClusteredService() {
        this(DEFAULT_DEDUP_CAPACITY, CanonicalSealerState.TICK_INTERVAL_MS);
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
            final byte[] snapshot = readSnapshot(snapshotImage, cluster.idleStrategy());
            this.state = CanonicalSealerState.load(snapshot, dedupCapacity);
            // The retained deque is NOT snapshotted (v1): nothing before the
            // restore point can ever be served, so the retention floors start
            // at the first frame this member CAN retain — record index
            // canonicalCount and boundary block blockNumber (the next ones to
            // be emitted). Leaving the floors at genesis would answer a
            // pre-snapshot replay request with a bogus REPLAY_DONE (a silent
            // canonical gap) instead of the honest REPLAY_UNAVAILABLE.
            this.firstRetainedIndex = state.canonicalCount();
            this.firstRetainedBlock = state.blockNumber();
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
        // The replay-drain timer lives in the leader's wheel too: re-arm it if
        // this member still has replays to serve (same lost-timer hazard).
        if (!pendingReplays.isEmpty()) {
            scheduleReplayTimer();
        }
    }

    @Override
    public void onSessionOpen(ClientSession session, long timestamp) {
        // Nothing session-specific to track; canonical state is global.
    }

    @Override
    public void onSessionClose(ClientSession session, long timestamp, CloseReason closeReason) {
        pendingReplays.remove(session.id());
    }

    @Override
    public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {
        if (length > KIND_OFFSET && buffer.getByte(offset + KIND_OFFSET) == KIND_REPLAY_REQUEST) {
            if (length < MIN_REPLAY_REQUEST_LEN) {
                onMalformedFrame("replay-request", length);
                return;
            }
            final long fromIndex =
                buffer.getLong(offset + 1, ByteOrder.LITTLE_ENDIAN);
            final long fromBlock =
                buffer.getLong(offset + 1 + Long.BYTES, ByteOrder.LITTLE_ENDIAN);
            handleReplayRequest(session, fromIndex, fromBlock);
            return;
        }
        if (length < MIN_INGRESS_LEN) {
            // Malformed / too-short envelope: cannot contain kind + 32-byte id.
            onMalformedFrame("ingress-envelope", length);
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
        if (correlationId == REPLAY_TIMER_CORRELATION_ID) {
            drainPendingReplays(System.nanoTime());
            if (!pendingReplays.isEmpty()) {
                scheduleReplayTimer(); // one-shot: re-arm while work remains
            }
            return;
        }
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
        writeSnapshot(snapshotPublication, state.takeSnapshot(), cluster.idleStrategy());
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

    // --- incremental replay drain --------------------------------------------

    /**
     * Drain pending replays in bounded chunks, driven by the
     * {@link #REPLAY_TIMER_CORRELATION_ID} timer. Egress offers are
     * member-local IO (only the leader's reach clients; follower offers are
     * mocked, so followers drain instantly), so per-member progress
     * differences here cannot diverge replicated state.
     */
    private void drainPendingReplays(final long nowNs) {
        final Iterator<Map.Entry<Long, PendingReplay>> it = pendingReplays.entrySet().iterator();
        while (it.hasNext()) {
            final Map.Entry<Long, PendingReplay> entry = it.next();
            final ClientSession session = cluster.getClientSession(entry.getKey());
            if (session == null || session.isClosing()) {
                it.remove();
                continue;
            }
            serveReplayChunk(session, entry.getValue(), it, nowNs);
        }
    }

    /**
     * Serve one bounded, non-blocking chunk of a pending replay. Back-pressure
     * pauses the drain until the next drain-timer event instead of spinning; a
     * session that accepts nothing for {@link #REPLAY_STALL_TIMEOUT_NS} is
     * closed (never silently skipped — a zombie the client cannot detect).
     */
    private int serveReplayChunk(
            final ClientSession session,
            final PendingReplay replay,
            final Iterator<Map.Entry<Long, PendingReplay>> it,
            final long nowNs) {
        if (replay.lastProgressNs < 0) {
            replay.lastProgressNs = nowNs;
        }
        // Eviction may have outrun an in-progress replay: frames the client
        // still needs are gone, so the only honest answer is UNAVAILABLE.
        if (replay.controlKind == 0
                && (replay.nextIndex < firstRetainedIndex || replay.nextBlock < firstRetainedBlock)) {
            replay.controlKind = EGRESS_KIND_REPLAY_UNAVAILABLE;
        }
        int sent = 0;
        if (replay.controlKind == 0) {
            for (final RetainedFrame f : retained) {
                final boolean wanted = f.boundary ? f.key >= replay.nextBlock : f.key >= replay.nextIndex;
                if (!wanted) {
                    continue; // below the request, or already served
                }
                retainedWrapBuffer.wrap(f.frame);
                if (!offerOnce(session, replay, retainedWrapBuffer, f.frame.length, it, nowNs)) {
                    return sent;
                }
                if (f.boundary) {
                    replay.nextBlock = f.key + 1;
                } else {
                    replay.nextIndex = f.key + 1;
                }
                replay.served++;
                if (++sent >= REPLAY_CHUNK_LIMIT) {
                    return sent;
                }
            }
            // Scanned to the retention tail: everything wanted has been served.
            replay.controlKind = EGRESS_KIND_REPLAY_DONE;
        }
        final boolean done = replay.controlKind == EGRESS_KIND_REPLAY_DONE;
        final int len = frameControl(
                controlBuffer,
                replay.controlKind,
                done ? state.canonicalCount() : firstRetainedIndex,
                done ? state.blockNumber() : firstRetainedBlock);
        if (offerOnce(session, replay, controlBuffer, len, it, nowNs)) {
            System.out.println("cluster REPLAY memberId=" + memberId
                + " session=" + session.id() + " from=(" + replay.fromIndex + "," + replay.fromBlock
                + ") " + (done ? "served=" + replay.served + " retained=" + retained.size()
                    : "UNAVAILABLE floor=(" + firstRetainedIndex + "," + firstRetainedBlock + ")"));
            it.remove();
            return sent + 1;
        }
        return sent;
    }

    /**
     * Single non-blocking offer attempt for the replay drain. Returns true
     * on success. On transient results just returns false (retried next duty
     * cycle) unless the session has stalled past the timeout; terminal results
     * close the session (except CLOSED, which already is) and drop the replay.
     */
    private boolean offerOnce(
            final ClientSession session,
            final PendingReplay replay,
            final DirectBuffer buffer,
            final int length,
            final Iterator<Map.Entry<Long, PendingReplay>> it,
            final long nowNs) {
        final long result = session.offer(buffer, 0, length);
        if (result >= 0) {
            replay.lastProgressNs = nowNs;
            return true;
        }
        if (result == Publication.BACK_PRESSURED
                || result == Publication.ADMIN_ACTION
                || result == Publication.NOT_CONNECTED) {
            if (nowNs - replay.lastProgressNs > REPLAY_STALL_TIMEOUT_NS) {
                System.out.println("cluster REPLAY memberId=" + memberId
                    + " session=" + session.id() + " STALLED — closing session");
                session.close();
                it.remove();
            }
            return false;
        }
        // Terminal (CLOSED / MAX_POSITION_EXCEEDED): drop the replay; close
        // unless the session is already closed.
        if (result != Publication.CLOSED) {
            session.close();
        }
        it.remove();
        return false;
    }

    // --- helpers ------------------------------------------------------------

    /**
     * Read the WHOLE snapshot byte stream off the snapshot image. Snapshots
     * larger than the MTU arrive as MANY fragments (and, above
     * {@code maxMessageLength}, as many messages — see {@link #writeSnapshot}),
     * so fragments are reassembled with an {@link ImageFragmentAssembler} and
     * messages concatenated until end-of-stream. A snapshot image that closes
     * early or carries no bytes is FATAL — never fabricate genesis state.
     */
    static byte[] readSnapshot(final Image snapshotImage, final IdleStrategy idleStrategy) {
        final ExpandableArrayBuffer assembled = new ExpandableArrayBuffer();
        final int[] size = {0};
        final ImageFragmentAssembler assembler = new ImageFragmentAssembler(
                (buffer, offset, length, header) -> {
                    assembled.putBytes(size[0], buffer, offset, length);
                    size[0] += length;
                });
        while (!snapshotImage.isEndOfStream()) {
            final int fragments = snapshotImage.poll(assembler, 16);
            if (fragments == 0) {
                if (snapshotImage.isClosed()) {
                    throw new IllegalStateException(
                        "snapshot image closed before end-of-stream (read " + size[0] + " bytes)");
                }
                idleStrategy.idle();
            } else {
                idleStrategy.reset();
            }
        }
        if (size[0] == 0) {
            throw new IllegalStateException("snapshot image was empty");
        }
        final byte[] snapshot = new byte[size[0]];
        assembled.getBytes(0, snapshot);
        return snapshot;
    }

    /**
     * Offer the full snapshot, chunked at the publication's
     * {@code maxMessageLength} so ANY dedup-window size round-trips. A
     * terminal offer result is FATAL: exiting silently would record an
     * empty/truncated snapshot, and the member restoring from it would
     * diverge (or refuse to start) with no recorded error.
     */
    static void writeSnapshot(
            final ExclusivePublication snapshotPublication,
            final byte[] snapshot,
            final IdleStrategy idleStrategy) {
        final UnsafeBuffer buf = new UnsafeBuffer(snapshot);
        final int maxChunk = snapshotPublication.maxMessageLength();
        int offset = 0;
        while (offset < snapshot.length) {
            final int chunk = Math.min(maxChunk, snapshot.length - offset);
            long result;
            while ((result = snapshotPublication.offer(buf, offset, chunk)) < 0) {
                if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                    throw new IllegalStateException("snapshot offer failed terminally (" + result
                        + ") at offset " + offset + "/" + snapshot.length);
                }
                idleStrategy.idle();
            }
            offset += chunk;
        }
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

    /** Arm the replay-drain timer for the next cluster-time millisecond. */
    private void scheduleReplayTimer() {
        final long deadline = cluster.time() + 1;
        while (!cluster.scheduleTimer(REPLAY_TIMER_CORRELATION_ID, deadline)) {
            cluster.idleStrategy().idle();
        }
    }

    /**
     * Register a client replay request. The retained frames are served
     * INCREMENTALLY from the replay-drain timer — never inline here on the
     * single cluster service thread (see {@link PendingReplay}) — and end
     * with a REPLAY_DONE marker (or REPLAY_UNAVAILABLE when eviction has
     * outrun the request). Runs identically on every member from the
     * replicated log; only the leader's session offers reach the client.
     */
    private void handleReplayRequest(final ClientSession session, final long fromIndex, final long fromBlock) {
        final PendingReplay replay = new PendingReplay(fromIndex, fromBlock);
        if (fromIndex < firstRetainedIndex || fromBlock < firstRetainedBlock) {
            replay.controlKind = EGRESS_KIND_REPLAY_UNAVAILABLE;
        }
        // A re-request from the same session (the client resends every 3s until
        // its cursor advances) simply replaces the in-progress drain.
        pendingReplays.put(session.id(), replay);
        scheduleReplayTimer();
    }

    /** Frame a control message {@code kind(1) | a(8) | b(8)} into {@code buf}. */
    private static int frameControl(final MutableDirectBuffer buf, final byte kind, final long a, final long b) {
        int pos = 0;
        buf.putByte(pos, kind);
        pos += Byte.BYTES;
        buf.putLong(pos, a, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, b, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        return pos;
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

    private void offerRelayed(final Relayed relayed) {
        final int len = frameRelayed(relayed);
        retain(len, false, relayed.index);
        // Broadcast the relayed canonical record to EVERY client session, not just
        // the sender. The executor replicas consume the canonical tx_ordering stream
        // from egress on their OWN sessions (they never publish ingress), so offering
        // only to the sending session (the sequencer) starved them: they received the
        // broadcast boundaries but no records, tripping the executor's
        // BoundaryMisaligned check (want_count>have_count). Mirrors offerBoundary.
        for (final ClientSession session : cluster.clientSessions()) {
            if (pendingReplays.containsKey(session.id())) {
                // Mid-replay sessions get this frame from the background drain
                // (it was retained above) — offering it live too would both
                // duplicate it and let replay back-pressure trip the live
                // path's deadline-then-close on a healthy catching-up client.
                continue;
            }
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
        retain(len, true, boundary.blockNumber);
        // Boundaries are broadcast to every open client session.
        for (final ClientSession session : cluster.clientSessions()) {
            if (pendingReplays.containsKey(session.id())) {
                continue; // served from the background drain; see offerRelayed
            }
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

    private void offerToSession(final ClientSession session, final int length) {
        final long deadline = System.nanoTime() + OFFER_DEADLINE_NS;
        long result;
        do {
            result = session.offer(egressBuffer, 0, length);
            if (result >= 0) {
                return;
            }
            if (!retryable(result)) {
                // Terminal result. CLOSED means the session is already gone;
                // any other terminal result (MAX_POSITION_EXCEEDED: the egress
                // publication hit its position limit and is permanently dead)
                // must CLOSE the session — returning silently would leave a
                // zombie kept alive by ingress keep-alives while every frame
                // for it is dropped.
                if (result != Publication.CLOSED) {
                    session.close();
                }
                return;
            }
        } while (System.nanoTime() < deadline);
        // Deadline exhausted on persistent back-pressure: this session's
        // subscriber has stopped draining. Close it rather than drop frames —
        // a closed session is a fact the client sees; a gap is silent corruption.
        session.close();
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

    /**
     * Count + log a dropped malformed frame. These are "should never happen"
     * paths on an authoritative stream: if the hand-synced Java/Rust envelope
     * ever drifts (see the TODO(envelope) above) the symptom must be a visible
     * counter, not silent record loss. Logged at power-of-two counts so a
     * framing-mismatch flood cannot drown stdout (which the chaos suite greps).
     */
    private void onMalformedFrame(final String what, final int length) {
        droppedFrameCount++;
        if (Long.bitCount(droppedFrameCount) == 1) {
            System.out.println("cluster DROPPED malformed " + what + " memberId=" + memberId
                + " length=" + length + " totalDropped=" + droppedFrameCount);
        }
    }
}
