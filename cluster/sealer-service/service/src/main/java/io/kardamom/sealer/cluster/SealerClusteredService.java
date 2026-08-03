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
import org.agrona.DirectBuffer;
import org.agrona.collections.LongHashSet;
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
 * as {@code { kind: u8, sender: 20B, nonce: u64 LE, canonical_id: 32B, payload }}
 * — the guard header ({@code sender}/{@code nonce}, #85 fix B) and the 32-byte
 * canonical id sit at FIXED offsets after the 1-byte {@code kind} tag, and the
 * opaque {@code payload} follows. We match that exact layout: sender at
 * {@link #SENDER_OFFSET}, nonce at {@link #NONCE_OFFSET}, id at
 * {@link #CANONICAL_ID_OFFSET}, relay from {@link #RELAY_OFFSET}.</p>
 *
 * <p>TODO(envelope): keep this byte framing in lockstep with the Rust app
 * envelope in {@code crates/cluster-adapter/src/wire.rs} (every field is at a
 * fixed offset; do not invent a different layout). If the Rust {@code kind}
 * discriminant gains variants, branch on {@code buffer.getByte(offset + KIND_OFFSET)}
 * here.</p>
 */
public final class SealerClusteredService implements ClusteredService {

    /** Offset of the 1-byte {@code kind} tag within the app envelope. */
    public static final int KIND_OFFSET = 0;
    /** Offset of the 20-byte sender in the guard header (#85 fix B). */
    public static final int SENDER_OFFSET = KIND_OFFSET + Byte.BYTES;
    /** Offset of the u64 LE nonce in the guard header. */
    public static final int NONCE_OFFSET = SENDER_OFFSET + CanonicalSealerState.SENDER_LEN;
    /** Offset of the 32-byte canonical id within the app envelope. */
    public static final int CANONICAL_ID_OFFSET = NONCE_OFFSET + Long.BYTES;
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
    /**
     * Egress-subscribe announcement: {@code [kind:2]} — the sending session is
     * a canonical-stream consumer and wants the per-record/per-boundary egress
     * broadcast. Publisher-only sessions (sequencers) never send it, so the
     * leader stops paying one unicast offer per record for sessions that drop
     * the payload client-side anyway.
     */
    public static final byte KIND_SUBSCRIBE = 2;
    /**
     * Batch of ingress records:
     * {@code [kind:3][count:u16 LE][per entry: len:u32 LE + entry bytes]},
     * each entry a complete single-record frame ({@code [kind:0][id:32][payload…]}).
     * Entries are processed EXACTLY like individually-offered records (same
     * dedup, same per-record relay), so determinism and the egress format are
     * unchanged — the batch only amortizes the ingress offer round trip.
     */
    public static final byte KIND_BATCH = 3;

    /** Minimum valid replay-request length: kind + from_index + from_block. */
    private static final int MIN_REPLAY_REQUEST_LEN = Byte.BYTES + Long.BYTES + Long.BYTES;

    /** Egress message kinds (first byte of every egress frame). */
    public static final byte EGRESS_KIND_RELAYED = 1;
    public static final byte EGRESS_KIND_BOUNDARY = 2;
    /** Replay refused: {@code [kind:3][oldest_index:u64][oldest_block:u64]}. */
    public static final byte EGRESS_KIND_REPLAY_UNAVAILABLE = 3;
    /** Replay complete: {@code [kind:4][up_to_index:u64][up_to_block:u64]}. */
    public static final byte EGRESS_KIND_REPLAY_DONE = 4;
    /**
     * Contiguity reject (#85 fix B):
     * {@code [kind:5][sender:20][nonce:u64][expected:u64]}, offered to the
     * OFFERING session only — the sequencer whose ref would have sealed a
     * canonical nonce gap rewinds its unconfirmed ledger to {@code expected}
     * and republishes the missing refs.
     */
    public static final byte EGRESS_KIND_CONTIGUITY_REJECT = 5;

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

    /** Tick cadence for the boundary timer (ms). Matches the 250 ms L2 tick. */
    private final long tickIntervalMs;
    private final int dedupCapacity;
    /** This cluster member's id, logged on role changes for the chaos suite. */
    private final int memberId;

    private Cluster cluster;
    private CanonicalSealerState state;

    /**
     * Session ids that announced themselves as canonical-stream consumers
     * (a {@link #KIND_SUBSCRIBE} frame, or any replay request). DETERMINISM:
     * mutated only from logged session messages and {@code onSessionClose}
     * (both log-driven), so every member holds the identical set and a new
     * leader fans out to the same sessions. Deliberately NOT snapshotted: a
     * restart-from-snapshot severs every client connection, each client
     * re-announces on its next session establishment, and until the first
     * announcement arrives {@link #offerToConsumers} falls back to
     * broadcast-to-all so nothing can starve.
     */
    private final LongHashSet consumerSessions = new LongHashSet();

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

    // Scratch buffers for ingress id/sender extraction and egress framing.
    // Reused to avoid per-message allocation on the single cluster service
    // thread.
    private final byte[] canonicalIdScratch = new byte[CanonicalSealerState.CANONICAL_ID_LEN];
    private final byte[] senderScratch = new byte[CanonicalSealerState.SENDER_LEN];
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
            // stdout for grep-ability (same contract as the role line below):
            // the cluster-member-rejoin chaos case asserts a wiped member came
            // back via SNAPSHOT restore, not silently at genesis.
            System.out.println("sealer snapshot RESTORED memberId=" + memberId
                + " block=" + state.blockNumber() + " canonicalCount=" + state.canonicalCount());
        } else {
            this.state = new CanonicalSealerState(dedupCapacity, CanonicalSealerState.GENESIS_BLOCK_NUMBER);
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
        consumerSessions.remove(session.id());
    }

    @Override
    public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {
        if (length > KIND_OFFSET && buffer.getByte(offset + KIND_OFFSET) == KIND_SUBSCRIBE) {
            consumerSessions.add(session.id());
            return;
        }
        if (length > KIND_OFFSET && buffer.getByte(offset + KIND_OFFSET) == KIND_REPLAY_REQUEST) {
            if (length < MIN_REPLAY_REQUEST_LEN) {
                onMalformedFrame("replay-request", length);
                return;
            }
            final long fromIndex =
                buffer.getLong(offset + 1, ByteOrder.LITTLE_ENDIAN);
            final long fromBlock =
                buffer.getLong(offset + 1 + Long.BYTES, ByteOrder.LITTLE_ENDIAN);
            // A replay request is a consumer announcing itself as surely as a
            // SUBSCRIBE frame does.
            consumerSessions.add(session.id());
            handleReplayRequest(session, fromIndex, fromBlock);
            // A replay request is exactly what a consumer sends when it sees
            // no egress — if that's because THIS member's boundary clock died
            // (lost pending timer after election churn), revive it now.
            maybeReviveBoundaryClock();
            return;
        }
        if (length > KIND_OFFSET && buffer.getByte(offset + KIND_OFFSET) == KIND_BATCH) {
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
                if (entryLen < MIN_INGRESS_LEN || pos + entryLen > limit) {
                    onMalformedFrame("batch-entry", entryLen);
                    return;
                }
                processRecord(session, buffer, pos, entryLen);
                pos += entryLen;
            }
            maybeReviveBoundaryClock();
            return;
        }
        if (length < MIN_INGRESS_LEN) {
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

    /**
     * Process one single-record ingress frame at {@code offset}: parse the
     * guard header (sender + nonce, #85 fix B) and the 32-byte canonical id
     * at their fixed offsets (the payload is relayed verbatim and never
     * inspected), dedup, contiguity-check, and relay if accepted. A
     * contiguity reject answers the OFFERING session with an
     * {@link #EGRESS_KIND_CONTIGUITY_REJECT} frame. Shared by the direct
     * path and each {@link #KIND_BATCH} entry.
     */
    private void processRecord(
            final ClientSession session, final DirectBuffer buffer, final int offset, final int length) {
        buffer.getBytes(offset + CANONICAL_ID_OFFSET, canonicalIdScratch);
        buffer.getBytes(offset + SENDER_OFFSET, senderScratch);
        final long nonce = buffer.getLong(offset + NONCE_OFFSET, ByteOrder.LITTLE_ENDIAN);
        final int payloadOffset = offset + RELAY_OFFSET;
        final int payloadLength = length - RELAY_OFFSET;
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
        outcome.relayed.ifPresent(this::offerRelayed);
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
        final MutableDirectBuffer buf = egressBuffer;
        int pos = 0;
        buf.putByte(pos, EGRESS_KIND_CONTIGUITY_REJECT);
        pos += Byte.BYTES;
        buf.putBytes(pos, senderScratch);
        pos += CanonicalSealerState.SENDER_LEN;
        buf.putLong(pos, nonce, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        buf.putLong(pos, expected, ByteOrder.LITTLE_ENDIAN);
        pos += Long.BYTES;
        offerToSession(session, pos);
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
        writeSnapshot(snapshotPublication, state.takeSnapshot(), cluster.idleStrategy());
        // stdout for grep-ability (same contract as the role line below). Every
        // member snapshots at the same replicated log position, so this line is
        // a CATCH-UP PROOF: a member that logs a TAKEN at a post-rejoin block
        // must have replayed the log all the way to that position. The
        // cluster-member-rejoin chaos case asserts a wiped member's count grows
        // (a follower that starts as follower never gets an onRoleChange, so
        // role lines cannot prove rejoin).
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
     * Serve a client replay request: re-offer every retained frame at/after
     * the requested cursor to the REQUESTING session only, then a REPLAY_DONE
     * marker (or REPLAY_UNAVAILABLE when eviction has outrun the request).
     * Runs identically on every member from the replicated log; only the
     * leader's session offers reach the client.
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
    private void handleReplayRequest(final ClientSession session, final long fromIndex, final long fromBlock) {
        if (fromIndex < firstRetainedIndex || fromBlock < firstRetainedBlock) {
            // stdout, like the role lines: grep-able next to the chaos suite's
            // other signals (the service has no other logger).
            System.out.println("cluster REPLAY memberId=" + memberId
                + " session=" + session.id() + " from=(" + fromIndex + "," + fromBlock
                + ") UNAVAILABLE floor=(" + firstRetainedIndex + "," + firstRetainedBlock + ")");
            offerControl(session, EGRESS_KIND_REPLAY_UNAVAILABLE, firstRetainedIndex, firstRetainedBlock);
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
        offerControl(session, EGRESS_KIND_REPLAY_DONE, state.canonicalCount(), state.blockNumber());
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

    /**
     * Offer arbitrary raw bytes (a retained frame) to one session, with the
     * same deadline-then-close semantics as {@link #offerToSession} — incl.
     * the F07.5 terminal-result close (a MAX_POSITION_EXCEEDED egress is
     * permanently dead; returning silently would leave a zombie session).
     */
    private boolean offerBytesToSession(final ClientSession session, final byte[] frame) {
        final UnsafeBuffer buf = new UnsafeBuffer(frame);
        final long deadline = System.nanoTime() + OFFER_DEADLINE_NS;
        long result;
        do {
            result = session.offer(buf, 0, frame.length);
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
                    closeSessionLoudly(session, "terminal offer result " + result);
                }
                return;
            }
        } while (System.nanoTime() < deadline);
        // Deadline exhausted on persistent back-pressure: this session's
        // subscriber has stopped draining. Close it rather than drop frames —
        // a gap is silent corruption. NOTE the close EVENT may never reach
        // the client (it rides the same wedged egress); the client's
        // delivered-frame liveness watchdog is the recovery path.
        closeSessionLoudly(session, "offer deadline exhausted (back-pressure)");
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
