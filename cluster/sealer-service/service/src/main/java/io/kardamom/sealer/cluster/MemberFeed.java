package io.kardamom.sealer.cluster;

import java.nio.ByteBuffer;
import java.util.Optional;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;

/**
 * The service side of the member feed: the stream of retained frames that
 * a member sends to its paired executor over IPC.
 *
 * <p>The service thread writes each frame into the frame ring, and the
 * {@link Feeder} thread offers it to the publication. The feeder passes the
 * replay requests of the executor back through the control ring. Each ring
 * has one writer thread and one reader thread, so the rings are
 * {@link OneToOneRingBuffer}s. No write waits: a full ring gaps a live
 * feed, and pauses a replay to the next duty cycle.</p>
 *
 * <p>Each ring message carries a generation as its type. The feeder starts
 * a new generation for each request and for each loss of the subscriber,
 * and it drops each frame of an older generation. So an answer starts
 * clean: no frame of an earlier answer that is still in the ring reaches
 * the executor after the new answer starts.</p>
 *
 * <p>The answer to a request walks the retained file from the cursor to
 * the head, at most {@link #WALK_BYTES} in each duty cycle, then sends
 * {@code REPLAY_DONE} and turns live. The walk reads the frames that the
 * service appends during the walk too, so no live frame is lost and none
 * comes before {@code REPLAY_DONE}.</p>
 */
final class MemberFeed implements Feed {

    /**
     * The most frame bytes that the walk moves in one duty cycle: about
     * 1,000 transfer frames. The service also runs the walk inside the idle
     * loops of a snapshot write and a timer re-arm, so a small step keeps
     * those loops short.
     */
    static final int WALK_BYTES = 256 << 10;
    /** The capacity of the control ring: requests are rare and small. */
    static final int CONTROL_RING_BYTES = 64 << 10;
    /** The most control messages that one duty cycle reads. */
    private static final int CONTROL_BATCH = 16;

    private enum State {
        /** No request, or the subscriber left: write nothing. */
        IDLE,
        /** Send the refusal in the staging buffer, then turn idle. */
        REFUSING,
        /** Walk the retained file from the cursor of the request. */
        WALKING,
        /** Carry each new retained frame. */
        LIVE,
        /** A frame did not fit: write nothing until the next request. */
        GAPPED
    }

    private final int memberId;
    private final RetainedFile file;
    /** Written by this thread, read by the feeder. */
    private final FrameRing frames;
    /** Written by the feeder, read by this thread. */
    private final RingBuffer control;
    private final AgentRunner feeder;
    private final UnsafeBuffer staging = new UnsafeBuffer(new byte[SealerWire.FEED_ANSWER_LEN]);
    private final EgressFrames codec = new EgressFrames(staging);
    /** A view of one frame of the retained file, during the walk. */
    private final UnsafeBuffer view = new UnsafeBuffer(0, 0);
    private final MessageHandler controlHandler = this::onControl;

    private State state = State.IDLE;
    /** The generation of the newest control message: the type of each frame that this feed writes. */
    private int generation;
    /** The newest request that this duty cycle read, or empty. */
    private Optional<FeedRequest> asked = Optional.empty();
    private FeedRequest request = new FeedRequest(0, 0, 0);
    /** The point after the frames that the answer carried. */
    private final FeedCursor cursor = new FeedCursor();
    private long walkPos;
    /** The frame bytes that the walk can still move in this duty cycle. */
    private long walkLeft;
    /** The length of the refusal in {@link #staging}. */
    private int refusalLength;

    private MemberFeed(
            final int memberId, final RetainedFile file, final FrameRing frames, final RingBuffer control,
            final AgentRunner feeder) {
        this.memberId = memberId;
        this.file = file;
        this.frames = frames;
        this.control = control;
        this.feeder = feeder;
    }

    /** Make the rings and start the feeder thread on them. */
    static MemberFeed start(final FeedConfig config, final RetainedFile file, final int memberId) {
        final RingBuffer frames = ring(config.ringBytes());
        final RingBuffer control = ring(CONTROL_RING_BYTES);
        final AgentRunner feeder = new Feeder(config, memberId, frames, control).start();
        System.out.println("sealer FEED START memberId=" + memberId + " aeronDir=" + config.aeronDir()
            + " ringBytes=" + config.ringBytes() + " termBytes=" + config.termBytes());
        return new MemberFeed(memberId, file, new FrameRing(frames), control, feeder);
    }

    /** A ring off the heap of {@code capacity} bytes, a power of two. */
    private static RingBuffer ring(final int capacity) {
        return new OneToOneRingBuffer(new UnsafeBuffer(
            ByteBuffer.allocateDirect(capacity + RingBufferDescriptor.TRAILER_LENGTH)));
    }

    @Override
    public void onFrame(final DirectBuffer staged, final int length) {
        if (state == State.LIVE && !carry(staged, length)) {
            gap();
        }
    }

    @Override
    public int doWork(final SealerEgress egress, final long upToIndex, final long upToBlock) {
        asked = Optional.empty();
        final int read = control.read(controlHandler, CONTROL_BATCH);
        asked.ifPresent(r -> answer(r, egress, upToIndex, upToBlock));
        return read + step(upToIndex, upToBlock);
    }

    @Override
    public void close() {
        CloseHelper.close(feeder);
    }

    /** Take one control message. A later message wins over an earlier one of the same duty cycle. */
    private void onControl(final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length) {
        generation = msgTypeId;
        asked = FeedRequest.parse(buffer, index, length);
        if (asked.isEmpty() && state != State.IDLE) {
            state = State.IDLE;
            log("IDLE the subscriber left");
        }
    }

    /** Start the answer to {@code next}: a refusal, or the walk from its cursor. */
    private void answer(final FeedRequest next, final SealerEgress egress, final long upToIndex, final long upToBlock) {
        request = next;
        cursor.reset(next.fromIndex(), next.fromBlock());
        final ReplayVerdict verdict = egress.replayVerdict(next.fromIndex(), next.fromBlock(), upToIndex, upToBlock);
        switch (verdict.kind()) {
            case AHEAD -> refuse(SealerWire.EGRESS_KIND_REPLAY_AHEAD, upToIndex, upToBlock, verdict);
            case REFUSED -> refuse(SealerWire.EGRESS_KIND_REPLAY_UNAVAILABLE,
                egress.firstRetainedIndex(), egress.firstRetainedBlock(), verdict);
            case SERVE -> {
                walkPos = file.scanStart(next.fromBlock());
                state = State.WALKING;
                log("REPLAY from=" + cursor);
            }
        }
    }

    private void refuse(final byte kind, final long a, final long b, final ReplayVerdict verdict) {
        refusalLength = codec.feedAnswer(kind, a, b, request.requestId());
        state = State.REFUSING;
        log("REPLAY from=" + cursor + " " + verdict.reason());
    }

    private int step(final long upToIndex, final long upToBlock) {
        return switch (state) {
            case REFUSING -> sendRefusal();
            case WALKING -> walk(upToIndex, upToBlock);
            case IDLE, LIVE, GAPPED -> 0;
        };
    }

    private int sendRefusal() {
        if (!frames.put(generation, staging, refusalLength)) {
            return 0;
        }
        state = State.IDLE;
        return 1;
    }

    /** Move the walk by at most {@link #WALK_BYTES}, one record in each step. */
    private int walk(final long upToIndex, final long upToBlock) {
        walkLeft = WALK_BYTES;
        boolean more = true;
        while (more) {
            more = walkStep(upToIndex, upToBlock);
        }
        return 1;
    }

    /**
     * Carry the record at the walk position when the request wants it, and
     * move past it. False when the walk stops in this duty cycle: at the
     * head, at a full ring, behind the front of the file, or at the end of
     * the budget.
     */
    private boolean walkStep(final long upToIndex, final long upToBlock) {
        if (walkPos >= file.headPos()) {
            finishWalk(upToIndex, upToBlock);
            return false;
        }
        if (walkPos < file.frontPos()) {
            // The floor passed the walk: the frames from here are gone.
            gap();
            return false;
        }
        file.view(walkPos, view);
        final int length = view.capacity();
        if (FrameKey.of(view).wanted(request.fromIndex(), request.fromBlock()) && !carry(view, length)) {
            return false;
        }
        walkPos = file.next(walkPos);
        walkLeft -= length;
        return walkLeft > 0;
    }

    private void finishWalk(final long upToIndex, final long upToBlock) {
        final int length = codec.feedAnswer(
            SealerWire.EGRESS_KIND_REPLAY_DONE, upToIndex, upToBlock, request.requestId());
        if (frames.put(generation, staging, length)) {
            state = State.LIVE;
            log("LIVE at=" + cursor);
        }
    }

    /** Write one retained frame, and move the cursor past it. False when the ring has no room. */
    private boolean carry(final DirectBuffer frame, final int length) {
        if (!frames.put(generation, frame, length)) {
            return false;
        }
        cursor.pass(frame);
        return true;
    }

    /** Write the gap marker into the reserve, and stop the answer. */
    private void gap() {
        final int length = codec.feedGap(cursor.index(), cursor.block(), request.requestId());
        final String lost = frames.putMarker(generation, staging, length) ? "" : " marker=LOST";
        state = State.GAPPED;
        log("GAPPED next=" + cursor + lost);
    }

    private void log(final String what) {
        System.out.println("sealer FEED memberId=" + memberId + " request=" + request.requestId() + " " + what);
    }
}
