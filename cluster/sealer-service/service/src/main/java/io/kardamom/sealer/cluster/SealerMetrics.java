package io.kardamom.sealer.cluster;

import io.kardamom.sealer.VersionSwitch;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.stream.IntStream;

/**
 * The pipeline metrics of the service thread, for the admin endpoint.
 *
 * <p>Only the clustered-service thread writes them, so a write is a plain
 * read and a release store, with no atomic add and no allocation. The admin
 * thread reads them with volatile loads. No map holds a series: each one is
 * a field or a slot of a fixed array.</p>
 *
 * <p>The values are member-local. They are not part of the replicated
 * state and never reach the log or a snapshot.</p>
 */
final class SealerMetrics {

    /** The outcomes of a frame that the service did not relay, with the label of each. */
    enum Frame {
        MALFORMED("malformed"),
        UNKNOWN_KIND("unknown_kind"),
        CONTIGUITY_REJECT("contiguity_reject"),
        PAST_DEADLINE("past_deadline"),
        WINDOW_FULL("window_full"),
        DA_LAG_REJECT("da_lag_reject"),
        RECORD_LAG_REJECT("record_lag_reject"),
        EXPIRED_REF("expired_ref"),
        ORIGIN_GAP("origin_gap"),
        REMOTE_ORIGIN_REJECT("remote_origin_reject");

        private final String label;

        Frame(final String label) {
            this.label = label;
        }
    }

    /**
     * The rules of the open block, for the admin thread: the store of the
     * retained frames, whether the egress keeps only its replay window, the
     * decision version, whether the rules of the carry window apply, the
     * state of each feature, and the state of the carry window. The
     * service sets a new value at the start and at each change.
     */
    record Rules(
            boolean fileStore,
            boolean windowExact,
            int decisionVersion,
            boolean carryWindow,
            List<VersionSwitch.Feature> features,
            VersionSwitch.Feature window) {
        /** The rules before the start. */
        static final Rules START = new Rules(false, false, VersionSwitch.OFF_VERSION, false, List.of(),
            VersionSwitch.Feature.none(VersionSwitch.FEATURE_CARRY_WINDOW));

        /** The store of the retained frames: {@code heap} or {@code file}. */
        String retainedStore() {
            return fileStore ? "file" : "heap";
        }
    }

    /**
     * The labels of the retryable offer results, at index {@code -result - 1}:
     * NOT_CONNECTED is -1, BACK_PRESSURED is -2, and ADMIN_ACTION is -3.
     */
    private static final String[] REFUSALS = {"not_connected", "back_pressured", "admin_action"};

    /** The histogram of the egress offer samples one offer in this many; a power of two. */
    static final int EGRESS_OFFER_SAMPLE = 64;

    /** The value of {@link #egressOfferStart} for an offer that is not sampled. */
    static final long NOT_SAMPLED = Long.MIN_VALUE;

    private final AtomicLong busyNs = new AtomicLong();
    private final Histogram message = new Histogram(Histogram.Layout.STEP);
    private final Histogram flushWindow = new Histogram(Histogram.Layout.STEP);
    private final Histogram boundary = new Histogram(Histogram.Layout.STEP);
    private final Histogram snapshot = new Histogram(Histogram.Layout.STEP);
    private final Histogram egressOffer = new Histogram(Histogram.Layout.STEP);
    private final Histogram ingressBatch = new Histogram(Histogram.Layout.COUNT);
    private final AtomicLong egressBacklogBytes = new AtomicLong();
    private final AtomicLong orderingWindow = new AtomicLong();
    private final AtomicLongArray frames = new AtomicLongArray(Frame.values().length);
    /** One slot for each value of the kind byte. */
    private final AtomicLongArray messages = new AtomicLongArray(256);
    private final AtomicLongArray refusals = new AtomicLongArray(REFUSALS.length);
    private final AtomicLong egressStalls = new AtomicLong();
    private final AtomicLong retainedFrames = new AtomicLong();
    private final AtomicLong retainedBytes = new AtomicLong();
    private final AtomicLong snapshotBytes = new AtomicLong();
    private final AtomicLong snapshotVersion = new AtomicLong();
    private final AtomicLong refRefused = new AtomicLong();
    private final AtomicLong fullRefBelowV5 = new AtomicLong();
    private final AtomicLong featureUnsupported = new AtomicLong();
    private volatile Rules rules = Rules.START;
    /** The offers since the last sampled one. Only the service thread reads it. */
    private int offersToSample;

    /** Add one to {@code counter}. Only one thread calls this. */
    private long increment(final AtomicLong counter) {
        final long next = counter.getPlain() + 1;
        counter.lazySet(next);
        return next;
    }

    /** Add one to slot {@code index} of {@code counters}. Only one thread calls this. */
    private long increment(final AtomicLongArray counters, final int index) {
        final long next = counters.getPlain(index) + 1;
        counters.lazySet(index, next);
        return next;
    }

    /** Count one frame of {@code outcome}, and return the total of that outcome. */
    long frame(final Frame outcome) {
        return increment(frames, outcome.ordinal());
    }

    /** The total of {@code outcome}, for any thread. */
    long frames(final Frame outcome) {
        return frames.get(outcome.ordinal());
    }

    /** Count one session message of kind {@code kind}. */
    void message(final byte kind) {
        increment(messages, kind & 0xFF);
    }

    /**
     * End one session message that started at {@code startedNs}: its step,
     * its busy time, and the records that the ordering window holds after it.
     */
    void messageDone(final long startedNs, final int windowSize) {
        final long elapsed = System.nanoTime() - startedNs;
        message.record(elapsed);
        busyNs.lazySet(busyNs.getPlain() + elapsed);
        orderingWindow.lazySet(windowSize);
    }

    /** Add the time from {@code startedNs} to now to the busy time. */
    void busySince(final long startedNs) {
        busyNs.lazySet(busyNs.getPlain() + System.nanoTime() - startedNs);
    }

    /** End one timer entry: its busy time, and the records that the ordering window holds after it. */
    void timerDone(final long startedNs, final int windowSize) {
        busySince(startedNs);
        orderingWindow.lazySet(windowSize);
    }

    /** Record one flush of the ordering window that admitted {@code records}, when it held any. */
    void windowFlushed(final long startedNs, final int records) {
        if (records > 0) {
            flushWindow.recordSince(startedNs);
        }
    }

    /** Record the boundary work of one boundary timer. */
    void boundaryDone(final long startedNs) {
        boundary.recordSince(startedNs);
    }

    /** Record one snapshot, and add its time to the busy time. */
    void snapshotDone(final long startedNs) {
        final long elapsed = System.nanoTime() - startedNs;
        snapshot.record(elapsed);
        busyNs.lazySet(busyNs.getPlain() + elapsed);
    }

    /** Record the entries of one batch message. */
    void ingressBatch(final int entries) {
        ingressBatch.record(entries);
    }

    /**
     * Start one offer to the consumers. It returns the clock for one offer
     * in {@link #EGRESS_OFFER_SAMPLE}, and {@link #NOT_SAMPLED} for the
     * others, so the record path reads the clock twice per 64 offers only.
     */
    long egressOfferStart() {
        offersToSample = (offersToSample + 1) & (EGRESS_OFFER_SAMPLE - 1);
        return offersToSample == 0 ? System.nanoTime() : NOT_SAMPLED;
    }

    /** End one offer to the consumers that {@link #egressOfferStart} started. */
    void egressOfferDone(final long startedNs) {
        if (startedNs != NOT_SAMPLED) {
            egressOffer.recordSince(startedNs);
        }
    }

    /** Count one offer that met the retryable result {@code result} and queued its frame. */
    void egressRefused(final long result) {
        increment(refusals, (int) -result - 1);
    }

    /** Count one session closed because its backlog made no progress. */
    void egressStall() {
        increment(egressStalls);
    }

    /** Set the bytes that the egress backlogs hold. */
    void egressBacklog(final long bytes) {
        egressBacklogBytes.lazySet(bytes);
    }

    /** Set the retained frames and their bytes, at the end of a log entry. */
    void retained(final long frames, final long bytes) {
        retainedFrames.lazySet(frames);
        retainedBytes.lazySet(bytes);
    }

    /** Set the version and the size of the newest snapshot that the member took or restored. */
    void snapshot(final int version, final long bytes) {
        snapshotVersion.lazySet(version);
        snapshotBytes.lazySet(bytes);
    }

    /** Count one transaction reference of record type 0 that the carry window refused, and return the total. */
    long refRefused() {
        return increment(refRefused);
    }

    /** Count one full reference that the member ordered below decision version 5. */
    void fullRefBelowV5() {
        increment(fullRefBelowV5);
    }

    /** Count one activation of the carry window on a member with the retained file, and return the total. */
    long featureUnsupported() {
        return increment(featureUnsupported);
    }

    /** Set the rules of the open block. */
    void rules(final Rules next) {
        rules = next;
    }

    /** The carry part of the admin status: the store, the carry window, and the retained frames. */
    MemberStatus.Carry carry() {
        final Rules now = rules;
        final VersionSwitch.Feature window = now.window();
        return new MemberStatus.Carry(now.retainedStore(), now.carryWindow(), window.activationOrigin(),
            window.activationMs(), retainedFrames.get(), retainedBytes.get());
    }

    /** Write every series. Only the admin thread calls this. */
    void renderTo(final MetricsText text) {
        text.family("kardamom_loop_nanoseconds_total", "counter",
                "The time of a thread loop in each state, in nanoseconds.")
            .sample("kardamom_loop_nanoseconds_total", "loop=\"sealer.service\",state=\"busy\"", busyNs.get());
        text.family("kardamom_step_seconds", "histogram", "The duration of one step of work, in seconds.");
        message.renderTo(text, "kardamom_step_seconds", "step=\"sealer.message\"");
        flushWindow.renderTo(text, "kardamom_step_seconds", "step=\"sealer.flush_window\"");
        boundary.renderTo(text, "kardamom_step_seconds", "step=\"sealer.boundary\"");
        snapshot.renderTo(text, "kardamom_step_seconds", "step=\"sealer.snapshot\"");
        egressOffer.renderTo(text, "kardamom_step_seconds", "step=\"sealer.egress_offer\"");
        text.family("kardamom_batch_items", "histogram", "The items in one batch.");
        ingressBatch.renderTo(text, "kardamom_batch_items", "batch=\"sealer.ingress_batch\"");
        text.family("kardamom_queue_depth", "gauge", "The items that wait in a queue.")
            .sample("kardamom_queue_depth", "queue=\"sealer.egress_backlog\"", egressBacklogBytes.get())
            .sample("kardamom_queue_depth", "queue=\"sealer.ordering_window\"", orderingWindow.get());
        renderCounters(text);
        renderCarry(text);
    }

    /** The retained frames, the snapshot, the rules of the open block, and the counters of the carry window. */
    private void renderCarry(final MetricsText text) {
        final Rules now = rules;
        text.single("kardamom_sealer_retained_frames", "gauge", "The retained frame count.", retainedFrames.get())
            .single("kardamom_sealer_retained_bytes", "gauge", "The bytes of the retained frames.", retainedBytes.get())
            .single("kardamom_sealer_retained_store", "gauge",
                "The store of the retained frames: 0 heap, 1 file.", now.fileStore() ? 1L : 0L)
            .single("kardamom_sealer_window_exact", "gauge",
                "1 when the egress keeps only its replay window, else 0.", now.windowExact() ? 1L : 0L)
            .single("kardamom_sealer_decision_version", "gauge",
                "The decision version of the open block.", now.decisionVersion())
            .single("kardamom_sealer_snapshot_bytes", "gauge",
                "The size of the newest snapshot that the member took or restored.", snapshotBytes.get())
            .single("kardamom_sealer_snapshot_version", "gauge",
                "The version of the newest snapshot that the member took or restored.", snapshotVersion.get())
            .single("kardamom_sealer_ref_refused_total", "counter",
                "The transaction references of record type 0 that the carry window refused.", refRefused.get())
            .single("kardamom_sealer_full_ref_below_v5_total", "counter",
                "The full references that the member ordered below decision version 5.", fullRefBelowV5.get())
            .single("kardamom_sealer_feature_unsupported_total", "counter",
                "The activations of the carry window on a member with the retained file.", featureUnsupported.get());
        text.family("kardamom_sealer_feature_state", "gauge",
            "The state of each feature: 0 no activation held, 1 held and off, 2 on.");
        now.features().forEach(f -> text.sample("kardamom_sealer_feature_state", featureLabel(f), f.state()));
        text.family("kardamom_sealer_feature_activation_ms", "gauge",
            "The time of the newest held activation of each feature, 0 when none.");
        now.features().forEach(f -> text.sample(
            "kardamom_sealer_feature_activation_ms", featureLabel(f), unsigned(f.activationMs())));
    }

    /** A u64 value as a gauge value: a time of {@code 2^64 - 1} keeps a feature off. */
    private static double unsigned(final long value) {
        return value >= 0 ? value : 0x1p64 + value;
    }

    private static String featureLabel(final VersionSwitch.Feature feature) {
        return "feature=\"" + feature.id() + "\"";
    }

    private void renderCounters(final MetricsText text) {
        text.family("kardamom_sealer_frames_total", "counter", "The frames that the service did not relay, by outcome.");
        Arrays.stream(Frame.values())
            .forEach(f -> text.sample("kardamom_sealer_frames_total", "outcome=\"" + f.label + "\"", frames(f)));
        text.family("kardamom_sealer_messages_total", "counter",
            "The session messages, by kind. A kind shows after its first message.");
        IntStream.range(0, messages.length())
            .filter(kind -> messages.get(kind) != 0)
            .forEach(kind -> text.sample("kardamom_sealer_messages_total", "kind=\"" + kind + "\"", messages.get(kind)));
        text.family("kardamom_sealer_egress_backpressure_total", "counter",
            "The egress offers that met a retryable result and queued their frame, by result.");
        IntStream.range(0, REFUSALS.length).forEach(i -> text.sample(
            "kardamom_sealer_egress_backpressure_total", "result=\"" + REFUSALS[i] + "\"", refusals.get(i)));
        text.single("kardamom_sealer_egress_stalls_total", "counter",
            "The sessions closed because their egress backlog made no progress.", egressStalls.get());
    }
}
