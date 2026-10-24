package io.kardamom.sealer.cluster;

/**
 * The state of the member feed on the admin status ({@code feed}) and the
 * gauge {@code kardamom_sealer_feed_state}. Each state has a fixed gauge
 * value, so a new order of the constants keeps the gauge.
 */
enum FeedState {
    /** The member runs no feed. */
    OFF("off", 0),
    /** No answer runs: no request came, or the subscriber left, or the answer was a refusal. */
    IDLE("idle", 1),
    /** The answer walks the retained frames from the cursor of the request. */
    WALKING("walking", 2),
    /** The answer sends each new retained frame. */
    LIVE("live", 3),
    /** The answer ended with a gap, and the feed waits for the next request. */
    GAPPED("gapped", 4);

    private final String label;
    private final int value;

    FeedState(final String label, final int value) {
        this.label = label;
        this.value = value;
    }

    /** The value of the {@code feed} field of the admin status. */
    String label() {
        return label;
    }

    /** The value of the gauge {@code kardamom_sealer_feed_state}. */
    int value() {
        return value;
    }
}
