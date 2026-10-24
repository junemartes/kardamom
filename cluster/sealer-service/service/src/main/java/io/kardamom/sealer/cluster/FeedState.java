package io.kardamom.sealer.cluster;

/**
 * The state of the member feed on the admin status ({@code feed}) and the
 * gauge {@code kardamom_sealer_feed_state}. The gauge value is the ordinal.
 */
enum FeedState {
    /** The member runs no feed. */
    OFF("off"),
    /** No answer runs: no request came, or the subscriber left, or the answer was a refusal. */
    IDLE("idle"),
    /** The answer walks the retained frames from the cursor of the request. */
    WALKING("walking"),
    /** The answer sends each new retained frame. */
    LIVE("live"),
    /** The answer ended with a gap, and the feed waits for the next request. */
    GAPPED("gapped");

    private final String label;

    FeedState(final String label) {
        this.label = label;
    }

    /** The value of the {@code feed} field of the admin status. */
    String label() {
        return label;
    }
}
