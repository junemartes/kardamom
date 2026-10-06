package io.kardamom.sealer.cluster;

/**
 * The pause between two runs of a background thread of this member.
 *
 * @param ms the pause, in milliseconds
 */
record Interval(long ms) {

    /** Wait one pause. Return false when the thread is interrupted, so its loop ends. */
    boolean await() {
        try {
            Thread.sleep(ms);
            return true;
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
