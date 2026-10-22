package io.kardamom.sealer.cluster;

/**
 * The hold of a member's outputs after its start.
 *
 * <p>When all members restart together with different log tails, one member
 * can vote in two terms. The leader of the lower term then commits entries
 * that the leader of the higher term later cuts back, and a consumer that
 * executed them must roll back. The cut happens only while the members
 * disagree on the leader. So a member sends nothing to its clients until it
 * is stable after its start ({@link StartHoldRule}):</p>
 * <ul>
 *   <li>its session egress queues every frame, the rules frame included, so
 *       no executor executes an entry and no sequencer sends a new record;</li>
 *   <li>its member feed answers no request of its executor.</li>
 * </ul>
 *
 * <p>The hold ends once and never comes back while the member runs: a later
 * election of a running cluster is ordinary Raft. The hold thread writes the
 * flag, and the service thread reads it.</p>
 */
final class StartHold {

    private volatile boolean held = true;

    /** A hold that is already over: a service that no member thread watches, as in the tests. */
    static StartHold released() {
        final StartHold hold = new StartHold();
        hold.release();
        return hold;
    }

    /** Whether the member still holds its outputs. */
    boolean held() {
        return held;
    }

    /** End the hold. */
    void release() {
        held = false;
    }
}
