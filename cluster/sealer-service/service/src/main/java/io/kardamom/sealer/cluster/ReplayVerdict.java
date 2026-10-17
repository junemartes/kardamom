package io.kardamom.sealer.cluster;

/**
 * The answer of a member to a replay request from a cursor. The cluster
 * egress and the member feed both answer with it, so a cursor gets the same
 * answer from both.
 *
 * @param kind   the answer
 * @param reason the text of the log line; empty for {@link Kind#SERVE}
 */
record ReplayVerdict(Kind kind, String reason) {

    /** The three answers to a replay request. */
    enum Kind {
        /** The cursor is past the head: answer {@code REPLAY_AHEAD} with the head. */
        AHEAD,
        /** The cursor names no point of the stream, or is below the floor: answer {@code REPLAY_UNAVAILABLE}. */
        REFUSED,
        /** Send the retained frames from the cursor, then {@code REPLAY_DONE}. */
        SERVE
    }

    static final ReplayVerdict SERVE = new ReplayVerdict(Kind.SERVE, "");
}
