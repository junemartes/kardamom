package io.kardamom.sealer.cluster;

import java.util.Optional;
import org.agrona.DirectBuffer;

/**
 * The member feed on the service thread: {@link MemberFeed}, or
 * {@link Off} on a member without a feed. Every call runs on the
 * clustered-service thread, and no call waits.
 */
sealed interface Feed permits MemberFeed, Feed.Off {

    /** Carry the retained frame in the first {@code length} bytes of {@code staged}, when the feed is live. */
    void onFrame(DirectBuffer staged, int length);

    /**
     * Read the control messages of the feeder, and move the answer to the
     * newest request on. {@code (upToIndex, upToBlock)} is the head: the
     * canonical count and the open block. Return the work count.
     */
    int doWork(SealerEgress egress, long upToIndex, long upToBlock);

    /** Stop the feeder thread. */
    void close();

    /** No feed: every call does nothing. */
    final class Off implements Feed {
        static final Off INSTANCE = new Off();

        private Off() {
        }

        @Override
        public void onFrame(final DirectBuffer staged, final int length) {
            // No feed carries the frame.
        }

        @Override
        public int doWork(final SealerEgress egress, final long upToIndex, final long upToBlock) {
            return 0;
        }

        @Override
        public void close() {
            // No feeder thread runs.
        }
    }

    /**
     * Start the feed of {@code config} on the retained file, or return
     * {@link Off} when either is empty. The settings refuse a feed without
     * the retained file at the start ({@link FeedConfig#parse}).
     */
    static Feed open(final Optional<FeedConfig> config, final Optional<RetainedFile> file, final int memberId) {
        return file.<Feed>flatMap(f -> config.map(c -> MemberFeed.start(c, f, memberId))).orElse(Off.INSTANCE);
    }
}
