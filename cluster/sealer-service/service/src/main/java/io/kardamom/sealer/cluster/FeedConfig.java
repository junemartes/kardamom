package io.kardamom.sealer.cluster;

import java.util.Optional;

/**
 * The settings of the member feed, parsed once at the start.
 *
 * <p>The ring holds the largest frame: Agrona takes a message of at most
 * an eighth of the ring, and a frame is shorter than one session message.
 * The feed term holds it too: Aeron takes a message of at most an eighth of
 * the term.</p>
 *
 * @param aeronDir  the directory of the media driver that the member shares with its executor
 * @param ringBytes the capacity of the frame ring, a power of two
 * @param termBytes the term length of the feed publication
 */
record FeedConfig(String aeronDir, int ringBytes, int termBytes) {

    /** The directory of the shared media driver. Unset or empty: no feed. */
    static final String DIR_SETTING = "kardamom.cluster.feedAeronDir";
    /** The capacity of the frame ring, in bytes. */
    static final String RING_SETTING = "kardamom.cluster.feedRingBytes";
    static final int DEFAULT_RING_BYTES = 16 << 20;
    /** The smallest ring that takes the largest frame. */
    static final int MIN_RING_BYTES = 8 * SealerWire.MAX_SESSION_MESSAGE_LEN;
    /** The smallest term that takes the largest frame: 8 MiB. */
    static final int TERM_BYTES = 8 * SealerWire.MAX_SESSION_MESSAGE_LEN;

    /**
     * The settings from their property values, or empty when
     * {@code aeronDir} is unset or empty.
     *
     * @param carryBytes whether the member keeps its retained frames in a file
     * @throws IllegalArgumentException when the feed is on without the retained file, or the ring does not parse
     */
    static Optional<FeedConfig> parse(final String aeronDir, final String ringBytes, final boolean carryBytes) {
        final Optional<String> dir = Optional.ofNullable(aeronDir).map(String::trim).filter(s -> !s.isEmpty());
        if (dir.isPresent() && !carryBytes) {
            throw new IllegalArgumentException(DIR_SETTING + " needs " + RetainedFile.Config.SETTING
                + "=true: the feed replays from the retained file");
        }
        return dir.map(d -> new FeedConfig(d, ring(ringBytes), TERM_BYTES));
    }

    private static int ring(final String ringBytes) {
        final int bytes = Optional.ofNullable(ringBytes).filter(s -> !s.isBlank())
            .map(s -> Integer.parseInt(s.trim()))
            .orElse(DEFAULT_RING_BYTES);
        if (bytes < MIN_RING_BYTES || Integer.bitCount(bytes) != 1) {
            throw new IllegalArgumentException(RING_SETTING + " must be a power of two of at least "
                + MIN_RING_BYTES + ", got " + bytes);
        }
        return bytes;
    }

    /**
     * The channel of the feed publication. The log is not sparse: the
     * driver writes every page of it when it creates the log, so the
     * memory of the log counts against the driver, not against the member.
     */
    String feedChannel() {
        return "aeron:ipc?term-length=" + termBytes + "|sparse=false";
    }
}
