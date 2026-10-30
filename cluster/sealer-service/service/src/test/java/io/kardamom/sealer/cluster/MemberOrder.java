package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteOrder;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * The canonical order of each member, read from its own member feed, as an
 * executor on its node reads it. Every member must hold the same canonical
 * frames, byte for byte, in the same order; each expected record appears
 * once, at contiguous indexes.
 */
final class MemberOrder {

    private static final int PAYLOAD_OFFSET = Byte.BYTES + Long.BYTES + Integer.BYTES;

    private MemberOrder() {
    }

    /**
     * Read the canonical frames of every node in {@code nodes} from
     * {@code (fromIndex, fromBlock)} until each holds every record in
     * {@code expected}, then compare them. Return the common frame count.
     */
    static int assertSame(
            final List<SealerNode> nodes, final long fromIndex, final long fromBlock, final Set<Integer> expected) {
        final List<List<FeedReader.Frame>> streams = nodes.stream()
            .map(n -> read(n, fromIndex, fromBlock, expected)).toList();
        final int common = streams.stream().mapToInt(List::size).min().orElse(0);
        IntStream.range(1, streams.size()).forEach(s -> IntStream.range(0, common).forEach(i -> assertArrayEquals(
            streams.get(0).get(i).bytes(), streams.get(s).get(i).bytes(),
            "member " + nodes.get(s).id + " differs from member " + nodes.get(0).id + " at canonical frame " + i)));
        final List<FeedReader.Frame> relayed = streams.get(0).subList(0, common).stream()
            .filter(f -> f.kind() == SealerWire.EGRESS_KIND_RELAYED).toList();
        final List<Integer> ids = relayed.stream().map(MemberOrder::recordOf).toList();
        assertEquals(ids.size(), Set.copyOf(ids).size(), "a record appears twice in the canonical order: " + ids);
        assertEquals(expected, ids.stream().filter(expected::contains).collect(Collectors.toSet()),
            "the common canonical order lacks a record");
        IntStream.range(1, relayed.size()).forEach(i -> assertEquals(relayed.get(i - 1).a() + 1, relayed.get(i).a(),
            "the canonical indexes have a gap"));
        return common;
    }

    /**
     * Require that a consumer applied the canonical frames of {@code node}, byte
     * for byte, in order, through at least {@code minFrames} frames, with every
     * record in {@code expected}.
     */
    static void assertConsumerMatches(
            final SealerNode node, final List<byte[]> consumed, final Set<Integer> expected) {
        final List<FeedReader.Frame> member = read(node, 0, io.kardamom.sealer.CanonicalSealerState.GENESIS_BLOCK_NUMBER,
            expected);
        final int common = Math.min(member.size(), consumed.size());
        IntStream.range(0, common).forEach(i -> assertArrayEquals(member.get(i).bytes(), consumed.get(i),
            "the consumer differs from member " + node.id + " at canonical frame " + i));
        assertEquals(expected, consumed.stream().map(FeedReader.Frame::new).map(MemberOrder::recordOf)
            .filter(expected::contains).collect(Collectors.toSet()), "the consumer lacks a record");
    }

    /** The test record number of a relayed frame, or -1 for another record. */
    static int recordOf(final FeedReader.Frame frame) {
        final UnsafeBuffer b = new UnsafeBuffer(frame.bytes());
        if (frame.kind() != SealerWire.EGRESS_KIND_RELAYED || frame.bytes().length < PAYLOAD_OFFSET + 32
            || b.getByte(PAYLOAD_OFFSET + 31) != (byte) 0x5A) {
            return -1;
        }
        return b.getInt(PAYLOAD_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    private static List<FeedReader.Frame> read(
            final SealerNode node, final long fromIndex, final long fromBlock, final Set<Integer> expected) {
        try (FeedReader reader = new FeedReader(node.feedDir())) {
            reader.request(fromIndex, fromBlock, 1);
            reader.pollUntil(frames -> frames.stream().anyMatch(f -> f.kind() == SealerWire.EGRESS_KIND_REPLAY_DONE)
                && frames.stream().map(MemberOrder::recordOf).collect(Collectors.toSet()).containsAll(expected));
            return reader.frames().stream().filter(FeedReader.Frame::canonical).toList();
        }
    }
}
