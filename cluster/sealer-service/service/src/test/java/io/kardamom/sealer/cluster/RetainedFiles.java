package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.aeron.test.Tests;
import io.aeron.test.cluster.TestNode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.TreeMap;
import java.util.function.IntFunction;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Reads the retained files of running TestCluster members from the test
 * thread. A member publishes its head at each boundary tick, and every byte
 * below a published head is in its file.
 */
final class RetainedFiles {

    /** The wait between two reads of a published head. */
    private static final long POLL_MS = 50;

    private RetainedFiles() {
    }

    static RetainedFile retained(final TestNode member) {
        return (RetainedFile) ((SealerTestService) member.service()).delegate().retained();
    }

    /** Wait until every member published a head at or above {@code position}. */
    static void awaitHeads(final List<TestNode> members, final long position) {
        members.forEach(member -> {
            while (retained(member).publishedHead() < position) {
                Tests.sleep(POLL_MS);
            }
        });
    }

    /** Wait for a boundary tick on {@code member} after this call, and return the head it published. */
    static long nextPublishedHead(final TestNode member) {
        final long before = retained(member).publishedHead();
        while (retained(member).publishedHead() == before) {
            Tests.sleep(POLL_MS);
        }
        return retained(member).publishedHead();
    }

    /** The segment files of {@code dir}, by start position. */
    static TreeMap<Long, Path> segments(final Path dir) {
        try (Stream<Path> paths = Files.list(dir)) {
            return paths.map(p -> SegmentDir.SEGMENT_NAME.matcher(p.getFileName().toString()))
                .filter(Matcher::matches)
                .collect(Collectors.toMap(m -> Long.parseLong(m.group(1)), m -> dir.resolve(m.group()),
                    (a, b) -> a, TreeMap::new));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The bytes of the retained file in {@code dir} at the positions {@code [0, upTo)}, read from disk. */
    static byte[] bytes(final Path dir, final long upTo) {
        final TreeMap<Long, Path> segments = segments(dir);
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        segments.headMap(upTo, false).forEach((start, path) -> {
            final Long next = segments.higherKey(start);
            final long end = next == null ? upTo : Math.min(upTo, next);
            out.writeBytes(read(path, Math.toIntExact(end - start)));
        });
        return out.toByteArray();
    }

    /** Assert that the file of each member, in the directory {@code dirOf} gives, holds the same bytes below {@code upTo}. */
    static void assertEqualFiles(final List<TestNode> members, final IntFunction<Path> dirOf, final long upTo) {
        final byte[] expected = bytes(dirOf.apply(members.get(0).index()), upTo);
        assertEquals(upTo, expected.length);
        members.forEach(member -> assertArrayEquals(expected, bytes(dirOf.apply(member.index()), upTo),
            "the retained file of member " + member.index()));
    }

    private static byte[] read(final Path path, final int length) {
        try (FileChannel channel = FileChannel.open(path)) {
            final ByteBuffer buf = ByteBuffer.allocate(length);
            channel.read(buf, 0);
            return buf.array();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
