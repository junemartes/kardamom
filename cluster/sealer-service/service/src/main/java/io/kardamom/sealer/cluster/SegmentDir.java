package io.kardamom.sealer.cluster;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The directory of a retained file. A segment file is named by the position
 * of its first record: {@code retained-<start>.seg}, with 20 digits.
 *
 * @param dir the directory
 */
record SegmentDir(Path dir) {

    /** The name of a segment file. Group 1 is the start position. */
    static final Pattern SEGMENT_NAME = Pattern.compile("retained-(\\d{20})\\.seg");

    /** The name of the segment that starts at {@code start}. */
    static String name(final long start) {
        return String.format("retained-%020d.seg", start);
    }

    /** The path of the segment that starts at {@code start}. */
    Path segment(final long start) {
        return dir.resolve(name(start));
    }

    /** The segment files in the directory, by start position. */
    TreeMap<Long, Path> list() throws IOException {
        try (Stream<Path> paths = Files.list(dir)) {
            return paths
                .map(p -> SEGMENT_NAME.matcher(p.getFileName().toString()))
                .filter(Matcher::matches)
                .collect(Collectors.toMap(m -> Long.parseLong(m.group(1)), m -> dir.resolve(m.group()),
                    (a, b) -> a, TreeMap::new));
        }
    }

    /** Map the segment that starts at {@code start}, little-endian. The mapping stays valid after the channel closes. */
    MappedByteBuffer map(final long start, final FileChannel.MapMode mode) throws IOException {
        final Path path = segment(start);
        final StandardOpenOption[] options = mode == FileChannel.MapMode.READ_ONLY
            ? new StandardOpenOption[] {StandardOpenOption.READ}
            : new StandardOpenOption[] {StandardOpenOption.READ, StandardOpenOption.WRITE};
        try (FileChannel channel = FileChannel.open(path, options)) {
            final MappedByteBuffer buffer = channel.map(mode, 0, channel.size());
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            return buffer;
        }
    }

    /** Sync the directory, so a new or renamed segment name survives a power loss. */
    void sync() throws IOException {
        try (FileChannel channel = FileChannel.open(dir, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }
}
