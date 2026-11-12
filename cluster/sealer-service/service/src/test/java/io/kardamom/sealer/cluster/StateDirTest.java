package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class StateDirTest {
    @TempDir
    Path root;

    @Test
    void archiveCleanupPreservesALiveNestedArchive() throws IOException {
        final Path shared = root.resolve("dir");
        Files.createDirectory(shared);
        final Path catalog = shared.resolve("archive.catalog");
        Files.writeString(catalog, "old");
        Files.writeString(shared.resolve("29-0.rec"), "executor recording");
        final Path memberCatalog = root.resolve("archive.catalog");
        final Path memberSegment = root.resolve("17-0.rec");
        Files.writeString(memberCatalog, "member catalog");
        Files.writeString(memberSegment, "member recording");
        try (FileChannel writer = FileChannel.open(catalog, StandardOpenOption.WRITE)) {
            new StateDir(root).clearArchive();
            assertTrue(Files.exists(catalog), "the live archive must keep its open catalog linked");
            writer.write(ByteBuffer.wrap(new byte[]{'n', 'e', 'w'}));
            assertEquals("new", Files.readString(catalog));
        }
        assertEquals("executor recording", Files.readString(shared.resolve("29-0.rec")));
        assertFalse(Files.exists(memberCatalog));
        assertFalse(Files.exists(memberSegment));
    }

    @Test
    void archiveCleanupCreatesAMissingRoot() {
        final Path archive = root.resolve("archive");
        new StateDir(archive).clearArchive();
        assertTrue(Files.isDirectory(archive));
    }

    @Test
    void clusterCleanupDeletesItsNestedState() throws IOException {
        final Path nested = root.resolve("state");
        Files.createDirectory(nested);
        Files.writeString(nested.resolve("recording.log"), "cluster state");
        new StateDir(root).clear();
        try (Stream<Path> paths = Files.list(root)) {
            assertEquals(0, paths.count());
        }
    }
}
