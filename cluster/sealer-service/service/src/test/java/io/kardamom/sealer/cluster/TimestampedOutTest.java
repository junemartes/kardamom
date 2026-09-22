package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.junit.jupiter.api.Test;

final class TimestampedOutTest {

    @Test
    void aLineStartsWithAUtcTimeAndKeepsItsText() {
        final ByteArrayOutputStream sink = new ByteArrayOutputStream();
        final PrintStream out = new TimestampedOut(new PrintStream(sink, true, StandardCharsets.UTF_8));

        out.println("cluster role=LEADER memberId=2");

        final String line = sink.toString(StandardCharsets.UTF_8).trim();
        final int space = line.indexOf(' ');
        // The prefix parses as an instant, and the chaos suite's match on a
        // part of the line still holds.
        Instant.parse(line.substring(0, space));
        assertTrue(line.endsWith(" cluster role=LEADER memberId=2"), line);
    }
}
