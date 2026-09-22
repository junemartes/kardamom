package io.kardamom.sealer.cluster;

import java.io.PrintStream;
import java.time.Instant;

/**
 * Stdout with the UTC time at the start of every line.
 *
 * <p>The node reports its roles, leadership terms, snapshots and clock
 * revivals on stdout, and the chaos suite reads them from the allocation
 * log. Nomad stores the lines without a time. A stall after an election
 * can then not be placed against the consumers' logs, which carry times:
 * issue #408 had a leader, five sealed blocks and a stall, and no way to
 * tell which member led when. Every reader of these lines matches a part
 * of the line, so a prefix breaks none of them.</p>
 */
final class TimestampedOut extends PrintStream {

    TimestampedOut(final PrintStream out) {
        super(out, true);
    }

    @Override
    public void println(final String line) {
        super.println(Instant.now() + " " + line);
    }
}
