package io.kardamom.sealer.cluster;

import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.security.MessageDigest;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * The segments of the retained file on the admin endpoint, for a member that
 * a peer seeds. {@code GET /retained} lists the segments, one
 * {@code <name> <size>} line each, and {@code GET /retained/<name>} answers
 * the bytes of one segment.
 *
 * <p>The endpoint serves each segment only below the published head: every
 * byte below it is a whole record. A segment that starts at or above the
 * published head is not listed.</p>
 *
 * <p>The file holds the bytes of every transaction above the posted head,
 * and the admin server listens on all addresses. So every request must
 * carry the shared token of the job in {@code Authorization: Bearer
 * <token>}; any other request answers 403. The requests run on their own
 * threads, so a slow copy does not hold up {@code /ready}.</p>
 *
 * @param dir     the directory of the retained file
 * @param head    the published head of the retained file
 * @param token   the shared token of the job
 * @param workers the threads of the segment requests
 */
record RetainedEndpoint(SegmentDir dir, LongSupplier head, String token, ExecutorService workers) {

    /** The path of the segment list. A segment is under it. */
    static final String PATH = "/retained";
    /** The setting of the shared token. The env var {@code KARDAMOM_RETAINED_TOKEN} also sets it. */
    static final String TOKEN_SETTING = "kardamom.cluster.retainedToken";
    private static final int THREADS = 2;
    private static final int OK = 200;
    private static final int FORBIDDEN = 403;
    private static final int NOT_FOUND = 404;

    /** An endpoint with its own daemon threads. */
    static RetainedEndpoint start(final SegmentDir dir, final LongSupplier head, final String token) {
        return new RetainedEndpoint(dir, head, token, Executors.newFixedThreadPool(THREADS, task -> {
            final Thread t = new Thread(task, "kardamom-admin-retained");
            t.setDaemon(true);
            return t;
        }));
    }

    /** The request header that carries the token. */
    static String authorization(final String token) {
        return "Bearer " + token;
    }

    /** Hand the request to the threads of the endpoint. */
    void handle(final HttpExchange exchange, final String path) {
        workers.execute(() -> answer(exchange, path));
    }

    void close() {
        workers.shutdownNow();
    }

    private void answer(final HttpExchange exchange, final String path) {
        try {
            if (!authorized(exchange)) {
                respond(exchange, FORBIDDEN, "{\"error\":\"the request needs the retained-file token\"}");
            } else if (PATH.equals(path)) {
                respond(exchange, OK, list());
            } else {
                serve(exchange, path.substring(PATH.length() + 1));
            }
        } catch (final IOException e) {
            System.out.println("cluster admin retained request FAILED path=" + path + ": " + e);
        } finally {
            exchange.close();
        }
    }

    private boolean authorized(final HttpExchange exchange) {
        final String sent = exchange.getRequestHeaders().getFirst("Authorization");
        return sent != null && MessageDigest.isEqual(
            sent.getBytes(StandardCharsets.UTF_8), authorization(token).getBytes(StandardCharsets.UTF_8));
    }

    /** The served segments by start position, each with the bytes it serves. */
    private TreeMap<Long, Long> served() throws IOException {
        final long below = head.getAsLong();
        return dir.list().headMap(below, false).entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> Math.min(e.getValue().toFile().length(), below - e.getKey()),
            (a, b) -> a, TreeMap::new));
    }

    private String list() throws IOException {
        return served().entrySet().stream()
            .map(e -> SegmentDir.name(e.getKey()) + " " + e.getValue() + "\n")
            .collect(Collectors.joining());
    }

    /**
     * The bytes of one segment below the published head. A name that is not
     * a served segment answers 404. An open file stays readable after the
     * purge deletes it.
     */
    private void serve(final HttpExchange exchange, final String name) throws IOException {
        final var match = SegmentDir.SEGMENT_NAME.matcher(name);
        final Long length = match.matches() ? served().get(Long.parseLong(match.group(1))) : null;
        if (length == null) {
            respond(exchange, NOT_FOUND, "{\"error\":\"no such segment\"}");
            return;
        }
        try (FileChannel channel = FileChannel.open(dir.dir().resolve(name))) {
            exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
            exchange.sendResponseHeaders(OK, length);
            try (OutputStream out = exchange.getResponseBody()) {
                send(channel, length, Channels.newChannel(out));
            }
        } catch (final NoSuchFileException e) {
            respond(exchange, NOT_FOUND, "{\"error\":\"no such segment\"}");
        }
    }

    private static void send(final FileChannel channel, final long length, final WritableByteChannel out)
            throws IOException {
        long sent = 0;
        while (sent < length) {
            sent = Math.addExact(sent, channel.transferTo(sent, length - sent, out));
        }
    }

    private static void respond(final HttpExchange exchange, final int code, final String body) throws IOException {
        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", code == OK ? "text/plain" : "application/json");
        exchange.sendResponseHeaders(code, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
