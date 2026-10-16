package io.kardamom.sealer.cluster;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The member's admin endpoint: a small HTTP server that reports the
 * {@link MemberStatus}.
 *
 * <p>{@code GET /status} answers 200 with the status JSON. {@code GET /ready}
 * answers 200 when the member is ready and 503 when it is not, with the same
 * JSON, so a service check reads the verdict from the status code and an
 * operator reads the reason from the body. {@code GET /metrics} answers the
 * gauges in the Prometheus text format.</p>
 *
 * <p>With a retained file, {@code GET /retained} lists its segment files,
 * one {@code <name> <size>} line each, and {@code GET /retained/<name>}
 * answers the bytes of one segment. A member that a peer seeds copies the
 * segments this way. Every other path answers 404. A failing status
 * supplier answers 503 with an error object. The server runs on two daemon
 * threads, so a segment copy does not hold up {@code /ready}, and the
 * server never keeps the JVM alive.</p>
 */
final class AdminServer implements AutoCloseable {
    private static final int OK = 200;
    private static final int NOT_FOUND = 404;
    private static final int UNAVAILABLE = 503;
    private static final int THREADS = 2;
    /** The path of the segment list. A segment is under it. */
    static final String RETAINED_PATH = "/retained";

    private final HttpServer server;
    private final Supplier<MemberStatus> status;
    private final long lagBytes;
    /** The directory of the retained file, or empty when the member keeps the frames on the heap. */
    private final Optional<Path> retainedDir;

    private AdminServer(
            final HttpServer server,
            final Supplier<MemberStatus> status,
            final long lagBytes,
            final Optional<Path> retainedDir) {
        this.server = server;
        this.status = status;
        this.lagBytes = lagBytes;
        this.retainedDir = retainedDir;
    }

    /**
     * Bind {@code 0.0.0.0:port} (0 picks a free port) and start serving.
     *
     * @throws IOException when the bind fails
     */
    static AdminServer start(
            final int port, final Supplier<MemberStatus> status, final long lagBytes)
            throws IOException {
        return start(port, status, lagBytes, Optional.empty());
    }

    /**
     * {@link #start(int, Supplier, long)}, and serve the segments of the
     * retained file in {@code retainedDir}.
     *
     * @throws IOException when the bind fails
     */
    static AdminServer start(
            final int port,
            final Supplier<MemberStatus> status,
            final long lagBytes,
            final Optional<Path> retainedDir)
            throws IOException {
        final HttpServer http = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0);
        final AdminServer admin = new AdminServer(http, status, lagBytes, retainedDir);
        http.createContext("/", admin::handle);
        http.setExecutor(Executors.newFixedThreadPool(THREADS, admin::daemonThread));
        http.start();
        return admin;
    }

    /** The port the server listens on. */
    int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private Thread daemonThread(final Runnable task) {
        final Thread t = new Thread(task, "kardamom-admin-endpoint");
        t.setDaemon(true);
        return t;
    }

    private void handle(final HttpExchange exchange) throws IOException {
        final String path = exchange.getRequestURI().getPath();
        if ("/status".equals(path)) {
            respondWithStatus(exchange, false);
        } else if ("/ready".equals(path)) {
            respondWithStatus(exchange, true);
        } else if ("/metrics".equals(path)) {
            respondWithMetrics(exchange);
        } else if (retainedDir.isPresent() && RETAINED_PATH.equals(path)) {
            respondWithSegmentList(exchange, retainedDir.get());
        } else if (retainedDir.isPresent() && path.startsWith(RETAINED_PATH + "/")) {
            respondWithSegment(exchange, retainedDir.get(), path.substring(RETAINED_PATH.length() + 1));
        } else {
            respond(exchange, NOT_FOUND, "{\"error\":\"not found\"}");
        }
    }

    private void respondWithMetrics(final HttpExchange exchange) throws IOException {
        final MemberStatus sample;
        try {
            sample = status.get();
        } catch (final RuntimeException e) {
            respond(exchange, UNAVAILABLE, "{\"error\":\"" + escape(String.valueOf(e)) + "\"}");
            return;
        }
        respond(exchange, OK, "text/plain; version=0.0.4", sample.toMetrics());
    }

    /** One {@code <name> <size>} line for each segment file, by start position. */
    private static void respondWithSegmentList(final HttpExchange exchange, final Path dir) throws IOException {
        final String body;
        try (Stream<Path> paths = Files.list(dir)) {
            body = paths
                .filter(p -> RetainedFile.SEGMENT_NAME.matcher(p.getFileName().toString()).matches())
                .sorted()
                .map(p -> p.getFileName() + " " + p.toFile().length() + "\n")
                .collect(Collectors.joining());
        }
        respond(exchange, OK, "text/plain", body);
    }

    /**
     * The bytes of one segment file. A name that is not a segment name, or
     * a segment that the purge deleted, answers 404. An open file stays
     * readable after the purge deletes it.
     */
    private static void respondWithSegment(final HttpExchange exchange, final Path dir, final String name)
            throws IOException {
        if (!RetainedFile.SEGMENT_NAME.matcher(name).matches()) {
            respond(exchange, NOT_FOUND, "{\"error\":\"not a segment\"}");
            return;
        }
        try (FileChannel channel = FileChannel.open(dir.resolve(name))) {
            exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
            exchange.sendResponseHeaders(OK, channel.size());
            try (OutputStream out = exchange.getResponseBody()) {
                Channels.newInputStream(channel).transferTo(out);
            }
        } catch (final NoSuchFileException e) {
            respond(exchange, NOT_FOUND, "{\"error\":\"no such segment\"}");
        }
    }

    /**
     * Answer with the status JSON. On {@code /ready} the status code carries
     * the verdict; on {@code /status} it is always 200.
     */
    private void respondWithStatus(final HttpExchange exchange, final boolean verdictInCode)
            throws IOException {
        final MemberStatus sample;
        try {
            sample = status.get();
        } catch (final RuntimeException e) {
            respond(exchange, UNAVAILABLE, "{\"error\":\"" + escape(String.valueOf(e)) + "\"}");
            return;
        }
        final boolean ready = !verdictInCode || sample.isReady(lagBytes);
        respond(exchange, ready ? OK : UNAVAILABLE, sample.toJson(lagBytes));
    }

    private static void respond(final HttpExchange exchange, final int code, final String body)
            throws IOException {
        respond(exchange, code, "application/json", body);
    }

    private static void respond(
            final HttpExchange exchange, final int code, final String contentType, final String body)
            throws IOException {
        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(code, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    /** Make an exception message safe inside a JSON string. */
    private static String escape(final String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", " ");
    }
}
