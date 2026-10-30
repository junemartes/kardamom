package io.kardamom.sealer.cluster;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * The network faults on the links between sealer members, inside one member
 * process. The receiving member applies them: every frame that arrives at
 * one of its UDP endpoints from a faulted peer is dropped, lost at random or
 * held back. A fault on the frames from member {@code m} to its peers is the
 * same fault as a filter on the outbound UDP of {@code m} to its peers, and
 * the traffic of {@code m} to its clients stays intact.
 *
 * <p>The test writes the plan to a file; a daemon thread reads it, writes the
 * frame counts of each source member back, and sends tick frames that wake
 * the endpoints whose frames are due. A member without the plan property
 * passes every frame.</p>
 *
 * <p>Properties of the member process:</p>
 * <ul>
 *   <li>{@code kardamom.test.links.plan}: the plan file.</li>
 *   <li>{@code kardamom.test.links.self}: the member id of this process.</li>
 *   <li>{@code kardamom.test.links.owners}: {@code id=ip/firstPort-lastPort}
 *       for each member, separated by {@code ;}. A frame belongs to the member
 *       of its source address, or to the member whose port block holds its
 *       source port.</li>
 * </ul>
 */
public final class LinkFaults {

    /** The members that the counters cover. */
    static final int MAX_MEMBERS = 8;
    private static final long PLAN_POLL_NS = TimeUnit.MILLISECONDS.toNanos(10);
    private static final long COUNT_WRITE_NS = TimeUnit.MILLISECONDS.toNanos(50);
    private static final long TICK_PARK_NS = TimeUnit.MILLISECONDS.toNanos(2);

    private static LinkFaults instance;

    /** What the endpoint does with one frame. */
    enum Verdict { PASS, DROP, HOLD }

    /** The fault on the frames of one source member. */
    record Fault(String mode, int source, double lossRate, long delayNs) {
        static final Fault NONE = new Fault("none", -1, 0, 0);

        static Fault parse(final String line) {
            final String[] f = line.trim().split("\\s+");
            final int source = Integer.parseInt(f[1]);
            return switch (f[0]) {
                case "cut" -> new Fault("cut", source, 1, 0);
                case "loss" -> new Fault("loss", source, Double.parseDouble(f[2]), 0);
                case "delay" -> new Fault("delay", source, 0, TimeUnit.MILLISECONDS.toNanos(Long.parseLong(f[2])));
                default -> throw new IllegalArgumentException("unknown link fault: " + line);
            };
        }
    }

    /** One member's address and the block of its wildcard ports. */
    record Owner(int member, InetAddress address, int firstPort, int lastPort) {
        static Owner parse(final String spec) {
            final String[] idAndRest = spec.split("=");
            final String[] ipAndPorts = idAndRest[1].split("/");
            final String[] ports = ipAndPorts[1].split("-");
            try {
                return new Owner(Integer.parseInt(idAndRest[0]), InetAddress.getByName(ipAndPorts[0]),
                    Integer.parseInt(ports[0]), Integer.parseInt(ports[1]));
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        boolean owns(final InetSocketAddress source) {
            return address.equals(source.getAddress())
                || (source.getPort() >= firstPort && source.getPort() <= lastPort);
        }
    }

    /** An endpoint that the tick thread wakes: its sockets, and the tick frame its poller dispatches. */
    interface Tickable {
        List<DatagramChannel> channels();

        ByteBuffer tick();
    }

    private final Path plan;
    private final int self;
    private final List<Owner> owners;
    private final DatagramChannel ticker;
    private final InetSocketAddress tickAddress;
    private final List<Tickable> tickables = new CopyOnWriteArrayList<>();
    private final AtomicLongArray passed = new AtomicLongArray(MAX_MEMBERS);
    private final AtomicLongArray dropped = new AtomicLongArray(MAX_MEMBERS);
    private final AtomicLongArray held = new AtomicLongArray(MAX_MEMBERS);
    private final AtomicLongArray released = new AtomicLongArray(MAX_MEMBERS);
    private volatile List<Fault> faults = List.of();
    private long planStamp = -1;
    private long nextPlanNs;
    private long nextCountsNs;
    /** The test JVM; the member stops when it dies. */
    private final Optional<ProcessHandle> parent = Optional.ofNullable(System.getProperty("kardamom.test.parentPid"))
        .flatMap(pid -> ProcessHandle.of(Long.parseLong(pid)));

    private LinkFaults(final Path plan, final int self, final List<Owner> owners) {
        this.plan = plan;
        this.self = self;
        this.owners = owners;
        try {
            this.ticker = DatagramChannel.open();
            ticker.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            ticker.configureBlocking(false);
            this.tickAddress = (InetSocketAddress) ticker.getLocalAddress();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The faults of this process, or empty when the process has no plan. */
    static synchronized Optional<LinkFaults> get() {
        final String planPath = System.getProperty("kardamom.test.links.plan");
        if (planPath == null) {
            return Optional.empty();
        }
        if (instance == null) {
            final List<Owner> owners = Arrays.stream(System.getProperty("kardamom.test.links.owners").split(";"))
                .map(Owner::parse).toList();
            instance = new LinkFaults(Path.of(planPath), Integer.getInteger("kardamom.test.links.self"), owners);
            final Thread thread = new Thread(instance::run, "kardamom-link-faults");
            thread.setDaemon(true);
            thread.start();
        }
        return Optional.of(instance);
    }

    void register(final Tickable endpoint) {
        tickables.add(endpoint);
    }

    boolean isTick(final InetSocketAddress source) {
        return tickAddress.equals(source);
    }

    /** The member that sent from {@code source}, or -1 for a client or an unknown sender. */
    int owner(final InetSocketAddress source) {
        return owners.stream().filter(o -> o.owns(source)).mapToInt(Owner::member).findFirst().orElse(-1);
    }

    /**
     * The verdict on one frame from {@code source}. A frame from a source that
     * still has held frames is held too, so the order of each link stays.
     */
    Verdict verdict(final InetSocketAddress source, final boolean sourceHasHeld) {
        final int member = owner(source);
        if (member < 0 || member == self) {
            return Verdict.PASS;
        }
        final Fault fault = faults.stream().filter(f -> f.source() == member).findFirst().orElse(Fault.NONE);
        final Verdict verdict = switch (fault.mode()) {
            case "cut" -> Verdict.DROP;
            case "loss" -> ThreadLocalRandom.current().nextDouble() < fault.lossRate() ? Verdict.DROP : Verdict.PASS;
            case "delay" -> Verdict.HOLD;
            default -> Verdict.PASS;
        };
        final Verdict ordered = verdict == Verdict.PASS && sourceHasHeld ? Verdict.HOLD : verdict;
        (switch (ordered) {
            case PASS -> passed;
            case DROP -> dropped;
            case HOLD -> held;
        }).incrementAndGet(member);
        return ordered;
    }

    /** The hold time of a frame from {@code source}: its delay, or none once the fault is gone. */
    long delayNs(final InetSocketAddress source) {
        final int member = owner(source);
        return faults.stream().filter(f -> f.source() == member).mapToLong(Fault::delayNs).findFirst().orElse(0);
    }

    void onReleased(final InetSocketAddress source) {
        released.incrementAndGet(Math.max(0, owner(source)));
    }

    private boolean ticking() {
        return faults.stream().anyMatch(f -> f.delayNs() > 0)
            || IntStream.range(0, MAX_MEMBERS).anyMatch(m -> held.get(m) > released.get(m));
    }

    private void run() {
        while (!Thread.currentThread().isInterrupted()) {
            step();
        }
    }

    /** One pass of the daemon thread: the parent check, the plan, the counts and the ticks. */
    private void step() {
        // A member whose test JVM died stops at once, so it holds no port and no tmpfs past its test.
        if (parent.filter(p -> !p.isAlive()).isPresent()) {
            Runtime.getRuntime().halt(1);
        }
        final long now = System.nanoTime();
        nextPlanNs = now >= nextPlanNs ? readPlan(now) : nextPlanNs;
        nextCountsNs = now >= nextCountsNs ? writeCounts(now) : nextCountsNs;
        sendTicks();
        java.util.concurrent.locks.LockSupport.parkNanos(TICK_PARK_NS);
    }

    private long readPlan(final long now) {
        try {
            final long stamp = Files.exists(plan) ? Files.getLastModifiedTime(plan).toMillis() ^ Files.size(plan) : 0;
            if (stamp != planStamp) {
                planStamp = stamp;
                faults = Files.exists(plan)
                    ? Files.readAllLines(plan).stream().filter(l -> !l.isBlank()).map(Fault::parse).toList()
                    : List.of();
            }
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        return now + PLAN_POLL_NS;
    }

    private long writeCounts(final long now) {
        final String text = IntStream.range(0, MAX_MEMBERS)
            .mapToObj(m -> m + " " + passed.get(m) + " " + dropped.get(m) + " " + held.get(m) + " " + released.get(m))
            .collect(Collectors.joining("\n", "", "\n"));
        final Path counts = plan.resolveSibling(plan.getFileName() + ".counts-" + self);
        final Path temp = plan.resolveSibling(plan.getFileName() + ".counts-" + self + ".tmp");
        try {
            Files.writeString(temp, text);
            Files.move(temp, counts, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        return now + COUNT_WRITE_NS;
    }

    private void sendTicks() {
        if (ticking()) {
            tickables.forEach(t -> t.channels().forEach(c -> tick(c, t.tick())));
        }
    }

    private void tick(final DatagramChannel channel, final ByteBuffer frame) {
        try {
            if (channel != null && channel.isOpen() && channel.getLocalAddress() instanceof InetSocketAddress local) {
                final InetAddress host = local.getAddress().isAnyLocalAddress()
                    ? owners.stream().filter(o -> o.member() == self).findFirst().orElseThrow().address()
                    : local.getAddress();
                ticker.send(frame, new InetSocketAddress(host, local.getPort()));
            }
        } catch (final IOException ignored) {
            // A socket that closes between the check and the send needs no tick.
        }
    }

    /** A zeroed frame of {@code length} bytes with the header type {@code type}: a valid frame of version 0. */
    static ByteBuffer tickFrame(final int length, final int type) {
        final UnsafeBuffer frame = new UnsafeBuffer(new byte[length]);
        frame.putInt(0, length, java.nio.ByteOrder.LITTLE_ENDIAN);
        frame.putShort(6, (short) type, java.nio.ByteOrder.LITTLE_ENDIAN);
        return ByteBuffer.wrap(frame.byteArray());
    }
}
