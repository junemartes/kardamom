package io.kardamom.sealer.cluster;

import io.aeron.driver.DefaultNameResolver;
import io.aeron.driver.NameResolver;
import java.net.InetAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The host name resolver of the member's media driver. It keeps the last
 * address of each name, and answers with that address when a lookup fails.
 *
 * <p>The members name each other by DNS names. The driver looks a name up
 * each time a component adds a channel, and again while a destination
 * sends no status. Every election adds channels: the consensus
 * publications, the log publication and its destinations, the log
 * subscription, and at its end the ingress subscription. A failed lookup
 * stops the election half way. A failed add of the ingress subscription
 * leaves a leader that no client can reach, because Aeron clears the
 * election before that add.</p>
 *
 * <p>A successful lookup always replaces the kept address, so a peer that
 * moves to a new address is found at the next lookup that succeeds. A name
 * that never resolved stays unresolved, and the add fails as before.</p>
 *
 * <p>The member's own name is pinned to the node address, with no lookup.
 * The member runs on the node that its name names, and the job passes the
 * address of that node, so the answer is the same as the DNS answer. A
 * lookup of the own name can only add a wait: a slow DNS agent then delays
 * each add of the member's own endpoints, also in the completion of an
 * election. A node that moves to a new address restarts the member with
 * the new address.</p>
 *
 * <p>The map is concurrent: the driver conductor and the driver's
 * asynchronous lookup tasks call {@link #resolve} on different threads.</p>
 */
final class PeerNameResolver implements NameResolver {

    private final NameResolver delegate;
    private final int memberId;
    /** The names that resolve with no lookup: the member's own name. */
    private final Map<String, InetAddress> pinned;
    private final Map<String, InetAddress> lastAddress = new ConcurrentHashMap<>();
    /** The names whose last lookup failed, so that each outage logs one line at its start and one at its end. */
    private final Set<String> failing = ConcurrentHashMap.newKeySet();

    PeerNameResolver(final int memberId, final Map<String, InetAddress> pinned) {
        this(memberId, pinned, DefaultNameResolver.INSTANCE);
    }

    PeerNameResolver(final int memberId, final Map<String, InetAddress> pinned, final NameResolver delegate) {
        this.memberId = memberId;
        this.pinned = Map.copyOf(pinned);
        this.delegate = delegate;
    }

    @Override
    public InetAddress resolve(final String name, final String uriParamName, final boolean isReResolution) {
        final InetAddress own = pinned.get(name);
        return own != null ? own : lookUp(name, uriParamName, isReResolution);
    }

    private InetAddress lookUp(final String name, final String uriParamName, final boolean isReResolution) {
        final InetAddress address = delegate.resolve(name, uriParamName, isReResolution);
        if (address == null) {
            return lookupFailed(name);
        }
        lastAddress.put(name, address);
        if (failing.remove(name)) {
            System.out.println("cluster DNS RESOLVED memberId=" + memberId + " name=" + name
                + " address=" + address.getHostAddress());
        }
        return address;
    }

    private InetAddress lookupFailed(final String name) {
        final InetAddress kept = lastAddress.get(name);
        if (failing.add(name)) {
            System.out.println("cluster DNS FAILED memberId=" + memberId + " name=" + name
                + (kept == null
                    ? " no last address; the add fails"
                    : " using the last address " + kept.getHostAddress()));
        }
        return kept;
    }
}
