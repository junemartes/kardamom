package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for the last-address rule of the member's name resolver. No DNS, no Aeron driver. */
final class PeerNameResolverTest {
    private static final String PEER = "sealer-0.node.dc1.consul";

    /** The answers of the node's DNS agent: a name with no entry does not resolve. */
    private final Map<String, InetAddress> dns = new HashMap<>();
    private final PeerNameResolver resolver =
        new PeerNameResolver(2, Map.of(), (name, param, isReResolution) -> dns.get(name));

    private static InetAddress address(final int last) throws Exception {
        return InetAddress.getByAddress(new byte[] {(byte) 192, (byte) 168, 56, (byte) last});
    }

    private InetAddress resolve() {
        return resolver.resolve(PEER, "endpoint", true);
    }

    @Test
    void aFailedLookupAnswersWithTheLastAddress() throws Exception {
        dns.put(PEER, address(17));
        assertEquals(address(17), resolve());
        dns.remove(PEER);
        assertEquals(address(17), resolve(), "an outage of the DNS agent must not lose a known peer");
        assertEquals(address(17), resolve());
    }

    @Test
    void aSuccessfulLookupReplacesTheLastAddress() throws Exception {
        dns.put(PEER, address(17));
        resolve();
        dns.put(PEER, address(30));
        assertEquals(address(30), resolve(), "a peer on a new address is found at the next good lookup");
        dns.remove(PEER);
        assertEquals(address(30), resolve());
    }

    @Test
    void aNameThatNeverResolvedStaysUnresolved() {
        assertNull(resolve());
    }

    @Test
    void thePinnedOwnNameResolvesWithNoLookup() throws Exception {
        final List<String> lookups = new ArrayList<>();
        final InetAddress peer = address(99);
        final PeerNameResolver own = new PeerNameResolver(0, Map.of(PEER, address(17)),
            (name, param, isReResolution) -> {
                lookups.add(name);
                return peer;
            });
        assertEquals(address(17), own.resolve(PEER, "endpoint", false));
        assertEquals(address(17), own.resolve(PEER, "endpoint", true));
        assertEquals(List.of(), lookups, "a slow DNS agent must not delay the own name");
        assertEquals(address(99), own.resolve("sealer-1.node.dc1.consul", "endpoint", false));
    }
}
