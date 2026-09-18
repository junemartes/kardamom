package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Pure-function tests for the {@link ClusterNode} launcher helpers. These
 * tests use no Aeron and no cluster boot, so they run fast. They pin the two
 * parsing helpers that turn the {@code -Dkardamom.cluster.members} string
 * into this member's endpoints and id.
 */
final class ClusterNodeTest {
    /** A realistic 3-member topology, matching deploy/cluster/nomad/cluster.nomad.hcl. */
    private static final String MEMBERS =
        "0,192.168.56.51:40200,192.168.56.51:40201,192.168.56.51:40202,192.168.56.51:40203,192.168.56.51:40204"
            + "|1,192.168.56.52:40200,192.168.56.52:40201,192.168.56.52:40202,192.168.56.52:40203,192.168.56.52:40204"
            + "|2,192.168.56.53:40200,192.168.56.53:40201,192.168.56.53:40202,192.168.56.53:40203,192.168.56.53:40204";

    @Test
    void memberEndpointsReturnsTheFiveEndpointsForEachId() {
        assertArrayEquals(
            new String[] {
                "192.168.56.51:40200", "192.168.56.51:40201", "192.168.56.51:40202",
                "192.168.56.51:40203", "192.168.56.51:40204",
            },
            ClusterNode.memberEndpoints(MEMBERS, 0));
        assertArrayEquals(
            new String[] {
                "192.168.56.52:40200", "192.168.56.52:40201", "192.168.56.52:40202",
                "192.168.56.52:40203", "192.168.56.52:40204",
            },
            ClusterNode.memberEndpoints(MEMBERS, 1));
        assertArrayEquals(
            new String[] {
                "192.168.56.53:40200", "192.168.56.53:40201", "192.168.56.53:40202",
                "192.168.56.53:40203", "192.168.56.53:40204",
            },
            ClusterNode.memberEndpoints(MEMBERS, 2));
    }

    @Test
    void memberEndpointsThrowsOnUnknownId() {
        assertThrows(IllegalArgumentException.class, () -> ClusterNode.memberEndpoints(MEMBERS, 7));
    }

    @Test
    void memberEndpointsThrowsOnMalformedEntry() {
        // Only 3 comma fields where 6 are required.
        assertThrows(
            IllegalArgumentException.class,
            () -> ClusterNode.memberEndpoints("0,192.168.56.51:40200,192.168.56.51:40201", 0));
    }

    @Test
    void memberIdForNodeIpResolvesEachNodeIp() {
        assertEquals(0, ClusterNode.memberIdForNodeIp(MEMBERS, "192.168.56.51"));
        assertEquals(1, ClusterNode.memberIdForNodeIp(MEMBERS, "192.168.56.52"));
        assertEquals(2, ClusterNode.memberIdForNodeIp(MEMBERS, "192.168.56.53"));
    }

    @Test
    void memberIdForNodeIpThrowsOnUnknownIp() {
        assertThrows(
            IllegalArgumentException.class,
            () -> ClusterNode.memberIdForNodeIp(MEMBERS, "10.0.0.9"));
    }

    @Test
    void memberIdForNodeIpThrowsOnNullOrEmptyNodeIp() {
        assertThrows(IllegalStateException.class, () -> ClusterNode.memberIdForNodeIp(MEMBERS, null));
        assertThrows(IllegalStateException.class, () -> ClusterNode.memberIdForNodeIp(MEMBERS, ""));
    }

    @Test
    void parseRemoteOriginsAcceptsACommaListOfU64ChainIds() {
        assertEquals(java.util.Set.of(), ClusterNode.parseRemoteOrigins(null));
        assertEquals(java.util.Set.of(), ClusterNode.parseRemoteOrigins(""));
        assertEquals(java.util.Set.of(), ClusterNode.parseRemoteOrigins(" , "));
        assertEquals(
            java.util.Set.of(412_347L, 412_399L),
            ClusterNode.parseRemoteOrigins("412347, 412399,"));
        // u64 values above Long.MAX_VALUE parse as unsigned.
        assertEquals(
            java.util.Set.of(-1L),
            ClusterNode.parseRemoteOrigins("18446744073709551615"));
        // A typo is fatal, never a silently disabled peer.
        assertThrows(IllegalStateException.class, () -> ClusterNode.parseRemoteOrigins("412347,abc"));
    }
}
