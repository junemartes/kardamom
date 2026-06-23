package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Pure-function tests for the {@link ClusterNode} launcher helpers. No Aeron / no
 * cluster boot, so these are fast. They pin the two parsing helpers that turn the
 * {@code -Dkardamom.cluster.members} string into this member's endpoints / id.
 */
final class ClusterNodeTest {
    /** Realistic 3-member topology (mirrors deploy/cluster/nomad/cluster.nomad.hcl). */
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
}
