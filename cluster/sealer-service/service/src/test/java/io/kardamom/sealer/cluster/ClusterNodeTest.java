package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.driver.NameResolver;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
    void peerConsensusEndpointsListsEveryOtherMember() {
        assertEquals("192.168.56.52:40201,192.168.56.53:40201", ClusterNode.peerConsensusEndpoints(MEMBERS, 0));
        assertEquals("192.168.56.51:40201,192.168.56.53:40201", ClusterNode.peerConsensusEndpoints(MEMBERS, 1));
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

    @Test
    void parseVoidConfigMakesAVoterMaskAndRefusesATypo() {
        assertEquals(0L, ClusterNode.parseVoidConfig(null, 16).voterMask);
        assertEquals(0, ClusterNode.parseVoidConfig(" ", 16).capacity, "no voters keeps no window");
        assertEquals(0b1_0111L, ClusterNode.parseVoidConfig("0, 1,2,4,", 16).voterMask);
        assertEquals(16, ClusterNode.parseVoidConfig("0", 16).capacity);
        // A typo is fatal, never a silently smaller voter set.
        assertThrows(IllegalStateException.class, () -> ClusterNode.parseVoidConfig("0,x", 16));
        assertThrows(IllegalStateException.class, () -> ClusterNode.parseVoidConfig("64", 16));
    }

    @Test
    void fileSyncLevelDefaultsToZeroAndRefusesAValueOutsideZeroToTwo() {
        final String key = "kardamom.cluster.fileSyncLevel";
        final String before = System.getProperty(key);
        try {
            System.clearProperty(key);
            assertEquals(0, ClusterNode.fileSyncLevel());
            System.setProperty(key, "2");
            assertEquals(2, ClusterNode.fileSyncLevel());
            System.setProperty(key, "3");
            assertThrows(IllegalArgumentException.class, ClusterNode::fileSyncLevel);
        } finally {
            if (before == null) {
                System.clearProperty(key);
            } else {
                System.setProperty(key, before);
            }
        }
    }

    @Test
    void tornLastFragmentIsRecognisedThroughTheCauseChain() {
        final RuntimeException torn = new RuntimeException("launch failed",
            new io.aeron.archive.client.ArchiveException(
                "ERROR - Found potentially incomplete last fragment straddling page boundary in file: /x/0-0.rec"
                + "\nRun `ArchiveTool verify` for corrective action!"));
        assertTrue(ClusterNode.isTornLastFragment(torn));
        assertFalse(ClusterNode.isTornLastFragment(new IllegalStateException("active Mark file detected")));
        assertFalse(ClusterNode.isTornLastFragment(new RuntimeException((String) null)));
    }

    @Test
    void aRecordLagBudgetAboveZeroIsRefusedWhileTheSnapshotDropsTheCursors() {
        assertEquals(0L, ClusterNode.requireRecordLagBudgetAllowed(0L, false), "0 is always allowed");
        final IllegalStateException e = assertThrows(
            IllegalStateException.class, () -> ClusterNode.requireRecordLagBudgetAllowed(16_384L, false));
        assertTrue(e.getMessage().contains("kardamom.cluster.recordLagBudget"), e.getMessage());
        assertTrue(e.getMessage().contains("version 10"), e.getMessage());
        assertTrue(e.getMessage().contains("version 11 lifts this check"), e.getMessage());
        assertEquals(16_384L, ClusterNode.requireRecordLagBudgetAllowed(16_384L, true),
            "a writer that keeps the cursors allows the budget");
    }

    @Test
    void thisReleaseDropsTheCursorsSoOnlyBudgetZeroStarts() {
        assertFalse(io.kardamom.sealer.CanonicalSealerState.snapshotKeepsRecordedCursors());
        assertThrows(IllegalStateException.class, () -> ClusterNode.requireRecordLagBudgetAllowed(
            1L, io.kardamom.sealer.CanonicalSealerState.snapshotKeepsRecordedCursors()));
    }

    @Test
    void theDecisionVersionMustBeTheVersionOfTheImage() {
        final int image = io.kardamom.sealer.CanonicalSealerState.DECISION_VERSION;
        assertEquals(image, ClusterNode.requireDecisionVersion(null), "an absent setting is the image version");
        assertEquals(image, ClusterNode.requireDecisionVersion(Integer.toString(image)));
        assertThrows(IllegalStateException.class,
            () -> ClusterNode.requireDecisionVersion(Integer.toString(image - 1)));
        assertThrows(NumberFormatException.class, () -> ClusterNode.requireDecisionVersion(""));
    }

    @Test
    void theSealerJobPassesTheDecisionVersionOfTheCode() throws IOException {
        final String job = Files.readString(Path.of(System.getProperty("kardamom.sealerJob")));
        final Matcher setting =
            Pattern.compile("-D" + Pattern.quote(ClusterNode.DECISION_VERSION_SETTING) + "=(\\d+)").matcher(job);
        assertTrue(setting.find(), "the sealer job passes the decision version");
        assertEquals(io.kardamom.sealer.CanonicalSealerState.DECISION_VERSION, Integer.parseInt(setting.group(1)));
        assertFalse(setting.find(), "the sealer job passes the decision version once");
    }

    /** Member names under the reserved .invalid domain: no lookup can resolve them. */
    private static final String NAMED_MEMBERS =
        "0,sealer-0.invalid:40200,sealer-0.invalid:40201,sealer-0.invalid:40202,sealer-0.invalid:40203,"
            + "sealer-0.invalid:40204|1,sealer-1.invalid:40200,sealer-1.invalid:40201,sealer-1.invalid:40202,"
            + "sealer-1.invalid:40203,sealer-1.invalid:40204";

    private static MemberContexts namedContexts() {
        return new MemberContexts("aeron", "cluster", "archive", ClusterNode.memberEndpoints(NAMED_MEMBERS, 0));
    }

    @Test
    void theNameResolverMapsTheOwnNameToTheNodeAddress() throws Exception {
        final NameResolver resolver = namedContexts().withPeerNames(0, "192.168.56.17").driver().nameResolver();
        assertEquals(InetAddress.getByName("192.168.56.17"),
            resolver.resolve("sealer-0.invalid", "endpoint", false));
        assertNull(resolver.resolve("sealer-1.invalid", "endpoint", false), "a peer name still needs a lookup");
    }

    @Test
    void theNameResolverWithNoNodeAddressKnowsNoName() {
        final NameResolver resolver = namedContexts().withPeerNames(0, null).driver().nameResolver();
        assertNull(resolver.resolve("sealer-0.invalid", "endpoint", false));
    }
}
