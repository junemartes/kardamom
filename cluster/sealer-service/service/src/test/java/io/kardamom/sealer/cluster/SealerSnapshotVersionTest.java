package io.kardamom.sealer.cluster;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.cluster.ClusterStubs.StubCluster;
import org.junit.jupiter.api.Test;

/**
 * The snapshot version a member reports: 0 before any snapshot, the
 * restored version after a restore, and the write version after a take.
 * The admin endpoint carries it, so a deploy can refuse a release that
 * cannot read the snapshot the member holds.
 */
class SealerSnapshotVersionTest {

    @Test
    void aFreshMemberHoldsNoSnapshot() {
        final SealerClusteredService service = new SealerClusteredService(64, 250, 0);
        service.onStart(new StubCluster(), null);
        assertEquals(0, service.latestSnapshotVersion());
    }

    @Test
    void aRestoreReportsTheRestoredVersion() {
        final SealerClusteredService writer = new SealerClusteredService(64, 250, 0);
        writer.onStart(new StubCluster(), null);
        final SealerClusteredService reader = new SealerClusteredService(64, 250, 1);
        reader.onStart(new StubCluster(), null);
        reader.restore(writer.snapshot());
        assertEquals(CanonicalSealerState.snapshotWriteVersion(), reader.latestSnapshotVersion());
    }
}
