package io.kardamom.sealer.cluster;

import io.aeron.Publication;
import io.aeron.cluster.ClusterControl;
import io.aeron.cluster.service.Cluster;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * The two cluster actions of a seeded member: it offers the seed record to
 * the log, and, once the record confirms the seed, it asks for a snapshot.
 */
final class SeedRecord {
    private final Cluster cluster;
    private final int memberId;

    SeedRecord(final Cluster cluster, final int memberId) {
        this.cluster = cluster;
        this.memberId = memberId;
    }

    /**
     * Offer a {@link SealerWire#KIND_SEED_EPOCH} record with {@code digest}.
     * Every member calls this at the same log position, as Aeron requires
     * of a service message: the leader's consensus module appends its copy,
     * and the followers drop theirs when that copy commits. A later term
     * offers again until a record commits; a second record confirms
     * nothing new.
     */
    void offer(final byte[] digest) {
        final UnsafeBuffer frame = new UnsafeBuffer(new byte[SealerWire.SEED_EPOCH_LEN]);
        frame.putByte(SealerWire.KIND_OFFSET, SealerWire.KIND_SEED_EPOCH);
        frame.putBytes(SealerWire.SEED_DIGEST_OFFSET, digest);
        long result;
        while ((result = cluster.offer(frame, 0, frame.capacity())) == Publication.BACK_PRESSURED
                || result == Publication.ADMIN_ACTION) {
            cluster.idleStrategy().idle();
        }
        if (result < 0) {
            System.out.println("sealer SEED-EPOCH offer FAILED memberId=" + memberId + " result=" + result
                + " — the next leadership term offers it again");
        }
    }

    /**
     * Ask this member's consensus module for a snapshot, when this member
     * leads. The control toggle takes the request only on the leader, and
     * the snapshot action then goes through the log to every member. A
     * refused request leaves the periodic snapshot to take one.
     */
    void requestSnapshot() {
        if (cluster.role() != Cluster.Role.LEADER) {
            return;
        }
        final AtomicCounter toggle = ClusterControl.findControlToggle(
            cluster.aeron().countersReader(), cluster.context().clusterId());
        final boolean requested = toggle != null && ClusterControl.ToggleState.SNAPSHOT.toggle(toggle);
        System.out.println("sealer seed SNAPSHOT memberId=" + memberId + " requested=" + requested);
    }
}
