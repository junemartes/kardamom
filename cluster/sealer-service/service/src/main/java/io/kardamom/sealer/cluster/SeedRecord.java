package io.kardamom.sealer.cluster;

import io.aeron.Publication;
import io.aeron.cluster.service.Cluster;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * The cluster action of a seeded member: it offers the seed record to the
 * log. Once the record confirms the seed, the service asks for a snapshot.
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
}
