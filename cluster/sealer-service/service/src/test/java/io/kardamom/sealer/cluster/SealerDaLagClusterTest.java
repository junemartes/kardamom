package io.kardamom.sealer.cluster;

import static io.kardamom.sealer.cluster.ClusterTestHarness.awaitCondition;
import static io.kardamom.sealer.cluster.IngressFrames.offerFrame;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.logbuffer.Header;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.cluster.TestCluster;
import io.aeron.test.cluster.TestNode;
import io.kardamom.sealer.CanonicalSealerState;
import io.kardamom.sealer.LagBudgets;
import io.kardamom.sealer.VoidLedger;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.agrona.DirectBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * The DA-lag guard on three real Raft members. The batcher posts nothing,
 * so the sealed head runs past the budget. The guard then refuses each new
 * transaction with the typed reject, every session reads a halted status,
 * and the boundaries keep sealing. The halt is replicated state: a new
 * leader after a leader kill keeps it. A posted cursor from the batcher
 * ends the halt, and the next transaction is ordered.
 */
@ExtendWith(InterruptingTestCallback.class)
class SealerDaLagClusterTest {

    private static final int MEMBER_COUNT = 3;
    private static final int DEDUP_CAPACITY = 8192;
    private static final long TICK_MS = 100L;
    private static final long BUDGET = 4L;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    /** The last status, the DA-lag rejects and the progress that a session reads. */
    static final class Guard implements EgressListener {
        long posted = -1;
        long sealed = -1;
        long budget = -1;
        boolean halted;
        long statuses;
        long rejects;
        long rejectSealed = -1;
        long rejectPosted = -1;
        long relayed;
        long maxBlock = Long.MIN_VALUE;

        @Override
        public void onMessage(
                final long clusterSessionId,
                final long timestamp,
                final DirectBuffer buffer,
                final int offset,
                final int length,
                final Header header) {
            final byte kind = buffer.getByte(offset);
            if (kind == SealerWire.EGRESS_KIND_STATUS) {
                // [kind][posted_head][sealed_head][budget][halted:u8]...
                posted = buffer.getLong(offset + 1, ByteOrder.LITTLE_ENDIAN);
                sealed = buffer.getLong(offset + 9, ByteOrder.LITTLE_ENDIAN);
                budget = buffer.getLong(offset + 17, ByteOrder.LITTLE_ENDIAN);
                halted = buffer.getByte(offset + 25) == 1;
                statuses++;
            } else if (kind == SealerWire.EGRESS_KIND_DA_LAG_REJECT) {
                // [kind][sender:20][nonce][sealed_head][posted_head][budget]
                rejects++;
                rejectSealed = buffer.getLong(offset + 29, ByteOrder.LITTLE_ENDIAN);
                rejectPosted = buffer.getLong(offset + 37, ByteOrder.LITTLE_ENDIAN);
            } else if (kind == SealerWire.EGRESS_KIND_RELAYED) {
                relayed++;
            } else if (kind == SealerWire.EGRESS_KIND_BOUNDARY) {
                maxBlock = Math.max(maxBlock, buffer.getLong(offset + 1, ByteOrder.LITTLE_ENDIAN));
            }
        }
    }

    private static byte[] sender() {
        final byte[] s = new byte[CanonicalSealerState.SENDER_LEN];
        Arrays.fill(s, (byte) 0xAA);
        return s;
    }

    private static SealerTestService member(final int memberId) {
        return new SealerTestService(new SealerClusteredService(
            DEDUP_CAPACITY, TICK_MS, memberId, Set.of(), VoidLedger.Config.DISABLED,
            CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS,
            CanonicalSealerState.DEFAULT_ORDERING_WINDOW, new LagBudgets(BUDGET, 0L)));
    }

    @Test
    @InterruptAfter(value = 60, unit = TimeUnit.SECONDS)
    void a_stalled_batcher_halts_new_transactions_across_a_leader_kill_and_its_cursor_resumes_them() {
        try (TestCluster cluster = ClusterTestHarness.start(systemTestWatcher,
                ClusterTestHarness.builder(MEMBER_COUNT, SealerDaLagClusterTest::member))) {
            cluster.awaitLeader();
            final Guard guard = new Guard();
            cluster.egressListener(guard);
            final AeronCluster client = cluster.connectClient();
            offerFrame(client, IngressFrames.subscribeFrame());

            // The batcher posts nothing: the sealed head passes the budget.
            awaitCondition(client, () -> guard.halted);
            assertEquals(0L, guard.posted, "nothing is posted");
            assertEquals(BUDGET, guard.budget);
            assertTrue(guard.sealed - guard.posted > BUDGET, "halted only past the budget");
            offerFrame(client, IngressFrames.recordFrame(1, sender(), 0));
            awaitCondition(client, () -> guard.rejects + guard.relayed > 0);
            assertEquals(1L, guard.rejects, "the guard refuses the transaction");
            assertEquals(0L, guard.rejectPosted);
            assertTrue(guard.rejectSealed > BUDGET, "the reject names the sealed head");
            final long haltedAt = guard.maxBlock;
            awaitCondition(client, () -> guard.maxBlock > haltedAt + 2);
            assertEquals(0L, guard.relayed, "the halt orders no transaction, and the boundaries go on");

            // A new leader keeps the halt: it is state of the replicated log.
            final TestNode oldLeader = cluster.findLeader();
            cluster.stopNode(oldLeader);
            cluster.awaitLeader(oldLeader.index());
            final AeronCluster again = cluster.reconnectClient();
            offerFrame(again, IngressFrames.subscribeFrame());
            final long statusesBefore = guard.statuses;
            awaitCondition(again, () -> guard.statuses > statusesBefore + 1);
            assertTrue(guard.halted, "the new leader still halts");
            offerFrame(again, IngressFrames.recordFrame(1, sender(), 0));
            awaitCondition(again, () -> guard.rejects + guard.relayed > 1);
            assertEquals(2L, guard.rejects, "the new leader refuses the transaction");
            assertEquals(0L, guard.relayed);

            // The batcher posts through the sealed head: the halt ends.
            final long sealed = guard.sealed;
            offerFrame(again, IngressFrames.postedCursorFrame(sealed));
            awaitCondition(again, () -> !guard.halted && guard.posted == sealed);
            offerFrame(again, IngressFrames.recordFrame(1, sender(), 0));
            awaitCondition(again, () -> guard.relayed == 1);
            assertEquals(2L, guard.rejects, "no reject after the resume");
        }
    }
}
