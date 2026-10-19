package io.kardamom.sealer;

import static io.kardamom.sealer.SealerStateFixtures.id;
import static io.kardamom.sealer.SealerStateFixtures.payload;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

/**
 * The decision rules: every state runs version 3 and writes snapshot
 * version 14. The snapshot tail keeps the activations that a snapshot of an
 * older release holds, so a member of that release restores the same rules.
 */
class DecisionRulesTest {

    /** The bytes of the tail in front of the held activations: the version, the sealed origin and the count. */
    private static final int TAIL_HEAD = Byte.BYTES + Long.BYTES + Byte.BYTES;

    /** The bytes of one held activation: its origin and its time. */
    private static final int HELD_LEN = 2 * Long.BYTES;

    /** A state at L1 origin {@code origin} with its first block sealed at 1 000 ms. */
    private static CanonicalSealerState sealedAt(final long origin) {
        final CanonicalSealerState state = new CanonicalSealerState(8);
        state.onOriginRecord(id(100 + (int) origin), origin, 1, payload("epoch"), 0L);
        state.onTick(1_000L);
        return state;
    }

    private static int snapshotVersion(final byte[] snapshot) {
        return ByteBuffer.wrap(snapshot).getInt(Integer.BYTES);
    }

    /** The offset of the tail in a snapshot that holds {@code held} activations. */
    private static int tailOf(final byte[] snapshot, final int held) {
        return snapshot.length - TAIL_HEAD - held * HELD_LEN;
    }

    /** {@code snapshot}, which holds no activation, with one held activation at {@code origin} and {@code ms}. */
    private static byte[] holding(final byte[] snapshot, final long origin, final long ms) {
        final ByteBuffer out = ByteBuffer.allocate(snapshot.length + HELD_LEN);
        out.put(snapshot).putLong(origin).putLong(ms);
        out.put(tailOf(snapshot, 0) + Byte.BYTES + Long.BYTES, (byte) 1);
        return out.array();
    }

    /** The bytes of version 13: version 14 with one held activation, without the sealed origin and the count. */
    private static byte[] versionThirteen(final byte[] fourteen) {
        final int tail = tailOf(fourteen, 1);
        final ByteBuffer out = ByteBuffer.allocate(fourteen.length - Long.BYTES - Byte.BYTES);
        out.put(fourteen, 0, tail + 1).put(fourteen, tail + TAIL_HEAD, HELD_LEN);
        out.putInt(Integer.BYTES, 13);
        return out.array();
    }

    @Test
    void every_state_runs_version_three_and_writes_snapshot_version_fourteen() {
        final CanonicalSealerState state = sealedAt(4);
        final byte[] snapshot = state.takeSnapshot();
        assertEquals(14, snapshotVersion(snapshot));
        assertEquals(DecisionRules.START_VIEW, state.decisionRules());
        final ByteBuffer tail = ByteBuffer.wrap(snapshot, tailOf(snapshot, 0), TAIL_HEAD);
        assertEquals(DecisionRules.VERSION, tail.get());
        assertEquals(4L, tail.getLong(), "the L1 origin of the last sealed block");
        assertEquals(0, tail.get());
    }

    /** A member of an older release restores the activation that it held, from a snapshot of either release. */
    @Test
    void a_snapshot_keeps_its_held_activations() {
        final byte[] snapshot = holding(sealedAt(4).takeSnapshot(), 4L, 9_000L);
        final CanonicalSealerState restored = CanonicalSealerState.load(snapshot, 8);
        assertEquals(new DecisionRules.View(3, 4L, 9_000L), restored.decisionRules());
        assertArrayEquals(snapshot, restored.takeSnapshot());
    }

    /** The two versions decide alike every frame that a client writes, so a tail of version 2 runs version 3. */
    @Test
    void a_tail_of_version_two_loads_as_version_three() {
        final byte[] snapshot = holding(sealedAt(4).takeSnapshot(), 4L, -1L);
        snapshot[tailOf(snapshot, 1)] = 2;
        final CanonicalSealerState restored = CanonicalSealerState.load(snapshot, 8);
        final byte[] written = restored.takeSnapshot();
        assertEquals(DecisionRules.VERSION, written[tailOf(written, 1)]);
    }

    /** A version-13 snapshot holds one activation, and a time of 0 reads as 1 ms. */
    @Test
    void a_version_thirteen_snapshot_restores_its_activation() {
        final byte[] fourteen = sealedAt(4).takeSnapshot();
        assertEquals(new DecisionRules.View(3, 4L, 9_000L),
            CanonicalSealerState.load(versionThirteen(holding(fourteen, 4L, 9_000L)), 8).decisionRules());
        assertEquals(new DecisionRules.View(3, 4L, 1L),
            CanonicalSealerState.load(versionThirteen(holding(fourteen, 4L, 0L)), 8).decisionRules());
    }

    @Test
    void a_version_twelve_snapshot_restores_version_three() {
        final CanonicalSealerState restored =
            CanonicalSealerState.load(sealedAt(4).takeSnapshotWithRetainedFile(), 8);
        assertEquals(DecisionRules.START_VIEW, restored.decisionRules());
    }

    @Test
    void a_snapshot_with_an_unknown_decision_version_or_too_many_activations_is_refused() {
        final byte[] snapshot = holding(sealedAt(4).takeSnapshot(), 4L, 0L);
        final int tail = tailOf(snapshot, 1);
        final byte[] badVersion = snapshot.clone();
        badVersion[tail] = 4;
        assertThrows(IllegalArgumentException.class, () -> CanonicalSealerState.load(badVersion, 8));
        final byte[] badCount = snapshot.clone();
        badCount[tail + Byte.BYTES + Long.BYTES] = DecisionRules.MAX_HELD + 1;
        assertThrows(IllegalArgumentException.class, () -> CanonicalSealerState.load(badCount, 8));
    }
}
