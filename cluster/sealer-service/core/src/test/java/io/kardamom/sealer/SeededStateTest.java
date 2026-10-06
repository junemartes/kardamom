package io.kardamom.sealer;

import static io.kardamom.sealer.SealerStateFixtures.NO_DEADLINE;
import static io.kardamom.sealer.SealerStateFixtures.id;
import static io.kardamom.sealer.SealerStateFixtures.payload;
import static io.kardamom.sealer.SealerStateFixtures.sender;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.kardamom.sealer.CanonicalSealerState.RecordOutcome;
import io.kardamom.sealer.CanonicalSealerState.SeedStatus;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * A state seeded at the head {@code H} of a state rebuilt from L1 opens
 * block {@code H + 1} at index {@code E_H}, expects each seeded sender's
 * next nonce, keeps its seed through a snapshot, and confirms the seed
 * only against a record with the same digest.
 */
class SeededStateTest {

    private static final long H = 100L;
    private static final long E_H = 500L;
    private static final long HEAD_TIME = 1_700_000_000_000L;
    private static final long ORIGIN = 77L;
    private static final long BUDGET = 3L;

    static SealerSeed seed(SealerSeed.Sender... senders) {
        final byte[] digest = new byte[SealerSeed.HASH_LEN];
        Arrays.fill(digest, (byte) 0x5E);
        return new SealerSeed(
            new SealerSeed.Head(412_346L, H, E_H, HEAD_TIME, ORIGIN),
            new byte[SealerSeed.HASH_LEN], List.of(senders), digest);
    }

    private static CanonicalSealerState seeded(int capacity, SealerSeed seed) {
        return CanonicalSealerState.seeded(
            seed, capacity, VoidLedger.Config.DISABLED,
            CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS,
            CanonicalSealerState.DEFAULT_ORDERING_WINDOW, BUDGET);
    }

    private static CanonicalSealerState reload(CanonicalSealerState state) {
        return CanonicalSealerState.load(
            ByteBuffer.wrap(state.takeSnapshot()), 64, Set.of(), VoidLedger.Config.DISABLED,
            CanonicalSealerState.DEFAULT_INCLUSION_HORIZON_BLOCKS,
            CanonicalSealerState.DEFAULT_ORDERING_WINDOW, BUDGET);
    }

    @Test
    void the_first_block_after_the_seed_is_h_plus_one_at_the_end_of_h() {
        final CanonicalSealerState state = seeded(64, seed());
        assertEquals(H + 1, state.blockNumber());
        assertEquals(E_H, state.canonicalCount());
        assertEquals(H, state.sealedHead());
        assertEquals(H, state.postedHead());
        assertFalse(state.daLagHalted(), "every block up to the seed is posted");
        assertEquals(ORIGIN, state.l1Origin());
        assertEquals(SeedStatus.PENDING, state.seedStatus());
        assertTrue(state.remoteOriginAllowlist().isEmpty(), "a seed carries no peer anchor");

        final RecordOutcome first = state.onRecord(id(1), sender(1), 0, NO_DEADLINE, payload("a"));
        assertEquals(E_H, first.relayed.orElseThrow().index);
        // A clock behind the head's timestamp still stamps a later one.
        assertEquals(new Boundary(H + 1, E_H + 1, HEAD_TIME + 1, ORIGIN), state.onTick(0L));
        assertEquals(new Boundary(H + 2, E_H + 1, HEAD_TIME + 250, ORIGIN), state.onTick(HEAD_TIME + 250));
    }

    @Test
    void an_empty_first_block_ends_at_e_h() {
        assertEquals(
            new Boundary(H + 1, E_H, HEAD_TIME + 250, ORIGIN), seeded(64, seed()).onTick(HEAD_TIME + 250));
    }

    @Test
    void a_seeded_sender_must_send_its_next_nonce() {
        final CanonicalSealerState state = seeded(64, seed(new SealerSeed.Sender(sender(1), 5L)));
        final RecordOutcome stale = state.onRecord(id(1), sender(1), 4, NO_DEADLINE, payload("s"));
        assertEquals(RecordOutcome.Kind.CONTIGUITY_REJECT, stale.kind);
        assertEquals(5L, stale.expectedNonce);
        assertEquals(RecordOutcome.Kind.RELAYED,
            state.onRecord(id(2), sender(1), 5, NO_DEADLINE, payload("n")).kind);
        assertEquals(RecordOutcome.Kind.RELAYED,
            state.onRecord(id(3), sender(2), 9, NO_DEADLINE, payload("u")).kind,
            "a sender the seed does not name starts at any nonce");
    }

    @Test
    void the_guard_keeps_the_most_recent_seeded_senders_up_to_its_capacity() {
        final CanonicalSealerState state = seeded(2, seed(
            new SealerSeed.Sender(sender(1), 1L),
            new SealerSeed.Sender(sender(2), 2L),
            new SealerSeed.Sender(sender(3), 3L)));
        assertEquals(2, state.trackedSenders());
        assertEquals(Optional.empty(), state.expectedNonceOf(sender(1)));
        assertEquals(Optional.of(2L), state.expectedNonceOf(sender(2)));
        assertEquals(Optional.of(3L), state.expectedNonceOf(sender(3)));
    }

    @Test
    void a_snapshot_keeps_the_seed_and_its_status() {
        final SealerSeed seed = seed(new SealerSeed.Sender(sender(1), 5L));
        final CanonicalSealerState restored = reload(seeded(64, seed));
        assertEquals(H + 1, restored.blockNumber());
        assertEquals(E_H, restored.canonicalCount());
        assertEquals(H, restored.postedHead());
        assertEquals(ORIGIN, restored.l1Origin());
        assertEquals(Optional.of(5L), restored.expectedNonceOf(sender(1)));
        assertEquals(SeedStatus.PENDING, restored.seedStatus());
        assertArrayEquals(seed.digest(), restored.seedDigest());
        assertEquals(new Boundary(H + 1, E_H, HEAD_TIME + 1, ORIGIN), restored.onTick(0L));

        assertTrue(restored.onSeedEpoch(seed.digest()));
        assertEquals(SeedStatus.CONFIRMED, reload(restored).seedStatus());
    }

    @Test
    void the_seed_confirms_once_and_only_with_its_digest() {
        final SealerSeed seed = seed();
        final CanonicalSealerState state = seeded(64, seed);
        final byte[] other = seed.digest().clone();
        other[0] ^= 1;
        assertThrows(IllegalStateException.class, () -> state.onSeedEpoch(other));
        assertEquals(SeedStatus.PENDING, state.seedStatus());
        assertTrue(state.onSeedEpoch(seed.digest()));
        assertFalse(state.onSeedEpoch(seed.digest()), "a second record confirms nothing new");
        assertEquals(SeedStatus.CONFIRMED, state.seedStatus());
    }

    @Test
    void a_state_started_at_genesis_refuses_a_seed_record() {
        final CanonicalSealerState genesis = new CanonicalSealerState(64);
        assertEquals(SeedStatus.GENESIS, genesis.seedStatus());
        final IllegalStateException e =
            assertThrows(IllegalStateException.class, () -> genesis.onSeedEpoch(seed().digest()));
        assertTrue(e.getMessage().contains("started at genesis"), e.getMessage());
    }

    @Test
    void a_version_9_snapshot_restores_a_state_started_at_genesis() {
        final byte[] current = seeded(64, seed()).takeSnapshot();
        final byte[] v9 = Arrays.copyOf(current, current.length - 1 - SealerSeed.HASH_LEN);
        ByteBuffer.wrap(v9).putInt(4, 9);
        final CanonicalSealerState restored = CanonicalSealerState.load(v9, 64);
        assertEquals(SeedStatus.GENESIS, restored.seedStatus());
        assertEquals(H, restored.postedHead());
    }
}
