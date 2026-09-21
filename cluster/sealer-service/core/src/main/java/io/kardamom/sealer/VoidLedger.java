package io.kardamom.sealer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The part of the canonical state that decides a void: the removal of one
 * ordered transaction reference that no consumer can execute.
 *
 * <p>The sealer orders a reference before the archives make the transaction
 * data durable, so a failure can leave an entry with no data. A consumer that
 * waits at such an entry sends a vote. When every configured voter has voted
 * for the same entry, the state appends a void record and every consumer
 * drops the entry at the same canonical index. One missing vote blocks the
 * void: a voter that has the data never votes, so an entry that one replica
 * can execute is never removed.</p>
 *
 * <p>The ledger has two parts.</p>
 * <ul>
 *   <li><b>The window</b> holds {@code (id, sender, nonce)} for the newest
 *       {@code capacity} canonical indices. A vote names only an index and a
 *       hash, because the voter has no envelope. The state needs the sender
 *       and the nonce to set the sender's expected nonce back. The window is
 *       a set of fixed arrays, so the order path pays one copy of 52 bytes
 *       for each record and no allocation.</li>
 *   <li><b>The votes</b> hold one voter mask for each index that has a vote.
 *       Only the failure path writes them.</li>
 * </ul>
 *
 * <p>Every change comes from the replicated log, and the snapshot holds the
 * two parts, so every member has the same ledger.</p>
 */
public final class VoidLedger {
    /**
     * Record type of a void record in a relayed payload:
     * {@code [tx_hash:32][record_type:4][index:u64 LE]}. Matches Rust
     * {@code RT_VOID}.
     */
    public static final byte RT_VOID = 4;

    /** The largest voter id plus one: a voter set is one 64-bit mask. */
    public static final int MAX_VOTERS = 64;

    /**
     * The most indices that can have open votes at one time. A lost entry
     * stops every consumer, so more than one open vote is already unusual.
     * The limit keeps the vote map, and its snapshot, small when a bad
     * client votes for every index in the window.
     */
    static final int MAX_OPEN_VOTES = 1024;

    private static final int ID_LEN = CanonicalSealerState.CANONICAL_ID_LEN;
    private static final int SENDER_LEN = CanonicalSealerState.SENDER_LEN;

    /** Snapshot bytes for one window entry: index, id, sender, nonce. */
    private static final int ENTRY_LEN = Long.BYTES + ID_LEN + SENDER_LEN + Long.BYTES;
    /** Snapshot bytes for one open vote: index, voter mask. */
    private static final int VOTE_LEN = Long.BYTES + Long.BYTES;

    /**
     * The configuration of a ledger. Every member must use the same values,
     * as for the dedup capacity, because they decide accept or refuse in the
     * replicated state machine. They are not in the snapshot.
     */
    public static final class Config {
        /** No voters: the ledger refuses every vote and keeps no window. */
        public static final Config DISABLED = new Config(0, 0L);

        /** How many of the newest canonical indices the window covers. */
        public final int capacity;
        /** Bit {@code n} is set when voter id {@code n} is a configured voter. */
        public final long voterMask;

        public Config(int capacity, long voterMask) {
            if (voterMask != 0L && capacity <= 0) {
                throw new IllegalArgumentException("void window capacity must be > 0, got " + capacity);
            }
            this.capacity = voterMask == 0L ? 0 : capacity;
            this.voterMask = voterMask;
        }
    }

    /** What the state needs to remove one entry. */
    public static final class Entry {
        public final long index;
        public final byte[] id;
        public final byte[] sender;
        public final long nonce;

        Entry(long index, byte[] id, byte[] sender, long nonce) {
            this.index = index;
            this.id = id;
            this.sender = sender;
            this.nonce = nonce;
        }
    }

    /** The result of one vote. */
    public enum Vote {
        /** The index is not a transaction reference in the window, or the hash is wrong. */
        REFUSED,
        /** This voter already voted for this index. Consumers send a vote again at an interval. */
        REPEATED,
        /** A new vote, and one configured voter or more has not voted. */
        COUNTED,
        /** Every configured voter has voted. The caller appends the void record. */
        DECIDED,
    }

    /** One vote result, with the removed entry when the vote decided the void. */
    public static final class Tally {
        public final Vote vote;
        public final Optional<Entry> entry;

        private Tally(Vote vote, Optional<Entry> entry) {
            this.vote = vote;
            this.entry = entry;
        }

        private static Tally of(Vote vote) {
            return new Tally(vote, Optional.empty());
        }
    }

    private final Config config;
    /** The canonical index that each window slot holds, or -1. Slot = index mod capacity. */
    private final long[] indexAt;
    private final byte[] ids;
    private final byte[] senders;
    private final long[] nonces;
    /** Open votes by index, in the order of their first vote. */
    private final LinkedHashMap<Long, Long> votes = new LinkedHashMap<>();

    VoidLedger(Config config) {
        this.config = config;
        this.indexAt = new long[config.capacity];
        this.ids = new byte[config.capacity * ID_LEN];
        this.senders = new byte[config.capacity * SENDER_LEN];
        this.nonces = new long[config.capacity];
        Arrays.fill(indexAt, -1L);
    }

    /** Record one ordered transaction reference. This is the only method on the order path. */
    void onReference(long index, byte[] id32, byte[] sender20, long nonce) {
        if (config.capacity == 0) {
            return;
        }
        int slot = slot(index);
        indexAt[slot] = index;
        System.arraycopy(id32, 0, ids, slot * ID_LEN, ID_LEN);
        System.arraycopy(sender20, 0, senders, slot * SENDER_LEN, SENDER_LEN);
        nonces[slot] = nonce;
    }

    /**
     * Count one vote. On {@link Vote#DECIDED} the entry leaves the window, so
     * a second void of the same index is refused, and the tally gives the
     * entry to the caller.
     *
     * @param canonicalCount the count of ordered records, which sets the
     *        oldest index that the window still covers
     */
    Tally onVote(int voterId, long index, byte[] hash32, long canonicalCount) {
        dropVotesBelow(canonicalCount - config.capacity);
        if (!isVoter(voterId) || !holds(index, hash32)) {
            return Tally.of(Vote.REFUSED);
        }
        Long open = votes.get(index);
        if (open == null && votes.size() >= MAX_OPEN_VOTES) {
            return Tally.of(Vote.REFUSED);
        }
        long before = open == null ? 0L : open;
        long after = before | (1L << voterId);
        if (after == before) {
            return Tally.of(Vote.REPEATED);
        }
        if (after != config.voterMask) {
            votes.put(index, after);
            return Tally.of(Vote.COUNTED);
        }
        votes.remove(index);
        return new Tally(Vote.DECIDED, Optional.of(take(index)));
    }

    /** How many configured voters have voted for {@code index}. */
    public int votesFor(long index) {
        return Long.bitCount(votes.getOrDefault(index, 0L));
    }

    /** How many voters the configuration names. */
    public int voterCount() {
        return Long.bitCount(config.voterMask);
    }

    /** The relayed payload of the void record for one entry. */
    static byte[] payload(Entry entry) {
        ByteBuffer buf = ByteBuffer.allocate(ID_LEN + Byte.BYTES + Long.BYTES)
            .order(java.nio.ByteOrder.LITTLE_ENDIAN);
        buf.put(entry.id);
        buf.put(RT_VOID);
        buf.putLong(entry.index);
        return buf.array();
    }

    private boolean isVoter(int voterId) {
        return voterId >= 0 && voterId < MAX_VOTERS && (config.voterMask & (1L << voterId)) != 0L;
    }

    private boolean holds(long index, byte[] hash32) {
        if (config.capacity == 0 || index < 0) {
            return false;
        }
        int slot = slot(index);
        int at = slot * ID_LEN;
        return indexAt[slot] == index
            && Arrays.equals(ids, at, at + ID_LEN,
                hash32, 0, ID_LEN);
    }

    private Entry take(long index) {
        int slot = slot(index);
        indexAt[slot] = -1L;
        return entryAt(slot, index);
    }

    private Entry entryAt(int slot, long index) {
        int idAt = slot * ID_LEN;
        int senderAt = slot * SENDER_LEN;
        return new Entry(
            index,
            Arrays.copyOfRange(ids, idAt, idAt + ID_LEN),
            Arrays.copyOfRange(senders, senderAt, senderAt + SENDER_LEN),
            nonces[slot]);
    }

    /** A vote for an index that left the window can never be decided. */
    private void dropVotesBelow(long oldest) {
        Iterator<Map.Entry<Long, Long>> it = votes.entrySet().iterator();
        while (it.hasNext()) {
            if (it.next().getKey() < oldest) {
                it.remove();
            }
        }
    }

    private int slot(long index) {
        return (int) (index % config.capacity);
    }

    // ── snapshot ────────────────────────────────────────────────────────────

    /** The window indices in the snapshot: those that the window still holds, oldest first. */
    private long[] heldIndices(long canonicalCount) {
        long oldest = Math.max(0L, canonicalCount - config.capacity);
        return java.util.stream.LongStream.range(oldest, canonicalCount)
            .filter(i -> indexAt[slot(i)] == i)
            .toArray();
    }

    /** The snapshot length in bytes: {@code entryCount(4) | entries | voteCount(4) | votes}. */
    int snapshotLen(long canonicalCount) {
        int held = config.capacity == 0 ? 0 : heldIndices(canonicalCount).length;
        return Integer.BYTES + held * ENTRY_LEN + Integer.BYTES + votes.size() * VOTE_LEN;
    }

    /** Write the window, oldest index first, then the open votes in insertion order. */
    void writeTo(ByteBuffer buf, long canonicalCount) {
        long[] held = config.capacity == 0 ? new long[0] : heldIndices(canonicalCount);
        buf.putInt(held.length);
        for (long index : held) {
            Entry e = entryAt(slot(index), index);
            buf.putLong(e.index);
            buf.put(e.id);
            buf.put(e.sender);
            buf.putLong(e.nonce);
        }
        buf.putInt(votes.size());
        for (Map.Entry<Long, Long> v : votes.entrySet()) {
            buf.putLong(v.getKey());
            buf.putLong(v.getValue());
        }
    }

    /**
     * Read what {@link #writeTo} wrote. A member with a smaller window than
     * the writer keeps only the newest entries: an older entry lands in a
     * slot that a newer entry then takes.
     */
    static VoidLedger readFrom(ByteBuffer buf, Config config) {
        VoidLedger ledger = new VoidLedger(config);
        int entryCount = buf.getInt();
        if (entryCount < 0 || (long) entryCount * ENTRY_LEN > buf.remaining()) {
            throw new IllegalArgumentException(
                "truncated snapshot: void entryCount " + entryCount + ", "
                    + buf.remaining() + " bytes remaining");
        }
        byte[] id = new byte[ID_LEN];
        byte[] sender = new byte[SENDER_LEN];
        for (int i = 0; i < entryCount; i++) {
            long index = buf.getLong();
            buf.get(id);
            buf.get(sender);
            ledger.onReference(index, id, sender, buf.getLong());
        }
        int voteCount = buf.getInt();
        if (voteCount < 0 || (long) voteCount * VOTE_LEN > buf.remaining()) {
            throw new IllegalArgumentException(
                "truncated snapshot: void voteCount " + voteCount + ", "
                    + buf.remaining() + " bytes remaining");
        }
        for (int i = 0; i < voteCount; i++) {
            long index = buf.getLong();
            ledger.votes.put(index, buf.getLong());
        }
        return ledger;
    }
}
