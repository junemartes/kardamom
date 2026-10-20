package io.kardamom.sealer;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * The replay windows of decision version 4: one sliding window for each
 * ingress image of {@code tx_data}. A transaction reference names its image
 * by the lane and the Aeron session id, and its message by the stream
 * position. Racing sequencers read the same image, so their copies of one
 * message carry the same three values.
 *
 * <p>A window holds the highest unit seen, and one bit for each of the
 * {@code width} units below it. A unit is a position divided by the Aeron
 * frame alignment, so each message has its own unit. The rules, in order:</p>
 * <ul>
 *   <li>A unit {@code width} or more below the highest is too old
 *       ({@link #EXPIRED}).</li>
 *   <li>A unit whose bit is set is a copy ({@link #DUPLICATE}).</li>
 *   <li>Otherwise the bit is set and the reference is new
 *       ({@link #FRESH}).</li>
 * </ul>
 *
 * <p>The state is a fixed set of slots in arrays, sized at construction. A
 * slot gets its bitmap when an image first takes it, and keeps it. A
 * reference to a known image allocates nothing. A new image takes a free
 * slot, else the slot whose image has been silent longest. "Silent" counts
 * in the canonical count of the state, so every member picks the same
 * slot.</p>
 *
 * <p>{@link #check} decides a reference and remembers it. {@link #mark}
 * then takes it into its window. A caller that refuses the reference for
 * another reason does not mark it, so a later copy is fresh.</p>
 *
 * <p>The slot count and the width are replicated configuration: every
 * member must use the same values, and the snapshot carries them.</p>
 */
public final class SessionWindows {

    /** The reference is new. The window now holds it. */
    public static final int FRESH = 0;
    /** The window already holds the reference. */
    public static final int DUPLICATE = 1;
    /** The reference is too far below the highest one of its image. */
    public static final int EXPIRED = 2;

    /** Aeron aligns each frame to 32 bytes, so a message spans at least one unit. */
    static final int UNIT_SHIFT = 5;

    /** The default slot count: lanes times ingress publications, with room for restarts. */
    public static final int DEFAULT_SLOTS = 256;

    /** The default width in units: 8 MiB of one image. */
    public static final int DEFAULT_WIDTH = 1 << 18;

    private static final long EMPTY = -1L;
    private static final int WORD_SHIFT = 6;

    private final int slots;
    private final int width;
    private final int wordsPerSlot;
    private final long[] keys;
    private final long[] highest;
    private final long[] lastSeen;
    private final long[][] bits;
    private final int[] table;
    private final int tableMask;
    private int used;
    /** The slot and the unit of the last {@link #check}. */
    private int checkedSlot;
    private long checkedUnit;

    /**
     * @param slots the number of images held at once, a power of two
     * @param width the window width in units, a power of two of at least 64
     */
    public SessionWindows(final int slots, final int width) {
        if (Integer.bitCount(slots) != 1 || Integer.bitCount(width) != 1 || width < 64) {
            throw new IllegalArgumentException(
                "slots and width must be powers of two, width at least 64: " + slots + ", " + width);
        }
        this.slots = slots;
        this.width = width;
        this.wordsPerSlot = width >>> WORD_SHIFT;
        this.keys = new long[slots];
        this.highest = new long[slots];
        this.lastSeen = new long[slots];
        this.bits = new long[slots][];
        this.table = new int[slots * 2];
        this.tableMask = slots * 2 - 1;
        Arrays.fill(keys, EMPTY);
        Arrays.fill(table, -1);
    }

    public int slots() {
        return slots;
    }

    public int width() {
        return width;
    }

    /** The number of images that hold a slot. */
    public int used() {
        return used;
    }

    /**
     * Decide one reference, and remember it for {@link #mark}.
     *
     * @param lane     the lane of the image
     * @param session  the Aeron session id of the image
     * @param position the stream position of the message, below 2^62
     * @param now      the canonical count of the state, for the silence order
     * @return {@link #FRESH}, {@link #DUPLICATE} or {@link #EXPIRED}
     */
    public int check(final int lane, final int session, final long position, final long now) {
        checkedSlot = slotOf(keyOf(lane, session));
        checkedUnit = position >>> UNIT_SHIFT;
        lastSeen[checkedSlot] = now;
        if (highest[checkedSlot] - checkedUnit >= width) {
            return EXPIRED;
        }
        final boolean seen = checkedUnit <= highest[checkedSlot]
            && (bits[checkedSlot][wordOf(checkedUnit)] & (1L << checkedUnit)) != 0;
        return seen ? DUPLICATE : FRESH;
    }

    /** Take the reference of the last {@link #check}, which was {@link #FRESH}, into its window. */
    public void mark() {
        final long[] window = bits[checkedSlot];
        final long top = highest[checkedSlot];
        if (checkedUnit > top) {
            clearAbove(window, top, checkedUnit);
            highest[checkedSlot] = checkedUnit;
        }
        window[wordOf(checkedUnit)] |= 1L << checkedUnit;
    }

    /** {@link #check}, then {@link #mark} when the reference is fresh. */
    public int offer(final int lane, final int session, final long position, final long now) {
        final int seen = check(lane, session, position, now);
        if (seen == FRESH) {
            mark();
        }
        return seen;
    }

    private int wordOf(final long unit) {
        return (int) ((unit & (width - 1)) >>> WORD_SHIFT);
    }

    /** Clear the bits of the units from {@code top + 1} through {@code unit}: they enter the window. */
    private void clearAbove(final long[] window, final long top, final long unit) {
        if (unit - top >= width) {
            Arrays.fill(window, 0L);
            return;
        }
        final long first = top + 1;
        final long firstWord = first >>> WORD_SHIFT;
        final long lastWord = unit >>> WORD_SHIFT;
        if (firstWord == lastWord) {
            clearRange(window, firstWord, -1L << first & -1L >>> (63 - (unit & 63)));
            return;
        }
        clearRange(window, firstWord, -1L << first);
        clearWords(window, firstWord + 1, lastWord);
        clearRange(window, lastWord, -1L >>> (63 - (unit & 63)));
    }

    /** Clear the masked bits of the ring word that holds window word {@code w}. */
    private void clearRange(final long[] window, final long w, final long mask) {
        window[(int) (w & (wordsPerSlot - 1))] &= ~mask;
    }

    /** Clear the window words from {@code from} up to {@code to}, not inclusive, around the ring. */
    private void clearWords(final long[] window, final long from, final long to) {
        final int start = (int) (from & (wordsPerSlot - 1));
        final int end = start + (int) (to - from);
        if (end <= wordsPerSlot) {
            Arrays.fill(window, start, end, 0L);
            return;
        }
        Arrays.fill(window, start, wordsPerSlot, 0L);
        Arrays.fill(window, 0, end - wordsPerSlot, 0L);
    }

    /** The slot of {@code key}: its own, a free one, or the one silent longest. */
    private int slotOf(final long key) {
        final int found = find(key);
        if (found >= 0) {
            return found;
        }
        final int slot = used < slots ? used++ : evictSilent();
        keys[slot] = key;
        highest[slot] = -1L;
        clearSlot(slot);
        insert(key, slot);
        return slot;
    }

    /** Give the slot an empty bitmap: a new array on its first use, the old one cleared after. */
    private void clearSlot(final int slot) {
        if (bits[slot] == null) {
            bits[slot] = new long[wordsPerSlot];
            return;
        }
        Arrays.fill(bits[slot], 0L);
    }

    /** The slot of {@code key}, or -1. The probe stops at the key or at the first empty entry. */
    private int find(final long key) {
        int i = hash(key);
        while (table[i] >= 0 && keys[table[i]] != key) {
            i = next(i);
        }
        return table[i];
    }

    private void insert(final long key, final int slot) {
        int i = hash(key);
        while (table[i] >= 0) {
            i = next(i);
        }
        table[i] = slot;
    }

    /** Free the slot whose image has been silent longest, and return it. The lowest slot wins a tie. */
    private int evictSilent() {
        int oldest = 0;
        for (int s = 1; s < slots; s++) {
            oldest = lastSeen[s] < lastSeen[oldest] ? s : oldest;
        }
        remove(keys[oldest]);
        return oldest;
    }

    /** Remove {@code key} from the open-address table, and shift its probe chain back. */
    private void remove(final long key) {
        int hole = hash(key);
        while (keys[table[hole]] != key) {
            hole = next(hole);
        }
        table[hole] = -1;
        for (int i = next(hole); table[i] >= 0; i = next(i)) {
            hole = shiftBack(hole, i);
        }
    }

    /** Move the entry at {@code i} into {@code hole} when its home allows it. Return the new hole. */
    private int shiftBack(final int hole, final int i) {
        final int home = hash(keys[table[i]]);
        final boolean movable = ((i - home) & tableMask) >= ((i - hole) & tableMask);
        if (!movable) {
            return hole;
        }
        table[hole] = table[i];
        table[i] = -1;
        return i;
    }

    private int next(final int i) {
        return (i + 1) & tableMask;
    }

    private int hash(final long key) {
        long z = key * 0x9E37_79B9_7F4A_7C15L;
        return (int) (z ^ (z >>> 32)) & tableMask;
    }

    private static long keyOf(final int lane, final int session) {
        return ((long) (lane & 0xFF) << 32) | (session & 0xFFFF_FFFFL);
    }

    /** The length of the snapshot section that {@link #writeTo} writes. */
    public int snapshotLen() {
        return Integer.BYTES * 3 + used * (Long.BYTES * 3 + wordsPerSlot * Long.BYTES);
    }

    /**
     * Write {@code [slots:i32][width:i32][used:i32]}, then for each used slot
     * in slot order {@code [key:u64][highest:u64][last_seen:u64][bits]}.
     */
    public void writeTo(final ByteBuffer buf) {
        buf.putInt(slots).putInt(width).putInt(used);
        for (int s = 0; s < used; s++) {
            writeSlot(buf, s);
        }
    }

    private void writeSlot(final ByteBuffer buf, final int s) {
        buf.putLong(keys[s]).putLong(highest[s]).putLong(lastSeen[s]);
        buf.asLongBuffer().put(bits[s]);
        buf.position(buf.position() + wordsPerSlot * Long.BYTES);
    }

    /** Read the section that {@link #writeTo} wrote. The settings must match this member's settings. */
    public static SessionWindows readFrom(final ByteBuffer buf, final int slots, final int width) {
        final int snapSlots = buf.getInt();
        final int snapWidth = buf.getInt();
        if (snapSlots != slots || snapWidth != width) {
            throw new IllegalStateException("session window settings differ from the snapshot: "
                + slots + "/" + width + " here, " + snapSlots + "/" + snapWidth + " in the snapshot");
        }
        final SessionWindows w = new SessionWindows(slots, width);
        final int count = buf.getInt();
        if (count < 0 || count > slots) {
            throw new IllegalArgumentException("bad snapshot session window count: " + count);
        }
        for (int s = 0; s < count; s++) {
            w.readSlot(buf);
        }
        return w;
    }

    private void readSlot(final ByteBuffer buf) {
        final int s = used++;
        keys[s] = buf.getLong();
        highest[s] = buf.getLong();
        lastSeen[s] = buf.getLong();
        bits[s] = new long[wordsPerSlot];
        buf.asLongBuffer().get(bits[s]);
        buf.position(buf.position() + wordsPerSlot * Long.BYTES);
        insert(keys[s], s);
    }
}
