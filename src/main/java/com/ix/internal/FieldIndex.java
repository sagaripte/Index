package com.ix.internal;

import java.util.*;

/**
 * Compact inverted index for equality-based field lookups.
 *
 * <h3>Memory layout vs the old Map&lt;Object, BitSet&gt; approach</h3>
 *
 * <pre>
 *   Old:  Map&lt;Object, BitSet&gt;
 *         = distinctValues × (40 bytes BitSet object header + long[] data)
 *         ≈ 40 × D + 8N bits   where D = distinct values, N = rows
 *
 *   New:  long[][] bitmaps   (one long[] per distinct value, no per-value object overhead)
 *         + Map&lt;Object, Integer&gt; valueToId  (one Integer box per distinct value)
 *         ≈ 16 × D + 8N bits
 * </pre>
 *
 * <p>For a field with 1 000 distinct values over 100 000 rows:
 * <ul>
 *   <li>Old: 40 000 bytes (BitSet headers) + 1 562 500 bytes (bits) ≈ 1.5 MB
 *   <li>New: 16 000 bytes (array headers) + 1 562 500 bytes (bits) ≈ 1.5 MB — same bits,
 *       but 24 KB less object overhead, and much better GC behaviour under high D
 * </ul>
 *
 * <p>For a field with 100 000 distinct values (e.g. a trade ID):
 * <ul>
 *   <li>Old: 4 MB of BitSet object headers alone
 *   <li>New: 1.6 MB — a 2.5× reduction in overhead
 * </ul>
 *
 * <h3>Encounter order</h3>
 * Values are stored in insertion order (matching the original {@code LinkedHashMap}
 * behaviour) so that {@code distinct()} and {@code groupBy()} return values in the
 * order they first appear in the dataset.
 */
public final class FieldIndex {

    /** value → stable integer id (insertion order preserved by LinkedHashMap) */
    private final Map<Object, Integer> valueToId = new LinkedHashMap<>();

    /**
     * One long[] per distinct value, indexed by id.
     * Each long[] has {@code words} longs, enough to cover all row indices.
     */
    private final List<long[]> bitmaps = new ArrayList<>();

    private final int words;  // ceil(totalRows / 64)

    public FieldIndex(int totalRows) {
        this.words = (totalRows + 63) / 64;
    }

    // ── Building ──────────────────────────────────────────────────────────────

    /** Record that row {@code rowIdx} has value {@code value}. */
    public void set(Object value, int rowIdx) {
        int id = valueToId.computeIfAbsent(value, k -> {
            bitmaps.add(new long[words]);
            return bitmaps.size() - 1;
        });
        long[] bm = bitmaps.get(id);
        bm[rowIdx >> 6] |= (1L << (rowIdx & 63));
    }

    // ── Querying ──────────────────────────────────────────────────────────────

    /**
     * Returns a fresh {@link BitSet} containing all rows where the field equals {@code value},
     * or {@code null} if no rows have that value.
     */
    public BitSet getBits(Object value, int totalRows) {
        Integer id = valueToId.get(value);
        if (id == null) return null;
        long[] src = bitmaps.get(id);
        BitSet result = new BitSet(totalRows);
        System.arraycopy(src, 0, result.data, 0, Math.min(src.length, result.data.length));
        return result;
    }

    /**
     * OR-unions the bits for all requested values into {@code out}.
     * Returns {@code true} if at least one value was found.
     */
    public boolean orInto(Object[] values, BitSet out) {
        boolean found = false;
        for (Object v : values) {
            Integer id = valueToId.get(v);
            if (id == null) continue;
            long[] src = bitmaps.get(id);
            long[] dst = out.data;
            for (int i = Math.min(src.length, dst.length) - 1; i >= 0; i--) dst[i] |= src[i];
            found = true;
        }
        return found;
    }

    /**
     * Returns true if the value's bitmap has at least one bit in common with {@code active}.
     * Used by {@code distinct()} to skip values with no active rows.
     */
    public boolean hasActiveRow(Object value, BitSet active) {
        Integer id = valueToId.get(value);
        if (id == null) return false;
        long[] bm = bitmaps.get(id);
        long[] ab = active.data;
        for (int i = Math.min(bm.length, ab.length) - 1; i >= 0; i--) {
            if ((bm[i] & ab[i]) != 0) return true;
        }
        return false;
    }

    /**
     * Returns {@code true} if the value's bitmap has at least one bit set anywhere.
     * Used by {@code distinct()} when there is no active filter (all rows live).
     */
    public boolean hasAnyRow(Object value) {
        Integer id = valueToId.get(value);
        if (id == null) return false;
        for (long w : bitmaps.get(id)) if (w != 0) return true;
        return false;
    }

    // ── Iteration ─────────────────────────────────────────────────────────────

    /** Number of distinct values indexed. */
    public int distinctCount() { return valueToId.size(); }

    /** The set of distinct values, in encounter (insertion) order. */
    public Set<Object> values() {
        return valueToId.keySet();
    }

    /**
     * Returns the bitmap for a specific value as a raw {@code long[]}, or null.
     * For use in {@code Aggregation.execute()} which needs to iterate rows group-by-group
     * when there is no active filter (all rows are live).
     */
    public long[] rawBits(Object value) {
        Integer id = valueToId.get(value);
        return id == null ? null : bitmaps.get(id);
    }

    /**
     * Returns the intersection of the group bitmap and the active-filter bitmap as a
     * raw {@code long[]}, or {@code null} if the group has no active rows.
     *
     * <p>Kept inside {@code com.ix.internal} so it can access {@link BitSet#data}
     * directly without making that field public.
     */
    public long[] intersect(Object value, BitSet active) {
        Integer id = valueToId.get(value);
        if (id == null) return null;
        long[] group = bitmaps.get(id);
        long[] ab    = active.data;
        int len = Math.min(group.length, ab.length);
        long[] result = new long[len];
        boolean any = false;
        for (int i = len - 1; i >= 0; i--) {
            result[i] = group[i] & ab[i];
            if (result[i] != 0) any = true;
        }
        return any ? result : null;
    }
}
