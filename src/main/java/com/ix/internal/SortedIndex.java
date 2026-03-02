package com.ix.internal;

import com.ix.Range;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

/**
 * A sorted index over a single field of a dataset.
 *
 * <h3>Memory layout</h3>
 * <p>For fields with a {@link LongCodec} (dates, numbers), values are stored as a
 * primitive {@code long[]} — 8 bytes/row. The {@code Comparable<?>[]} object-array
 * fallback (used for String and other types) costs ~8 bytes for the reference plus
 * 16–24 bytes for the object itself.
 *
 * <p>Both representations store a parallel {@code int[] rowIndices} (4 bytes/row)
 * mapping sorted position → original row index.
 *
 * <h3>Total overhead</h3>
 * <ul>
 *   <li>Codec path (Date, Double, …): <b>12 bytes/row/indexed-field</b>
 *   <li>Fallback path (String): <b>12 bytes/row/indexed-field</b> for the int[] +
 *       object-array pointer, plus JVM object overhead for each distinct String value
 * </ul>
 *
 * <h3>Query cost</h3>
 * O(log n) binary search to find bounds, O(k) to mark k matching rows in a BitSet.
 *
 * <p>Built lazily on first range filter call for a field, then cached in the root Index.
 */
public final class SortedIndex {

    // Exactly one of these is non-null depending on whether a LongCodec was available
    private final long[]         sortedKeys;    // codec path: encoded primitive values, ascending
    private final Comparable<?>[] sortedValues;  // fallback path: object values, ascending

    private final int[]          rowIndices;    // original row index per sorted position
    private final LongCodec<?>   codec;         // null on fallback path

    private SortedIndex(long[] sortedKeys, LongCodec<?> codec, int[] rowIndices) {
        this.sortedKeys   = sortedKeys;
        this.sortedValues = null;
        this.rowIndices   = rowIndices;
        this.codec        = codec;
    }

    private SortedIndex(Comparable<?>[] sortedValues, int[] rowIndices) {
        this.sortedKeys   = null;
        this.sortedValues = sortedValues;
        this.rowIndices   = rowIndices;
        this.codec        = null;
    }

    /**
     * Build a SortedIndex for the given field over the full dataset.
     * O(n log n) — sorts once, cached after that.
     *
     * <p>Tries to use a {@link LongCodec} for the field's value type to avoid
     * allocating a {@code Comparable<?>[]} object array. Falls back to the object
     * array path if no codec is registered (e.g. String fields).
     */
    public static <T, V extends Comparable<V>> SortedIndex build(
            List<T> data, Function<T, V> field) {

        int n = data.size();
        Integer[] order = new Integer[n];
        for (int i = 0; i < n; i++) order[i] = i;

        @SuppressWarnings("unchecked")
        V[] values = (V[]) new Comparable[n];
        for (int i = 0; i < n; i++) values[i] = field.apply(data.get(i));

        Arrays.sort(order, Comparator.comparing(i -> values[i]));

        // Detect codec from first non-null value
        LongCodec<V> codec = null;
        for (V v : values) {
            if (v != null) { codec = LongCodec.forValue(v); break; }
        }

        int[] rowIndices = new int[n];
        for (int i = 0; i < n; i++) rowIndices[i] = order[i];

        if (codec != null) {
            // Codec path: store as long[]
            long[] sortedKeys = new long[n];
            for (int i = 0; i < n; i++) sortedKeys[i] = codec.encode(values[order[i]]);
            return new SortedIndex(sortedKeys, codec, rowIndices);
        } else {
            // Fallback: store as Comparable<?>[]
            Comparable<?>[] sortedValues = new Comparable[n];
            for (int i = 0; i < n; i++) sortedValues[i] = values[order[i]];
            return new SortedIndex(sortedValues, rowIndices);
        }
    }

    /**
     * Returns a BitSet of all rows whose field value falls within {@code range}.
     * O(log n + k) where k = number of matching rows.
     */
    public <V extends Comparable<V>> BitSet query(Range<V> range, int totalRows) {
        int lo, hi;
        if (sortedKeys != null) {
            @SuppressWarnings("unchecked")
            LongCodec<V> c = (LongCodec<V>) codec;
            lo = findLoKeyed(range, c);
            hi = findHiKeyed(range, c);
        } else {
            lo = findLo(range);
            hi = findHi(range);
        }

        BitSet result = new BitSet(totalRows);
        for (int i = lo; i <= hi; i++) {
            result.set(rowIndices[i]);
        }
        return result;
    }

    // ── Codec path: binary search on long[] ──────────────────────────────────

    private <V extends Comparable<V>> int findLoKeyed(Range<V> range, LongCodec<V> c) {
        if (range.lo == null) return 0;
        long target = c.encode(range.lo);
        int pos = binarySearchKeys(target, c);
        if (pos < 0) {
            return -pos - 1;
        }
        if (!range.loInclusive) {
            while (pos < sortedKeys.length && c.compare(sortedKeys[pos], target) == 0) pos++;
        } else {
            while (pos > 0 && c.compare(sortedKeys[pos - 1], target) == 0) pos--;
        }
        return pos;
    }

    private <V extends Comparable<V>> int findHiKeyed(Range<V> range, LongCodec<V> c) {
        if (range.hi == null) return sortedKeys.length - 1;
        long target = c.encode(range.hi);
        int pos = binarySearchKeys(target, c);
        if (pos < 0) {
            return -pos - 2;
        }
        if (!range.hiInclusive) {
            while (pos >= 0 && c.compare(sortedKeys[pos], target) == 0) pos--;
        } else {
            while (pos < sortedKeys.length - 1 && c.compare(sortedKeys[pos + 1], target) == 0) pos++;
        }
        return pos;
    }

    private <V extends Comparable<V>> int binarySearchKeys(long target, LongCodec<V> c) {
        int low = 0, high = sortedKeys.length - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int cmp = c.compare(sortedKeys[mid], target);
            if      (cmp < 0) low  = mid + 1;
            else if (cmp > 0) high = mid - 1;
            else              return mid;
        }
        return -(low + 1);
    }

    // ── Fallback path: binary search on Comparable<?>[] ──────────────────────

    @SuppressWarnings("unchecked")
    private <V extends Comparable<V>> int findLo(Range<V> range) {
        if (range.lo == null) return 0;
        int pos = binarySearch(range.lo);
        if (pos < 0) {
            return -pos - 1;
        }
        if (!range.loInclusive) {
            while (pos < sortedValues.length
                    && ((Comparable<V>) sortedValues[pos]).compareTo(range.lo) == 0) pos++;
        } else {
            while (pos > 0
                    && ((Comparable<V>) sortedValues[pos - 1]).compareTo(range.lo) == 0) pos--;
        }
        return pos;
    }

    @SuppressWarnings("unchecked")
    private <V extends Comparable<V>> int findHi(Range<V> range) {
        if (range.hi == null) return sortedValues.length - 1;
        int pos = binarySearch(range.hi);
        if (pos < 0) {
            return -pos - 2;
        }
        if (!range.hiInclusive) {
            while (pos >= 0
                    && ((Comparable<V>) sortedValues[pos]).compareTo(range.hi) == 0) pos--;
        } else {
            while (pos < sortedValues.length - 1
                    && ((Comparable<V>) sortedValues[pos + 1]).compareTo(range.hi) == 0) pos++;
        }
        return pos;
    }

    @SuppressWarnings("unchecked")
    private <V extends Comparable<V>> int binarySearch(V target) {
        int low = 0, high = sortedValues.length - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int cmp = ((Comparable<V>) sortedValues[mid]).compareTo(target);
            if      (cmp < 0) low  = mid + 1;
            else if (cmp > 0) high = mid - 1;
            else              return mid;
        }
        return -(low + 1);
    }
}
