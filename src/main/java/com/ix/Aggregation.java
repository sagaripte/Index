package com.ix;

import com.ix.internal.BitSet;
import com.ix.internal.FieldIndex;

import java.util.*;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

/**
 * Fluent groupBy aggregation builder returned by {@link Index#groupBy(Function)}.
 *
 * <p>Each aggregation can be registered with an optional name for refactor-safe access:
 *
 * <pre>
 *   Aggregation.Results&lt;String&gt; byDesk = idx
 *       .groupBy(Trade::getDesk)
 *       .sum("pnl",      Trade::getPnl)
 *       .sum("notional", Trade::getNotional)
 *       .count()
 *       .execute();
 *
 *   for (Aggregation.Row&lt;String&gt; row : byDesk) {
 *       System.out.println(row.key() + " pnl=" + row.sum("pnl") + " notional=" + row.sum("notional"));
 *   }
 * </pre>
 *
 * <p>Positional access ({@code row.sum(0)}) is still supported for brevity when names aren't needed.
 *
 * @param <T> domain type
 * @param <K> groupBy key type
 */
public final class Aggregation<T, K> {

    private final Index<T>       source;
    private final Function<T, K> keyFn;

    private final List<ToDoubleFunction<T>> sumFns  = new ArrayList<>();
    private final List<ToDoubleFunction<T>> avgFns  = new ArrayList<>();
    private final List<ToDoubleFunction<T>> minFns  = new ArrayList<>();
    private final List<ToDoubleFunction<T>> maxFns  = new ArrayList<>();

    // Parallel name lists — null entry means no name was given for that slot
    private final List<String> sumNames = new ArrayList<>();
    private final List<String> avgNames = new ArrayList<>();
    private final List<String> minNames = new ArrayList<>();
    private final List<String> maxNames = new ArrayList<>();

    private boolean includeCount = false;

    // package-private — created only by Index
    Aggregation(Index<T> source, Function<T, K> keyFn) {
        this.source = source;
        this.keyFn  = keyFn;
    }

    // ── Builder methods (unnamed) ─────────────────────────────────────────────

    public Aggregation<T, K> sum(ToDoubleFunction<T> field) {
        sumFns.add(field); sumNames.add(null); return this;
    }

    public Aggregation<T, K> avg(ToDoubleFunction<T> field) {
        avgFns.add(field); avgNames.add(null); return this;
    }

    public Aggregation<T, K> min(ToDoubleFunction<T> field) {
        minFns.add(field); minNames.add(null); return this;
    }

    public Aggregation<T, K> max(ToDoubleFunction<T> field) {
        maxFns.add(field); maxNames.add(null); return this;
    }

    // ── Builder methods (named) ───────────────────────────────────────────────

    public Aggregation<T, K> sum(String name, ToDoubleFunction<T> field) {
        sumFns.add(field); sumNames.add(name); return this;
    }

    public Aggregation<T, K> avg(String name, ToDoubleFunction<T> field) {
        avgFns.add(field); avgNames.add(name); return this;
    }

    public Aggregation<T, K> min(String name, ToDoubleFunction<T> field) {
        minFns.add(field); minNames.add(name); return this;
    }

    public Aggregation<T, K> max(String name, ToDoubleFunction<T> field) {
        maxFns.add(field); maxNames.add(name); return this;
    }

    public Aggregation<T, K> count() {
        includeCount = true; return this;
    }

    /**
     * Executes the aggregation in a single pass over the filtered rows.
     *
     * <p>Uses the FieldIndex for the groupBy key to iterate group-by-group,
     * intersecting each group's raw bitmap with the active filter bitmap.
     * Avoids calling {@code keyFn.apply()} per row.
     */
    @SuppressWarnings("unchecked")
    public Results<K> execute() {
        int nSum = sumFns.size(), nAvg = avgFns.size();
        int nMin = minFns.size(), nMax = maxFns.size();

        // Accumulator slot layout per group:
        //   [sum...] [avgSum...] [avgCount...] [min...] [max...]
        int sumBase  = 0;
        int avgSBase = nSum;
        int avgCBase = nSum + nAvg;
        int minBase  = nSum + nAvg * 2;
        int maxBase  = minBase + nMin;
        int slots    = maxBase + nMax;

        Map<K, double[]> groups = new LinkedHashMap<>();
        // Row counts stored separately as longs to avoid double precision loss
        Map<K, long[]> counts = includeCount ? new LinkedHashMap<>() : null;

        FieldIndex fi = source.getOrBuildIndex(keyFn);
        BitSet activeBits = source.bits();

        for (Object keyObj : fi.values()) {
            // Compute effective rows: intersect group bitmap with active filter
            long[] effective;
            if (activeBits == null) {
                effective = fi.rawBits(keyObj);
            } else {
                effective = fi.intersect(keyObj, activeBits);
            }

            // Skip group if no active rows
            if (effective == null) continue;
            int rowIdx = nextSetBit(effective, 0);
            if (rowIdx < 0) continue;

            K key = (K) keyObj;
            double[] acc = new double[slots];
            for (int i = 0; i < nMin; i++) acc[minBase + i] = Double.MAX_VALUE;
            for (int i = 0; i < nMax; i++) acc[maxBase + i] = -Double.MAX_VALUE;
            groups.put(key, acc);
            long[] cnt = null;
            if (includeCount) { cnt = new long[1]; counts.put(key, cnt); }

            while (rowIdx >= 0) {
                T item = source.data.get(rowIdx);
                for (int i = 0; i < nSum; i++) acc[sumBase  + i] += sumFns.get(i).applyAsDouble(item);
                for (int i = 0; i < nAvg; i++) { acc[avgSBase + i] += avgFns.get(i).applyAsDouble(item); acc[avgCBase + i]++; }
                for (int i = 0; i < nMin; i++) acc[minBase  + i]  = Math.min(acc[minBase + i], minFns.get(i).applyAsDouble(item));
                for (int i = 0; i < nMax; i++) acc[maxBase  + i]  = Math.max(acc[maxBase + i], maxFns.get(i).applyAsDouble(item));
                if (cnt != null) cnt[0]++;
                rowIdx = nextSetBit(effective, rowIdx + 1);
            }
        }

        // Build name → slot index maps (only for named entries)
        Map<String, Integer> sumIdx = buildNameIndex(sumNames);
        Map<String, Integer> avgIdx = buildNameIndex(avgNames);
        Map<String, Integer> minIdx = buildNameIndex(minNames);
        Map<String, Integer> maxIdx = buildNameIndex(maxNames);

        // Build result rows
        List<Row<K>> rows = new ArrayList<>(groups.size());
        for (Map.Entry<K, double[]> entry : groups.entrySet()) {
            K key = entry.getKey();
            double[] acc = entry.getValue();
            double[] sums = Arrays.copyOfRange(acc, sumBase,  sumBase  + nSum);
            double[] avgs = new double[nAvg];
            for (int i = 0; i < nAvg; i++)
                avgs[i] = acc[avgCBase + i] == 0 ? 0 : acc[avgSBase + i] / acc[avgCBase + i];
            double[] mins = Arrays.copyOfRange(acc, minBase, minBase + nMin);
            double[] maxs = Arrays.copyOfRange(acc, maxBase, maxBase + nMax);
            long count = includeCount ? counts.get(key)[0] : -1;
            rows.add(new Row<>(key, sums, avgs, mins, maxs, count, sumIdx, avgIdx, minIdx, maxIdx));
        }

        return new Results<>(rows);
    }

    /** Builds a name → slot-index map from a names list, skipping null entries. */
    private static Map<String, Integer> buildNameIndex(List<String> names) {
        Map<String, Integer> idx = new HashMap<>();
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            if (name != null) idx.put(name, i);
        }
        return idx.isEmpty() ? Collections.emptyMap() : idx;
    }

    /** nextSetBit on a raw long[] — avoids wrapping in a BitSet object. */
    private static int nextSetBit(long[] words, int fromIndex) {
        int w = fromIndex >> 6;
        if (w >= words.length) return -1;
        long word = words[w] >>> (fromIndex & 63);
        if (word != 0) return fromIndex + Long.numberOfTrailingZeros(word);
        for (w++; w < words.length; w++) {
            if (words[w] != 0) return (w << 6) + Long.numberOfTrailingZeros(words[w]);
        }
        return -1;
    }

    // ── Result types ──────────────────────────────────────────────────────────

    /** Iterable result of a groupBy aggregation. */
    public static final class Results<K> implements Iterable<Row<K>> {
        private final Map<K, Row<K>> map;

        Results(List<Row<K>> rows) {
            map = new LinkedHashMap<>(rows.size() * 2);
            for (Row<K> r : rows) map.put(r.key, r);
        }

        public int groupCount() { return map.size(); }

        /** Look up the result row for a specific group key. Returns null if absent. O(1). */
        public Row<K> get(K key) { return map.get(key); }

        @Override public Iterator<Row<K>> iterator() { return map.values().iterator(); }

        public void forEach(java.util.function.Consumer<? super Row<K>> action) { map.values().forEach(action); }
    }

    /** One result row — the group key plus all aggregated values. */
    public static final class Row<K> {
        private final K        key;
        private final double[] sums;
        private final double[] avgs;
        private final double[] mins;
        private final double[] maxs;
        private final long     count;

        // name → slot index, for named access; empty if no names were registered
        private final Map<String, Integer> sumIdx;
        private final Map<String, Integer> avgIdx;
        private final Map<String, Integer> minIdx;
        private final Map<String, Integer> maxIdx;

        Row(K key, double[] sums, double[] avgs, double[] mins, double[] maxs, long count,
            Map<String, Integer> sumIdx, Map<String, Integer> avgIdx,
            Map<String, Integer> minIdx, Map<String, Integer> maxIdx) {
            this.key    = key;
            this.sums   = sums;
            this.avgs   = avgs;
            this.mins   = mins;
            this.maxs   = maxs;
            this.count  = count;
            this.sumIdx = sumIdx;
            this.avgIdx = avgIdx;
            this.minIdx = minIdx;
            this.maxIdx = maxIdx;
        }

        /** The group key value (type-safe — same type as the groupBy field). */
        public K key() { return key; }

        /** Sum by name — e.g. {@code row.sum("pnl")}. Throws if the name wasn't registered. */
        public double sum(String name) { return sums[idx(sumIdx, name, "sum")]; }

        /** Average by name. Throws if the name wasn't registered. */
        public double avg(String name) { return avgs[idx(avgIdx, name, "avg")]; }

        /** Min by name. Throws if the name wasn't registered. */
        public double min(String name) { return mins[idx(minIdx, name, "min")]; }

        /** Max by name. Throws if the name wasn't registered. */
        public double max(String name) { return maxs[idx(maxIdx, name, "max")]; }

        /** Sum at position i (0-based, in the order .sum() was called). */
        public double sum(int i) { return sums[i]; }

        /** Average at position i. */
        public double avg(int i) { return avgs[i]; }

        /** Min at position i. */
        public double min(int i) { return mins[i]; }

        /** Max at position i. */
        public double max(int i) { return maxs[i]; }

        /** Row count for this group. -1 if .count() was not called. */
        public long count() { return count; }

        private static int idx(Map<String, Integer> index, String name, String op) {
            Integer i = index.get(name);
            if (i == null) throw new IllegalArgumentException(
                "No " + op + " registered with name '" + name + "'");
            return i;
        }

        @Override public String toString() {
            return "Row{key=" + key + ", sums=" + Arrays.toString(sums)
                + ", avgs=" + Arrays.toString(avgs) + ", count=" + count + "}";
        }
    }
}
