package com.ix;

import com.ix.internal.BitSet;
import com.ix.internal.FieldIndex;
import com.ix.internal.SortedIndex;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A queryable, typed index over an existing collection of objects.
 *
 * <p>Wraps a {@code List<T>} and provides filtering, aggregation, and groupBy
 * without schema declaration or losing your domain type.
 *
 * <p>Two filter forms with different performance characteristics:
 * <ul>
 *   <li>{@link #filter(Function, Object[])} — field equality, <b>O(1) lookup</b> after first use.
 *       Builds an inverted index on the field lazily and reuses it on subsequent calls.
 *   <li>{@link #filter(Predicate)} — arbitrary predicate, always <b>O(n) scan</b>.
 *       Use for complex expressions that can't be expressed as field equality.
 * </ul>
 *
 * <pre>
 *   Index&lt;Trade&gt; idx = Index.of(trades);
 *
 *   // Fast — builds currency index on first call, reuses on subsequent calls
 *   double usdPnl = idx.filter(Trade::getCurrency, "USD")
 *                      .sum(Trade::getPnl);
 *
 *   // Also fast — index already built
 *   double eurPnl = idx.filter(Trade::getCurrency, "EUR")
 *                      .sum(Trade::getPnl);
 *
 *   // Scan — for predicates that can't be indexed
 *   double activePnl = idx.filter(t -> t.getPnl() > 0)
 *                         .sum(Trade::getPnl);
 *
 *   // Chainable — index lookup then scan on the reduced set
 *   Aggregation.Results&lt;String&gt; result = idx
 *       .filter(Trade::getCurrency, "USD")
 *       .filter(Trade::getDesk, "EQD", "FX")
 *       .groupBy(Trade::getRegion)
 *       .sum(Trade::getPnl)
 *       .count()
 *       .execute();
 * </pre>
 *
 * @param <T> your domain type — POJO, record, or any object
 */
public final class Index<T> implements Iterable<T> {

    final List<T>  data;    // original objects, never copied — package-private for Aggregation
    private final BitSet   bits;    // active rows (null = all rows = unfiltered root)
    private final int      size;    // number of active rows

    /**
     * Shared, lazily-populated inverted index (equality lookups).
     * Keyed by field function identity; value is a compact FieldIndex.
     */
    private final Map<Function<T, ?>, FieldIndex> fieldIndex;

    /**
     * Shared, lazily-populated sorted index (range queries).
     * Keyed by field function identity; value is a sorted (key, rowIndex) structure.
     */
    private final Map<Function<T, ?>, SortedIndex> sortedIndex;

    /**
     * Shared, lazily-populated unique index (point lookups on high-cardinality fields).
     * Keyed by field function identity; value is a HashMap from field value to row index.
     */
    private final Map<Function<T, ?>, Map<Object, Integer>> uniqueIndex;

    /**
     * Shared across all derived views — lazily populated on first empty() call.
     * AtomicReference ensures at most one instance is created under concurrent access,
     * consistent with the synchronized treatment of the index maps.
     */
    private final AtomicReference<Index<T>> emptyRef;

    // ── Construction ──────────────────────────────────────────────────────────

    private Index(List<T> data, BitSet bits, int size,
                  Map<Function<T, ?>, FieldIndex> fieldIndex,
                  Map<Function<T, ?>, SortedIndex> sortedIndex,
                  Map<Function<T, ?>, Map<Object, Integer>> uniqueIndex,
                  AtomicReference<Index<T>> emptyRef) {
        this.data        = data;
        this.bits        = bits;
        this.size        = size;
        this.fieldIndex  = fieldIndex;
        this.sortedIndex = sortedIndex;
        this.uniqueIndex = uniqueIndex;
        this.emptyRef    = emptyRef;
    }

    /** Create an Index over a list. O(1) — no data is copied, no index is built yet. */
    public static <T> Index<T> of(List<T> data) {
        return new Index<>(data, null, data.size(),
            Collections.synchronizedMap(new IdentityHashMap<>()),
            Collections.synchronizedMap(new IdentityHashMap<>()),
            Collections.synchronizedMap(new IdentityHashMap<>()),
            new AtomicReference<>());
    }

    /** Create an Index from a stream. The stream is consumed once into a list. */
    public static <T> Index<T> of(Stream<T> stream) {
        return of(stream.toList());
    }

    /** Create an Index from any iterable. */
    public static <T> Index<T> of(Iterable<T> iterable) {
        List<T> list = new ArrayList<>();
        iterable.forEach(list::add);
        return of(list);
    }

    // ── Filtering ─────────────────────────────────────────────────────────────

    /**
     * Filter by field equality — <b>O(1) lookup after first use</b>.
     *
     * <p>On the first call for a given field, scans the full dataset once to build
     * an inverted index for that field. All subsequent calls (any value, any filter
     * chain) reuse that index.
     *
     * <pre>
     *   idx.filter(Trade::getCurrency, "USD")
     *   idx.filter(Trade::getDesk, "EQD", "FX")   // multi-value OR
     * </pre>
     */
    @SafeVarargs
    public final <V> Index<T> filter(Function<T, V> field, V... values) {
        FieldIndex fi = getOrBuildIndex(field);

        BitSet matched = new BitSet(data.size());
        if (!fi.orInto(values, matched)) return empty();

        // Intersect with current active rows (if this is a filtered view)
        if (bits != null) matched.and(bits);

        int count = matched.cardinality();
        if (count == 0) return empty();

        return new Index<>(data, matched, count, fieldIndex, sortedIndex, uniqueIndex, emptyRef);
    }

    /**
     * Filter by arbitrary predicate — <b>always O(n) scan</b>.
     *
     * <p>Use this for complex expressions ({@code t -> t.getPnl() > 0 && ...}).
     * For simple field equality, prefer {@link #filter(Function, Object[])} which
     * uses an index.
     */
    public Index<T> filter(Predicate<T> predicate) {
        int total = data.size();
        BitSet next = new BitSet(total);
        int count = 0;

        if (bits == null) {
            for (int i = 0; i < total; i++) {
                if (predicate.test(data.get(i))) { next.set(i); count++; }
            }
        } else {
            int i = bits.nextSetBit(0);
            while (i >= 0) {
                if (predicate.test(data.get(i))) { next.set(i); count++; }
                i = bits.nextSetBit(i + 1);
            }
        }

        if (count == 0) return empty();
        return new Index<>(data, next, count, fieldIndex, sortedIndex, uniqueIndex, emptyRef);
    }

    /**
     * Filter by range — <b>O(log n + k)</b> after first use on a field.
     *
     * <p>Builds a sorted index for the field on first call (O(n log n)), then
     * binary-searches on every subsequent call. Works for any {@link Comparable}
     * field — dates, timestamps, numbers, strings.
     *
     * <pre>
     *   import static com.ix.Range.*;
     *
     *   idx.filter(Trade::getTradeDate, between(LocalDate.of(2024,1,1), LocalDate.of(2024,3,31)))
     *   idx.filter(Trade::getTradeDate, after(LocalDate.of(2024,1,1)))
     *   idx.filter(Trade::getAmount,    above(1_000_000.0))
     * </pre>
     */
    public <V extends Comparable<V>> Index<T> filter(Function<T, V> field, Range<V> range) {
        SortedIndex si = getOrBuildSortedIndex(field);
        BitSet matched = si.query(range, data.size());

        if (bits != null) matched.and(bits);

        int count = matched.cardinality();
        if (count == 0) return empty();

        return new Index<>(data, matched, count, fieldIndex, sortedIndex, uniqueIndex, emptyRef);
    }

    // ── Scalar aggregations ───────────────────────────────────────────────────

    /** Sum of a numeric field across all matching rows. */
    public double sum(ToDoubleFunction<T> field) {
        double total = 0;
        for (T item : this) total += field.applyAsDouble(item);
        return total;
    }

    /** Average of a numeric field across all matching rows. Returns 0 if empty. */
    public double avg(ToDoubleFunction<T> field) {
        if (size == 0) return 0;
        return sum(field) / size;
    }

    /** Max of a numeric field. Returns Double.NaN if empty. */
    public double max(ToDoubleFunction<T> field) {
        double max = Double.NEGATIVE_INFINITY;
        boolean any = false;
        for (T item : this) { max = Math.max(max, field.applyAsDouble(item)); any = true; }
        return any ? max : Double.NaN;
    }

    /** Min of a numeric field. Returns Double.NaN if empty. */
    public double min(ToDoubleFunction<T> field) {
        double min = Double.POSITIVE_INFINITY;
        boolean any = false;
        for (T item : this) { min = Math.min(min, field.applyAsDouble(item)); any = true; }
        return any ? min : Double.NaN;
    }

    /** Number of active rows. */
    public int count() { return size; }

    // ── Grouping ──────────────────────────────────────────────────────────────

    /**
     * Start a groupBy aggregation.
     *
     * <pre>
     *   idx.groupBy(Trade::getDesk).sum(Trade::getPnl).count().execute();
     * </pre>
     */
    public <K> Aggregation<T, K> groupBy(Function<T, K> keyFn) {
        return new Aggregation<>(this, keyFn);
    }

    // ── Distinct ──────────────────────────────────────────────────────────────

    /**
     * All distinct values of a field across active rows, in encounter order.
     *
     * <p>If the field has already been indexed (via a prior {@link #filter(Function, Object[])}
     * call), this is O(distinct values) — the value map keys are read directly.
     * Otherwise falls back to an O(n) scan without building a full index.
     * Safe to call on any field, including high-cardinality ones like trade IDs.
     */
    @SuppressWarnings("unchecked")
    public <K> List<K> distinct(Function<T, K> field) {
        // Use the existing index if already built — avoids a scan and is O(distinct values).
        FieldIndex fi = fieldIndex.get(field);
        if (fi != null) {
            List<K> result = new ArrayList<>();
            for (Object v : fi.values()) {
                if (bits == null ? fi.hasAnyRow(v) : fi.hasActiveRow(v, bits)) {
                    result.add((K) v);
                }
            }
            return result;
        }

        // No index yet — scan without building one (safe for high-cardinality fields).
        LinkedHashSet<K> seen = new LinkedHashSet<>();
        if (bits == null) {
            for (T item : data) seen.add((K) field.apply(item));
        } else {
            int i = bits.nextSetBit(0);
            while (i >= 0) { seen.add((K) field.apply(data.get(i))); i = bits.nextSetBit(i + 1); }
        }
        return new ArrayList<>(seen);
    }

    // ── Collection views ─────────────────────────────────────────────────────

    /** Returns active rows as a new List&lt;T&gt;. */
    public List<T> toList() {
        List<T> result = new ArrayList<>(size);
        for (T item : this) result.add(item);
        return result;
    }

    /** Returns a Stream&lt;T&gt; over active rows. */
    public Stream<T> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    /** Iterates active rows as T — your domain type, unchanged. */
    @Override
    public Iterator<T> iterator() {
        if (bits == null) return data.iterator();
        return new Iterator<>() {
            int next = bits.nextSetBit(0);
            @Override public boolean hasNext() { return next >= 0; }
            @Override public T next() {
                T item = data.get(next);
                next = bits.nextSetBit(next + 1);
                return item;
            }
        };
    }

    // ── Internal / package-private ────────────────────────────────────────────

    /** Active-row BitSet — null means all rows. Used by Aggregation. */
    BitSet bits() { return bits; }

    /**
     * Returns (and lazily builds) the FieldIndex for a field.
     * Scans the full dataset exactly once per field, then caches the result.
     * Thread-safe: the backing map is a synchronized IdentityHashMap and
     * computeIfAbsent is called under synchronization.
     * Package-private so Aggregation can use it for indexed groupBy.
     */
    <V> FieldIndex getOrBuildIndex(Function<T, V> field) {
        // Fast path — already built
        FieldIndex existing = fieldIndex.get(field);
        if (existing != null) return existing;

        int total = data.size();

        // Phase 1: count distinct values with early bail to avoid building a
        // full FieldIndex for high-cardinality fields (e.g. trade IDs).
        if (total > 0) {
            int limit = total / 2;
            Set<Object> seen = new HashSet<>();
            for (int i = 0; i < total; i++) {
                seen.add(field.apply(data.get(i)));
                if (seen.size() > limit) {
                    throw new IllegalArgumentException(
                        "filter/groupBy called on a high-cardinality field: more than " +
                        limit + " distinct values across " + total + " rows (>" +
                        (100 * limit / total) + "%). " +
                        "Building an inverted index on near-unique fields wastes memory. " +
                        "Use filter(Predicate) for point lookups on unique fields instead.");
                }
            }
        }

        // Phase 2: build the full index. computeIfAbsent is atomic under the
        // synchronized wrapper, so at most one thread builds each field index.
        return fieldIndex.computeIfAbsent(field, f -> {
            FieldIndex fi = new FieldIndex(total);
            for (int i = 0; i < total; i++) fi.set(f.apply(data.get(i)), i);
            return fi;
        });
    }

    /** Returns (and lazily builds) the sorted index for a field. O(n log n) once, cached after. */
    @SuppressWarnings("unchecked")
    private <V extends Comparable<V>> SortedIndex getOrBuildSortedIndex(Function<T, V> field) {
        SortedIndex existing = sortedIndex.get(field);
        if (existing != null) return existing;
        return sortedIndex.computeIfAbsent(field, f -> SortedIndex.build(data, (Function<T, V>) f));
    }

    private Index<T> empty() {
        Index<T> e = emptyRef.get();
        if (e == null) {
            Index<T> created = new Index<>(data, new BitSet(0), 0, fieldIndex, sortedIndex, uniqueIndex, emptyRef);
            emptyRef.compareAndSet(null, created);
            e = emptyRef.get();
        }
        return e;
    }

    // ── Unique lookup ─────────────────────────────────────────────────────────

    /**
     * Look up a single row by a unique (or high-cardinality) field — <b>O(1) after first call</b>.
     *
     * <p>Unlike {@link #filter(Function, Object[])}, this does not check for high cardinality
     * and is the correct method for fields like trade ID or UUID. Builds a {@code HashMap}
     * on first call and caches it on the root index.
     *
     * <p>If multiple rows share the same key value, the first one encountered is returned.
     *
     * <pre>
     *   Trade t = idx.unique(Trade::getId, "T-001");  // null if not found
     * </pre>
     *
     * @return the matching row, or {@code null} if no row has that key value
     */
    public <V> T unique(Function<T, V> field, V key) {
        Map<Object, Integer> map = uniqueIndex.computeIfAbsent(field, f -> {
            Map<Object, Integer> m = new HashMap<>(data.size() * 2);
            // Iterate in reverse so the first occurrence wins on duplicate keys
            for (int i = data.size() - 1; i >= 0; i--) {
                m.put(f.apply(data.get(i)), i);
            }
            return m;
        });

        Integer rowIdx = map.get(key);
        if (rowIdx == null) return null;
        // If this is a filtered view, only return the row if it's in the active set
        if (bits != null && !bits.get(rowIdx)) return null;
        return data.get(rowIdx);
    }
}
