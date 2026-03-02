package com.ix;

/**
 * Describes a range constraint for use with {@link Index#filter(java.util.function.Function, Range)}.
 *
 * <p>Works with any {@link Comparable} field — dates, timestamps, numbers, strings.
 *
 * <pre>
 *   import static com.ix.Range.*;
 *
 *   idx.filter(Trade::getTradeDate, between(LocalDate.of(2024,1,1), LocalDate.of(2024,3,31)))
 *   idx.filter(Trade::getTradeDate, after(LocalDate.of(2024,1,1)))
 *   idx.filter(Trade::getTradeDate, before(LocalDate.of(2024,1,1)))
 *   idx.filter(Trade::getAmount,    above(1_000_000.0))
 *   idx.filter(Trade::getAmount,    below(500.0))
 * </pre>
 *
 * <p>{@code after} and {@code before} are inclusive. {@code above} and {@code below} are exclusive.
 *
 * @param <V> the field value type — must be Comparable
 */
public final class Range<V extends Comparable<V>> {

    public final V       lo;           // null = unbounded lower
    public final V       hi;           // null = unbounded upper
    public final boolean loInclusive;
    public final boolean hiInclusive;

    private Range(V lo, boolean loInclusive, V hi, boolean hiInclusive) {
        this.lo          = lo;
        this.hi          = hi;
        this.loInclusive = loInclusive;
        this.hiInclusive = hiInclusive;
    }

    /** lo &lt;= value &lt;= hi (both inclusive). */
    public static <V extends Comparable<V>> Range<V> between(V lo, V hi) {
        return new Range<>(lo, true, hi, true);
    }

    /** value &gt;= lo (inclusive lower bound). */
    public static <V extends Comparable<V>> Range<V> after(V lo) {
        return new Range<>(lo, true, null, true);
    }

    /** value &lt;= hi (inclusive upper bound). */
    public static <V extends Comparable<V>> Range<V> before(V hi) {
        return new Range<>(null, true, hi, true);
    }

    /** value &gt; lo (exclusive lower bound). */
    public static <V extends Comparable<V>> Range<V> above(V lo) {
        return new Range<>(lo, false, null, true);
    }

    /** value &lt; hi (exclusive upper bound). */
    public static <V extends Comparable<V>> Range<V> below(V hi) {
        return new Range<>(null, true, hi, false);
    }

    /** True if {@code value} falls within this range. */
    @SuppressWarnings("unchecked")
    public boolean contains(Comparable<?> value) {
        Comparable<V> v = (Comparable<V>) value;
        if (lo != null) {
            int cmp = v.compareTo(lo);
            if (cmp < 0 || (cmp == 0 && !loInclusive)) return false;
        }
        if (hi != null) {
            int cmp = v.compareTo(hi);
            if (cmp > 0 || (cmp == 0 && !hiInclusive)) return false;
        }
        return true;
    }

    @Override public String toString() {
        String loStr = lo == null ? "(-∞" : (loInclusive ? "[" + lo : "(" + lo);
        String hiStr = hi == null ? "+∞)" : (hiInclusive ? hi + "]" : hi + ")");
        return loStr + ", " + hiStr;
    }
}
