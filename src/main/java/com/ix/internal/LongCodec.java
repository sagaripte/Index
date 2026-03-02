package com.ix.internal;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Encodes a value type V to/from a {@code long} for storage in a primitive array.
 *
 * <p>Using {@code long[]} instead of {@code Comparable<?>[]} for sorted index values
 * halves the per-row memory from ~8 bytes (object reference) to 4 bytes (int-sized
 * packed into long[]), eliminates GC pressure, and improves cache performance during
 * binary search.
 *
 * <p>Factory methods cover all common field types. For custom types implement this
 * interface directly.
 *
 * @param <V> the field value type
 */
public interface LongCodec<V> {

    /** Encode a value to its long representation. */
    long encode(V value);

    /** Decode a long back to the original value type. */
    V decode(long bits);

    /** Compare two encoded longs as if comparing the original values. */
    int compare(long a, long b);

    // ── Built-in codecs ───────────────────────────────────────────────────────

    LongCodec<LocalDate> LOCAL_DATE = new LongCodec<>() {
        @Override public long encode(LocalDate v)  { return v.toEpochDay(); }
        @Override public LocalDate decode(long b)  { return LocalDate.ofEpochDay(b); }
        @Override public int compare(long a, long b) { return Long.compare(a, b); }
    };

    LongCodec<Instant> INSTANT = new LongCodec<>() {
        @Override public long encode(Instant v)    { return v.toEpochMilli(); }
        @Override public Instant decode(long b)    { return Instant.ofEpochMilli(b); }
        @Override public int compare(long a, long b) { return Long.compare(a, b); }
    };

    LongCodec<LocalDateTime> LOCAL_DATE_TIME = new LongCodec<>() {
        // Encode as epoch-millisecond (millisecond precision)
        @Override public long encode(LocalDateTime v)   { return v.toInstant(java.time.ZoneOffset.UTC).toEpochMilli(); }
        @Override public LocalDateTime decode(long b)   { return LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(b), java.time.ZoneOffset.UTC); }
        @Override public int compare(long a, long b)    { return Long.compare(a, b); }
    };

    LongCodec<Long> LONG = new LongCodec<>() {
        @Override public long encode(Long v)       { return v; }
        @Override public Long decode(long b)       { return b; }
        @Override public int compare(long a, long b) { return Long.compare(a, b); }
    };

    LongCodec<Integer> INTEGER = new LongCodec<>() {
        @Override public long encode(Integer v)    { return v; }
        @Override public Integer decode(long b)    { return (int) b; }
        @Override public int compare(long a, long b) { return Long.compare(a, b); }
    };

    /**
     * Double codec: uses a sign-bit flip so that encoded longs sort in the same
     * order as the original doubles (including negatives and NaN).
     *
     * <p>Positive doubles: flip sign bit only (XOR Long.MIN_VALUE) so they sort above zero.
     * Negative doubles: flip all bits so they sort below zero in correct order.
     * This is the standard "radix-sortable" IEEE 754 encoding used by e.g. Lucene.
     */
    LongCodec<Double> DOUBLE = new LongCodec<>() {
        // Bit 63 set in IEEE 754 means negative. Flip all bits for negatives so they
        // sort below zero; flip only bit 63 for positives so they sort above zero.
        private long toSortable(long bits) {
            return bits < 0 ? ~bits : bits ^ Long.MIN_VALUE;
        }
        // Inverse: sortable values from positive doubles have bit 63 set (< 0 as signed);
        // sortable values from negative doubles have bit 63 clear (>= 0 as signed).
        private long fromSortable(long s) {
            return s < 0 ? s ^ Long.MIN_VALUE : ~s;
        }
        @Override public long encode(Double v)       { return toSortable(Double.doubleToLongBits(v)); }
        @Override public Double decode(long b)       { return Double.longBitsToDouble(fromSortable(b)); }
        @Override public int compare(long a, long b) { return Long.compareUnsigned(a, b); }
    };

    /**
     * Float codec: same sign-bit flip as the Double codec applied to int bits.
     */
    LongCodec<Float> FLOAT = new LongCodec<>() {
        private int toSortable(int bits) {
            return bits < 0 ? ~bits : bits ^ Integer.MIN_VALUE;
        }
        private int fromSortable(int s) {
            return s < 0 ? s ^ Integer.MIN_VALUE : ~s;
        }
        @Override public long encode(Float v)        { return toSortable(Float.floatToIntBits(v)); }
        @Override public Float decode(long b)        { return Float.intBitsToFloat(fromSortable((int) b)); }
        @Override public int compare(long a, long b) { return Long.compareUnsigned(a, b); }
    };

    // ── Codec resolution ──────────────────────────────────────────────────────

    /**
     * Returns the built-in codec for a value, or null if none is registered.
     * Used by SortedIndex.build() to auto-detect the codec from the first value seen.
     */
    @SuppressWarnings("unchecked")
    static <V> LongCodec<V> forValue(V value) {
        if (value instanceof LocalDate)     return (LongCodec<V>) LOCAL_DATE;
        if (value instanceof Instant)       return (LongCodec<V>) INSTANT;
        if (value instanceof LocalDateTime) return (LongCodec<V>) LOCAL_DATE_TIME;
        if (value instanceof Long)          return (LongCodec<V>) LONG;
        if (value instanceof Integer)       return (LongCodec<V>) INTEGER;
        if (value instanceof Double)        return (LongCodec<V>) DOUBLE;
        if (value instanceof Float)         return (LongCodec<V>) FLOAT;
        return null;
    }
}
