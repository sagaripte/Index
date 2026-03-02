package com.ix.internal;

import java.util.Arrays;

/**
 * Compact bit vector using long[] words.
 * Used internally to represent sets of matching row indices during query execution.
 */
public class BitSet {

    long[] data;

    public BitSet(int sizeInBits) {
        data = new long[(sizeInBits + 63) / 64];
    }

    public void clear() {
        Arrays.fill(data, 0L);
    }

    public void fill() {
        Arrays.fill(data, ~0L);
    }

    public void resize(int sizeInBits) {
        data = Arrays.copyOf(data, (sizeInBits + 63) / 64);
    }

    public int cardinality() {
        int sum = 0;
        for (long l : data) sum += Long.bitCount(l);
        return sum;
    }

    /** Deep copy of another BitSet's data into this one. */
    public void replace(BitSet another) {
        this.data = another.data.clone();
    }

    public boolean get(int i) {
        return (data[i / 64] & (1L << (i % 64))) != 0;
    }

    public void set(int i) {
        data[i / 64] |= (1L << (i % 64));
    }

    public void unset(int i) {
        data[i / 64] &= ~(1L << (i % 64));
    }

    public int nextSetBit(int i) {
        int x = i / 64;
        if (x >= data.length) return -1;
        long w = data[x] >>> (i % 64);
        if (w != 0) return i + Long.numberOfTrailingZeros(w);
        for (++x; x < data.length; ++x) {
            if (data[x] != 0) return x * 64 + Long.numberOfTrailingZeros(data[x]);
        }
        return -1;
    }

    public void and(BitSet another) {
        long[] o = another.data;
        int min = Math.min(data.length, o.length);
        for (int i = min - 1; i >= 0; i--) data[i] &= o[i];
        for (int i = min; i < data.length; i++) data[i] = 0;  // AND with 0 beyond other's range
    }

    public void or(BitSet another) {
        long[] o = another.data;
        for (int i = Math.min(data.length, o.length) - 1; i >= 0; i--) data[i] |= o[i];
    }

    public void xor(BitSet another) {
        long[] o = another.data;
        for (int i = Math.min(data.length, o.length) - 1; i >= 0; i--) data[i] ^= o[i];
    }

    public void not() {
        for (int i = data.length - 1; i >= 0; i--) data[i] = ~data[i];
    }

    public void and(int pos, long word) { data[pos] &= word; }
    public void xor(int pos, long word) { data[pos] ^= word; }
}
