/*
 * The MIT License
 *
 * Copyright 2015 ro0t.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package SecondarySort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author ro0t
 */
//To Do : binary comparator
public class LongPair implements WritableComparable<LongPair> {

    private long first;
    private long second;

    public LongPair() {
        this.first = 0l;
        this.second = 0l;
    }

    public LongPair(long first, long second) {
        set(first, second);
    }

    private void set(long first, long second) {
        this.first = first;
        this.second = second;
    }

    public long getFirst() {
        return first;
    }

    public long getSecond() {
        return second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(first);
        out.writeLong(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readLong();
        second = in.readLong();
    }

    @Override
    public int hashCode() {
        return ((int) first * 163 + (int) second);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof LongPair) {
            LongPair ip = (LongPair) o;
            return first == ip.first && second == ip.second;
        }
        return false;
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }

//  @Override
    public int compareTo(LongPair ip) {
        int cmp = compare(first, ip.first);
        if (cmp != 0) {
            return cmp;
        }
        return compare(second, ip.second);
    }

    /**
     * Convenience method for comparing two long /long + int.
     */
    public static int compare(long a, int b) {
        return (a < b ? -1 : (a == b ? 0 : 1));
    }

    public static int compare(int a, long b) {
        return (a < b ? -1 : (a == b ? 0 : 1));
    }

    public static int compare(long a, long b) {
        return (a < b ? -1 : (a == b ? 0 : 1));
    }

    /**
     * A Comparator optimized for LongPair.
     */
    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(LongPair.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int cp1 = WritableComparator.compareBytes(b1, s1, Long.SIZE / 8, b2, s2, Long.SIZE / 8);
            if (cp1 != 0) {
                return cp1;
            } else {
                cp1 = WritableComparator.compareBytes(b1, s1 + Long.SIZE / 8, Long.SIZE / 8, b2, s2 + Long.SIZE / 8, Long.SIZE / 8);
                return cp1;
            }
        }
    }

    public static class HPartitioner extends Partitioner<LongPair, Text> {

        @Override
        public int getPartition(LongPair key, Text value, int i) {
            Text k = new Text(String.valueOf(key.getFirst()));
            return (k.hashCode() & Integer.MAX_VALUE) % i;
        }
    }

    public static class GroupComparator extends WritableComparator {

        public GroupComparator() {
            super(LongPair.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int cp1 = WritableComparator.compareBytes(b1, s1, Long.SIZE / 8, b2, s2, Long.SIZE / 8);
            return cp1;

        }
    }

    static {  // register default comparator
        WritableComparator.define(LongPair.class, new Comparator());
    }
}
