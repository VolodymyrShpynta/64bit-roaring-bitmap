package com.my.samle.bitmap;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * Created by Volodymyr Shpynta on 24.10.16.
 */
public class RoaringBitmap64BitTest {
    @Test
    public void testAndOperation() {
        final RoaringBitmap64Bit bitmap1 = RoaringBitmap64Bit.bitmapOf(1000000000000L, 2000000000000000L, Long.MAX_VALUE, 123456789000L);
        final RoaringBitmap64Bit bitmap2 = RoaringBitmap64Bit.bitmapOf(1000000000000L, Long.MAX_VALUE - 100);

        bitmap1.and(bitmap2);
        System.out.println("AND result: " + bitmap1);
        assertThat(bitmap1, is(RoaringBitmap64Bit.bitmapOf(1000000000000L)));
        assertThat(bitmap1.getCardinality(), is(1));
    }

    @Test
    public void testOrOperation() {
        final RoaringBitmap64Bit bitmap1 = RoaringBitmap64Bit.bitmapOf(1000000000000L, 1, Long.MAX_VALUE, 123456789000L);
        final RoaringBitmap64Bit bitmap2 = RoaringBitmap64Bit.bitmapOf(1000000000000L, Long.MAX_VALUE - 100);

        bitmap1.or(bitmap2);
        System.out.println("OR result: " + bitmap1);
        assertThat(bitmap1, is(RoaringBitmap64Bit.bitmapOf(1000000000000L, 1, Long.MAX_VALUE, Long.MAX_VALUE - 100, 123456789000L)));
        assertThat(bitmap1.getCardinality(), is(5));
    }

    @Test
    public void testAndNotOperation() {
        final RoaringBitmap64Bit bitmap1 = RoaringBitmap64Bit.bitmapOf(1000000000000L, 2000000000000000L, Long.MAX_VALUE, 123456789000L);
        final RoaringBitmap64Bit bitmap2 = RoaringBitmap64Bit.bitmapOf(1000000000000L, Long.MAX_VALUE - 100);

        bitmap1.andNot(bitmap2);
        System.out.println("AND NOT result: " + bitmap1);
        assertThat(bitmap1, is(RoaringBitmap64Bit.bitmapOf(2000000000000000L, Long.MAX_VALUE, 123456789000L)));
        assertThat(bitmap1.getCardinality(), is(3));
    }

    @Test
    public void testToListAndToArray() {
        final RoaringBitmap64Bit longBitmap1 = new RoaringBitmap64Bit();
        final int elementsCount = 10;
        final List<Long> expectingResults = new ArrayList<>(elementsCount);

        for (long i = 1; i <= elementsCount; i++) {
            longBitmap1.add(i);
            expectingResults.add(i);
        }

        assertThat(longBitmap1.getCardinality(), is(elementsCount));
        assertThat(longBitmap1.toList(), is(expectingResults));
        assertThat(longBitmap1.toArray(), is(expectingResults.toArray()));
    }

    @Test
    public void testNoDuplicates() {
        final RoaringBitmap64Bit longBitmap1 = new RoaringBitmap64Bit();
        final int elementsCount = 10;
        final List<Long> expectingResults = new ArrayList<>(elementsCount);

        for (long i = 1; i <= elementsCount; i++) {
            longBitmap1.add(i);
            longBitmap1.add(i); //Add duplicate
            expectingResults.add(i);
        }

        assertThat(longBitmap1.getCardinality(), is(elementsCount));
        assertThat(longBitmap1.toList(), is(expectingResults));
        assertThat(longBitmap1.toArray(), is(expectingResults.toArray()));
    }

    @Test
    public void testAndForLargeBitmaps() {
        final RoaringBitmap64Bit longBitmap1 = new RoaringBitmap64Bit();
        final RoaringBitmap64Bit longBitmap2 = new RoaringBitmap64Bit();

        final int elementsCount = 1000000;
        final int offset = elementsCount / 2;
        final List<Long> expectingResults = new ArrayList<>(elementsCount - offset);

        for (long i = 1; i <= elementsCount; i++) {
            final long insertingValue1 = Integer.MAX_VALUE + i;
            final long insertingValue2 = Integer.MAX_VALUE + i + offset;
            longBitmap1.add(insertingValue1);
            longBitmap2.add(insertingValue2);

            if (insertingValue2 <= (long) Integer.MAX_VALUE + elementsCount) {
                expectingResults.add(insertingValue2);
            }
        }

        System.out.println("Starting AND operation...");
        final long startTime = System.currentTimeMillis();
        longBitmap1.and(longBitmap2);
        final long endTime = System.currentTimeMillis();
        System.out.println("AND execution time (ms): " + ((endTime - startTime)));

        assertThat(longBitmap1.toList(), is(expectingResults));
    }

    @Test
    public void testOrForLargeBitmaps() {
        final RoaringBitmap64Bit longBitmap1 = new RoaringBitmap64Bit();
        final RoaringBitmap64Bit longBitmap2 = new RoaringBitmap64Bit();

        final int elementsCount = 1000000;
        final int offset = elementsCount / 2;
        final Set<Long> expectingResults = new HashSet<>(elementsCount + offset);

        for (long i = 1; i <= elementsCount; i++) {
            final long insertingValue1 = Integer.MAX_VALUE + i;
            final long insertingValue2 = Integer.MAX_VALUE + i + offset;
            longBitmap1.add(insertingValue1);
            longBitmap2.add(insertingValue2);

            expectingResults.add(insertingValue1);
            expectingResults.add(insertingValue2);
        }

        System.out.println("Starting OR operation...");
        final long startTime = System.currentTimeMillis();
        longBitmap1.or(longBitmap2);
        final long endTime = System.currentTimeMillis();
        System.out.println("OR execution time (ms): " + ((endTime - startTime)));

        assertThat(new HashSet<>(longBitmap1.toList()), is(expectingResults));
    }

    @Test
    public void testAndNotForLargeBitmaps() {
        final RoaringBitmap64Bit longBitmap1 = new RoaringBitmap64Bit();
        final RoaringBitmap64Bit longBitmap2 = new RoaringBitmap64Bit();

        final int elementsCount = 1000000;
        final int offset = elementsCount / 2;
        final Set<Long> expectingResults = new HashSet<>(elementsCount - offset);

        for (long i = 1; i <= elementsCount; i++) {
            final long insertingValue1 = Integer.MAX_VALUE + i;
            final long insertingValue2 = Integer.MAX_VALUE + i + offset;
            longBitmap1.add(insertingValue1);
            longBitmap2.add(insertingValue2);

            if (insertingValue1 <= (long) Integer.MAX_VALUE + offset) {
                expectingResults.add(insertingValue1);
            }
        }

        System.out.println("Starting AND NOT operation...");
        final long startTime = System.currentTimeMillis();
        longBitmap1.andNot(longBitmap2);
        final long endTime = System.currentTimeMillis();
        System.out.println("AND NOT execution time (ms): " + ((endTime - startTime)));

        assertThat(new HashSet<>(longBitmap1.toList()), is(expectingResults));
    }
}