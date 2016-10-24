package com.my.samle.bitmap;

import com.my.samle.bitmap.utils.Pair;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.roaringbitmap.RoaringBitmap;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.my.samle.bitmap.utils.BooleanUtils.not;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.ArrayUtils.toObject;

/**
 * Created by Volodymyr Shpynta on 24.10.16.
 */

@EqualsAndHashCode
@Slf4j
public class RoaringBitmap64Bit implements Iterable<Long> {

    private static final ForkJoinPool forkJoinPool = new ForkJoinPool(10);

    private Map<Integer, RoaringBitmap> highLowBitsContainer = new HashMap<>();
    private RoaringBitmap highBitsBitmap = new RoaringBitmap();

    public void add(final long x) {
        final int highBits = getHighBits(x);
        final int lowBits = getLowBits(x);

        getOrCreateLowBitsBitmap(highBits).add(lowBits);
        highBitsBitmap.add(highBits);
    }

    public static RoaringBitmap64Bit bitmapOf(final long... dat) {
        final RoaringBitmap64Bit ans = new RoaringBitmap64Bit();
        for (final long i : dat) {
            ans.add(i);
        }
        return ans;
    }

    /**
     * In-place bitwise AND (intersection) operation. The current bitmap is modified.
     *
     * @param bitmap2 other bitmap
     */
    public void and(final RoaringBitmap64Bit bitmap2) {
        final RoaringBitmap highBitsAndResult = RoaringBitmap.and(highBitsBitmap, bitmap2.highBitsBitmap);
        try {
            highLowBitsContainer = forkJoinPool.submit(() ->
                            Arrays.stream(highBitsAndResult.toArray())
                                    .parallel()
                                    .boxed()
                                    .map(highBit -> lowBitsAnd(this, bitmap2, highBit))
                                    .filter(this::notEmptyLowBits)
                                    .collect(Collectors.toMap(Pair::getKey, Pair::getValue))
            ).get();
            refreshHighBitsBitmap();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Unable to do AND operation", e);
            throw new IllegalArgumentException("Unable to do AND operation", e);
        }
    }

    /**
     * In-place bitwise OR (union) operation. The current bitmap is modified.
     *
     * @param bitmap2 other bitmap
     */
    public void or(final RoaringBitmap64Bit bitmap2) {
        highBitsBitmap = RoaringBitmap.or(highBitsBitmap, bitmap2.highBitsBitmap);
        try {
            highLowBitsContainer = forkJoinPool.submit(() ->
                            Arrays.stream(highBitsBitmap.toArray())
                                    .parallel()
                                    .boxed()
                                    .map(highBit -> lowBitsOr(this, bitmap2, highBit))
                                    .collect(Collectors.toMap(Pair::getKey, Pair::getValue))
            ).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Unable to do AND operation", e);
            throw new IllegalArgumentException("Unable to do AND operation", e);
        }
    }

    /**
     * In-place bitwise ANDNOT (difference) operation. The current bitmap is modified.
     *
     * @param bitmap2 other bitmap
     */
    public void andNot(final RoaringBitmap64Bit bitmap2) {
        final List<Integer> highBits = asList(toObject(highBitsBitmap.toArray()));
        final Map<Integer, RoaringBitmap> resultHighLowBitsContainer = new ConcurrentHashMap<>(highBits.size());
        try {
            forkJoinPool.submit(() ->
                    highBits.stream().parallel()
                            .forEach(highBit -> {
                                final RoaringBitmap resultLowBits = highLowBitsContainer.get(highBit);
                                Optional.ofNullable(bitmap2.highLowBitsContainer.get(highBit))
                                        .ifPresent(resultLowBits::andNot);
                                if (resultLowBits.isEmpty()) {
                                    removeFromHighBits(highBit);
                                } else {
                                    resultHighLowBitsContainer.put(highBit, resultLowBits);
                                }
                            })).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Unable to do AND operation", e);
            throw new IllegalArgumentException("Unable to do AND operation", e);
        }
        highLowBitsContainer = resultHighLowBitsContainer;
    }

    public int getCardinality() {
        int result = 0;
        for (final RoaringBitmap lowBits : highLowBitsContainer.values()) {
            result += lowBits.getCardinality();
        }
        return result;
    }

    public List<Long> toList() {
        final List<Long> result = new ArrayList<>(getCardinality());
        for (final Long aLong : this) {
            result.add(aLong);
        }
        return result;
    }

    public Long[] toArray() {
        final Long[] result = new Long[getCardinality()];
        int i = 0;
        for (final Long aLong : this) {
            result[i] = aLong;
            i++;
        }
        return result;
    }

    private void refreshHighBitsBitmap() {
        highBitsBitmap.clear();
        highLowBitsContainer.keySet()
                .forEach(highBitsBitmap::add);
    }


    private boolean notEmptyLowBits(Pair<Integer, RoaringBitmap> bitsPair) {
        return not(bitsPair.getValue().isEmpty());
    }

    private Pair<Integer, RoaringBitmap> lowBitsAnd(RoaringBitmap64Bit bitmap1, RoaringBitmap64Bit bitmap2, Integer highBit) {
        RoaringBitmap lowBits1 = bitmap1.highLowBitsContainer.get(highBit);
        RoaringBitmap lowBits2 = bitmap2.highLowBitsContainer.get(highBit);
        return new Pair<>(highBit, RoaringBitmap.and(lowBits1, lowBits2));
    }

    private Pair<Integer, RoaringBitmap> lowBitsOr(RoaringBitmap64Bit bitmap1, RoaringBitmap64Bit bitmap2, Integer highBit) {
        RoaringBitmap lowBits1 = bitmap1.highLowBitsContainer.get(highBit);
        RoaringBitmap lowBits2 = bitmap2.highLowBitsContainer.get(highBit);
        return new Pair<>(highBit, or(lowBits1, lowBits2));
    }

    private RoaringBitmap or(final RoaringBitmap bitmap1, final RoaringBitmap bitmap2) {
        if (bitmap1 != null && bitmap2 != null) {
            bitmap1.or(bitmap2);
            return bitmap1;
        }
        if (bitmap1 != null) {
            return bitmap1;
        }
        if (bitmap2 != null) {
            return bitmap2;
        }
        throw new IllegalArgumentException("Two bitmaps can't be null");
    }

    private synchronized void removeFromHighBits(final Integer highBit) {
        highBitsBitmap.remove(highBit);
    }

    private RoaringBitmap getOrCreateLowBitsBitmap(final int highBits) {
        return Optional.ofNullable(highLowBitsContainer.get(highBits))
                .orElseGet(() -> {
                    RoaringBitmap lowBits = new RoaringBitmap();
                    highLowBitsContainer.put(highBits, lowBits);
                    return lowBits;
                });
    }

    private int getHighBits(final long x) {
        return (int) (x >>> 32);
    }

    private int getLowBits(final long x) {
        return (int) x;
    }

    private long toUnsignedInt(final int x) {
        return x & 0x00000000FFFFFFFFL;
    }

    private long composeLong(final int highBits, final int lowBits) {
        return (toUnsignedInt(highBits) << 32) | toUnsignedInt(lowBits);
    }

    @Override
    public String toString() {
        final StringBuilder answer = new StringBuilder();
        final Iterator<Long> iterator = this.iterator();
        answer.append("{");
        if (iterator.hasNext()) {
            answer.append(iterator.next());
        }
        while (iterator.hasNext()) {
            answer.append(",");
            answer.append(iterator.next());
        }
        answer.append("}");
        return answer.toString();
    }

    @Override
    public Iterator<Long> iterator() {
        return new Iterator<Long>() {
            private int highBitId = 0;
            private final List<Integer> highBits = new ArrayList<>(highLowBitsContainer.keySet());
            private Iterator<Integer> lowBitsIterator = getLowBitsIterator(highBitId);

            @Override
            public boolean hasNext() {
                int highbitId = highBitId;
                Iterator<Integer> iterator = lowBitsIterator;
                while (highbitId < highBits.size()) {
                    if (iterator.hasNext()) {
                        return true;
                    }
                    highbitId++;
                    if (highbitId < highBits.size()) {
                        iterator = getLowBitsIterator(highbitId);
                    }
                }
                return false;
            }

            @Override
            public Long next() {
                while (highBitId < highBits.size()) {
                    if (lowBitsIterator.hasNext()) {
                        final Integer lowBits = lowBitsIterator.next();
                        return composeLong(highBits.get(highBitId), lowBits);
                    }
                    highBitId++;
                    if (highBitId < highBits.size()) {
                        lowBitsIterator = getLowBitsIterator(highBitId);
                    }
                }
                return null;
            }

            private Iterator<Integer> getLowBitsIterator(final int highBitId) {
                return Optional.ofNullable(highBitId)
                        .filter(v -> !highBits.isEmpty())
                        .filter(v -> !highLowBitsContainer.isEmpty())
                        .map(highBits::get)
                        .map(highLowBitsContainer::get)
                        .map(RoaringBitmap::iterator)
                        .orElse(null);
            }
        };
    }

    @Override
    public void forEach(final Consumer<? super Long> action) {
        throw new UnsupportedOperationException("Method is not implemented yet");
    }

    @Override
    public Spliterator<Long> spliterator() {
        throw new UnsupportedOperationException("Method is not implemented yet");
    }
}
