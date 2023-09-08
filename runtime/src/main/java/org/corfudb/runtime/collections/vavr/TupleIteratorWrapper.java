package org.corfudb.runtime.collections.vavr;

import javax.annotation.Nonnull;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * A thin utility wrapper to help efficiently convert from VAVR's
 * Tuple2 Iterator to Java's Map.Entry Iterator.
 * @param <K> The type of the key.
 * @param <V> The type of the value.
 *
 * Created by jielu, munshedm, and zfrenette.
 */
public class TupleIteratorWrapper<K, V> implements Iterator<Map.Entry<K, V>> {

    private final Iterator<AbstractMap.SimpleEntry<K, V>> iterator;

    public TupleIteratorWrapper(@Nonnull Iterator<AbstractMap.SimpleEntry<K, V>> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Map.Entry<K, V> next() {
        if (hasNext()) {
            AbstractMap.SimpleEntry<K, V> tuple2 = iterator.next();
            return new AbstractMap.SimpleEntry<>(tuple2.getKey(), tuple2.getValue());
        }
        throw new NoSuchElementException();
    }
}
