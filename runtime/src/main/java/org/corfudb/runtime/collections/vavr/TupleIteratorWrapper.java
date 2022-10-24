package org.corfudb.runtime.collections.vavr;

import io.vavr.Tuple2;

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

    private final io.vavr.collection.Iterator<Tuple2<K, V>> iterator;

    public TupleIteratorWrapper(@Nonnull io.vavr.collection.Iterator<Tuple2<K, V>> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Map.Entry<K, V> next() {
        if (hasNext()) {
            Tuple2<K, V> tuple2 = iterator.next();
            return new AbstractMap.SimpleEntry<>(tuple2._1, tuple2._2);
        }
        throw new NoSuchElementException();
    }
}
