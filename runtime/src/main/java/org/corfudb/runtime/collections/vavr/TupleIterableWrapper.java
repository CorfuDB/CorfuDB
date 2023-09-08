package org.corfudb.runtime.collections.vavr;

import javax.annotation.Nonnull;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;

/**
 * A thin utility wrapper to help efficiently convert from VAVR's
 * Tuple2 Iterable to Java's Map.Entry Iterable.
 * @param <K> The type of the key.
 * @param <V> The type of the value.
 *
 * Created by jielu, munshedm, and zfrenette.
 */
public class TupleIterableWrapper<K, V> implements Iterable<Map.Entry<K, V>> {

    private final Iterator<AbstractMap.SimpleEntry<K, V>> iterator;

    public TupleIterableWrapper(@Nonnull Iterator<AbstractMap.SimpleEntry<K, V>> iterator) {
        this.iterator = iterator;
    }

    @Override
    public void forEach(Consumer<? super Map.Entry<K, V>> action) {
        iterator.forEachRemaining(entry -> action.accept(entry));
    }

    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
        return new TupleIteratorWrapper<>(iterator);
    }

    @Override
    public Spliterator<Map.Entry<K, V>> spliterator() {
        return spliterator(iterator);
    }

    public static <K, V> Spliterator<Map.Entry<K, V>> spliterator(@Nonnull Iterator<AbstractMap.SimpleEntry<K, V>> iterator){
        int characteristics = Spliterator.IMMUTABLE;
        return Spliterators.spliteratorUnknownSize(new TupleIteratorWrapper<>(iterator), characteristics);
    }
}
