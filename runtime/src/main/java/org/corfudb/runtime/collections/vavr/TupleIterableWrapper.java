package org.corfudb.runtime.collections.vavr;

import io.vavr.Tuple2;
import io.vavr.collection.Traversable;

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

    private final Traversable<Tuple2<K, V>> traversable;

    public TupleIterableWrapper(@Nonnull Traversable<Tuple2<K, V>> traversable) {
        this.traversable = traversable;
    }

    @Override
    public void forEach(Consumer<? super Map.Entry<K, V>> action) {
        traversable.forEach(tuple2 ->
                action.accept(new AbstractMap.SimpleEntry<>(tuple2._1, tuple2._2)));
    }

    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
        return new TupleIteratorWrapper<>(traversable.iterator());
    }

    @Override
    public Spliterator<Map.Entry<K, V>> spliterator() {
        return spliterator(traversable);
    }

    public static <K, V> Spliterator<Map.Entry<K, V>> spliterator(@Nonnull Traversable<Tuple2<K, V>> traversable){
        int characteristics = Spliterator.IMMUTABLE;
        if (traversable.isDistinct()) {
            characteristics |= Spliterator.DISTINCT;
        }
        if (traversable.isOrdered()) {
            characteristics |= (Spliterator.SORTED | Spliterator.ORDERED);
        }
        if (traversable.isSequential()) {
            characteristics |= Spliterator.ORDERED;
        }
        if (traversable.hasDefiniteSize()) {
            characteristics |= (Spliterator.SIZED | Spliterator.SUBSIZED);
            return Spliterators.spliterator(new TupleIteratorWrapper<>(traversable.iterator()), traversable.length(), characteristics);
        } else {
            return Spliterators.spliteratorUnknownSize(new TupleIteratorWrapper<>(traversable.iterator()), characteristics);
        }
    }
}
