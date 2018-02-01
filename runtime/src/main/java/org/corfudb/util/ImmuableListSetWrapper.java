package org.corfudb.util;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * An immutable set implementation that is backed by a list. This type is useful when
 * we have lists that we know conform to the set constraints, but creating the set is
 * too expensive.
 *
 */

public class ImmuableListSetWrapper<E> implements Set<E> {
    final List<E> internalList;

    public ImmuableListSetWrapper(@Nonnull List<E> internalList) {
        this.internalList = internalList;
    }

    @Override
    public int size() {
        return internalList.size();
    }

    @Override
    public boolean isEmpty() {
        return internalList.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return internalList.contains(o);
    }

    @Override
    public Iterator<E> iterator() {
        return internalList.iterator();
    }

    @Override
    public void forEach(Consumer<? super E> action) {
        internalList.forEach(action);
    }

    @Override
    public Object[] toArray() {
        return internalList.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return internalList.toArray(a);
    }

    @Override
    public boolean add(E e) {
        throw new UnsupportedOperationException("Immutable set can't add");
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException("Immutable set can't remove");
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return internalList.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        throw new UnsupportedOperationException("Immutable set can't add");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("Immutable set can't remove");
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException("Immutable set can't remove");
    }

    @Override
    public boolean removeIf(Predicate<? super E> filter) {
        throw new UnsupportedOperationException("Immutable set can't remove");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Immutable set can't clear");
    }

    @Override
    public Spliterator<E> spliterator() {
        return internalList.spliterator();
    }

    @Override
    public Stream<E> stream() {
        return internalList.stream();
    }

    @Override
    public Stream<E> parallelStream() {
        return internalList.stream();
    }
}
