package org.corfudb.runtime.collections;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.annotations.Accessor;
import org.corfudb.annotations.ConflictParameter;
import org.corfudb.annotations.CorfuObject;
import org.corfudb.annotations.DontInstrument;
import org.corfudb.annotations.Mutator;
import org.corfudb.annotations.MutatorAccessor;
import org.corfudb.annotations.TransactionalMethod;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@CorfuObject
@Slf4j
public class FCorfuTable<K, V> implements ICorfuMap<K, V> {

    /**
     * Denotes a function that supplies the unique name of an index registered to
     * {@link FCorfuTable}.
     */
    @FunctionalInterface
    public interface IndexName extends Supplier<String> {
    }

    /**
     * Denotes a function that takes as input the key and value of an {@link FCorfuTable}
     * record, and computes the associated index value for the record.
     *
     * @param <K>
     *            type of the record key.
     * @param <V>
     *            type of the record value.
     * @param <I>
     *            type of the index value computed.
     */
    @FunctionalInterface
    public interface IndexFunction<K, V, I extends Comparable<?>> extends BiFunction<K, V, I> {
    }

    /**
     * Descriptor of named {@link IndexFunction} entry.
     *
     * @param <K>
     *            type of the record key associated with {@code IndexKey}.
     * @param <V>
     *            type of the record value associated with {@code IndexKey}.
     * @param <I>
     *            type of the index value computed using the {@code IndexKey}.
     */
    public static class Index<K, V, I extends Comparable<?>> {
        private IndexName name;
        private IndexFunction<K, V, I> indexFunction;

        public Index(IndexName name, IndexFunction<K, V, I> indexFunction) {
            this.name = name;
            this.indexFunction = indexFunction;
        }

        public IndexName getName() {
            return name;
        }

        public IndexFunction<K, V, I> getIndexFunction() {
            return indexFunction;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Index)) return false;
            Index<?, ?, ?> index = (Index<?, ?, ?>) o;
            return Objects.equals(name.get(), index.name.get());
        }

        @Override
        public int hashCode() {
            return Objects.hash(name.get());
        }
    }

    /**
     * Registry hosting of a collection of {@link Index}.
     *
     * @param <K>
     *            type of the record key associated with {@code Index}.
     * @param <V>
     *            type of the record value associated with {@code Index}.
     */
    public interface IndexRegistry<K, V>
            extends Iterable<Index<K, V, ? extends Comparable<?>>> {

        IndexRegistry<?, ?> EMPTY = new IndexRegistry<Object, Object>() {
            @Override
            public <I extends Comparable<?>> Optional<IndexFunction<Object, Object, I>> get(IndexName name) {
                return Optional.empty();
            }

            @Override
            public Iterator<Index<Object, Object, ? extends Comparable<?>>> iterator() {
                return Collections.emptyIterator();
            }
        };

        /**
         * Obtain the {@link IndexFunction} via its registered {@link IndexName}.
         *
         * @param name
         *            name of the {@code IndexKey} previously registered.
         * @param <I>
         *            type of the {@code IndexKey}.
         *
         * @return    the instance of {@link IndexFunction} registered to the lookup name.
         */
        <I extends Comparable<?>> Optional<IndexFunction<K, V, I>> get(IndexName name);

        /**
         * Obtain a static {@link IndexRegistry} with no registered {@link IndexFunction}s.
         *
         * @param <K>
         *            type of the record key associated with {@code Index}.
         * @param <V>
         *            type of the record value associated with {@code Index}.
         *
         * @return    a static instance of {@link IndexRegistry}.
         */
        static <K, V> IndexRegistry<K, V> empty() {
            @SuppressWarnings("unchecked")
            IndexRegistry<K, V> result = (IndexRegistry<K, V>) EMPTY;
            return result;
        }
    }

    private Map<K, V> mainMap = new HashMap<>();
    private Set<Index<K, V, ? extends Comparable>> indexSpec = new HashSet<>();
    private final Map<String, Map<Comparable, Map<K, V>>> secondaryIndexes = new HashMap<>();

    public FCorfuTable(IndexRegistry<K, V> indices) {
        for (Index<K, V, ? extends Comparable> index : indices) {
            secondaryIndexes.put(index.getName().get(), new HashMap<>());
            indexSpec.add(index);
        }
    }

    public FCorfuTable() {
        this(IndexRegistry.empty());
        log.info("Created table without a secondary index.");
    }

    /**
     * Get a mapping using the specified index function.
     *
     * @param indexName      Name of the the secondary index to query.
     * @param indexKey       The index key used to query the secondary index
     * @return               A collection of Map.Entry<K, V>
     */
    @SuppressWarnings("unchecked")
    @Accessor
    public @Nonnull
    <I extends Comparable<I>>
    Collection<Entry<K, V>> getByIndex(@Nonnull IndexName indexName, I indexKey) {
        String name = indexName.get();
        Map<K, V> res = secondaryIndexes.get(name).get(indexKey);
        if (res == null) {
            return Collections.emptySet();
        }

        return secondaryIndexes.get(name).get(indexKey).entrySet();
    }


    /**
     * Scan and filter using the specified index function and projection.
     *
     * @param indexName          Name of the the secondary index to query.
     * @param entryPredicate     The predicate to scan and filter with.
     * @param indexKey           A collection of Map.Entry<K, V>
     * @return
     */
    @Accessor
    public @Nonnull
    <I extends Comparable<I>>
    Collection<Map.Entry<K, V>> getByIndexAndFilter(@Nonnull IndexName indexName,
                                                    @Nonnull Predicate<? super Entry<K, V>>
                                                            entryPredicate,
                                                    I indexKey) {
        Stream<Entry<K, V>> entryStream;

        if (secondaryIndexes.isEmpty()) {
            // If there are no index functions, use the entire map
            entryStream = mainMap.entrySet().parallelStream();
            log.debug("getByIndexAndFilter: Attempted getByIndexAndFilter without indexing");
        } else {
            if (secondaryIndexes.get(indexName.get()).get(indexKey) == null) {
                entryStream = Stream.empty();
            } else {
                entryStream = secondaryIndexes.get(indexName.get()).get(indexKey).entrySet().stream();
            }
        }

        return entryStream.filter(entryPredicate).collect(Collectors.toCollection(ArrayList::new));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Accessor
    public int size() {
        return mainMap.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Accessor
    public boolean isEmpty() {
        return mainMap.isEmpty();
    }

    /**
     * Return whether this table has secondary indexes or not.
     *
     * @return True, if secondary indexes are present. False otherwise.
     */
    @Accessor
    public boolean hasSecondaryIndices() {
        return !secondaryIndexes.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Accessor
    public boolean containsKey(@ConflictParameter Object key) {
        return mainMap.containsKey(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Accessor
    public boolean containsValue(Object value) {
        return mainMap.containsValue(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Accessor
    public V get(@ConflictParameter Object key) {
        return mainMap.get(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @MutatorAccessor(name = "put", undoFunction = "undoPut", undoRecordFunction = "undoPutRecord")
    public V put(@ConflictParameter K key, V value) {
        V previous = mainMap.put(key, value);
        // If we have index functions, update the secondary indexes.
        if (!secondaryIndexes.isEmpty()) {
            unmapSecondaryIndexes(key, previous);
            mapSecondaryIndexes(key, value);
        }
        return previous;
    }

    @DontInstrument
    protected V undoPutRecord(FCorfuTable<K, V> table, K key, V value) {
        return table.mainMap.get(key);
    }

    @DontInstrument
    protected void undoPut(FCorfuTable<K, V> table, V undoRecord, K key, V value) {
        // Same as undoRemove (restore previous value)
        undoRemove(table, undoRecord, key);
    }

    @DontInstrument
    Object[] putAllConflictFunction(Map<? extends K, ? extends V> m) {
        return m.keySet().stream()
                .map(Object::hashCode)
                .toArray(Object[]::new);
    }

    enum UndoNullable {
        NULL;
    }

    /**
     * Generate an undo record for putAll, given the previous state of the map
     * and the parameters to the putAll call.
     *
     * @param previousState The previous state of the map
     * @param m             The map from the putAll call
     * @return An undo record, which for a putAll is all the
     * previous entries in the map.
     */
    @DontInstrument
    @SuppressWarnings("unchecked")
    Map<K, V> undoPutAllRecord(FCorfuTable<K, V> previousState,
                               Map<? extends K, ? extends V> m) {
        ImmutableMap.Builder<K, V> builder = ImmutableMap.builder();
        m.keySet().forEach(k -> builder.put(k,
                (previousState.get(k) == null
                        ? (V) UndoNullable.NULL
                        : previousState.get(k))));
        return builder.build();
    }

    /**
     * Undo a remove, given the current state of the map, an undo record
     * and the arguments to the remove command to undo.
     *
     * @param table      The state of the map after the put to undo
     * @param undoRecord The undo record generated by undoRemoveRecord
     */
    @DontInstrument
    void undoPutAll(FCorfuTable<K, V> table, Map<K, V> undoRecord,
                    Map<? extends K, ? extends V> m) {
        undoRecord.entrySet().forEach(e -> {
                    if (e.getValue() == UndoNullable.NULL) {
                        undoRemove(table, null, e.getKey());
                    } else {
                        undoRemove(table, e.getValue(), e.getKey());
                    }
                }
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Mutator(name = "put", noUpcall = true)
    public void insert(@ConflictParameter K key, V value) {
        V previous = mainMap.put(key, value);
        // If we have index functions, update the secondary indexes.
        if (!secondaryIndexes.isEmpty()) {
            unmapSecondaryIndexes(key, previous);
            mapSecondaryIndexes(key, value);
        }
    }

    /**
     * Returns a filtered {@link List} view of the values contained in this map.
     * This method has a memory/CPU advantage over the map iterators as no deep copy
     * is actually performed.
     *
     * @param p java predicate (function to evaluate)
     * @return a view of the values contained in this map meeting the predicate condition.
     */
    @Accessor
    public List<V> scanAndFilter(Predicate<? super V> p) {
        return mainMap.values().parallelStream()
                .filter(p)
                .collect(Collectors.toCollection(ArrayList::new));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Accessor
    public Collection<Map.Entry<K, V>> scanAndFilterByEntry(Predicate<? super Map.Entry<K, V>>
                                                                    entryPredicate) {
        return mainMap.entrySet().parallelStream()
                .filter(entryPredicate)
                .collect(Collectors.toCollection(ArrayList::new));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @MutatorAccessor(name = "remove", undoFunction = "undoRemove",
            undoRecordFunction = "undoRemoveRecord")
    @SuppressWarnings("unchecked")
    public V remove(@ConflictParameter Object key) {
        V previous = mainMap.remove(key);
        unmapSecondaryIndexes((K) key, previous);
        return previous;
    }

    @DontInstrument
    protected V undoRemoveRecord(FCorfuTable<K, V> table, K key) {
        return table.mainMap.get(key);
    }

    @DontInstrument
    protected void undoRemove(FCorfuTable<K, V> table, V undoRecord, K key) {
        if (undoRecord == null) {
            V previous = table.mainMap.remove(key);
            table.unmapSecondaryIndexes(key, previous);
        } else {
            V previous = table.mainMap.put(key, undoRecord);
            if (!table.secondaryIndexes.isEmpty()) {
                table.unmapSecondaryIndexes(key, previous);
                table.mapSecondaryIndexes(key, undoRecord);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Mutator(name = "remove", noUpcall = true)
    public void delete(@ConflictParameter K key) {
        V previous = mainMap.remove(key);
        unmapSecondaryIndexes(key, previous);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Mutator(name = "putAll",
            undoFunction = "undoPutAll",
            undoRecordFunction = "undoPutAllRecord",
            conflictParameterFunction = "putAllConflictFunction")
    public void putAll(@Nonnull Map<? extends K, ? extends V> m) {
        // If we have no index functions, then just directly put all
        if (secondaryIndexes.isEmpty()) {
            mainMap.putAll(m);
        } else {
            // Otherwise we must update all secondary indexes
            // TODO: Do this in parallel (need to acquire update locks, potentially)
            m.entrySet().stream()
                    .forEach(e -> {
                        V previous = mainMap.put(e.getKey(), e.getValue());
                        unmapSecondaryIndexes(e.getKey(), previous);
                        mapSecondaryIndexes(e.getKey(), e.getValue());
                    });
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Mutator(name = "clear", reset = true)
    public void clear() {
        mainMap.clear();
        secondaryIndexes.values().stream().forEach(m -> m.clear());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Accessor
    public @Nonnull
    Set<K> keySet() {
        return ImmutableSet.copyOf(mainMap.keySet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Accessor
    public @Nonnull
    Collection<V> values() {
        return ImmutableList.copyOf(mainMap.values());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Accessor
    public @Nonnull
    Set<Entry<K, V>> entrySet() {
        return ImmutableSet.copyOf(mainMap.entrySet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Accessor
    public V getOrDefault(Object key, V defaultValue) {
        return mainMap.getOrDefault(key, defaultValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Accessor
    public void forEach(BiConsumer<? super K, ? super V> action) {
        mainMap.forEach(action);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @TransactionalMethod
    public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
        Objects.requireNonNull(function);
        for (Map.Entry<K, V> entry : entrySet()) {
            K k;
            V v;
            try {
                k = entry.getKey();
                v = entry.getValue();
            } catch (IllegalStateException ise) {
                // this usually means the entry is no longer in the map.
                throw new ConcurrentModificationException(ise);
            }

            // ise thrown from function is not a cme.
            v = function.apply(k, v);

            try {
                insert(k, v);
            } catch (IllegalStateException ise) {
                // this usually means the entry is no longer in the map.
                throw new ConcurrentModificationException(ise);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @TransactionalMethod
    public V putIfAbsent(K key, V value) {
        V v = get(key);
        if (v == null) {
            v = put(key, value);
        }

        return v;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @TransactionalMethod
    public boolean remove(Object key, Object value) {
        Object curValue = get(key);
        if (!Objects.equals(curValue, value)
                || (curValue == null && !containsKey(key))) {
            return false;
        }
        remove(key);
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @TransactionalMethod
    public V replace(K key, V value) {
        V curValue;
        if (((curValue = get(key)) != null) || containsKey(key)) {
            curValue = put(key, value);
        }
        return curValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @TransactionalMethod
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {

        Objects.requireNonNull(mappingFunction);
        V v;
        if ((v = get(key)) == null) {
            V newValue;
            if ((newValue = mappingFunction.apply(key)) != null) {
                put(key, newValue);
                return newValue;
            }
        }

        return v;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @TransactionalMethod
    public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V>
            remappingFunction) {
        Objects.requireNonNull(remappingFunction);
        V oldValue;
        if ((oldValue = get(key)) != null) {
            V newValue = remappingFunction.apply(key, oldValue);
            if (newValue != null) {
                put(key, newValue);
                return newValue;
            } else {
                remove(key);
                return null;
            }
        } else {
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @TransactionalMethod
    public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V>
            remappingFunction) {
        Objects.requireNonNull(remappingFunction);
        Objects.requireNonNull(value);
        V oldValue = get(key);
        V newValue = (oldValue == null) ? value :
                remappingFunction.apply(oldValue, value);
        if (newValue == null) {
            remove(key);
        } else {
            put(key, newValue);
        }
        return newValue;
    }

    /**
     * Maps the secondary indexes for a given key value pair.
     *
     * @param key   the primary key associated with the indexing.
     * @param value the value to map.
     */
    protected void mapSecondaryIndexes(K key, V value) {
        if (value != null) {
            // Map entry into secondary indexes
            for (Index<K, V, ? extends Comparable> index : indexSpec) {
                String indexName = index.getName().get();
                Map<Comparable, Map<K, V>> secondaryIndex = secondaryIndexes.get(indexName);
                Comparable indexKey = index.getIndexFunction().apply(key, value);

                Map<K, V> slot = secondaryIndex.computeIfAbsent(indexKey, k -> new HashMap<>());
                slot.put(key, value);
            }
        }
    }

    /**
     * Unmaps the secondary indexes for a given key value pair.
     *
     * @param key   The primary key (index) for the mapping.
     * @param value The value to unmap.
     */
    @SuppressWarnings("unchecked")
    protected void unmapSecondaryIndexes(K key, V value) {

        if (value != null) {
            // Map entry into secondary indexes
            for (Index<K, V, ? extends Comparable> index : indexSpec) {
                String indexName = index.getName().get();
                Map<Comparable, Map<K, V>> secondaryIndex = secondaryIndexes.get(indexName);
                Comparable indexKey = index.indexFunction.apply(key, value);
                Map<K, V> slot = secondaryIndex.get(indexKey);

                if (slot == null) {
                    return;
                }
                slot.remove(key, value);
            }
        }
    }
}