package org.corfudb.runtime.collections;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import lombok.Getter;
import org.corfudb.annotations.Accessor;
import org.corfudb.annotations.ConflictParameter;
import org.corfudb.annotations.CorfuObject;
import org.corfudb.annotations.DontInstrument;
import org.corfudb.annotations.Mutator;
import org.corfudb.annotations.MutatorAccessor;

/** The CorfuTable implements a simple key-value store.
 *
 * <p>The primary interface to the CorfuTable is a Map, where the keys must be unique and
 * each key is mapped to exactly one value. Null values are not permitted.
 *
 * <p>The CorfuTable also supports an unlimited number of secondary indexes, which
 * the user provides at construction time as an enum which implements the IndexSpecificaiton
 * interface. An index specification consists of a IndexFunction, which returns a set of secondary
 * keys (indexes) the value should be mapped to. Secondary indexes are many-to-many: values
 * can be mapped to multiple indexes, and indexes can be mapped to multiples values. Each
 * IndexSpecification also specifies a projection function, which specifies a transformation
 * that can be done on a retrieval on the index. A common projection is to emit only the
 * values.
 *
 * @param <K>   The type of the primary key.
 * @param <V>   The type of the values to be mapped.
 * @param <F>   The type of the index specification enumeration.
 * @param <I>   The type of the index.
 */
@CorfuObject
public class CorfuTable<K ,V, F extends Enum<F> & CorfuTable.IndexSpecification, I>
        implements Map<K, V> {

    /**
     * The interface for an indexing function.
     * @param <K> The type of the key used in indexing.
     * @param <V> The type of the value used in indexing.
     * @param <I> The type of index key.
     */
    @FunctionalInterface
    public interface IndexFunction<K, V, I> {
        @Nonnull Collection<I> generateIndex(K k, V v);
    }

    /**
     * The interface for a projection function.
     *
     * <p> NOTE: The projection function MUST return a new (preferably immutable) collection,
     * the collection of entries passed during this function are NOT safe to use
     * outside the context of this function.
     *
     * @param <K>   The type of the key used in projection.
     * @param <V>   The type of the value used in projection.
     * @param <I>   The type of the index used in projection.
     * @param <P>   The type of the projection returned.
     */
    @FunctionalInterface
    public interface ProjectionFunction<K, V, I, P> {
        @Nonnull
        Stream<P> generateProjection(I index,
                                     @Nonnull Stream<Map.Entry<K, V>> entryStream);
    }

    /**
     * The interface for a index specification, which consists of a indexing function
     * and a projection functino.
     * @param <K>   The type of the primary key on the map.
     * @param <V>   The type of the value on the map.
     * @param <I>   The type of the index.
     * @param <P>   The type returned by the projection function.
     */
    public interface IndexSpecification<K, V, I, P> {
        IndexFunction<K, V, I> getIndexFunction();
        ProjectionFunction<K, V, I, P> getProjectionFunction();
    }

    /** An index specification enumeration which has no index specifications.
     *  Using this enumeration effectively disables secondary indexes.
     */
    enum NoSecondaryIndex implements IndexSpecification {
        ;

        @Override
        public IndexFunction getIndexFunction() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ProjectionFunction getProjectionFunction() {
            throw new UnsupportedOperationException();
        }
    }

    /** The "main" map which contains the primary key-value mappings. */
    protected final Map<K,V> mainMap = new HashMap<>();

    protected Set<F> indexFunctions;

    protected Map<F, Multimap<I, Map.Entry<K,V>>> indexMap;

    /** Generate a table with the given set of indexes. */
    public CorfuTable(Class<F> indexFunctionEnumClass) {
        indexerClass = indexFunctionEnumClass;
        indexMap = new HashMap<>();
        indexFunctions = EnumSet.allOf(indexFunctionEnumClass);
        indexFunctions.forEach(f -> indexMap.put(f, ArrayListMultimap.create()));
    }

    /** Default constructor. Generates a table without any secondary indexes. */
    public CorfuTable() {
        indexFunctions = Collections.emptySet();
        indexMap = Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public int size() {
        return mainMap.size();
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public boolean isEmpty() {
        return mainMap.isEmpty();
    }

    @Accessor
    public boolean hasIndices() {return !indexFunctions.isEmpty();}

    @Getter
    public Class indexerClass;

    /** {@inheritDoc} */
    @Override
    @Accessor
    public boolean containsKey(@ConflictParameter Object key) {
        return mainMap.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public boolean containsValue(Object value) {
        return mainMap.containsValue(value);
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public V get(@ConflictParameter Object key) {
        return mainMap.get(key);
    }



    /** Get a mapping using the specified index function.
     *
     * @param indexFunction The index function to use.
     * @param index         The index
     * @return
     */
    @SuppressWarnings("unchecked")
    public Collection<Object> getByIndex(@Nonnull F indexFunction, I index) {
        return (Collection<Object>) indexFunction.getProjectionFunction()
                                .generateProjection(index, indexMap
                                .get(indexFunction).get(index).parallelStream())
                                .collect(Collectors.toList());
    }

    /**
     * Register new index class
     *
     * This replaces the current index.
     *
     * @param indexFunctionEnumClass
     */
    public void registerIndex(Class indexFunctionEnumClass) {
        indexerClass = indexFunctionEnumClass;
        indexMap = new HashMap<>();
        indexFunctions = EnumSet.allOf(indexFunctionEnumClass);
        indexFunctions.forEach(f -> indexMap.put(f, ArrayListMultimap.create()));
        mainMap.forEach(this::mapSecondaryIndexes);
    }

    /** {@inheritDoc} */
    @Override
    @MutatorAccessor(name = "put", undoFunction = "undoPut", undoRecordFunction = "undoPutRecord")
    public V put(@ConflictParameter K key, V value) {
        V previous = mainMap.put(key, value);
        // If we have index functions, update the secondary indexes.
        if (indexFunctions.size() > 0) {
            unmapSecondaryIndexes(key, previous);
            mapSecondaryIndexes(key, value);
        }
        return previous;
    }

    @DontInstrument
    protected V undoPutRecord(CorfuTable<K, V, F, I> table, K key, V value) {
        return table.mainMap.get(key);
    }

    @DontInstrument
    protected void undoPut(CorfuTable<K, V, F, I> table, V undoRecord, K key, V value) {
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

    /** Generate an undo record for putAll, given the previous state of the map
     * and the parameters to the putAll call.
     *
     * @param previousState     The previous state of the map
     * @param m                 The map from the putAll call
     * @return                  An undo record, which for a putAll is all the
     *                          previous entries in the map.
     */
    @DontInstrument
    @SuppressWarnings("unchecked")
    Map<K,V> undoPutAllRecord(CorfuTable<K, V, F, I> previousState,
                              Map<? extends K, ? extends V> m) {
        ImmutableMap.Builder<K,V> builder = ImmutableMap.builder();
        m.keySet().forEach(k -> builder.put(k,
                (previousState.get(k) == null
                        ? (V) CorfuTable.UndoNullable.NULL
                        : previousState.get(k))));
        return builder.build();
    }

    /** Undo a remove, given the current state of the map, an undo record
     * and the arguments to the remove command to undo.
     *
     * @param table           The state of the map after the put to undo
     * @param undoRecord    The undo record generated by undoRemoveRecord
     */
    @DontInstrument
     void undoPutAll(CorfuTable<K, V, F, I> table, Map<K,V> undoRecord,
                            Map<? extends K, ? extends V> m) {
        undoRecord.entrySet().forEach(e -> {
                    if (e.getValue() == CorfuTable.UndoNullable.NULL) {
                        undoRemove(table, null, e.getKey());
                    } else {
                        undoRemove(table, e.getValue(), e.getKey());
                    }
                }
        );
    }

    @Mutator(name = "put", noUpcall = true)
    void insert(@ConflictParameter K key, V value) {
        V previous = mainMap.put(key, value);
        // If we have index functions, update the secondary indexes.
        if (indexFunctions.size() > 0) {
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
        return mainMap.values().parallelStream().filter(p).collect(Collectors.toList());
    }

    /**
     * Returns a {@link Collection} filtered by entries (keys and/or values).
     * This method has a memory/CPU advantage over the map iterators as no deep copy
     * is actually performed.
     *
     * @param entryPredicate java predicate (function to evaluate)
     * @return a view of the entries contained in this map meeting the predicate condition.
     */
    @Accessor
    public Collection<Map.Entry<K, V>> scanAndFilterByEntry(Predicate<? super Map.Entry<K, V>>
                                                                    entryPredicate) {
        return mainMap.entrySet().parallelStream().filter(entryPredicate).collect(Collectors
                .toCollection(ArrayList::new));
    }

    /** {@inheritDoc} */
    @Override
    @MutatorAccessor(name = "remove", undoFunction = "undoRemove",
                                undoRecordFunction = "undoRemoveRecord")
    @SuppressWarnings("unchecked")
    public V remove(@ConflictParameter Object key) {
        V previous =  mainMap.remove(key);
        unmapSecondaryIndexes((K) key, previous);
        return previous;
    }

    @DontInstrument
    protected V undoRemoveRecord(CorfuTable<K, V, F, I> table, K key) {
        return table.mainMap.get(key);
    }

    @DontInstrument
    protected void undoRemove(CorfuTable<K, V, F, I> table, V undoRecord, K key) {
        if (undoRecord == null) {
            V previous =  table.mainMap.remove(key);
            table.unmapSecondaryIndexes(key, previous);
        } else {
            V previous = table.mainMap.put(key, undoRecord);
            if (table.indexFunctions.size() > 0) {
                table.unmapSecondaryIndexes(key, previous);
                table.mapSecondaryIndexes(key, undoRecord);
            }
        }
    }

    @Mutator(name = "remove", noUpcall = true)
    void delete(@ConflictParameter K key) {
        V previous =  mainMap.remove(key);
        unmapSecondaryIndexes(key, previous);
    }

    /** {@inheritDoc} */
    @Override
    @Mutator(name = "putAll",
            undoFunction = "undoPutAll",
            undoRecordFunction = "undoPutAllRecord",
            conflictParameterFunction = "putAllConflictFunction")
    public void putAll(@Nonnull Map<? extends K, ? extends V> m) {
        // If we have no index functions, then just directly put all
        if (indexFunctions.isEmpty()) {
            mainMap.putAll(m);
        }
        // Otherwise we must update all secondary indexes
        // TODO: Do this in parallel (need to acquire update locks, potentially)
        m.entrySet().stream()
                .forEach(e -> {
                    V previous = mainMap.put(e.getKey(), e.getValue());
                    unmapSecondaryIndexes(e.getKey(), previous);
                    mapSecondaryIndexes(e.getKey(), e.getValue());
                });
    }

    /** {@inheritDoc} */
    @Override
    @Mutator(name = "clear", reset = true)
    public void clear() {
        mainMap.clear();
        indexMap.values().parallelStream().forEach(m -> m.clear());
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public @Nonnull Set<K> keySet() {
        return ImmutableSet.copyOf(mainMap.keySet());
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public @Nonnull Collection<V> values() {
        return ImmutableList.copyOf(mainMap.values());
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public @Nonnull Set<Entry<K, V>> entrySet() {
        return ImmutableSet.copyOf(mainMap.entrySet());
    }

    /** Unmaps the secondary indexes for a given key value pair.
     *
     * @param key           The primary key (index) for the mapping.
     * @param value         The value to unmap.
     */
    protected void unmapSecondaryIndexes(K key, V value) {
        if (value != null) {
            final Map.Entry<K, V> previousEntry
                    = new AbstractMap.SimpleImmutableEntry<>(key, value);
            indexFunctions.parallelStream()
                    .forEach(f -> f.getIndexFunction().generateIndex(key, value)
                            .forEach(i -> indexMap.get(f).remove((I) i, previousEntry)));

        }
    }

    /** Maps the secondary indexes for a given key value pair.
     *
     * @param key       The primary key (index) for the mapping.
     * @param value     The value to map.
     */
    @SuppressWarnings("unchecked")
    protected void mapSecondaryIndexes(K key, V value) {
        Map.Entry<K, V> entry = new AbstractMap.SimpleImmutableEntry<>(key, value);
        indexFunctions.parallelStream()
                .forEach(f -> f.getIndexFunction().generateIndex(key, value)
                        .forEach(i -> indexMap.get(f).put((I) i, entry)));
    }

}
