package org.corfudb.runtime.collections;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.annotations.Accessor;
import org.corfudb.annotations.ConflictParameter;
import org.corfudb.annotations.CorfuObject;
import org.corfudb.annotations.DontInstrument;
import org.corfudb.annotations.Mutator;
import org.corfudb.annotations.MutatorAccessor;
import org.corfudb.annotations.PassThrough;
import org.corfudb.annotations.TransactionalMethod;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.util.ImmutableListSetWrapper;
import org.corfudb.protocols.logprotocol.SMRRecordLocator;
import org.corfudb.runtime.object.ICorfuExecutionContext;
import org.corfudb.runtime.object.ICorfuVersionPolicy;

/** The CorfuTable implements a simple key-value store.
 *
 * <p>The primary interface to the CorfuTable is a Map, where the keys must be unique and
 * each key is mapped to exactly one value. Null values are not permitted.
 *
 * <p>The CorfuTable also supports an unlimited number of secondary indexes, which
 * the user provides at construction time as an enum which implements the IndexSpecification
 * interface. An index specification consists of a IndexFunction, which returns a set of secondary
 * keys (indexes) the value should be mapped to. Secondary indexes are many-to-many: values
 * can be mapped to multiple indexes, and indexes can be mapped to multiples values. Each
 * IndexSpecification also specifies a projection function, which specifies a transformation
 * that can be done on a retrieval on the index. A common projection is to emit only the
 * values.
 *
 * @param <K>   The type of the primary key.
 * @param <V>   The type of the values to be mapped.
 */
@Slf4j
@CorfuObject
public class CorfuTable<K ,V> implements
        ICorfuTable<K, V>, ICorfuSMR<CorfuTable<K, V>> {

    // Accessor/Mutator threads can interleave in a way that create a deadlock because they can create a
    // circular dependency between the VersionLockedObject(VLO) lock and the common forkjoin thread pool. In order
    // to break the dependency, parallel stream operations have to execute on a separate pool that applications
    // cant use. For example, if there are 4 accessor threads all using the common forkjoin pool, one of the threads
    // can acquire the VLO lock and cause the other 3 threads to wait, but after acquiring the VLO lock, the thread
    // gets block on parallel stream, because the pool is exhausted with threads that are trying to acquire the VLO
    // look, which creates a circular dependency. In other words, a deadlock.
    private final static ForkJoinPool pool = new ForkJoinPool(Runtime.getRuntime().availableProcessors() - 1,
            pool -> {
                final ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                worker.setName("CorfuTable-Forkjoin-pool-" + worker.getPoolIndex());
                return worker;
            }, null, true);

    // The "main" map which contains the primary key-value mappings.
    private final ContextAwareMap<K,V> mainMap;
    private final Set<Index.Spec<K, V, ? extends Comparable>> indexSpec;
    private final Map<String, Map<Comparable, Map<K, V>>> secondaryIndexes;
    private final CorfuTable<K, V> optimisticTable;
    private final VersionPolicy versionPolicy;

    @Getter
    private ILocatorStore<K> locatorStore = new MapLocatorStore<>();

    public CorfuTable(ContextAwareMap<K,V> mainMap,
                      Set<Index.Spec<K, V, ? extends Comparable>> indexSpec,
                      Map<String, Map<Comparable, Map<K, V>>> secondaryIndexe,
                      CorfuTable<K, V> optimisticTable) {
        this.mainMap = mainMap;
        this.indexSpec = indexSpec;
        this.secondaryIndexes = secondaryIndexe;
        this.optimisticTable = optimisticTable;
        this.versionPolicy = ICorfuVersionPolicy.DEFAULT;
    }

    /**
     * The main constructor that generates a table with a given implementation of the
     * {@link StreamingMap} along with {@link Index.Registry} and {@link VersionPolicy}
     * specification.
     */
    public CorfuTable(Index.Registry<K, V> indices,
                      Supplier<ContextAwareMap<K, V>> streamingMapSupplier,
                      VersionPolicy versionPolicy) {
        this.indexSpec = new HashSet<>();
        this.secondaryIndexes = new HashMap<>();
        this.mainMap = streamingMapSupplier.get();
        this.versionPolicy = versionPolicy;

        this.optimisticTable = new CorfuTable<>(this.mainMap.getOptimisticMap(), this.indexSpec,
                this.secondaryIndexes, null);

        indices.forEach(index -> {
            secondaryIndexes.put(index.getName().get(), new HashMap<>());
            indexSpec.add(index);
        });

        if (!secondaryIndexes.isEmpty()) {
            log.info(
                "CorfuTable: creating CorfuTable with the following indexes: {}",
                secondaryIndexes.keySet()
            );
        }
    }

    /**
     * Generate a table with a given implementation for the {@link StreamingMap} and
     * {@link Index.Registry}.
     */
    public CorfuTable(Index.Registry<K, V> indices,
                      Supplier<ContextAwareMap<K, V>> streamingMapSupplier) {
        this(indices, streamingMapSupplier, ICorfuVersionPolicy.DEFAULT);
    }

    /**
     * Generate a table with a given implementation for the {@link StreamingMap},
     * and {@link VersionPolicy}.
     */
    public CorfuTable(Supplier<ContextAwareMap<K, V>> streamingMapSupplier,
                      VersionPolicy versionPolicy) {
        this(Index.Registry.empty(), streamingMapSupplier, versionPolicy);
    }

    /**
     * Generate a table with the given {@link Index.Registry}.
     */
    public CorfuTable(Index.Registry<K, V> indexRegistry) {
        this(indexRegistry, () -> new StreamingMapDecorator<>(new HashMap<>()),
                ICorfuVersionPolicy.DEFAULT);
    }

    /**
     * Default constructor. Generates a table without any secondary indexes.
     */
    public CorfuTable() {
        this(Index.Registry.empty());
    }

    /** Helper function to get a map (non-secondary index) Corfu table.
     *
     * @param <K>           Key type
     * @param <V>           Value type
     * @return              A type token to pass to the builder.
     */
    static <K, V> TypeToken<CorfuTable<K, V>> getMapType() {
        return new TypeToken<CorfuTable<K, V>>() {};
    }

    /** Helper function to get a Corfu Table.
     *
     * @param <K>                   Key type
     * @param <V>                   Value type
     * @return                      A type token to pass to the builder.
     */
    static <K, V> TypeToken<CorfuTable<K, V>> getTableType() {
        return new TypeToken<CorfuTable<K, V>>() {};
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

    /** Return whether this table has secondary indexes or not.
     *
     * @return  True, if secondary indexes are present. False otherwise.
     */
    @Accessor
    public boolean hasSecondaryIndices() {
        return !secondaryIndexes.isEmpty();
    }

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

    /**
     * Get a mapping using the specified index function.
     *
     * @param indexName Name of the the secondary index to query.
     * @param indexKey  The index key used to query the secondary index
     * @return A collection of Map.Entry<K, V>
     */
    @SuppressWarnings("unchecked")
    @Accessor
    public @Nonnull
    <I extends Comparable<I>>
    Collection<Entry<K, V>> getByIndex(@Nonnull Index.Name indexName, I indexKey) {
        String secondaryIndex = indexName.get();
        Map<Comparable, Map<K, V>> secondaryMap;
        if (secondaryIndexes.containsKey(secondaryIndex) &&
                ((secondaryMap = secondaryIndexes.get(secondaryIndex)) != null)) {
            // If secondary index exists and function for this index is not null
            Map<K, V> res = secondaryMap.get(indexKey);

            return res == null ?
                    Collections.emptySet() :
                    new HashSet<>(res.entrySet());
        }

        // If index is not specified, the lookup by index API must fail.
        log.error("CorfuTable: secondary index " + secondaryIndex + " does not exist for this table, cannot complete the get by index.");
        throw new IllegalArgumentException("Secondary Index " + secondaryIndex + " is not defined.");
    }

    /**
     * Scan and filter using the specified index function and projection.
     *
     * @param indexName      Name of the the secondary index to query.
     * @param entryPredicate The predicate to scan and filter with.
     * @param indexKey       A collection of Map.Entry<K, V>
     * @return
     */
    @Accessor
    public @Nonnull
    <I extends Comparable<I>>
    Collection<Map.Entry<K, V>> getByIndexAndFilter(@Nonnull Index.Name indexName,
                                                    @Nonnull Predicate<? super Entry<K, V>>
                                                            entryPredicate,
                                                    I indexKey) {
        Stream<Entry<K, V>> entryStream;
        String secondaryIndex = indexName.get();
        Map<Comparable, Map<K, V>> secondaryMap;

        if (secondaryIndexes.containsKey(secondaryIndex) &&
                ((secondaryMap = secondaryIndexes.get(secondaryIndex)) != null)) {
            if (secondaryMap.get(indexKey) == null) {
                entryStream = Stream.empty();
            } else {
                entryStream = secondaryMap.get(indexKey).entrySet().stream();
            }

            return entryStream.filter(entryPredicate).collect(Collectors.toCollection(ArrayList::new));
        }

        // If there are is no index function available, fail
        log.error("CorfuTable: secondary index " + secondaryIndex + " does not exist for this table, cannot complete the get by index and filter.");
        throw new IllegalArgumentException("Secondary Index " + secondaryIndex + " is not defined.");
    }

    /** {@inheritDoc} */
    @Override
    @MutatorAccessor(name = "put", undoFunction = "undoPut", undoRecordFunction = "undoPutRecord",
            garbageIdentifyFunction = "identifyPutGarbage")
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
    protected V undoPutRecord(CorfuTable<K, V> table, K key, V value) {
        return table.mainMap.get(key);
    }

    @DontInstrument
    protected void undoPut(CorfuTable<K, V> table, V undoRecord, K key, V value) {
        // Same as undoRemove (restore previous value)
        undoRemove(table, undoRecord, key);
    }

    @DontInstrument
    protected List<Object> identifyPutGarbage(Object locator, K key, V value) {
        return new ArrayList<>(locatorStore.addUnsafe(key, (SMRRecordLocator) locator));
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
    Map<K,V> undoPutAllRecord(CorfuTable<K, V> previousState,
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
    void undoPutAll(CorfuTable<K, V> table, Map<K,V> undoRecord,
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

    /** {@inheritDoc} */
    @Override
    @Mutator(name = "put", noUpcall = true)
    public void insert(@ConflictParameter K key, V value) {
        // This is just a stub, the annotation processor will generate an update with
        // put(key, value), since this method doesn't require an upcall therefore no
        // operations are needed to be executed on the internal data structure
    }

    /**
     * Returns a filtered {@link List} view of the values contained in this map.
     * This method has a memory/CPU advantage over the map iterators as no deep copy
     * is actually performed.
     *
     * @param valuePredicate java predicate (function to evaluate)
     * @return a view of the values contained in this map meeting the predicate condition.
     */
    @Accessor
    public List<V> scanAndFilter(Predicate<? super V> valuePredicate) {
        return pool.submit(() -> mainMap.entryStream()
                .map(Entry::getValue).filter(valuePredicate)
                .collect(Collectors.toCollection(ArrayList::new))).join();
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public Collection<Map.Entry<K, V>> scanAndFilterByEntry(
            Predicate<? super Map.Entry<K, V>> entryPredicate) {
        return pool.submit(() -> mainMap.entryStream()
                .filter(entryPredicate)
                .collect(Collectors.toCollection(ArrayList::new))).join();
    }

    /** {@inheritDoc} */
    @Override
    @MutatorAccessor(name = "remove", undoFunction = "undoRemove", undoRecordFunction = "undoRemoveRecord",
            garbageIdentifyFunction = "identifyRemoveGarbage")
    @SuppressWarnings("unchecked")
    public V remove(@ConflictParameter Object key) {
        V previous =  mainMap.remove(key);
        unmapSecondaryIndexes((K) key, previous);
        return previous;
    }

    @DontInstrument
    protected V undoRemoveRecord(CorfuTable<K, V> table, K key) {
        return table.mainMap.get(key);
    }

    @DontInstrument
    protected void undoRemove(CorfuTable<K, V> table, V undoRecord, K key) {
        if (undoRecord == null) {
            V previous =  table.mainMap.remove(key);
            table.unmapSecondaryIndexes(key, previous);
        } else {
            V previous = table.mainMap.put(key, undoRecord);
            if (!table.secondaryIndexes.isEmpty()) {
                table.unmapSecondaryIndexes(key, previous);
                table.mapSecondaryIndexes(key, undoRecord);
            }
        }
    }

    @DontInstrument
    protected List<Object> identifyRemoveGarbage(Object locator, K key) {
        // TODO(Xin): Distinguish put and remove in the future
        return new ArrayList<>(locatorStore.addUnsafe(key, (SMRRecordLocator) locator));
    }

    /** {@inheritDoc} */
    @Override
    @Mutator(name = "remove", noUpcall = true)
    public void delete(@ConflictParameter K key) {
        // This is just a stub, the annotation processor will generate an update with
        // remove(key), since this method doesn't require an upcall therefore no
        // operations are needed to be executed on the internal data structure
    }

    /** {@inheritDoc} */
    @Override
    @TransactionalMethod
    public void putAll(@Nonnull Map<? extends K, ? extends V> m) {
        m.entrySet().stream().forEach(entry -> put(entry.getKey(), entry.getValue()));
    }

    /** {@inheritDoc} */
    @Override
    @Mutator(name = "clear", garbageIdentificationFunction = "identifyClearGarbage", reset = true)
    public void clear() {
        mainMap.clear();
        secondaryIndexes.values().forEach(Map::clear);
    }

    @DontInstrument
    protected List<Object> identifyClearGarbage(Object locator) {
        return new ArrayList<>(locatorStore.clearUnsafe((SMRRecordLocator) locator));
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public @Nonnull Set<K> keySet() {
        return mainMap.keySet()
                .stream()
                .collect(ImmutableSet.toImmutableSet());
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public @Nonnull Collection<V> values() {
        return mainMap.values()
                .stream()
                .collect(ImmutableSet.toImmutableSet());
    }

    /**
     * Makes a shallow copy of the map (i.e. all map entries), but not the
     * key/values (i.e only the mappings are copied).
     **/
    @Override
    @Accessor
    public @Nonnull Set<Entry<K, V>> entrySet() {
        return ImmutableListSetWrapper.fromMap(mainMap);
    }

    /**
     * Present the content of a {@link CorfuTable} via the {@link Stream} interface.
     * Because the stream can point to other resources managed off-heap, its necessary
     * to explicitly close it after consumption.
     *
     * @return stream of entries
     */
    @Accessor
    public @Nonnull Stream<Entry<K, V>> entryStream() {
        return mainMap.entryStream();
    }


    /** {@inheritDoc} */
    @Override
    @Accessor
    public V getOrDefault(Object key, V defaultValue) {
        return mainMap.getOrDefault(key, defaultValue);
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public void forEach(BiConsumer<? super K, ? super V> action) {
        mainMap.forEach(action);
    }

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
    @Override
    @TransactionalMethod
    public V putIfAbsent(K key, V value) {
        V v = get(key);
        if (v == null) {
            v = put(key, value);
        }

        return v;
    }

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
    @Override
    @TransactionalMethod
    public V replace(K key, V value) {
        V curValue;
        if (((curValue = get(key)) != null) || containsKey(key)) {
            curValue = put(key, value);
        }
        return curValue;
    }

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
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
     * Unmaps the secondary indexes for a given key value pair.
     *
     * @param key   The primary key (index) for the mapping.
     * @param value The value to unmap.
     */
    @DontInstrument
    @SuppressWarnings("unchecked")
    protected void unmapSecondaryIndexes(K key, V value) {
        if (value == null) {
            return;
        }

        try {
            // Map entry into secondary indexes
            for (Index.Spec<K, V, ? extends Comparable> index : indexSpec) {
                String indexName = index.getName().get();
                Map<Comparable, Map<K, V>> secondaryIndex = secondaryIndexes.get(indexName);
                for (Comparable indexKey : index.getMultiValueIndexFunction().apply(key, value)) {
                    Map<K, V> slot = secondaryIndex.get(indexKey);
                    if (slot != null) {
                        slot.remove(key, value);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Received an exception while computing the index. " +
                    "This is most likely an issue with the client's indexing function. {}", e);
            // The index might be corrupt, and the only way to ensure safety is to disable
            // indexing for this table.
            clearIndex();
            // In case of both a transactional and non-transactional operation, the client
            // is going to receive UnrecoverableCorfuError along with the appropriate cause.
            throw e;
        }
    }

    /**
     * Maps the secondary indexes for a given key value pair.
     *
     * @param key   the primary key associated with the indexing.
     * @param value the value to map.
     */
    @DontInstrument
    @SuppressWarnings("unchecked")
    protected void mapSecondaryIndexes(K key, V value) {
        if (value == null) {
            log.warn("Attempting to build an index with a null value, skipping.");
            return;
        }

        try {
            // Map entry into secondary indexes
            for (Index.Spec<K, V, ? extends Comparable> index : indexSpec) {
                String indexName = index.getName().get();
                Map<Comparable, Map<K, V>> secondaryIndex = secondaryIndexes.get(indexName);
                for (Comparable indexKey : index.getMultiValueIndexFunction().apply(key, value)) {
                    Map<K, V> slot = secondaryIndex.computeIfAbsent(indexKey, k -> new HashMap<>());
                    slot.put(key, value);
                }
            }
        } catch (Exception e) {
            log.error("Received an exception while computing the index. " +
                    "This is most likely an issue with the client's indexing function.", e);
            // The index might be corrupt, and the only way to ensure safety is to disable
            // indexing for this table.
            clearIndex();
            // In case of both a transactional and non-transactional operation, the client
            // is going to receive UnrecoverableCorfuError along with the appropriate cause.
            throw e;
        }
    }

    /**
     *  Disable all secondary indices for this table. Only used during error-recovery.
     */
    @DontInstrument
    protected void clearIndex() {
        indexSpec.clear();
        secondaryIndexes.clear();
    }

    /**
     * {@inheritDoc}
     */
    @PassThrough
    @Override
    public CorfuTable<K, V> getContext(ICorfuExecutionContext.Context context) {
        if (context == OPTIMISTIC) {
            return optimisticTable;
        } else {
            return this;
        }
    }

    /**
     * {@inheritDoc}
     */
    @DontInstrument
    @Override
    public VersionPolicy getVersionPolicy() {
        return versionPolicy;
    }

    /**
     * {@inheritDoc}
     */
    @PassThrough
    @Override
    public void close() {
        this.mainMap.close();
    }

    /**
     * {@inheritDoc}
     */
    @DontInstrument
    @Override
    public void closeWrapper() {
        this.mainMap.close();
    }
}
