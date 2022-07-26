package org.corfudb.runtime.collections;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.annotations.Accessor;
import org.corfudb.annotations.ConflictParameter;
import org.corfudb.annotations.CorfuObject;
import org.corfudb.annotations.DontInstrument;
import org.corfudb.annotations.Mutator;
import org.corfudb.annotations.MutatorAccessor;
import org.corfudb.annotations.PassThrough;
import org.corfudb.runtime.object.ICorfuExecutionContext;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.ICorfuVersionPolicy;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

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
public class CorfuTable<K, V> implements ICorfuTable<K, V>, ICorfuSMR<CorfuTable<K, V>> {

    // The "main" map which contains the primary key-value mappings.
    private final ContextAwareMap<K, V> mainMap;
    private final Set<Index.Spec<K, V, ?>> indexSpec;
    private final Map<String, Map<Object, Map<K, V>>> secondaryIndexes;
    private final Map<String, String> secondaryIndexesAliasToPath;
    private final CorfuTable<K, V> optimisticTable;
    private final VersionPolicy versionPolicy;

    public CorfuTable(ContextAwareMap<K, V> mainMap,
                      Set<Index.Spec<K, V, ?>> indexSpec,
                      Map<String, Map<Object, Map<K, V>>> secondaryIndexes,
                      CorfuTable<K, V> optimisticTable) {
        this.mainMap = mainMap;
        this.indexSpec = indexSpec;
        this.secondaryIndexes = secondaryIndexes;
        this.secondaryIndexesAliasToPath = new HashMap<>();
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
        this.secondaryIndexesAliasToPath = new HashMap<>();
        this.mainMap = streamingMapSupplier.get();
        this.versionPolicy = versionPolicy;
        this.optimisticTable = new CorfuTable<>(this.mainMap.getOptimisticMap(), this.indexSpec,
                this.secondaryIndexes, null);

        indices.forEach(index -> {
            secondaryIndexes.put(index.getName().get(), new HashMap<>());
            secondaryIndexesAliasToPath.put(index.getAlias().get(), index.getName().get());
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
    public static <K, V> TypeToken<CorfuTable<K, V>> getTableType() {
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
    <I>
    Collection<Map.Entry<K, V>> getByIndex(@Nonnull Index.Name indexName, I indexKey) {
        String secondaryIndex = indexName.get();
        Map<Object, Map<K, V>> secondaryMap;
        if ((secondaryIndexes.containsKey(secondaryIndex) &&
                ((secondaryMap = secondaryIndexes.get(secondaryIndex)) != null)) || secondaryIndexesAliasToPath.containsKey(secondaryIndex)
                && ((secondaryMap = secondaryIndexes.get(secondaryIndexesAliasToPath.get(secondaryIndex))) != null)) {
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

    /** {@inheritDoc} */
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
    protected V undoPutRecord(CorfuTable<K, V> table, K key, V value) {
        return table.mainMap.get(key);
    }

    @DontInstrument
    protected void undoPut(CorfuTable<K, V> table, V undoRecord, K key, V value) {
        // Same as undoRemove (restore previous value)
        undoRemove(table, undoRecord, key);
    }

    enum UndoNullable {
        NULL;
    }

    /** {@inheritDoc} */
    @Override
    @Mutator(name = "put", noUpcall = true)
    public void insert(@ConflictParameter K key, V value) {
        // This is just a stub, the annotation processor will generate an update with
        // put(key, value), since this method doesn't require an upcall therefore no
        // operations are needed to be executed on the internal data structure
    }

    /** {@inheritDoc} */
    @MutatorAccessor(name = "remove", undoFunction = "undoRemove",
            undoRecordFunction = "undoRemoveRecord")
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
    @Mutator(name = "clear", reset = true)
    public void clear() {
        mainMap.clear();
        secondaryIndexes.values().forEach(Map::clear);
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public @Nonnull Set<K> keySet() {
        return ImmutableSet.copyOf(mainMap.keySet());
    }

    /**
     * Present the content of a {@link CorfuTable} via the {@link Stream} interface.
     * Because the stream can point to other resources managed off-heap, its necessary
     * to explicitly close it after consumption.
     *
     * @return stream of entries
     */
    @Accessor
    public @Nonnull Stream<Map.Entry<K, V>> entryStream() {
        return mainMap.entryStream();
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
            for (Index.Spec<K, V, ?> index : indexSpec) {
                String indexName = index.getName().get();
                Map<Object, Map<K, V>> secondaryIndex = secondaryIndexes.get(indexName);
                for (Object indexKey : index.getMultiValueIndexFunction().apply(key, value)) {
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
            for (Index.Spec<K, V, ?> index : indexSpec) {
                String indexName = index.getName().get();
                Map<Object, Map<K, V>> secondaryIndex = secondaryIndexes.get(indexName);
                for (Object indexKey : index.getMultiValueIndexFunction().apply(key, value)) {
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
