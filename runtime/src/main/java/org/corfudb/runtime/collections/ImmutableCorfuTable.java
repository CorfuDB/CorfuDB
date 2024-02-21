package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import io.micrometer.core.instrument.Counter;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.runtime.collections.vavr.HashArrayMappedTrie;
import org.corfudb.runtime.collections.vavr.HashArrayMappedTrieModule;
import org.corfudb.runtime.collections.vavr.IterableWrapper;
import org.corfudb.runtime.object.ConsistencyView;
import org.corfudb.runtime.object.InMemorySMRSnapshot;
import org.corfudb.runtime.object.SMRSnapshot;
import org.corfudb.runtime.object.SnapshotGenerator;
import org.corfudb.runtime.object.VersionedObjectIdentifier;
import org.corfudb.runtime.view.ObjectOpenOption;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A thin immutable and persistent wrapper around HAMT implementation.
 * Keys must be unique and each key is mapped to exactly one value. Null keys
 * and values are not permitted.
 * @param <K> The type of the primary key.
 * @param <V> The type of the values to be mapped.
 *
 * Created by jielu, munshedm, and zfrenette.
 */
@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ImmutableCorfuTable<K, V> implements
        SnapshotGenerator<ImmutableCorfuTable<K, V>>,
        ConsistencyView {

    // The "main" map which contains the primary key-value mappings.
    private final HashArrayMappedTrie<K, V> mainMap;

    // Data structures for secondary indexes.
    private final SecondaryIndexesWrapper<K, V> secondaryIndexesWrapper;

    /**
     * Get a type token for this particular type of ImmutableCorfuTable.
     * @param <K> The key type.
     * @param <V> The value type.
     * @return A type token to pass to the builder.
     */
    public static <K, V> TypeToken<ImmutableCorfuTable<K, V>> getTypeToken() {
        return new TypeToken<ImmutableCorfuTable<K, V>>() {};
    }

    public ImmutableCorfuTable() {
        this.mainMap = HashArrayMappedTrie.empty();
        this.secondaryIndexesWrapper = new SecondaryIndexesWrapper<>();
    }

    public ImmutableCorfuTable(@NonNull final Index.Registry<K, V> indices) {
        this.mainMap = HashArrayMappedTrie.empty();
        this.secondaryIndexesWrapper = new SecondaryIndexesWrapper<>(indices);
    }

    @Override
    public SMRSnapshot<ImmutableCorfuTable<K, V>> generateSnapshot(@NonNull VersionedObjectIdentifier version) {
        return new InMemorySMRSnapshot<>(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<SMRSnapshot<ImmutableCorfuTable<K, V>>> generateTargetSnapshot(
            VersionedObjectIdentifier version,
            ObjectOpenOption objectOpenOption,
            SMRSnapshot<ImmutableCorfuTable<K, V>> previousSnapshot) {
        if (objectOpenOption == ObjectOpenOption.NO_CACHE) {
            // Always release the previous target version.
            previousSnapshot.release();
            return Optional.of(new InMemorySMRSnapshot<>(this));
        }

        return Optional.empty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<SMRSnapshot<ImmutableCorfuTable<K, V>>> generateIntermediarySnapshot(
            VersionedObjectIdentifier version,
            ObjectOpenOption objectOpenOption) {
        if (objectOpenOption == ObjectOpenOption.NO_CACHE) {
            // We never generate an intermediary version.
            return Optional.empty();
        }

        return Optional.of(new InMemorySMRSnapshot<>(this));
    }

    @Override
    public void close() {
        // Noop.
    }

    /**
     * Get the value associated with the provided key.
     * @param key The key used to perform the query.
     * @return The value associated with the provided key. or null
     * if no such mapping exists.
     */
    public V get(@NonNull K key) {
        return mainMap.get(key).orElse(null);
    }

    /**
     * Insert a key-value pair, overwriting any previous mapping. This method updates
     * secondary indexes if applicable.
     * @param key   The key to insert.
     * @param value The value to insert.
     * @return An ImmutableCorfuTable containing the new key-value mapping.
     */
    public ImmutableCorfuTable<K, V> put(@NonNull K key, @NonNull V value) {
        SecondaryIndexesWrapper<K, V> newSecondaryIndexesWrapper = mainMap.get(key)
                .map(prev -> secondaryIndexesWrapper.unmapSecondaryIndexes(key, prev))
                .orElse(secondaryIndexesWrapper);

        HashArrayMappedTrie<K, V> newMainMap = mainMap.put(key, value);
        Optional<HashArrayMappedTrieModule.LeafSingleton<K, V>> leafSingleton = newMainMap.getNode(key);
        newSecondaryIndexesWrapper = newSecondaryIndexesWrapper.mapSecondaryIndexes(key, value, leafSingleton);
        return new ImmutableCorfuTable<>(newMainMap, newSecondaryIndexesWrapper);
    }

    /**
     * Delete a key-value pair, updating secondary indexes if applicable.
     * @param key The key to remove.
     * @return An ImmutableCorfuTable where the mapping given from the
     * provided key is removed.
     */
    public ImmutableCorfuTable<K, V> remove(@NonNull K key) {
        SecondaryIndexesWrapper<K, V> newSecondaryIndexesWrapper = mainMap
                .get(key)
                .map(prev -> secondaryIndexesWrapper.unmapSecondaryIndexes(key, prev))
                .orElse(secondaryIndexesWrapper);

        return new ImmutableCorfuTable<>(mainMap.remove(key), newSecondaryIndexesWrapper);
    }

    /**
     * Compute the size of this ImmutableCorfuTable.
     * @return The number of key-value pairs in this ImmutableCorfuTable.
     */
    public int size() {
        return mainMap.size();
    }

    /**
     * Returns true if a mapping for the specified key exists.
     * @param key The key whose presence is to be tested.
     * @return True if and only if a mapping for the provided key exists.
     */
    public boolean containsKey(@NonNull K key) {
        return mainMap.containsKey(key);
    }

    /**
     * Removes all of the key-value pair mappings.
     * @return An ImmutableCorfuTable with all key-value mappings removed.
     */
    public ImmutableCorfuTable<K, V> clear() {
        return new ImmutableCorfuTable<>(
                HashArrayMappedTrie.empty(),
                secondaryIndexesWrapper.clear()
        );
    }

    /**
     * Returns a set of all keys present.
     * @return A set containing all keys present in this ImmutableCorfuTable.
     */
    public java.util.Set<K> keySet() {
        return mainMap.getKeySet();
    }

    /**
     * Returns the key-value mappings through the Java stream interface.
     * @return A stream containing all key-value mappings in this ImmutableCorfuTable.
     */
    public Stream<java.util.Map.Entry<K, V>> entryStream() {
        return StreamSupport.stream(IterableWrapper.spliterator(mainMap.iterator()), true);
    }

    /**
     * Get a mapping using the specified index function.
     * @param indexName Name of the secondary index to query.
     * @param indexKey The index key used to query the secondary index
     * @param <I> The type of the index key.
     * @return An Iterable of map entries satisfying this index query.
     */
    public <I> Iterable<Map.Entry<K, V>> getByIndex(@NonNull final Index.Name indexName, I indexKey) {
        Optional<HashArrayMappedTrie<K, V>> trie = secondaryIndexesWrapper.contains(indexName.get(), indexKey);
        if (!trie.isPresent()) {
            return Collections.emptySet();
        }
        return new IterableWrapper<>(trie.get().iterator());
    }

    @AllArgsConstructor
    @Getter
    private static class IndexMapping<K, V> {
        // Secondary index mapping from the mapping function -> values
        private final HashArrayMappedTrie<Object, HashArrayMappedTrie<K, V>> mapping;
        private final Index.Spec<K, V, ?> index;
        private final Optional<Counter> cacheReuseCounter = MicroMeterUtils.counter("mvo.cache.reuse", "type", "reuse");
        private final Optional<Counter> indexMapPutCounter = MicroMeterUtils.counter("mvo.cache.index_map", "type", "total");

        public IndexMapping<K, V> cleanUp(@NonNull K key, @NonNull V value) {
            HashArrayMappedTrie<Object, HashArrayMappedTrie<K, V>> updatedMapping = mapping;

            Iterable<?> mappedValues = index.getMultiValueIndexFunction().apply(key, value);

            for (Object indexKey: mappedValues) {
                HashArrayMappedTrie<K, V> slot = updatedMapping.get(indexKey).orElse(null);
                if (slot != null) {
                    boolean valuePresented = slot.get(key).filter(value::equals).isPresent();
                    if (valuePresented) {
                        slot = slot.remove(key);
                    }

                    // Update mapping index
                    updatedMapping = updatedMapping.put(indexKey, slot);

                    // Clean up empty slot
                    if (slot.isEmpty()) {
                        updatedMapping = updatedMapping.remove(indexKey);
                    }
                }
            }

            return new IndexMapping<>(updatedMapping, index);
        }

        public IndexMapping<K, V> update(@NonNull K key, @NonNull V value,
                                         Optional<HashArrayMappedTrieModule.LeafSingleton<K, V>> leafNode) {
            HashArrayMappedTrie<Object, HashArrayMappedTrie<K, V>> updatedMapping = mapping;

            Iterable<?> mappedValues = index.getMultiValueIndexFunction().apply(key, value);
            for (Object indexKey: mappedValues) {
                HashArrayMappedTrie<K, V> slot;
                if (leafNode.isPresent()) {
                    slot = updatedMapping.getOrElse(indexKey, HashArrayMappedTrie.empty()).putNode(leafNode.get());
                    cacheReuseCounter.ifPresent(Counter::increment);
                } else {
                    slot = updatedMapping.getOrElse(indexKey, HashArrayMappedTrie.empty()).put(key, value);
                }
                updatedMapping = updatedMapping.put(indexKey, slot);
                indexMapPutCounter.ifPresent(Counter::increment);
            }

            return new IndexMapping<>(updatedMapping, index);
        }
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    private static class SecondaryIndexesWrapper<K, V> {
        private final HashArrayMappedTrie<String, Integer> secondaryIndexPositions;
        private final List<IndexMapping<K, V>> secondaryIndexMappings;
        private final HashArrayMappedTrie<String, String> secondaryIndexesAliasToPath;

        private SecondaryIndexesWrapper() {
            this.secondaryIndexPositions = HashArrayMappedTrie.empty();
            this.secondaryIndexesAliasToPath = HashArrayMappedTrie.empty();
            this.secondaryIndexMappings = new ArrayList<>();
        }

        private SecondaryIndexesWrapper(@NonNull final Index.Registry<K, V> indices) {
            HashArrayMappedTrie<String, Integer> indexPositions = HashArrayMappedTrie.empty();
            HashArrayMappedTrie<String, String> indexesAliasToPath = HashArrayMappedTrie.empty();
            List<IndexMapping<K, V>> indexMappings = new ArrayList<>();
            int counter = 0;
            for (Index.Spec<K, V, ?> index : indices) {
                indexPositions = indexPositions.put(index.getName().get(), counter);
                indexMappings.add(new IndexMapping<>(HashArrayMappedTrie.empty(), index));
                indexesAliasToPath = indexesAliasToPath.put(index.getAlias().get(), index.getName().get());
                counter++;
            }

            this.secondaryIndexPositions = indexPositions;
            this.secondaryIndexMappings = indexMappings;
            this.secondaryIndexesAliasToPath = indexesAliasToPath;

            if (!isEmpty()) {
                log.info(
                        "ImmutableCorfuTable: creating PersistentCorfuTable with the following indexes: {}",
                        secondaryIndexPositions.getKeySet()
                );
            }
        }

        private <I> Optional<HashArrayMappedTrie<K, V>> contains(@NonNull final String index, I indexKey) {
            if (secondaryIndexPositions.containsKey(index)) {
                return secondaryIndexMappings.get(secondaryIndexPositions.get(index).get()).getMapping().get(indexKey);
            }

            if (secondaryIndexesAliasToPath.containsKey(index)) {
                final String path = secondaryIndexesAliasToPath.get(index).get();
                if (secondaryIndexPositions.containsKey(path)) {
                    return secondaryIndexMappings.get(secondaryIndexPositions.get(path).get()).getMapping().get(indexKey);
                }
            }

            // If index is not specified, the lookup by index API must fail.
            log.error("ImmutableCorfuTable: secondary index " + index +
                    " does not exist for this table, cannot complete the get by index.");
            throw new IllegalArgumentException("Secondary Index " + index + " is not defined.");
        }

        private boolean isEmpty() {
            return secondaryIndexPositions.isEmpty();
        }

        private SecondaryIndexesWrapper<K, V> clear() {
            List<IndexMapping<K, V>> clearedIndexes = new ArrayList<>(Collections.nCopies(secondaryIndexPositions.size(), null));
            for (Iterator<Map.Entry<String, Integer>> it = secondaryIndexPositions.iterator(); it.hasNext(); ) {
                Map.Entry<String, Integer> indexPositionTuple = it.next();
                clearedIndexes.set(indexPositionTuple.getValue(),
                        new IndexMapping<>(HashArrayMappedTrie.empty(), secondaryIndexMappings.get(indexPositionTuple.getValue()).getIndex()));
            }

            return new SecondaryIndexesWrapper<>(secondaryIndexPositions, clearedIndexes, secondaryIndexesAliasToPath);
        }

        private SecondaryIndexesWrapper<K, V> unmapSecondaryIndexes(@NonNull K key, @NonNull V value) {
            try {
                List<IndexMapping<K, V>> unmappedIndexMappings = secondaryIndexMappings;

                for (Iterator<Map.Entry<String, Integer>> it = secondaryIndexPositions.iterator(); it.hasNext(); ) {
                    Map.Entry<String, Integer> indexPositionTuple = it.next();
                    final IndexMapping<K, V> currentMapping = unmappedIndexMappings.get(indexPositionTuple.getValue());
                    unmappedIndexMappings.set(indexPositionTuple.getValue(), currentMapping.cleanUp(key, value));
                }

                return new SecondaryIndexesWrapper<>(secondaryIndexPositions, unmappedIndexMappings, secondaryIndexesAliasToPath);
            } catch (Exception ex) {
                log.error("Received an exception while computing the index. " +
                        "This is most likely an issue with the client's indexing function.", ex);

                // In case of both a transactional and non-transactional operation, the client
                // is going to receive UnrecoverableCorfuError along with the appropriate cause.
                throw ex;
            }
        }

        private SecondaryIndexesWrapper<K, V> mapSecondaryIndexes(@NonNull K key, @NonNull V value,
                                                                  Optional<HashArrayMappedTrieModule.LeafSingleton<K, V>> leafNode) {
            try {
                List<IndexMapping<K, V>> mappedIndexMappings = secondaryIndexMappings;

                // Map entry into secondary indexes
                for (Iterator<Map.Entry<String, Integer>> it = secondaryIndexPositions.iterator(); it.hasNext(); ) {
                    Map.Entry<String, Integer> indexPositionTuple = it.next();
                    final IndexMapping<K, V> updatedSecondaryIndex = mappedIndexMappings.get(indexPositionTuple.getValue()).update(key, value, leafNode);
                    mappedIndexMappings.set(indexPositionTuple.getValue(), updatedSecondaryIndex);
                }

                return new SecondaryIndexesWrapper<>(secondaryIndexPositions, mappedIndexMappings, secondaryIndexesAliasToPath);
            } catch (Exception ex) {
                log.error("Received an exception while computing the index. " +
                        "This is most likely an issue with the client's indexing function.", ex);

                // In case of both a transactional and non-transactional operation, the client
                // is going to receive UnrecoverableCorfuError along with the appropriate cause.
                throw ex;
            }
        }
    }
}
