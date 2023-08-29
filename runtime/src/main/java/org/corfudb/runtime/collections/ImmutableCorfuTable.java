package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import io.micrometer.core.instrument.Counter;
import io.vavr.Tuple2;
import io.vavr.control.Option;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.runtime.collections.vavr.HashArrayMappedTrie;
import org.corfudb.runtime.collections.vavr.HashArrayMappedTrieModule;
import org.corfudb.runtime.collections.vavr.TupleIterableWrapper;
import org.corfudb.runtime.object.ConsistencyView;
import org.corfudb.runtime.object.InMemorySMRSnapshot;
import org.corfudb.runtime.object.SMRSnapshot;
import org.corfudb.runtime.object.SnapshotGenerator;
import org.corfudb.runtime.object.VersionedObjectIdentifier;
import org.corfudb.runtime.view.ObjectOpenOption;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A thin immutable and persistent wrapper around VAVR's map implementation.
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

    public ImmutableCorfuTable(@Nonnull final Index.Registry<K, V> indices) {
        this.mainMap = HashArrayMappedTrie.empty();
        this.secondaryIndexesWrapper = new SecondaryIndexesWrapper<>(indices);
    }

    @Override
    public SMRSnapshot<ImmutableCorfuTable<K, V>> generateSnapshot(@Nonnull VersionedObjectIdentifier version) {
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
    public V get(@Nonnull K key) {
        return mainMap.get(key).getOrNull();
    }

    /**
     * Insert a key-value pair, overwriting any previous mapping. This method updates
     * secondary indexes if applicable.
     * @param key   The key to insert.
     * @param value The value to insert.
     * @return An ImmutableCorfuTable containing the new key-value mapping.
     */
    public ImmutableCorfuTable<K, V> put(@Nonnull K key, @Nonnull V value) {
        SecondaryIndexesWrapper<K, V> newSecondaryIndexesWrapper = mainMap.get(key)
                .map(prev -> secondaryIndexesWrapper.unmapSecondaryIndexes(key, prev))
                .getOrElse(secondaryIndexesWrapper);

        HashArrayMappedTrie<K, V> newMainMap = mainMap.put(key, value);
        Option<HashArrayMappedTrieModule.LeafSingleton<K, V>> leafSingleton = newMainMap.getNode(key);
        newSecondaryIndexesWrapper = newSecondaryIndexesWrapper.mapSecondaryIndexes(key, value, leafSingleton);
        return new ImmutableCorfuTable<>(newMainMap, newSecondaryIndexesWrapper);
    }

    /**
     * Delete a key-value pair, updating secondary indexes if applicable.
     * @param key The key to remove.
     * @return An ImmutableCorfuTable where the mapping given from the
     * provided key is removed.
     */
    public ImmutableCorfuTable<K, V> remove(@Nonnull K key) {
        SecondaryIndexesWrapper<K, V> newSecondaryIndexesWrapper = mainMap
                .get(key)
                .map(prev -> secondaryIndexesWrapper.unmapSecondaryIndexes(key, prev))
                .getOrElse(secondaryIndexesWrapper);

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
    public boolean containsKey(@Nonnull K key) {
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
        return mainMap.iterator().map(Tuple2::_1).toJavaSet();
    }

    /**
     * Returns the key-value mappings through the Java stream interface.
     * @return A stream containing all key-value mappings in this ImmutableCorfuTable.
     */
    public Stream<java.util.Map.Entry<K, V>> entryStream() {
        return StreamSupport.stream(TupleIterableWrapper.spliterator(mainMap.iterator()), true);
    }

    /**
     * Get a mapping using the specified index function.
     * @param indexName Name of the secondary index to query.
     * @param indexKey The index key used to query the secondary index
     * @param <I> The type of the index key.
     * @return An Iterable of map entries satisfying this index query.
     */
    public <I> Iterable<java.util.Map.Entry<K, V>> getByIndex(@Nonnull final Index.Name indexName, I indexKey) {
        Option<HashArrayMappedTrie<K, V>> trie = secondaryIndexesWrapper.contains(indexName.get(), indexKey);
        if (trie.isEmpty()) {
            return Collections.emptySet();
        }
        return new TupleIterableWrapper<>(trie.get().iterator());
    }

    @AllArgsConstructor
    @Getter
    private static class IndexMapping<K, V> {
        // Secondary index mapping from the mapping function -> values
        private final HashArrayMappedTrie<Object, HashArrayMappedTrie<K, V>> mapping;
        private final Index.Spec<K, V, ?> index;
        private final Optional<Counter> cacheReuseCounter = MicroMeterUtils.counter("mvo.cache.reuse", "type", "reuse");
        private final Optional<Counter> indexMapPutCounter = MicroMeterUtils.counter("mvo.cache.index_map", "type", "total");

        public IndexMapping<K, V> cleanUp(@Nonnull K key, @Nonnull V value) {
            HashArrayMappedTrie<Object, HashArrayMappedTrie<K, V>> updatedMapping = mapping;

            Iterable<?> mappedValues = index.getMultiValueIndexFunction().apply(key, value);

            for (Object indexKey: mappedValues) {
                HashArrayMappedTrie<K, V> slot = updatedMapping.get(indexKey).getOrNull();
                if (slot != null) {
                    boolean valuePresented = slot.get(key).contains(value);
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

        public IndexMapping<K, V> update(@Nonnull K key, @Nonnull V value,
                                         Option<HashArrayMappedTrieModule.LeafSingleton<K, V>> leafNode) {
            HashArrayMappedTrie<Object, HashArrayMappedTrie<K, V>> updatedMapping = mapping;

            Iterable<?> mappedValues = index.getMultiValueIndexFunction().apply(key, value);
            for (Object indexKey: mappedValues) {
                //do mainmap.get(key) to get the leafSingleton class
                HashArrayMappedTrie<K, V> slot;
                if (!leafNode.isEmpty()) {
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
        private final HashArrayMappedTrie<String, IndexMapping<K, V>> secondaryIndexes;
        private final HashArrayMappedTrie<String, String> secondaryIndexesAliasToPath;

        private SecondaryIndexesWrapper() {
            this.secondaryIndexes = HashArrayMappedTrie.empty();
            this.secondaryIndexesAliasToPath = HashArrayMappedTrie.empty();
        }

        private SecondaryIndexesWrapper(@Nonnull final Index.Registry<K, V> indices) {
            HashArrayMappedTrie<String, IndexMapping<K, V>> indexes = HashArrayMappedTrie.empty();
            HashArrayMappedTrie<String, String> indexesAliasToPath = HashArrayMappedTrie.empty();

            for (Index.Spec<K, V, ?> index : indices) {
                indexes = indexes.put(index.getName().get(), new IndexMapping<>(HashArrayMappedTrie.empty(), index));
                indexesAliasToPath = indexesAliasToPath.put(index.getAlias().get(), index.getName().get());
            }

            this.secondaryIndexes = indexes;
            this.secondaryIndexesAliasToPath = indexesAliasToPath;

            if (!isEmpty()) {
                log.info(
                        "ImmutableCorfuTable: creating PersistentCorfuTable with the following indexes: {}",
                        secondaryIndexes.iterator().map(Tuple2::_1)
                );
            }
        }

        private <I> Option<HashArrayMappedTrie<K, V>> contains(@Nonnull final String index, I indexKey) {
            if (secondaryIndexes.containsKey(index)) {
                return secondaryIndexes.get(index).get().getMapping().get(indexKey);
            }

            if (secondaryIndexesAliasToPath.containsKey(index)) {
                final String path = secondaryIndexesAliasToPath.get(index).get();
                if (secondaryIndexes.containsKey(path)) {
                    return secondaryIndexes.get(path).get().getMapping().get(indexKey);
                }
            }

            // If index is not specified, the lookup by index API must fail.
            log.error("ImmutableCorfuTable: secondary index " + index +
                    " does not exist for this table, cannot complete the get by index.");
            throw new IllegalArgumentException("Secondary Index " + index + " is not defined.");
        }

        private boolean isEmpty() {
            return secondaryIndexes.isEmpty();
        }

        private SecondaryIndexesWrapper<K, V> clear() {
            HashArrayMappedTrie<String, IndexMapping<K, V>> clearedIndexes = HashArrayMappedTrie.empty();
            for (Tuple2<String, IndexMapping<K, V>> index : secondaryIndexes.iterator()) {
                clearedIndexes = clearedIndexes.put(index._1(),
                        new IndexMapping<>(HashArrayMappedTrie.empty(), index._2().getIndex()));
            }

            return new SecondaryIndexesWrapper<>(clearedIndexes, secondaryIndexesAliasToPath);
        }

        private SecondaryIndexesWrapper<K, V> unmapSecondaryIndexes(@Nonnull K key, @Nonnull V value) {
            try {
                HashArrayMappedTrie<String, IndexMapping<K, V>> unmappedSecondaryIndexes = secondaryIndexes;

                for (Tuple2<String, IndexMapping<K, V>> secondaryIndex : secondaryIndexes) {
                    final IndexMapping<K, V> currentMapping = secondaryIndex._2();
                    final String indexName = currentMapping.getIndex().getName().get();
                    unmappedSecondaryIndexes = unmappedSecondaryIndexes.put(indexName, currentMapping.cleanUp(key, value));
                }

                return new SecondaryIndexesWrapper<>(unmappedSecondaryIndexes, secondaryIndexesAliasToPath);
            } catch (Exception ex) {
                log.error("Received an exception while computing the index. " +
                        "This is most likely an issue with the client's indexing function.", ex);

                // In case of both a transactional and non-transactional operation, the client
                // is going to receive UnrecoverableCorfuError along with the appropriate cause.
                throw ex;
            }
        }

        private SecondaryIndexesWrapper<K, V> mapSecondaryIndexes(@Nonnull K key, @Nonnull V value,
                                                                  Option<HashArrayMappedTrieModule.LeafSingleton<K, V>> leafNode) {
            try {
                HashArrayMappedTrie<String, IndexMapping<K, V>> mappedSecondaryIndexes = secondaryIndexes;

                // Map entry into secondary indexes
                for (Tuple2<String, IndexMapping<K, V>> secondaryIndexTuple : secondaryIndexes) {
                    final IndexMapping<K, V> updatedSecondaryIndex = secondaryIndexTuple._2().update(key, value, leafNode);
                    final String indexName = secondaryIndexTuple._2().getIndex().getName().get();
                    mappedSecondaryIndexes = mappedSecondaryIndexes.put(indexName, updatedSecondaryIndex);
                }

                return new SecondaryIndexesWrapper<>(mappedSecondaryIndexes, secondaryIndexesAliasToPath);
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
