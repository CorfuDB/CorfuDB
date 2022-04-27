package org.corfudb.runtime.collections;

import io.vavr.collection.HashMap;
import io.vavr.collection.HashSet;
import io.vavr.collection.Map;
import io.vavr.collection.Set;
import io.vavr.control.Option;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Stream;

@Slf4j
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class PersistentHashMapWrapper<K, V> {

    // The "main" map which contains the primary key-value mappings.
    private Map<K, V> mainMap;

    // Secondary indexes.
    private SecondaryIndexesWrapper<K, V> secondaryIndexesWrapper;

    public PersistentHashMapWrapper() {
        this.mainMap = HashMap.empty();
        this.secondaryIndexesWrapper = new SecondaryIndexesWrapper<>();
    }

    public PersistentHashMapWrapper(@Nonnull final Index.Registry<K, V> indices) {
        this.mainMap = HashMap.empty();
        this.secondaryIndexesWrapper = new SecondaryIndexesWrapper<>(indices);
    }

    /**
     *
     * @param key
     * @return
     */
    public V get(@Nonnull K key) {
        return mainMap.get(key).getOrNull();
    }

    /**
     *
     * @param key
     * @param value
     * @return
     */
    public PersistentHashMapWrapper<K, V> put(@Nonnull K key, @Nonnull V value) {
        SecondaryIndexesWrapper<K, V> newSecondaryIndexesWrapper = secondaryIndexesWrapper;
        if (!secondaryIndexesWrapper.isEmpty()) {
            V prev = get(key);
            if (prev != null) {
                // TODO: Combine unmap and map?
                newSecondaryIndexesWrapper = newSecondaryIndexesWrapper.unmapSecondaryIndexes(key, prev);
            }

            newSecondaryIndexesWrapper = newSecondaryIndexesWrapper.mapSecondaryIndexes(key, value);
        }

        return new PersistentHashMapWrapper<>(mainMap.put(key, value), newSecondaryIndexesWrapper);
    }

    /**
     *
     * @param key
     * @return
     */
    public PersistentHashMapWrapper<K, V> remove(@Nonnull K key) {
        SecondaryIndexesWrapper<K, V> newSecondaryIndexesWrapper = secondaryIndexesWrapper;
        if (!secondaryIndexesWrapper.isEmpty()) {
            V prev = get(key);
            if (prev != null) {
                newSecondaryIndexesWrapper = newSecondaryIndexesWrapper.unmapSecondaryIndexes(key, prev);
            }
        }

        return new PersistentHashMapWrapper<>(mainMap.remove(key), newSecondaryIndexesWrapper);
    }

    /**
     *
     * @return
     */
    public int size() {
        return mainMap.size();
    }

    /**
     *
     * @param key
     * @return
     */
    public boolean containsKey(@Nonnull K key) {
        return mainMap.containsKey(key);
    }

    /**
     *
     * @return
     */
    public PersistentHashMapWrapper<K, V> clear() {
        return new PersistentHashMapWrapper<>(
                HashMap.empty(),
                secondaryIndexesWrapper.clear()
        );
    }

    /**
     *
     * @return
     */
    public java.util.Set<K> keySet() {
        // TODO: Consider alternative to avoid conversion to Java Set.
        return mainMap.keySet().toJavaSet();
    }

    /**
     *
     * @return
     */
    public Stream<java.util.Map.Entry<K, V>> entryStream() {
        // TODO: Consider alternative to avoid conversion to Java Map.
        return mainMap.toJavaMap().entrySet().stream();
    }

    /**
     *
     * @param indexName
     * @param indexKey
     * @param <I>
     * @return
     */
    public <I> Collection<java.util.Map.Entry<K, V>> getByIndex(@Nonnull final Index.Name indexName, I indexKey) {
        // TODO: Consider alternative to avoid conversion to Java Map.
        final String secondaryIndexName = indexName.get();
        Option<Map<K, V>> optMap = secondaryIndexesWrapper.contains(secondaryIndexName, indexKey);

        if (!optMap.isEmpty()) {
            return optMap.get().toJavaMap().entrySet();
        }

        return Collections.emptySet();
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    private static class SecondaryIndexesWrapper<K, V> {
        private Set<Index.Spec<K, V, ?>> indexSpec;
        private Map<String, Map<Object, Map<K, V>>> secondaryIndexes;
        private Map<String, String> secondaryIndexesAliasToPath;

        private SecondaryIndexesWrapper() {
            indexSpec = HashSet.empty();
            secondaryIndexes = HashMap.empty();
            secondaryIndexesAliasToPath = HashMap.empty();
        }

        private SecondaryIndexesWrapper(@Nonnull final Index.Registry<K, V> indices) {
            this();

            indices.forEach(index -> {
                indexSpec = indexSpec.add(index);
                secondaryIndexes = secondaryIndexes.put(index.getName().get(), HashMap.empty());
                secondaryIndexesAliasToPath = secondaryIndexesAliasToPath
                        .put(index.getAlias().get(), index.getName().get());
            });

            if (!isEmpty()) {
                log.info(
                        "PersistentCorfuTable: creating PersistentCorfuTable with the following indexes: {}",
                        secondaryIndexes.keySet()
                );
            }
        }

        private <I> Option<Map<K, V>> contains(@Nonnull final String index, I indexKey) {
            if (secondaryIndexes.containsKey(index)) {
                return secondaryIndexes.get(index).get().get(indexKey);
            }

            if (secondaryIndexesAliasToPath.containsKey(index)) {
                final String path = secondaryIndexesAliasToPath.get(index).get();
                if (secondaryIndexes.containsKey(path)) {
                    return secondaryIndexes.get(path).get().get(indexKey);
                }
            }

            // If index is not specified, the lookup by index API must fail.
            log.error("PersistentCorfuTable: secondary index " + index +
                    " does not exist for this table, cannot complete the get by index.");
            throw new IllegalArgumentException("Secondary Index " + index + " is not defined.");
        }

        private boolean isEmpty() {
            return secondaryIndexes.isEmpty();
        }

        private SecondaryIndexesWrapper<K, V> clear() {
            Map<String, Map<Object, Map<K, V>>> clearedIndexes = HashMap.empty();
            for (String indexName : secondaryIndexes.keysIterator()) {
                clearedIndexes = clearedIndexes.put(indexName, HashMap.empty());
            }

            return new SecondaryIndexesWrapper<>(indexSpec, clearedIndexes, secondaryIndexesAliasToPath);
        }

        private SecondaryIndexesWrapper<K, V> unmapSecondaryIndexes(@Nonnull K key, @Nonnull V value) {
            try {
                Map<String, Map<Object, Map<K, V>>> unmappedSecondaryIndexes = secondaryIndexes;
                // Map entry into secondary indexes
                for (Index.Spec<K, V, ?> index : indexSpec) {
                    final String indexName = index.getName().get();
                    Map<Object, Map<K, V>> secondaryIndex = unmappedSecondaryIndexes.get(indexName).get();
                    for (Object indexKey : index.getMultiValueIndexFunction().apply(key, value)) {
                        final Option<Map<K, V>> optSlot = secondaryIndex.get(indexKey);
                        if (!optSlot.isEmpty()) {
                            // TODO: VAVR does not have a removeIf - Simplify?
                            final Option<V> optValue = optSlot.get().get(key);
                            if (!optValue.isEmpty() && value.equals(optValue.get())) {
                                secondaryIndex = secondaryIndex.put(indexKey, optSlot.get().remove(key));
                            }
                        }
                    }

                    unmappedSecondaryIndexes = unmappedSecondaryIndexes.put(indexName, secondaryIndex);
                }

                return new SecondaryIndexesWrapper<>(indexSpec, unmappedSecondaryIndexes, secondaryIndexesAliasToPath);
            } catch (Exception ex) {
                log.error("Received an exception while computing the index. " +
                        "This is most likely an issue with the client's indexing function.", ex);

                // The index might be corrupt, and the only way to ensure safety is to disable
                // indexing for this table.
                invalidateIndexes();

                // In case of both a transactional and non-transactional operation, the client
                // is going to receive UnrecoverableCorfuError along with the appropriate cause.
                throw ex;
            }
        }

        private SecondaryIndexesWrapper<K, V> mapSecondaryIndexes(@Nonnull K key, @Nonnull V value) {
            try {
                Map<String, Map<Object, Map<K, V>>> mappedSecondaryIndexes = secondaryIndexes;

                // Map entry into secondary indexes
                for (Index.Spec<K, V, ?> index : indexSpec) {
                    final String indexName = index.getName().get();
                    Map<Object, Map<K, V>> secondaryIndex = mappedSecondaryIndexes.get(indexName).get();
                    for (Object indexKey : index.getMultiValueIndexFunction().apply(key, value)) {
                        final Map<K, V> slot = secondaryIndex.getOrElse(indexKey, HashMap.empty()).put(key, value);
                        secondaryIndex = secondaryIndex.put(indexKey, slot);
                    }

                    mappedSecondaryIndexes = mappedSecondaryIndexes.put(indexName, secondaryIndex);
                }

                return new SecondaryIndexesWrapper<>(indexSpec, mappedSecondaryIndexes, secondaryIndexesAliasToPath);
            } catch (Exception ex) {
                log.error("Received an exception while computing the index. " +
                        "This is most likely an issue with the client's indexing function.", ex);

                // The index might be corrupt, and the only way to ensure safety is to disable
                // indexing for this table.
                invalidateIndexes();

                // In case of both a transactional and non-transactional operation, the client
                // is going to receive UnrecoverableCorfuError along with the appropriate cause.
                throw ex;
            }
        }

        // TODO: Needed? Can corruption still occur?
        private void invalidateIndexes() {
            indexSpec = HashSet.empty();
            secondaryIndexes = HashMap.empty();
            secondaryIndexesAliasToPath = HashMap.empty();
        }
    }
}
