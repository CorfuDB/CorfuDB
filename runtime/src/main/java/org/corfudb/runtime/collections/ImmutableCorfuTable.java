package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.HashSet;
import io.vavr.collection.Map;
import io.vavr.collection.Set;
import io.vavr.control.Option;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.Index.Spec;
import org.corfudb.runtime.object.ICorfuExecutionContext;
import org.corfudb.runtime.object.ICorfuSMR;

import javax.annotation.Nonnull;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
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
// TODO: don't need to implement ICorfuSMR?
public class ImmutableCorfuTable<K, V> implements ICorfuSMR<ImmutableCorfuTable<K, V>> {

    // The "main" map which contains the primary key-value mappings.
    private final Map<K, V> mainMap;

    // Data structures for secondary indexes.
    private final SecondaryIndexesWrapper<K, V> secondaryIndexesWrapper;

    /**
     * Get a type token for this particular type of ImmutableCorfuTable.
     * @param <K> The key type.
     * @param <V> The value type.
     * @return A type token to pass to the builder.
     */
    public static <K, V> TypeToken<ImmutableCorfuTable<K, V>> getTableType() {
        return new TypeToken<ImmutableCorfuTable<K, V>>() {};
    }

    public ImmutableCorfuTable() {
        this.mainMap = HashMap.empty();
        this.secondaryIndexesWrapper = new SecondaryIndexesWrapper<>();
    }

    public ImmutableCorfuTable(@Nonnull final Index.Registry<K, V> indices) {
        this.mainMap = HashMap.empty();
        this.secondaryIndexesWrapper = new SecondaryIndexesWrapper<>(indices);
    }

    @Override
    public ImmutableCorfuTable<K, V> getContext(ICorfuExecutionContext.Context context) {
        return this;
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
        SecondaryIndexesWrapper<K, V> newSecondaryIndexesWrapper = secondaryIndexesWrapper;
        if (!secondaryIndexesWrapper.isEmpty()) {
            V prev = get(key);
            if (prev != null) {
                newSecondaryIndexesWrapper = newSecondaryIndexesWrapper.unmapSecondaryIndexes(key, prev);
            }

            newSecondaryIndexesWrapper = newSecondaryIndexesWrapper.mapSecondaryIndexes(key, value);
        }

        return new ImmutableCorfuTable<>(mainMap.put(key, value), newSecondaryIndexesWrapper);
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
                HashMap.empty(),
                secondaryIndexesWrapper.clear()
        );
    }

    /**
     * Returns a set of all keys present.
     * @return A set containing all keys present in this ImmutableCorfuTable.
     */
    public java.util.Set<K> keySet() {
        // TODO: Can this call to toJavaSet() be avoided?
        return mainMap.keySet().toJavaSet();
    }

    /**
     * Returns the key-value mappings through the Java stream interface.
     * @return A stream containing all key-value mappings in this ImmutableCorfuTable.
     */
    public Stream<java.util.Map.Entry<K, V>> entryStream() {
        return StreamSupport.stream(spliterator(mainMap), true);
    }

    /**
     * Get a mapping using the specified index function.
     * @param indexName Name of the secondary index to query.
     * @param indexKey The index key used to query the secondary index
     * @param <I> The type of the index key.
     * @return A collection of map entries satisfying this index query.
     */
    public <I> Collection<java.util.Map.Entry<K, V>> getByIndex(@Nonnull final Index.Name indexName, I indexKey) {
        final String secondaryIndexName = indexName.get();
        Option<Map<K, V>> optMap = secondaryIndexesWrapper.contains(secondaryIndexName, indexKey);

        if (!optMap.isEmpty()) {
            // TODO: Can this call to toJavaMap() be avoided?
            return optMap.get().toJavaMap().entrySet();
        }

        return Collections.emptySet();
    }

    private class TupleIteratorWrapper implements Iterator<java.util.Map.Entry<K, V>> {

        final io.vavr.collection.Iterator<Tuple2<K, V>> iterator;

        public TupleIteratorWrapper(@Nonnull io.vavr.collection.Iterator<Tuple2<K, V>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public java.util.Map.Entry<K, V> next() {
            if (hasNext()) {
                Tuple2<K, V> tuple2 = iterator.next();
                return new AbstractMap.SimpleEntry<>(tuple2._1, tuple2._2);
            }
            throw new NoSuchElementException();
        }
    }

    private Spliterator<java.util.Map.Entry<K, V>> spliterator(@Nonnull Map<K, V> map){
        int characteristics = Spliterator.IMMUTABLE;
        if (map.isDistinct()) {
            characteristics |= Spliterator.DISTINCT;
        }
        if (map.isOrdered()) {
            characteristics |= (Spliterator.SORTED | Spliterator.ORDERED);
        }
        if (map.isSequential()) {
            characteristics |= Spliterator.ORDERED;
        }
        if (map.hasDefiniteSize()) {
            characteristics |= (Spliterator.SIZED | Spliterator.SUBSIZED);
            return Spliterators.spliterator(new TupleIteratorWrapper(map.iterator()), map.length(), characteristics);
        } else {
            return Spliterators.spliteratorUnknownSize(new TupleIteratorWrapper(map.iterator()), characteristics);
        }
    }

    @AllArgsConstructor
    public static class IndexMapping<K, V> {
        //secondary index mapping from the mapping function -> values
        private final Map<Object, Map<K, V>> mapping;
        private final Spec<K, V, ?> index;

        public IndexMapping<K, V> cleanUp(K key, V value) {
            Map<Object, Map<K, V>> updatedMapping = mapping;

            Iterable<?> staleValues = index.getMultiValueIndexFunction().apply(key, value);

            for (Object indexKey: staleValues) {
                if (!updatedMapping.containsKey(indexKey)) {
                    return this;
                }

                Map<K, V> slot = updatedMapping.get(indexKey).get();
                boolean valuePresented = slot.get(key).contains(value);
                if (valuePresented) {
                    slot = slot.remove(key);
                }

                //update mapping index
                updatedMapping = updatedMapping.put(indexKey, slot);

                //clean up empty slot
                if (slot.isEmpty()) {
                    updatedMapping = updatedMapping.remove(indexKey);
                }
            }

            return new IndexMapping<>(updatedMapping);
        }
    }

    @AllArgsConstructor(access = AccessLevel.PRIVATE)
    private static class SecondaryIndexesWrapper<K, V> {
        private Set<Index.Spec<K, V, ?>> indexSpec;
        private Map<String, IndexMapping<K, V>> secondaryIndexes;
        private Map<String, String> secondaryIndexesAliasToPath;

        private SecondaryIndexesWrapper() {
            indexSpec = HashSet.empty();
            secondaryIndexes = HashMap.empty();
            secondaryIndexesAliasToPath = HashMap.empty();
        }

        private SecondaryIndexesWrapper(@Nonnull final Index.Registry<K, V> indices) {
            this();

            indices.forEach((Spec<K, V, ?> index) -> {
                indexSpec = indexSpec.add(index);
                secondaryIndexes = secondaryIndexes.put(index.getName().get(), new IndexMapping<>(HashMap.empty(), index));
                secondaryIndexesAliasToPath = secondaryIndexesAliasToPath
                        .put(index.getAlias().get(), index.getName().get());
            });

            if (!isEmpty()) {
                log.info(
                        "ImmutableCorfuTable: creating PersistentCorfuTable with the following indexes: {}",
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
            log.error("ImmutableCorfuTable: secondary index " + index +
                    " does not exist for this table, cannot complete the get by index.");
            throw new IllegalArgumentException("Secondary Index " + index + " is not defined.");
        }

        private boolean isEmpty() {
            return secondaryIndexes.isEmpty();
        }

        private SecondaryIndexesWrapper<K, V> clear() {
            Map<String, Map<Object, Map<K, V>>> clearedIndexes = HashMap.empty();
            for (Tuple2<String, ?> index : secondaryIndexes.iterator()) {
                clearedIndexes = clearedIndexes.put(index._1(), HashMap.empty());
            }

            return new SecondaryIndexesWrapper<>(indexSpec, clearedIndexes, secondaryIndexesAliasToPath);
        }

        private SecondaryIndexesWrapper<K, V> unmapSecondaryIndexes(@Nonnull K key, @Nonnull V value) {
            try {
                Map<String, IndexMapping<K, V>> unmappedSecondaryIndexes = secondaryIndexes;
                // Map entry into secondary indexes
                for (IndexMapping<K, V> secondaryIndex : secondaryIndexes) {
                    IndexMapping<K, V> updatedSecondaryIndex = secondaryIndex.cleanUp(key, value);

                    unmappedSecondaryIndexes = unmappedSecondaryIndexes.put(indexName, updatedSecondaryIndex);
                }

                return new SecondaryIndexesWrapper<>(indexSpec, unmappedSecondaryIndexes, secondaryIndexesAliasToPath);
            } catch (Exception ex) {
                log.error("Received an exception while computing the index. " +
                        "This is most likely an issue with the client's indexing function.", ex);

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

                // In case of both a transactional and non-transactional operation, the client
                // is going to receive UnrecoverableCorfuError along with the appropriate cause.
                throw ex;
            }
        }
    }
}
