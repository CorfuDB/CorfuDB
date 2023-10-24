package org.corfudb.runtime.collections.index;

import com.google.common.collect.Streams;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.DiskBackedCorfuTable;
import org.corfudb.runtime.collections.index.Index.Spec;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.object.RocksDbStore;
import org.corfudb.util.serializer.ISerializer;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class IndexStore<K, V> {
    // Index.
    @Default
    private final Map<String, String> secondaryIndexesAliasToPath = new HashMap<>();
    @Default
    private final Map<String, Byte> indexToId = new HashMap<>();
    @Default
    private final Set<Spec<K, V, ?>> indexSpec = new HashSet<>();

    private final ISerializer serializer;

    private final RocksDbStore<DiskBackedCorfuTable<K, V>> rocksApi;

    public IndexStore(Index.Registry<K, V> indices,
                      ISerializer serializer,
                      RocksDbStore<DiskBackedCorfuTable<K, V>> rocksApi) {
        this.rocksApi = rocksApi;
        this.serializer = serializer;

        byte indexId = 0;
        for (Spec<K, V, ?> index : indices) {
            this.secondaryIndexesAliasToPath.put(index.getAlias().get(), index.getName().get());
            this.indexSpec.add(index);
            this.indexToId.put(index.getName().get(), indexId++);
        }
    }

    /**
     * Update secondary indexes with new mappings.
     *
     * @param key      key
     * @param value    value
     * @param previous previous value
     * @throws RocksDBException rocks db error
     */
    public void remap(K key, V value, V previous, ByteBuf valuePayload) throws RocksDBException {
        if (indexSpec.isEmpty()) {
            return;
        }

        serializer.serialize(value, valuePayload);

        unmapSecondaryIndexes(key, previous);
        mapSecondaryIndexes(key, value, valuePayload.readableBytes());
    }

    public void unmapSecondaryIndexes(@NonNull K primaryKey, @Nullable V value) throws RocksDBException {
        if (Objects.isNull(value)) {
            return;
        }

        for (Spec<K, V, ?> index : indexSpec) {
            Iterable<?> mappedValues = index.getMultiValueIndexFunction().apply(primaryKey, value);
            for (Object secondaryKey : mappedValues) {
                // Protobuf 3 does not allow for optional fields, so the secondary
                // key should never be null.
                if (Objects.isNull(secondaryKey)) {
                    log.warn("{}: null secondary keys are not supported.", index.getName());
                    continue;
                }

                final ByteBuf serializedCompoundKey = getCompoundKey(
                        indexToId.get(index.getName().get()), secondaryKey, primaryKey);
                try {
                    rocksApi.delete(rocksApi.getSecondaryIndexColumnFamily(), serializedCompoundKey);
                } finally {
                    serializedCompoundKey.release();
                }
            }
        }
    }

    public void mapSecondaryIndexes(@NonNull K primaryKey, @NonNull V value, int valueSize) throws RocksDBException {

        for (Spec<K, V, ?> index : indexSpec) {
            Iterable<?> mappedValues = index.getMultiValueIndexFunction().apply(primaryKey, value);
            for (Object secondaryKey : mappedValues) {
                // Protobuf 3 does not allow for optional fields, so the secondary
                // key should never be null.
                if (Objects.isNull(secondaryKey)) {
                    log.warn("{}: null secondary keys are not supported.", index.getName());
                    continue;
                }

                final ByteBuf serializedCompoundKey = getCompoundKey(
                        indexToId.get(index.getName().get()), secondaryKey, primaryKey);

                // We need to persist the actual value size, since multi-get API
                // requires an allocation of a direct buffer.
                final ByteBuf serializedIndexValue = Unpooled.buffer();
                serializedIndexValue.writeInt(valueSize);

                try {
                    rocksApi.insert(
                            rocksApi.getSecondaryIndexColumnFamily(),
                            serializedCompoundKey,
                            serializedIndexValue
                    );
                } finally {
                    serializedCompoundKey.release();
                    serializedIndexValue.release();
                }
            }
        }
    }

    /**
     * Return a compound key consisting of: Index ID (1 byte) + Secondary Key Hash (4 bytes) +
     * Serialized Secondary Key (Arbitrary) + Serialized Primary Key (Arbitrary)
     *
     * @param indexId      a mapping (byte) that represents the specific index name/spec
     * @param secondaryKey secondary key
     * @param primaryKey   primary key
     * @return byte representation of the compound key
     */
    private ByteBuf getCompoundKey(byte indexId, Object secondaryKey, K primaryKey) {
        final ByteBuf compositeKey = Unpooled.buffer();

        // Write the index ID (1 byte).
        compositeKey.writeByte(indexId);
        final int hashStart = compositeKey.writerIndex();

        // Move the index beyond the hash (4 bytes).
        compositeKey.writerIndex(compositeKey.writerIndex() + Integer.BYTES);

        // Serialize and write the secondary key and save the offset.
        final int secondaryStart = compositeKey.writerIndex();
        serializer.serialize(secondaryKey, compositeKey);
        final int secondaryLength = compositeKey.writerIndex() - secondaryStart;

        // Serialize and write the primary key and save the offset.
        serializer.serialize(primaryKey, compositeKey);
        final int end = compositeKey.writerIndex();

        // Move the pointer to the hash offset and write the hash.
        compositeKey.writerIndex(hashStart);
        int hash = DiskBackedCorfuTable.hashBytes(compositeKey.array(), secondaryStart, secondaryLength);
        compositeKey.writeInt(hash);

        // Move the pointer to the end.
        compositeKey.writerIndex(end);

        return compositeKey;
    }

    public <I> Iterable<Map.Entry<K, V>> getByIndex(
            @NonNull final Index.Name indexName, @NonNull I indexKey) {

        final List<ByteBuffer> keys = new ArrayList<>();
        final List<ByteBuffer> values = new ArrayList<>();

        try {
            String secondaryIndex = indexName.get();
            if (!secondaryIndexesAliasToPath.containsKey(secondaryIndex)) {
                return null;
            }

            byte indexId = indexToId.get(secondaryIndexesAliasToPath.get(secondaryIndex));
            rocksApi.prefixScan(rocksApi.getSecondaryIndexColumnFamily(),
                    indexId, indexKey, serializer, keys, values);

            // Prevent the keys from being consumed.
            final List<ByteBuffer> duplicateKeys = keys.stream()
                    .map(ByteBuffer::duplicate)
                    .collect(Collectors.toList());

            rocksApi.multiGet(rocksApi.getDefaultColumnFamily(), duplicateKeys, values);

            return Streams.zip(keys.stream(), values.stream(), (key, value) ->
                    new AbstractMap.SimpleEntry<>(
                            serializer.<K>deserializeTyped(Unpooled.wrappedBuffer(key), null),
                            serializer.<V>deserializeTyped(Unpooled.wrappedBuffer(value), null)
                    )).collect(Collectors.toList());
        } catch (RocksDBException e) {
            throw new UnrecoverableCorfuError(e);
        }
    }
}
