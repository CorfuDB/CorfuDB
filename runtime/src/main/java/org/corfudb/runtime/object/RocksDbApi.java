package org.corfudb.runtime.object;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import lombok.NonNull;
import org.corfudb.runtime.collections.DiskBackedCorfuTable;
import org.corfudb.runtime.collections.RocksDbEntryIterator;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.PrimitiveSerializer;
import org.corfudb.util.serializer.Serializers;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Transaction;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.corfudb.util.Utils.startsWith;

/**
 * Interface that is required to unify {@link RocksDB} and
 * {@link Transaction} interfaces.
 *
 * @param <S>
 */
public interface RocksDbApi<S extends SnapshotGenerator<S>> {

    byte[] get(@NonNull ColumnFamilyHandle columnHandle,
               @NonNull ByteBuf keyPayload) throws RocksDBException;

    void insert(@NonNull ColumnFamilyHandle columnHandle,
                @NonNull ByteBuf keyPayload,
                @NonNull ByteBuf valuePayload) throws RocksDBException;

    void delete(@NonNull ColumnFamilyHandle columnHandle,
                @NonNull ByteBuf keyPayload) throws RocksDBException;


    <K, V> RocksDbEntryIterator<K, V> getIterator(@NonNull ISerializer serializer);

    RocksIterator getRawIterator(ReadOptions readOptions, ColumnFamilyHandle columnFamilyHandle);

    Set<ByteBuf> prefixScan(ColumnFamilyHandle secondaryIndexesHandle,
                            byte indexId, Object secondaryKey,
                            ISerializer serializer);

    /**
     * Given a secondary key, return all primary keys that map to it.
     *
     * @param secondaryKey secondary key we are searching for
     * @param secondaryIndexesHandle table family where secondary indexes reside
     * @param indexId a static numerical representation of the index name
     * @param serializer serializer used for (de)serializing keys/values
     * @param readOptions default read options
     * @return A set of serialized primary keys associated with
     *         the specified secondary key.
     */
    default Set<ByteBuf> prefixScan(Object secondaryKey,
            ColumnFamilyHandle secondaryIndexesHandle, byte indexId,
            ISerializer serializer, ReadOptions originalReadOptions) {

        ReadOptions readOptions = new ReadOptions(originalReadOptions)
                .setPrefixSameAsStart(true);

        final ByteBuf serializedSecondaryKey = Unpooled.buffer();
        serializer.serialize(secondaryKey, serializedSecondaryKey);
        final ByteBuf compositeKey = Unpooled.buffer();

        // Write the index ID (1 byte).
        compositeKey.writeByte(indexId);
        compositeKey.writeInt(DiskBackedCorfuTable.hashBytes(serializedSecondaryKey.array(),
                serializedSecondaryKey.arrayOffset(), serializedSecondaryKey.readableBytes()));

        final byte[] partialPrefix = ByteBufUtil.getBytes(compositeKey);
        compositeKey.writeBytes(serializedSecondaryKey);
        final byte[] prefix = ByteBufUtil.getBytes(compositeKey);


        Set<ByteBuf> results = new HashSet<>();

        try (RocksIterator entryIterator = getRawIterator(readOptions, secondaryIndexesHandle)) {
            entryIterator.seek(partialPrefix);
            while (entryIterator.isValid() && !startsWith(entryIterator.key(), partialPrefix)) {
                entryIterator.next();
            }

            while (entryIterator.isValid() && startsWith(entryIterator.key(), prefix)) {
                final ByteBuf serializedPrimaryKey = Unpooled.wrappedBuffer(entryIterator.key(),
                        prefix.length, entryIterator.key().length - prefix.length);
                results.add(serializedPrimaryKey);
                entryIterator.next();
            }
        } catch (Exception unexpected) {
            throw new UnrecoverableCorfuError(unexpected);
        } finally {
            serializedSecondaryKey.release();
            compositeKey.release();
        }

        return results;
    }

    void clear() throws RocksDBException;

    long exactSize();

    void close() throws RocksDBException;
}
