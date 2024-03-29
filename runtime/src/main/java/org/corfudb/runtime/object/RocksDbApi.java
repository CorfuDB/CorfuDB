package org.corfudb.runtime.object;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import lombok.NonNull;
import org.corfudb.runtime.collections.DiskBackedCorfuTable;
import org.corfudb.runtime.collections.RocksDbEntryIterator;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.util.serializer.ISerializer;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Transaction;

import java.nio.ByteBuffer;
import java.util.List;

import static org.corfudb.util.Utils.startsWith;

/**
 * Interface that is required to unify {@link RocksDB} and
 * {@link Transaction} interfaces.
 */
public interface RocksDbApi {

    String ESTIMATE_SIZE = "rocksdb.estimate-num-keys";
    boolean ALLOCATE_DIRECT_BUFFERS = true;

    byte[] get(@NonNull ColumnFamilyHandle columnHandle,
               @NonNull ByteBuf keyPayload) throws RocksDBException;

    void multiGet(
            @NonNull ColumnFamilyHandle columnFamilyHandle,
            @NonNull List<ByteBuffer> keys,
            @NonNull List<ByteBuffer> values) throws RocksDBException;

    void insert(@NonNull ColumnFamilyHandle columnHandle,
                @NonNull ByteBuf keyPayload,
                @NonNull ByteBuf valuePayload) throws RocksDBException;

    void delete(@NonNull ColumnFamilyHandle columnHandle,
                @NonNull ByteBuf keyPayload) throws RocksDBException;


    <K, V> RocksDbEntryIterator<K, V> getIterator(@NonNull ISerializer serializer);

    RocksIterator getRawIterator(ReadOptions readOptions, ColumnFamilyHandle columnFamilyHandle);

    /**
     * Given a secondary key, return all primary keys that map to it.
     *
     * @param secondaryIndexesHandle table family where secondary indexes reside     * @param serializer
     * @param indexId a static numerical representation of the index name
     * @param secondaryKey secondary key that we are searching for
     * @param keys the result of this API
     * @param values the result of this API
     */
    void prefixScan(
            ColumnFamilyHandle secondaryIndexesHandle,
            byte indexId, Object secondaryKey,
            ISerializer serializer,
            List<ByteBuffer> keys,
            List<ByteBuffer> values);

    /**
     * Given a secondary key, return all primary keys that map to it.
     *
     * @param secondaryKey secondary key that we are searching for
     * @param secondaryIndexesHandle table family where secondary indexes reside
     * @param serializer serializer used for (de)serializing keys/values
     * @param originalReadOptions {@link ReadOptions} used when performing prefix scan
     * @param keys the result of this API
     * @param values the result of this API
     * @param allocateDirectBuffer whether to allocate a direct buffer for results
     */
    default void prefixScan(
            Object secondaryKey, ColumnFamilyHandle secondaryIndexesHandle, byte indexId,
            ISerializer serializer, ReadOptions originalReadOptions,
            List<ByteBuffer> keys, List<ByteBuffer> values, boolean allocateDirectBuffer) {

        Preconditions.checkState(keys.isEmpty(), "Key set should always be empty.");
        Preconditions.checkState(values.isEmpty(), "Value set should always be empty.");

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

        try (RocksIterator entryIterator = getRawIterator(readOptions, secondaryIndexesHandle)) {
            entryIterator.seek(partialPrefix);
            while (entryIterator.isValid() && !startsWith(entryIterator.key(), partialPrefix)) {
                entryIterator.next();
            }

            while (entryIterator.isValid() && startsWith(entryIterator.key(), prefix)) {
                final ByteBuffer serializedPrimaryKey = ByteBuffer
                        .allocateDirect(entryIterator.key().length - prefix.length)
                        .put(entryIterator.key(), prefix.length, entryIterator.key().length - prefix.length);

                keys.add(serializedPrimaryKey);
                if (allocateDirectBuffer) {
                    final int valueSize = Unpooled.wrappedBuffer(entryIterator.value()).readInt();
                    values.add(ByteBuffer.allocateDirect(valueSize));
                }

                entryIterator.next();
            }
        } catch (Exception unexpected) {
            throw new UnrecoverableCorfuError(unexpected);
        } finally {
            serializedSecondaryKey.release();
            compositeKey.release();
        }

        keys.forEach(ByteBuffer::flip);
        readOptions.close();
    }

    void clear() throws RocksDBException;

    long exactSize();

    OptimisticTransactionDB getRocksDb();

    default long estimateSize() {
        try {
            return getRocksDb().getLongProperty(ESTIMATE_SIZE);
        } catch (RocksDBException e) {
            throw new UnrecoverableCorfuError(e);
        }
    }

    void close() throws RocksDBException;
}
