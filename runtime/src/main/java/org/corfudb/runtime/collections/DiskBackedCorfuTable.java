package org.corfudb.runtime.collections;

import io.micrometer.core.instrument.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.object.ConsistencyOptions;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.RocksStubTx;
import org.corfudb.runtime.object.RocksTableApi;
import org.corfudb.runtime.object.ISMRSnapshot;
import org.corfudb.runtime.object.RocksSource;
import org.corfudb.runtime.object.RocksTx;
import org.corfudb.runtime.object.ViewGenerator;
import org.corfudb.util.serializer.ISerializer;
import org.rocksdb.CompactionOptionsUniversal;
import org.rocksdb.CompressionType;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteOptions;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
@AllArgsConstructor
@Builder(toBuilder=true)
public class DiskBackedCorfuTable<K, V> implements ICorfuSMR<DiskBackedCorfuTable<K, V>>,
        ViewGenerator<DiskBackedCorfuTable<K, V>> {

    public static final String DISK_BACKED = "diskBacked";
    public static final String TRUE = "true";
    public static final int BOUND = 100;
    public static final int SAMPLING_RATE = 40;

    private final WriteOptions writeOptions = new WriteOptions()
            .setDisableWAL(true)
            .setSync(false);

    static {
        RocksDB.loadLibrary();
    }

    /**
     * A set of options defined for disk-backed {@link PersistedStreamingMap}.
     *
     * For a set of options that dictate RocksDB memory usage can be found here:
     * https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB
     *
     * Block Cache:  Which can be set via Options::setTableFormatConfig.
     *               Out of box, RocksDB will use LRU-based block cache
     *               implementation with 8MB capacity.
     * Index/Filter: Is a function of the block cache. Generally it inflates
     *               the block cache by about 50%. The exact number can be
     *               retrieved via "rocksdb.estimate-table-readers-mem"
     *               property.
     * Write Buffer: Also known as memtable is defined by the ColumnFamilyOptions
     *               option. The default is 64 MB.
     */
    public static Options getPersistedStreamingMapOptions() {
        final int maxSizeAmplificationPercent = 50;
        final Options options = new Options();

        options.setCreateIfMissing(true);
        options.setCompressionType(CompressionType.LZ4_COMPRESSION);

        // Set a threshold at which full compaction will be triggered.
        // This is important as it purges tombstoned entries.
        final CompactionOptionsUniversal compactionOptions = new CompactionOptionsUniversal();
        compactionOptions.setMaxSizeAmplificationPercent(maxSizeAmplificationPercent);
        options.setCompactionOptionsUniversal(compactionOptions);
        return options;
    }

    private final AtomicInteger dataSetSize = new AtomicInteger();
    private final CorfuRuntime corfuRuntime;
    private final ISerializer serializer;
    private final RocksTableApi<DiskBackedCorfuTable<K, V>> tableView;
    private final RocksSource<DiskBackedCorfuTable<K, V>> tableSource;
    private final String absolutePathString;
    private final Options rocksDbOptions;
    private final ConsistencyOptions consistencyOptions;
    private final OptimisticTransactionDB rocksDb;

    public DiskBackedCorfuTable(@NonNull Path dataPath,
                                @NonNull Options rocksDbOptions,
                                @NonNull ISerializer serializer,
                                @NonNull CorfuRuntime corfuRuntime) {
        this(dataPath, rocksDbOptions,
                ConsistencyOptions.builder().build(),
                serializer, corfuRuntime);
    }

    public DiskBackedCorfuTable(@NonNull Path dataPath,
                                @NonNull Options rocksDbOptions,
                                @NonNull ConsistencyOptions consistencyOptions,
                                @NonNull ISerializer serializer,
                                @NonNull CorfuRuntime corfuRuntime) {
        this.absolutePathString = dataPath.toFile().getAbsolutePath();
        this.rocksDbOptions = rocksDbOptions;
        this.consistencyOptions = consistencyOptions;

        try {
            RocksDB.destroyDB(this.absolutePathString, this.rocksDbOptions);
            this.rocksDb = OptimisticTransactionDB.open(rocksDbOptions, dataPath.toFile().getAbsolutePath());
            this.tableSource = new RocksSource<>(
                    this.rocksDb,
                    this.writeOptions
            );
            this.tableView = this.tableSource;
        } catch (RocksDBException e) {
            throw new UnrecoverableCorfuError(e);
        }

        this.serializer = serializer;
        this.corfuRuntime = corfuRuntime;
    }

    public V get(@NonNull Object key) {
        Optional<Timer.Sample> recordSample = MicroMeterUtils.startTimer(
                SAMPLING_RATE > ThreadLocalRandom.current().nextInt(BOUND));

        final ByteBuf keyPayload = Unpooled.buffer();

        try {
            // Serialize in the try-catch block to release ByteBuf when an exception occurs.
            serializer.serialize(key, keyPayload);
            byte[] value = tableView.get(keyPayload);
            if (value == null) {
                return null;
            }
            return (V) serializer.deserialize(Unpooled.wrappedBuffer(value), corfuRuntime);
        } catch (RocksDBException ex) {
            throw new UnrecoverableCorfuError(ex);
        } finally {
            keyPayload.release();
            MicroMeterUtils.time(recordSample, "corfu_table.read.timer", DISK_BACKED, TRUE);
        }
    }

    public boolean containsKey(@NonNull Object key) {
        final ByteBuf keyPayload = Unpooled.buffer();

        try {
            // Serialize in the try-catch block to release ByteBuf when an exception occurs.
            serializer.serialize(key, keyPayload);
            byte[] value = tableView.get(keyPayload);
            return value != null;
        } catch (RocksDBException ex) {
            throw new UnrecoverableCorfuError(ex);
        } finally {
            keyPayload.release();
        }
    }

    public DiskBackedCorfuTable<K, V> put(@NonNull K key, @NonNull V value) {
        Optional<Timer.Sample> recordSample = MicroMeterUtils.startTimer(
                SAMPLING_RATE > ThreadLocalRandom.current().nextInt(BOUND));

        final ByteBuf keyPayload = Unpooled.buffer();
        final ByteBuf valuePayload = Unpooled.buffer();

        try {
            // Serialize in the try-catch block to release ByteBuf when an exception occurs.
            serializer.serialize(key, keyPayload);
            serializer.serialize(value, valuePayload);
            tableView.insert(keyPayload, valuePayload);
            return this;
        } catch (RocksDBException ex) {
            throw new UnrecoverableCorfuError(ex);
        } finally {
            keyPayload.release();
            valuePayload.release();
            MicroMeterUtils.time(recordSample, "corfu_table.write.timer", DISK_BACKED, TRUE);
        }
    }

    public DiskBackedCorfuTable<K, V> remove(@NonNull Object key) {
        final ByteBuf keyPayload = Unpooled.buffer();

        try {
            // Serialize in the try-catch block to release ByteBuf when an exception occurs.
            serializer.serialize(key, keyPayload);
            tableView.delete(keyPayload);
            return this;
        } catch (RocksDBException ex) {
            throw new UnrecoverableCorfuError(ex);
        } finally {
            keyPayload.release();
        }
    }

    public Stream<Map.Entry<K, V>> entryStream() {
        final RocksDbEntryIterator<K, V> entryIterator = tableView.getIterator(serializer);
        Stream<Map.Entry<K, V>> resStream = StreamSupport.stream(Spliterators.spliteratorUnknownSize(entryIterator,
                Spliterator.ORDERED), false);
        resStream.onClose(entryIterator::close);
        return resStream;
    }

    @Override
    public void close() {
        try {
            rocksDb.close();
            RocksDB.destroyDB(absolutePathString, rocksDbOptions);
            log.info("Cleared RocksDB data on {}", absolutePathString);
        } catch (RocksDBException e) {
            throw new UnrecoverableCorfuError(e);
        }
    }

    @Override
    public DiskBackedCorfuTable<K, V> getContext(Context context) {
        return null;
    }

    @Override
    public ISMRSnapshot<DiskBackedCorfuTable<K, V>> getSnapshot() {
        return tableSource.getSnapshot(this);
    }

    private DiskBackedCorfuTable<K, V> newView(RocksTableApi<DiskBackedCorfuTable<K, V>> view) {
        return toBuilder().tableView(view).build();
    }

    @Override
    public DiskBackedCorfuTable<K, V> newView(Snapshot snapshot) {
        if (consistencyOptions.isReadYourWrites()) {
            return newView(new RocksTx<>(rocksDb, writeOptions, snapshot));
        } else {
            return newView(new RocksStubTx<>(rocksDb, snapshot));
        }
    }
}
