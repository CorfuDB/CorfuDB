package org.corfudb.runtime.collections;

import com.google.common.collect.Streams;
import com.google.common.math.Quantiles;
import com.google.gson.Gson;
import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.AlphaChars;
import net.jqwik.api.constraints.Size;
import net.jqwik.api.constraints.StringLength;
import net.jqwik.api.constraints.UniqueElements;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.function.Failable;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuOptions.ConsistencyModel;
import org.corfudb.runtime.CorfuOptions.SizeComputationModel;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.object.PersistenceOptions;
import org.corfudb.runtime.object.PersistenceOptions.PersistenceOptionsBuilder;
import org.corfudb.runtime.object.RocksDbReadCommittedTx;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.test.SampleSchema;
import org.corfudb.test.SampleSchema.EventInfo;
import org.corfudb.test.SampleSchema.Uuid;
import org.corfudb.util.serializer.ISerializer;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.rocksdb.Env;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileManager;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.corfudb.common.metrics.micrometer.MeterRegistryProvider.MeterRegistryInitializer.initClientMetrics;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class ExtensibleCacheTest extends AbstractViewTest implements AutoCloseable {

    private static final String defaultTableName = "diskBackedTable";
    private static final String diskBackedDirectory = "/tmp/";
    private static final Options defaultOptions = new Options().setCreateIfMissing(true);
    private static final ISerializer defaultSerializer = new PojoSerializer(String.class);
    private static final int SAMPLE_SIZE = 100;
    private static final int NUM_OF_TRIES = 1;
    private static final boolean ENABLE_READ_YOUR_WRITES = true;
    private static final boolean EXACT_SIZE = true;


    public ExtensibleCacheTest() {
        AbstractViewTest.initEventGroup();
        resetTests();
    }

    @Override
    public void close() {
        super.cleanupBuffers();
        AbstractViewTest.cleanEventGroup();
    }

    private <V> ExtensibleCache<String, V> setupTable(
            String streamName, boolean readYourWrites, boolean exact_size,
            Options options, ISerializer serializer) {

        PersistenceOptionsBuilder persistenceOptions = PersistenceOptions.builder()
                .dataPath(Paths.get(diskBackedDirectory, streamName));
        if (!readYourWrites) {
            persistenceOptions.consistencyModel(ConsistencyModel.READ_COMMITTED);
        }

        if (exact_size) {
            persistenceOptions.sizeComputationModel(SizeComputationModel.EXACT_SIZE);
        } else {
            persistenceOptions.sizeComputationModel(SizeComputationModel.ESTIMATE_NUM_KEYS);
        }

        getDefaultRuntime().getSerializers().registerSerializer(serializer);

        return new ExtensibleCache<>(persistenceOptions.build(), options, serializer, Index.Registry.empty());
    }

    private ExtensibleCache<String, String> setupTable(String streamName, boolean readYourWrites) {
        return setupTable(streamName, readYourWrites, EXACT_SIZE, defaultOptions, defaultSerializer);
    }

    private ExtensibleCache<String, String> setupTable() {
        return setupTable(defaultTableName, ENABLE_READ_YOUR_WRITES);
    }

    /**
     * Executed the specified function in a transaction.
     *
     * @param functor function which will be executed within a transaction
     * @return the address of the commit
     */
    private long executeTx(Runnable functor) {
        long commitAddress;
        getDefaultRuntime().getObjectsView().TXBegin();
        try {
            functor.run();
        } finally {
            commitAddress = getDefaultRuntime().getObjectsView().TXEnd();
        }

        return commitAddress;
    }

    @Override
    public void resetTests() {
        RocksDB.loadLibrary();
        super.resetTests();
    }

    @Property(tries = NUM_OF_TRIES)
    void nonTxInsertGetRemove(@ForAll @Size(SAMPLE_SIZE) Set<String> intended) {
        resetTests();
        try (final ExtensibleCache<String, String> table = setupTable()) {
            intended.forEach(value -> table.insert(value, value));
            Assertions.assertEquals(table.size(), intended.size());
            intended.forEach(value -> Assertions.assertEquals(table.get(value), value));
            intended.forEach(table::delete);
        }
    }


    /**
     * Single type POJO serializer.
     */
    public static class PojoSerializer implements ISerializer {

        private static final int SERIALIZER_OFFSET = 29;  // Random number.
        private final Gson gson = new Gson();
        private final Class<?> clazz;

        PojoSerializer(Class<?> clazz) {
            this.clazz = clazz;
        }

        @Override
        public byte getType() {
            return SERIALIZER_OFFSET;
        }

        @Override
        public Object deserialize(ByteBuf b, CorfuRuntime rt) {
            return gson.fromJson(new String(ByteBufUtil.getBytes(b)), clazz);
        }

        @Override
        public void serialize(Object o, ByteBuf b) {
            b.writeBytes(gson.toJson(o).getBytes());
        }
    }

    /**
     * Sample POJO class.
     */
    @Data
    @Builder
    public static class Pojo {
        public final String payload;
    }
}
