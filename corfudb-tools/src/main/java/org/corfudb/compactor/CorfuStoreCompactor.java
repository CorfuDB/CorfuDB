package org.corfudb.compactor;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.util.serializer.DynamicProtobufSerializer;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.ProtobufSerializer;
import org.corfudb.util.serializer.Serializers;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.corfudb.runtime.view.TableRegistry.getFullyQualifiedTableName;
import static org.slf4j.Logger.ROOT_LOGGER_NAME;

/**
 * CorfuStoreCompactor compacts all CorfuStoreData.
 * Created by zlokhandwala on 11/5/19.
 */
@Slf4j
public class CorfuStoreCompactor {
    private static final String LOG_LEVEL = "INFO";

    private final CorfuRuntime runtime;
    private final CorfuStore corfuStore;

    private static final String CHECKPOINT_TABLE_NAME = "CheckpointTable";
    private static final String TRIM_TABLE_NAME = "TrimTable";

    private static final String CORFU_STORE = "corfuStore";

    private final ScheduledExecutorService service;

    private final Duration interval;

    public CorfuStoreCompactor(@Nonnull CorfuRuntime corfuRuntime,
                               @Nonnull Duration interval) {
        configureLogger();
        this.runtime = corfuRuntime;
        this.corfuStore = new CorfuStore(corfuRuntime);
        this.interval = interval;

        this.service = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).build());
        log.info("Successfully initialized Compactor.");
    }

    /**
     * Fetches the checkpoint table.
     * This table stores the checkpoint token.
     *
     * @return Checkpoint table instance.
     */
    private CorfuTable<String, Token> getCheckpointTable() {
        return runtime.getObjectsView().build()
                .setStreamName(CHECKPOINT_TABLE_NAME)
                .setTypeToken(new TypeToken<CorfuTable<String, Token>>() {
                })
                .open();
    }

    /**
     * Fetches the Trim Table.
     * This table stores the observed trim points. A prefix trim is invoked on the next cycle.
     *
     * @return Trim table instance.
     */
    private CorfuTable<String, Token> getTrimTable() {
        return runtime.getObjectsView().build()
                .setStreamName(TRIM_TABLE_NAME)
                .setTypeToken(new TypeToken<CorfuTable<String, Token>>() {
                })
                .open();
    }

    /**
     * Initializes the Compaction task.
     *
     * @return Scheduled Future waiting on the task.
     */
    public ScheduledFuture initialize() {
        return service.scheduleAtFixedRate(() -> {
                    try {
                        compact();
                    } catch (Exception th) {
                        log.error("Compaction task failed : ", th);
                    }
                },
                0L,
                interval.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    /**
     * Runs a trim and checkpoint task.
     */
    public void compact() {
        trim();
        log.info("Trim task completed.");
        checkpoint();
        log.info("Checkpoint task completed.");
    }

    /**
     * Configures logger.
     */
    private static void configureLogger() {
        final Logger root = (Logger) LoggerFactory.getLogger(ROOT_LOGGER_NAME);
        final Level level = Level.toLevel(LOG_LEVEL);
        root.setLevel(level);
    }

    /**
     * Returns a CorfuTable instance using the dynamic serializer.
     *
     * @param tableName  Namespace and tableName of the table.
     * @param serializer Instance of the dynamic Serializer.
     * @return Returns an instance of the CorfuTable.
     */
    private CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> getTable(TableName tableName, ISerializer serializer) {
        String streamName = getFullyQualifiedTableName(tableName.getNamespace(), tableName.getTableName());
        return runtime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<CorfuDynamicKey, CorfuDynamicRecord>>() {
                })
                .setStreamName(streamName)
                .setSerializer(serializer)
                .open();
    }

    /**
     * Checkpoints all Tables opened by CorfuStore.
     */
    private void checkpoint() {
        Collection<TableName> tableNames = corfuStore.listTables(null);
        ISerializer protobufSerializer = Serializers.getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE);

        try {
            ISerializer dynamicProtobufSerializer = new DynamicProtobufSerializer(runtime);
            Serializers.registerSerializer(dynamicProtobufSerializer);

            MultiCheckpointWriter<CorfuTable> mcw = new MultiCheckpointWriter<>();
            mcw.addMap(getCheckpointTable());
            mcw.addMap(getTrimTable());
            Token trimToken = mcw.appendCheckpoints(runtime, "corfu-tools");

            for (TableName tableName : tableNames) {
                CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> table = getTable(tableName, dynamicProtobufSerializer);
                mcw = new MultiCheckpointWriter<>();
                mcw.addMap(table);
                long startTime = System.currentTimeMillis();
                log.info("Starting checkpointing for namespace: {}, table name: {}",
                        tableName.getNamespace(), tableName.getTableName());
                Token token = mcw.appendCheckpoints(runtime, "corfu-tools");
                long endTime = System.currentTimeMillis();
                log.info("Completed checkpointing for namespace: {}, table name: {} with {} entries in {}ms",
                        tableName.getNamespace(), tableName.getTableName(), table.size(), (endTime - startTime));
                trimToken = Token.min(trimToken, token);

                runtime.getObjectsView().getObjectCache().clear();
                runtime.getStreamsView().getStreamCache().clear();
            }

            getCheckpointTable().put(CORFU_STORE, trimToken);
        } finally {
            Serializers.registerSerializer(protobufSerializer);
        }
    }

    /**
     * Performs a prefix trim on the last observed trim point.
     */
    private void trim() {
        Token trimToken = getTrimTable().get(CORFU_STORE);

        if (trimToken != null) {
            runtime.getAddressSpaceView().prefixTrim(trimToken);
            runtime.getAddressSpaceView().gc();
            runtime.getAddressSpaceView().invalidateClientCache();
            runtime.getAddressSpaceView().invalidateServerCaches();
            log.info("Trimmed at {}", trimToken);
        }

        Token lastCheckpointToken = getCheckpointTable().get(CORFU_STORE);
        if (lastCheckpointToken != null) {
            getTrimTable().put(CORFU_STORE, lastCheckpointToken);
            log.info("New trim address learnt {}", lastCheckpointToken);
        }
    }
}
