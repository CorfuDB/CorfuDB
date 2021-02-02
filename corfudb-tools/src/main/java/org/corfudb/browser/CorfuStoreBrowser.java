package org.corfudb.browser;

import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.ExampleSchemas.ExampleTableName;
import org.corfudb.runtime.ExampleSchemas.ManagedMetadata;
import org.corfudb.runtime.collections.CorfuStoreShim;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.collections.PersistedStreamingMap;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.StreamingMap;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TableSchema;
import org.corfudb.runtime.collections.ManagedTxnContext;
import org.corfudb.runtime.object.ICorfuVersionPolicy;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.serializer.DynamicProtobufSerializer;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.rocksdb.Options;

import com.google.protobuf.util.JsonFormat;

/**
 * This is the CorfuStore Browser Tool which prints data in a given namespace and table.
 *
 * - Created by pmajmudar on 10/16/2019.
 */
@Slf4j
public class CorfuStoreBrowser {
    private final CorfuRuntime runtime;
    private final String diskPath;

    /**
     * Creates a CorfuBrowser which connects a runtime to the server.
     * @param runtime CorfuRuntime which has connected to the server
     */
    public CorfuStoreBrowser(CorfuRuntime runtime) {
        this.runtime = runtime;
        this.diskPath = null;
    }

    /**
     * Creates a CorfuBrowser which connects a runtime to the server.
     * @param runtime CorfuRuntime which has connected to the server
     * @param diskPath path to temp disk directory for loading large tables
     *                 that won't fit into memory
     */
    public CorfuStoreBrowser(CorfuRuntime runtime, String diskPath) {
        this.runtime = runtime;
        this.diskPath = diskPath;
    }

    /**
     * Validate that the namespace is not null
     * @param namespace
     */
    private static void verifyNamespace(String namespace) {
        if (namespace == null) {
            throw new IllegalArgumentException("Please specify --namespace");
        }
    }

    /**
     * Validate that both namespace and tablename are present
     * @param namespace - the namespace where the table belongs
     * @param tableName - table name without the namespace
     */
    private static void verifyNamespaceAndTablename(String namespace, String tableName) {
        verifyNamespace(namespace);
        if (tableName == null) {
            throw new IllegalArgumentException("Please specify --tablename");
        }
    }

    /**
     * Fetches the table from the given namespace
     * @param namespace Namespace of the table
     * @param tableName Tablename
     * @return CorfuTable
     */
    public CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> getTable(
        String namespace, String tableName) {
        log.info("Namespace: {}", namespace);
        log.info("TableName: {}", tableName);

        ISerializer dynamicProtobufSerializer =
                new DynamicProtobufSerializer(runtime);
        Serializers.registerSerializer(dynamicProtobufSerializer);
        String fullTableName = TableRegistry.getFullyQualifiedTableName(namespace, tableName);
        SMRObject.Builder<CorfuTable<CorfuDynamicKey, CorfuDynamicRecord>> corfuTableBuilder =
        runtime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<CorfuDynamicKey, CorfuDynamicRecord>>() {})
                .setStreamName(fullTableName)
                .setSerializer(dynamicProtobufSerializer);

        if (diskPath != null) {
            final Options options = new Options().setCreateIfMissing(true);
            final Supplier<StreamingMap<CorfuDynamicKey, CorfuDynamicRecord>> mapSupplier = () ->
                    new PersistedStreamingMap<>(
                            Paths.get(diskPath),
                            options,
                            dynamicProtobufSerializer, runtime);
            corfuTableBuilder.setArguments(mapSupplier, ICorfuVersionPolicy.MONOTONIC);
        }

        return corfuTableBuilder.open();
    }

    /**
     * Prints the payload and metadata in the given table
     * @param namespace - the namespace where the table belongs
     * @param tablename - table name without the namespace
     * @return - number of entries in the table
     */
    public int printTable(String namespace, String tablename) {
        verifyNamespaceAndTablename(namespace, tablename);
        StringBuilder builder;

        CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> table = getTable(namespace, tablename);
        int size = table.size();
        final int batchSize = 50;
        Stream<Map.Entry<CorfuDynamicKey, CorfuDynamicRecord>> entryStream = table.entryStream();
        final Iterable<List<Map.Entry<CorfuDynamicKey, CorfuDynamicRecord>>> partitions =
                Iterables.partition(entryStream::iterator, batchSize);
        for (List<Map.Entry<CorfuDynamicKey, CorfuDynamicRecord>> partition : partitions) {
            for (Map.Entry<CorfuDynamicKey, CorfuDynamicRecord> entry : partition) {
                try {
                    builder = new StringBuilder("\nKey:\n")
                            .append(JsonFormat.printer().print(entry.getKey().getKey()))
                            .append("\nPayload:\n")
                            .append(entry.getValue() != null && entry.getValue().getPayload() != null ?
                                    JsonFormat.printer().print(entry.getValue().getPayload())   : "")
                            .append("\nMetadata:\n")
                            .append(entry.getValue() != null && entry.getValue().getMetadata() != null ?
                                    JsonFormat.printer().print(entry.getValue().getMetadata()): "")
                            .append("\n====================\n");
                    log.info(builder.toString());
                } catch (InvalidProtocolBufferException e) {
                    log.error("invalid protobuf: ", e);
                }
            }
        }
        return size;
    }

    /**
     * List all tables in CorfuStore
     * @param namespace - the namespace where the table belongs
     * @return - number of tables in this namespace
     */
    public int listTables(String namespace)
    {
        verifyNamespace(namespace);
        int numTables = 0;
        log.info("\n=====Tables=======\n");
        for (TableName tableName : runtime.getTableRegistry()
                .listTables(namespace)) {
            log.info("Table: " + tableName.getTableName());
            log.info("Namespace: " + tableName.getNamespace());
            numTables++;
        }
        log.info("\n======================\n");
        return numTables;
    }

    /**
     * Print information about a specific table in CorfuStore
     * @param namespace - the namespace where the table belongs
     * @param tablename - table name without the namespace
     * @return - number of entries in the table
     */
    public int printTableInfo(String namespace, String tablename) {
        verifyNamespaceAndTablename(namespace, tablename);
        log.info("\n======================\n");
        String fullName = TableRegistry.getFullyQualifiedTableName(namespace, tablename);
        UUID streamUUID = UUID.nameUUIDFromBytes(fullName.getBytes());
        CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> table = getTable(namespace, tablename);
        int tableSize = table.size();
        log.info("Table {} in namespace {} with ID {} has {} entries",
                tablename, namespace, streamUUID.toString(), tableSize);
        log.info("\n======================\n");
        return tableSize;
    }

    /**
     * Clear the table contents
     * @param namespace - the namespace where the table belongs
     * @param tablename - table name without the namespace
     * @return - number of entries in the table before clearing the table
     */
    public int dropTable(String namespace, String tablename) {
        verifyNamespaceAndTablename(namespace, tablename);
        log.info("\n======================\n");
        String fullName = TableRegistry.getFullyQualifiedTableName(namespace, tablename);
        UUID streamUUID = UUID.nameUUIDFromBytes(fullName.getBytes());
        CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> table = getTable(namespace, tablename);
        int tableSize = table.size();
        log.info("Table {} in namespace {} with ID {} with {} entries will be dropped...",
                tablename, namespace, streamUUID.toString(), tableSize);
        table.clear();
        log.info("Table cleared successfully");
        log.info("\n======================\n");
        return tableSize;
    }

    /**
     * Loads the table with random data
     * @param namespace - the namespace where the table belongs
     * @param tablename - table name without the namespace
     * @param numItems - total number of items to load
     * @param batchSize - number of items in each transaction
     * @param itemSize - size of each item - a random string array
     * @return - number of entries in the table
     */
    public int loadTable(String namespace, String tablename, int numItems, int batchSize, int itemSize) {
        verifyNamespaceAndTablename(namespace, tablename);
        CorfuStoreShim store = new CorfuStoreShim(runtime);
        try {
            TableOptions.TableOptionsBuilder<Object, Object> optionsBuilder = TableOptions.builder();
            if (diskPath != null) {
                optionsBuilder.persistentDataPath(Paths.get(diskPath));
            }
            final Table<ExampleTableName, ExampleTableName, ManagedMetadata> table = store.openTable(
                    namespace, tablename,
                    ExampleTableName.class,
                    ExampleTableName.class,
                    ManagedMetadata.class,
                    optionsBuilder.build());

            byte[] array = new byte[itemSize];

            /*
             * Random bytes are needed to bypass the compression.
             * If we don't use random bytes, compression will reduce the size of the payload siginficantly
             * increasing the time it takes to load data if we are trying to fill up disk.
             */
            new Random().nextBytes(array);
            ExampleTableName dummyVal = ExampleTableName.newBuilder().setNamespace(namespace+tablename)
                    .setTableName(new String(array, StandardCharsets.UTF_16)).build();
            log.info("WARNING: Loading {} items of {} size in {} batchSized transactions into {}${}",
                    numItems, itemSize, batchSize, namespace, tablename);
            int itemsRemaining = numItems;
            while (itemsRemaining > 0) {
                ManagedTxnContext tx = store.tx(namespace);
                for (int j = batchSize; j > 0 && itemsRemaining > 0; j--, itemsRemaining--) {
                    ExampleTableName dummyKey = ExampleTableName.newBuilder()
                            .setNamespace(Integer.toString(itemsRemaining))
                            .setTableName(Integer.toString(j)).build();
                    tx.putRecord(table, dummyKey, dummyVal, ManagedMetadata.getDefaultInstance());
                }
                long address = tx.commit();
                log.info("loadTable: Txn at address {}. Items  now left {}", address,
                        itemsRemaining);
            }
        } catch (Exception e) {
            log.error("loadTable: {} {} {} {} failed.", namespace, tablename, numItems, batchSize, e);
        }
        return (int)(Math.ceil((double)numItems/batchSize));
    }

    /**
     * Subscribe to and just dump the updates read from a table
     * @param namespace namespace to listen on
     * @param tableName tableName to subscribe to
     * @param stopAfter number of updates to stop listening at
     * @return number of updates read so far
     */
    public long listenOnTable(String namespace, String tableName, int stopAfter) {
        verifyNamespaceAndTablename(namespace, tableName);
        CorfuStoreShim store = new CorfuStoreShim(runtime);
        final Table<ExampleTableName, ExampleTableName, ManagedMetadata> table;
        try {
            TableOptions.TableOptionsBuilder<Object, Object> optionsBuilder = TableOptions.builder();
            if (diskPath != null) {
                optionsBuilder.persistentDataPath(Paths.get(diskPath));
            }
            table = store.openTable(
                    namespace, tableName,
                    ExampleTableName.class,
                    ExampleTableName.class,
                    ManagedMetadata.class,
                    optionsBuilder.build());
        } catch (Exception ex) {
            log.error("Unable to open table " + namespace + "$" + tableName);
            throw new RuntimeException("Unable to open table.");
        }

        int tableSize = table.count();
        log.info("Listening to updates on Table {} in namespace {} with size {} ID {}...",
                tableName, namespace, tableSize, table.getStreamUUID().toString());

        class StreamDumper implements StreamListener {
            @Getter
            AtomicLong txnRead;

            @Getter
            volatile boolean isError;

            public StreamDumper() {
                this.txnRead = new AtomicLong(0);
            }

            @Override
            public void onNext(CorfuStreamEntries results) {
                log.info("onNext invoked with {}. Read so far {}", results.getEntries().size(),
                        txnRead.get());
                results.getEntries().forEach((schema, entries) -> {
                    if (!schema.getTableName().equals(tableName)) {
                        log.warn("Not my table {}", schema);
                        return;
                    }
                    entries.forEach(entry -> {
                        try {
                            String builder = "\nKey:\n" +
                                    JsonFormat.printer().print(entry.getKey()) +
                                    "\nPayload:\n" +
                                    (entry.getPayload() != null ?
                                            JsonFormat.printer().print(entry.getPayload()) : "") +
                                    "\nMetadata:\n" +
                                    (entry.getMetadata() != null ?
                                            JsonFormat.printer().print(entry.getMetadata()) : "") +
                                    "\nOperation:\n" +
                                    entry.getOperation().toString() +
                                    "\n====================\n"+
                                    "\n====================\n";
                            log.info(builder);
                            long now = System.currentTimeMillis();
                            long recordInsertedAt = ((ManagedMetadata)entry.getMetadata()).getLastModifiedTime();
                            log.info("\n Time since insert: "+(now - recordInsertedAt)+"ms\n");
                            txnRead.incrementAndGet();
                        } catch (InvalidProtocolBufferException e) {
                            log.error("invalid protobuf: ", e);
                        }
                    });
                });
            }

            @Override
            public void onError(Throwable throwable) {
                isError = true;
                log.error("Subscriber hit error", throwable);
            }
        }

        StreamDumper streamDumper = new StreamDumper();
        List<String> tablesOfInterest = Collections.singletonList(tableName);
        String streamTag = ExampleTableName.getDescriptor().getOptions()
                .getExtension(CorfuOptions.tableSchema).getStreamTag(0);
        store.subscribeListener(streamDumper, namespace, streamTag, tablesOfInterest, null);
        while (streamDumper.getTxnRead().get() < stopAfter || streamDumper.isError()) {
            final int SLEEP_DURATION_MILLIS = 100;
            try {
                TimeUnit.MILLISECONDS.sleep(SLEEP_DURATION_MILLIS);
            } catch (InterruptedException e) {
                log.error("listenOnTable: Interrupted while sleeping", e);
            }
        }

        store.unsubscribeListener(streamDumper);

        return streamDumper.getTxnRead().get();
    }
}
