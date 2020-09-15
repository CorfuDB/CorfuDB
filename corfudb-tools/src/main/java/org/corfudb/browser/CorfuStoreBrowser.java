package org.corfudb.browser;

import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.collections.PersistedStreamingMap;
import org.corfudb.runtime.collections.StreamingMap;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxBuilder;
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
                            .append(JsonFormat.printer().print(entry.getValue().getPayload()))
                            .append("\nMetadata:\n")
                            .append(JsonFormat.printer().print(entry.getValue().getMetadata()))
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
        CorfuStore store = new CorfuStore(runtime);
        try {
            TableOptions.TableOptionsBuilder<Object, Object> optionsBuilder = TableOptions.builder();
            if (diskPath != null) {
                optionsBuilder.persistentDataPath(Paths.get(diskPath));
            }
            store.openTable(namespace, tablename,
                    TableName.getDefaultInstance().getClass(),
                    TableName.getDefaultInstance().getClass(),
                    TableName.getDefaultInstance().getClass(),
                    optionsBuilder.build());

            byte[] array = new byte[itemSize];

            /*
             * Random bytes are needed to bypass the compression.
             * If we don't use random bytes, compression will reduce the size of the payload siginficantly
             * increasing the time it takes to load data if we are trying to fill up disk.
             */
            new Random().nextBytes(array);
            TableName dummyVal = TableName.newBuilder().setNamespace(namespace+tablename)
                    .setTableName(new String(array, StandardCharsets.UTF_16)).build();
            log.info("WARNING: Loading {} items of {} size in {} batchSized transactions into {}${}",
                    numItems, itemSize, batchSize, namespace, tablename);
            int itemsRemaining = numItems;
            while (itemsRemaining > 0) {
                log.info("loadTable: Items left {}", itemsRemaining);
                TxBuilder tx = store.tx(namespace);
                for (int j = batchSize; j > 0 && itemsRemaining > 0; j--, itemsRemaining--) {
                    TableName dummyKey = TableName.newBuilder()
                            .setNamespace(Integer.toString(itemsRemaining))
                            .setTableName(Integer.toString(j)).build();
                    tx.update(tablename, dummyKey, dummyVal, dummyVal);
                }
                tx.commit();
            }
        } catch (Exception e) {
            log.error("loadTable: {} {} {} {} failed.", namespace, tablename, numItems, batchSize, e);
        }
        return (int)(Math.ceil((double)numItems/batchSize));
    }
}
