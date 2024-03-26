package org.corfudb.browser;

import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.protobuf.Any;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.ProtobufFileName;
import org.corfudb.runtime.CorfuStoreMetadata.TableDescriptors;
import org.corfudb.runtime.CorfuStoreMetadata.TableMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.ExampleSchemas.ExampleTableName;
import org.corfudb.runtime.ExampleSchemas.ManagedMetadata;
import org.corfudb.runtime.collections.CorfuRecord;
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
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ICorfuVersionPolicy;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.serializer.DynamicProtobufSerializer;
import org.rocksdb.Options;

import com.google.protobuf.util.JsonFormat;

import javax.annotation.Nonnull;

/**
 * This is the CorfuStore Browser/Editor Tool which prints data in a given
 * namespace and table.
 *
 * - Created by pmajmudar on 10/16/2019.
 */
@Slf4j
@SuppressWarnings("checkstyle:printLine")
public class CorfuStoreBrowserEditor {
    private final CorfuRuntime runtime;
    private final String diskPath;
    private final DynamicProtobufSerializer dynamicProtobufSerializer;
    private final String QUOTE = "\"";

    /**
     * Creates a CorfuBrowser which connects a runtime to the server.
     * @param runtime CorfuRuntime which has connected to the server
     */
    public CorfuStoreBrowserEditor(CorfuRuntime runtime) {
        this(runtime, null);
    }

    /**
     * Creates a CorfuBrowser which connects a runtime to the server.
     * @param runtime CorfuRuntime which has connected to the server
     * @param diskPath path to temp disk directory for loading large tables
     *                 that won't fit into memory
     */
    public CorfuStoreBrowserEditor(CorfuRuntime runtime, String diskPath) {
        this.runtime = runtime;
        this.diskPath = diskPath;
        dynamicProtobufSerializer =
            new DynamicProtobufSerializer(runtime);
        runtime.getSerializers().registerSerializer(dynamicProtobufSerializer);
    }

    /**
     * Print ILogData metadata map for a given address
     *
     * @param address specific address to read metadata map from
     * @return
     */
    public EnumMap<IMetadata.LogUnitMetadataType, Object> printMetadataMap(long address) {
        ILogData data = runtime.getAddressSpaceView().read(address);
        System.out.println("\n========== Metadata Map ==========\n");
        for(Map.Entry<IMetadata.LogUnitMetadataType, Object> entry : data.getMetadataMap().entrySet()) {
            System.out.println(entry.getKey() + "  :: " + entry.getValue());
        }
        System.out.println("\n==================================\n");


        return data.getMetadataMap();
    }

    /**
     * Fetches the table from the given namespace
     * @param namespace Namespace of the table
     * @param tableName Tablename
     * @return CorfuTable
     */
    public CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> getTable(
        String namespace, String tableName) {
        System.out.println("Namespace: " + namespace);
        System.out.println("TableName: " + tableName);

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
        if (namespace.equals(TableRegistry.CORFU_SYSTEM_NAMESPACE)
                && tablename.equals(TableRegistry.REGISTRY_TABLE_NAME)) {
            // TableDescriptors are an internal type that use Any protobuf.
            // JsonFormat has a known bug where it fails to print Any protobuf payloads
            // So to work around this bug, avoid dumping the TableDescriptor table directly.
            return printTableRegistry();
        }
        CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> table =
            getTable(namespace, tablename);
        int size = table.size();
        final int batchSize = 50;
        Stream<Map.Entry<CorfuDynamicKey, CorfuDynamicRecord>> entryStream = table.entryStream();
        final Iterable<List<Map.Entry<CorfuDynamicKey, CorfuDynamicRecord>>> partitions =
                Iterables.partition(entryStream::iterator, batchSize);
        for (List<Map.Entry<CorfuDynamicKey, CorfuDynamicRecord>> partition : partitions) {
            for (Map.Entry<CorfuDynamicKey, CorfuDynamicRecord> entry : partition) {
                printKey(entry);
                printPayload(entry);
                printMetadata(entry);
            }
        }
        return size;
    }

    private void printKey(Map.Entry<CorfuDynamicKey, CorfuDynamicRecord> entry) {
        StringBuilder builder;
        try {
            builder = new StringBuilder("\nKey:\n")
                    .append(JsonFormat.printer().print(entry.getKey().getKey()));
            System.out.println(builder.toString());
        } catch (Exception e) {
            log.error("invalid key: ", e);
        }
    }

    private void printPayload(Map.Entry<CorfuDynamicKey, CorfuDynamicRecord> entry) {
        StringBuilder builder;
        if (entry.getValue().getPayload() == null) {
            log.error("payload is NULL");
            return;
        }

        try {
            builder = new StringBuilder("\nPayload:\n")
                    .append(JsonFormat.printer().print(entry.getValue().getPayload()));
            System.out.println(builder.toString());
        } catch (Exception e) {
            log.error("invalid payload: ", e);
        }
    }

    private int printTableRegistry() {
        for (Map.Entry<TableName, CorfuRecord<TableDescriptors, TableMetadata>> entry :
                dynamicProtobufSerializer.getCachedRegistryTable().entrySet()) {
            try {
                StringBuilder builder = new StringBuilder("\nKey:\n")
                        .append(JsonFormat.printer().print(entry.getKey()));
                System.out.println(builder.toString());
            } catch (Exception e) {
                log.error("Unable to print tableName of this registry table key {}", entry.getKey());
            }
            try {
                StringBuilder builder = new StringBuilder();
                builder.append("\nkeyType = \"" + entry.getValue().getPayload().getKey().getTypeUrl() + QUOTE);
                builder.append("\npayloadType = \"" + entry.getValue().getPayload().getValue().getTypeUrl() + QUOTE);
                builder.append("\nmetadataType = \"" + entry.getValue().getPayload().getMetadata().getTypeUrl() + QUOTE);
                builder.append("\nProtobuf Source Files: \"" +
                        entry.getValue().getPayload().getFileDescriptorsMap().keySet()
                );
                System.out.println(builder.toString());
            } catch (Exception e) {
                log.error("Unable to extract payload fields from registry table key {}", entry.getKey());
            }

            try {
                StringBuilder builder = new StringBuilder("\nMetadata:\n")
                        .append(JsonFormat.printer().print(entry.getValue().getMetadata()));
                System.out.println(builder.toString());
            } catch (Exception e) {
                log.error("Unable to print metadata section of registry table");
            }
        }

        return dynamicProtobufSerializer.getCachedRegistryTable().size();
    }

    private void printMetadata(Map.Entry<CorfuDynamicKey, CorfuDynamicRecord> entry) {
        StringBuilder builder;
        if (entry.getValue().getMetadata() == null) {
            log.error("metadata is NULL");
            return;
        }
        try {
            builder = new StringBuilder("\nMetadata:\n")
                    .append(JsonFormat.printer().print(entry.getValue().getMetadata()));
            System.out.println(builder.toString());
        } catch (Exception e) {
            log.error("invalid metadata: ", e);
        }
    }

    /**
     * List all tables in CorfuStore
     * @param namespace - the namespace where the table belongs
     * @return - number of tables in this namespace
     */
    public int listTables(String namespace)
    {
        int numTables = 0;
        System.out.println("\n=====Tables=======\n");
        for (TableName tableName : listTablesInNamespace(namespace)) {
            System.out.println("Table: " + tableName.getTableName());
            System.out.println("Namespace: " + tableName.getNamespace());
            numTables++;
        }
        System.out.println("\n======================\n");
        return numTables;
    }

    private List<TableName> listTablesInNamespace(String namespace) {
        return dynamicProtobufSerializer.getCachedRegistryTable().keySet()
            .stream()
            .filter(tableName -> namespace == null || tableName.getNamespace().equals(namespace))
            .collect(Collectors.toList());
    }

    /**
     * Print information about a specific table in CorfuStore
     * @param namespace - the namespace where the table belongs
     * @param tablename - table name without the namespace
     * @return - number of entries in the table
     */
    public int printTableInfo(String namespace, String tablename) {
        System.out.println("\n======================\n");
        String fullName = TableRegistry.getFullyQualifiedTableName(namespace, tablename);
        UUID streamUUID = UUID.nameUUIDFromBytes(fullName.getBytes());
        CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> table =
            getTable(namespace, tablename);
        int tableSize = table.size();
        System.out.println("Table " + tablename + " in namespace " + namespace +
            " with ID " + streamUUID.toString() + " has " + tableSize + " entries");
        System.out.println("\n======================\n");
        return tableSize;
    }

    /**
     * Helper to analyze all the protobufs used in this cluster
     */
    public int printAllProtoDescriptors() {
        int numProtoFiles = -1;
        System.out.println("=========PROTOBUF FILE NAMES===========");
        for (ProtobufFileName protoFileName :
            dynamicProtobufSerializer.getCachedProtobufDescriptorTable().keySet()) {
            try {
                System.out.println(JsonFormat.printer().print(protoFileName));
            } catch (InvalidProtocolBufferException e) {
                log.error("Unable to print protobuf for key {}", protoFileName, e);
            }
            numProtoFiles++;
        }
        System.out.println("=========PROTOBUF FILE DESCRIPTORS ===========");
        for (ProtobufFileName protoFileName :
            dynamicProtobufSerializer.getCachedProtobufDescriptorTable().keySet()) {
            try {
                System.out.println(JsonFormat.printer().print(protoFileName));
                System.out.println(JsonFormat.printer().print(
                    dynamicProtobufSerializer.getCachedProtobufDescriptorTable()
                        .get(protoFileName).getPayload())
                );
            } catch (InvalidProtocolBufferException e) {
                log.error("Unable to print protobuf for key {}", protoFileName, e);
            }
        }
        return numProtoFiles;
    }

    /**
     * Clear the table contents
     * @param namespace - the namespace where the table belongs
     * @param tablename - table name without the namespace
     * @return - number of entries in the table before clearing the table
     */
    public int clearTable(String namespace, String tablename) {
        System.out.println("\n======================\n");
        String fullName = TableRegistry.getFullyQualifiedTableName(namespace, tablename);
        UUID streamUUID = UUID.nameUUIDFromBytes(fullName.getBytes());
        try {
            runtime.getObjectsView().TXBegin();
            CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> table =
                getTable(namespace, tablename);
            int tableSize = table.size();
            System.out.println("Table " + tablename + " in namespace " + namespace
                + " with ID " + streamUUID.toString() + " with " + tableSize +
                " entries will be dropped...");
            table.clear();
            runtime.getObjectsView().TXEnd();
            System.out.println("Table cleared successfully");
            System.out.println("\n======================\n");
            return tableSize;
        } catch (TransactionAbortedException e) {
            log.error("Drop Table Transaction Aborted");
        } finally {
            if (TransactionalContext.isInTransaction()) {
                runtime.getObjectsView().TXAbort();
            }
        }
        return -1;
    }

    /**
     * Edit a record in a table and namespace
     * @param namespace namespace of the table
     * @param tableName name of the table
     * @param keyToEdit JSON string representing the key whose corresponding
     *  record is to be editted
     * @param newRecord JSON string representing the new value to be inserted
     *  against keyToEdit
     * @return CorfuDynamicRecord the edited CorfuDynamicRecord.  null if no
     *  record was edited, either due to an error or key not found.
     */
    public CorfuDynamicRecord editRecord(String namespace, String tableName,
                                         String keyToEdit, String newRecord) {
        System.out.println("\n======================\n");
        String fullName = TableRegistry.getFullyQualifiedTableName(namespace,
            tableName);
        UUID streamUUID = CorfuRuntime.getStreamID(fullName);

        TableName tableNameProto = TableName.newBuilder().setTableName(tableName)
            .setNamespace(namespace).build();

        Any defaultKeyAny =
            dynamicProtobufSerializer.getCachedRegistryTable().get(tableNameProto)
            .getPayload().getKey();
        Any defaultValueAny =
            dynamicProtobufSerializer.getCachedRegistryTable().get(tableNameProto)
            .getPayload().getValue();
        DynamicMessage keyMsg =
            dynamicProtobufSerializer.createDynamicMessageFromJson(defaultKeyAny,
                keyToEdit);
        DynamicMessage newValueMsg =
            dynamicProtobufSerializer.createDynamicMessageFromJson(defaultValueAny,
            newRecord);

        if (keyMsg == null || newValueMsg == null) {
            return null;
        }

        CorfuDynamicKey dynamicKey =
            new CorfuDynamicKey(defaultKeyAny.getTypeUrl(), keyMsg);

        try {
            CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> table =
                    getTable(namespace, tableName);
            runtime.getObjectsView().TXBegin();
            CorfuDynamicRecord editedRecord = null;
            if (table.containsKey(dynamicKey)) {
                CorfuDynamicRecord oldRecord = table.get(dynamicKey);

                if (oldRecord == null) {
                    log.warn("Unexpected Null Value found for key {} in table " +
                            "{} and namespace {}.  Stream Id {}", keyToEdit, tableName,
                        namespace, streamUUID);
                } else {
                    System.out.println("Editing record with Key " + keyToEdit +
                        " in table " + tableName + " and namespace " + namespace +
                        " with new record " + newRecord + ".  Stream Id " +
                        streamUUID);
                    String payloadTypeUrl = oldRecord.getPayloadTypeUrl();
                    String metadataTypeUrl = oldRecord.getMetadataTypeUrl();
                    DynamicMessage metadata = oldRecord.getMetadata();
                    editedRecord = new CorfuDynamicRecord(payloadTypeUrl,
                        newValueMsg, metadataTypeUrl, metadata);
                    table.put(dynamicKey, editedRecord);
                }
            } else {
                log.warn("Record with key {} not found in table {} and namespace {}. " +
                    " Stream Id {}.", keyToEdit, tableName, namespace, streamUUID);
            }
            runtime.getObjectsView().TXEnd();
            System.out.println("\n======================\n");
            return editedRecord;
        } catch (TransactionAbortedException e) {
            log.error("Transaction to edit record aborted.", e);
        } finally {
            if (TransactionalContext.isInTransaction()) {
                runtime.getObjectsView().TXAbort();
            }
        }
        return null;
    }

    public CorfuDynamicRecord addRecord(String namespace, String tableName,
                                        String newKey, String newValue,
                                        String newMetadata) {
        System.out.println("\n======================\n");

        TableName tableNameProto = TableName.newBuilder().setTableName(tableName)
            .setNamespace(namespace).build();

        if (!dynamicProtobufSerializer.getCachedRegistryTable()
            .containsKey(tableNameProto)) {
            log.error("Table {} in namespace {} does not exist.", tableName,
                namespace);
            return null;
        }

        Any defaultKeyAny =
            dynamicProtobufSerializer.getCachedRegistryTable().get(tableNameProto)
                .getPayload().getKey();
        Any defaultValueAny =
            dynamicProtobufSerializer.getCachedRegistryTable().get(tableNameProto)
                .getPayload().getValue();
        Any defaultMetadataAny =
            dynamicProtobufSerializer.getCachedRegistryTable().get(tableNameProto)
                .getPayload().getMetadata();

        DynamicMessage newKeyMsg =
            dynamicProtobufSerializer.createDynamicMessageFromJson(defaultKeyAny,
                newKey);
        DynamicMessage newValueMsg =
            dynamicProtobufSerializer.createDynamicMessageFromJson(defaultValueAny,
                newValue);

        // If the table did not have metadata configured, then when the table's entry
        // is created in the registry table, its section of google.protobuf.Any metadata = 4;
        // is never filled in. So reading that unfilled section into defaultMetadataAny gets us
        // a defaultInstance of the Any type which has type Url as an empty string.
        if (!defaultMetadataAny.getTypeUrl().isEmpty() && newMetadata == null) {
            log.error("Please supply metadata! Table has metadata schema defined as {}. At least pass an '{ }'",
                defaultMetadataAny.getTypeUrl());
            return null;
        }
        DynamicMessage newMetadataMsg =
            !defaultMetadataAny.getTypeUrl().isEmpty() ?
                dynamicProtobufSerializer.createDynamicMessageFromJson(defaultMetadataAny,
                    newMetadata) : null;

        // Metadata can be empty or null but key or value should not
        if (newKeyMsg == null || newValueMsg == null) {
            log.error("New Key or Value message is null");
            return null;
        }

        CorfuDynamicKey dynamicKey =
            new CorfuDynamicKey(defaultKeyAny.getTypeUrl(), newKeyMsg);
        CorfuDynamicRecord dynamicRecord =
            new CorfuDynamicRecord(defaultValueAny.getTypeUrl(), newValueMsg,
                defaultMetadataAny.getTypeUrl(), newMetadataMsg);

        try {
            CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> table =
                getTable(namespace, tableName);
            runtime.getObjectsView().TXBegin();
            table.insert(dynamicKey, dynamicRecord);
            runtime.getObjectsView().TXEnd();
            System.out.println("\n======================\n");
            return dynamicRecord;
        } catch (TransactionAbortedException e) {
            log.error("Transaction to add record aborted.", e);
        } finally {
            if (TransactionalContext.isInTransaction()) {
                runtime.getObjectsView().TXAbort();
            }
        }
        return null;
    }


    /**
     * Delete a record in a table and namespace
     * @param namespace namespace of the table
     * @param tableName name of the table
     * @param keyToDelete JSON string representing the key protobuf that needs to be deleted.
     * @return number of keys deleted.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    public int deleteRecord(String namespace, String tableName, String keyToDelete) {
        System.out.println("\n======================\n");
        String fullName = TableRegistry.getFullyQualifiedTableName(namespace,
                tableName);
        UUID streamUUID = CorfuRuntime.getStreamID(fullName);

        TableName tableNameProto = TableName.newBuilder().setTableName(tableName)
                .setNamespace(namespace).build();

        Any defaultKeyAny =
                dynamicProtobufSerializer.getCachedRegistryTable().get(tableNameProto)
                        .getPayload().getKey();
        DynamicMessage keyMsg =
                dynamicProtobufSerializer.createDynamicMessageFromJson(defaultKeyAny,
                        keyToDelete);

        CorfuDynamicKey dynamicKey =
                new CorfuDynamicKey(defaultKeyAny.getTypeUrl(), keyMsg);
        CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> table =
                getTable(namespace, tableName);
        int numKeysDeleted = -1;
        try {
            runtime.getObjectsView().TXBegin();
            if (!table.containsKey(dynamicKey)) {
                System.out.println("Key "+keyToDelete+" not found in "+fullName);
                runtime.getObjectsView().TXEnd();
                numKeysDeleted = 0;
                return numKeysDeleted;
            }
            System.out.println("Deleting record with Key " + keyToDelete +
                    " in table " + tableName + " and namespace " + namespace +
                    ".  Stream Id " + streamUUID);
            table.delete(dynamicKey);
            runtime.getObjectsView().TXEnd();
            System.out.println("\n======================\n");
            numKeysDeleted = 1;
        } catch (TransactionAbortedException e) {
            log.error("Transaction to delete record {} aborted.", keyToDelete, e);
        } finally {
            if (TransactionalContext.isInTransaction()) {
                runtime.getObjectsView().TXAbort();
            }
        }
        return numKeysDeleted;
    }

    /**
     * Loads the table with random data
     * @param namespace - the namespace where the table belongs
     * @param tableName - table name without the namespace
     * @param numItems - total number of items to load
     * @param batchSize - number of items in each transaction
     * @param itemSize - size of each item - a random string array
     * @return - number of entries in the table
     */
    public int loadTable(String namespace, String tableName, int numItems, int batchSize, int itemSize) {
        CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> table =
                getTable(namespace, tableName);
        int size = table.size();
        if (size == 0) {
            log.error("Currently unable to load data into empty tables. item size = {}", itemSize);
            return 0;
        }

        CorfuDynamicKey oneKey = table.keySet().stream().findAny().get();
        CorfuDynamicRecord oneRecord = table.get(oneKey);
        try {
            int itemsRemaining = numItems;
            while (itemsRemaining > 0) {
                runtime.getObjectsView().TXBegin();
                for (int j = batchSize; j > 0 && itemsRemaining > 0; j--, itemsRemaining--) {
                    table.put(oneKey, oneRecord);
                }
                final long address = runtime.getObjectsView().TXEnd();
                System.out.println("loadTable: Txn at address "
                    + address + " Items  now left " + itemsRemaining);
            }
        } catch (Exception e) {
            log.error("loadTable: {} {} {} {} failed.", namespace, tableName, numItems, batchSize, e);
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
        CorfuStoreShim store = new CorfuStoreShim(runtime);
        final Table<ExampleTableName, ExampleTableName, ManagedMetadata> table;
        try {
            TableOptions.TableOptionsBuilder optionsBuilder = TableOptions.builder();
            if (diskPath != null) {
                optionsBuilder.persistentDataPath(Paths.get(diskPath));
            }
            table = store.openTable(
                    namespace, tableName,
                    ExampleTableName.class,
                    ExampleTableName.class,
                    ManagedMetadata.class,
                    TableOptions.fromProtoSchema(ExampleTableName.class, optionsBuilder.build())
            );
        } catch (Exception ex) {
            log.error("Unable to open table " + namespace + "$" + tableName);
            throw new RuntimeException("Unable to open table.");
        }

        int tableSize = table.count();
        System.out.println("Listening to updates on Table " + tableName +
            " in namespace " + namespace + " with size " + tableSize + " ID " +
            table.getStreamUUID().toString());

        class StreamDumper implements StreamListener {
            @Getter
            final
            AtomicLong txnRead;

            @Getter
            volatile boolean isError;

            public StreamDumper() {
                this.txnRead = new AtomicLong(0);
            }

            @Override
            public void onNext(CorfuStreamEntries results) {
                System.out.println("onNext invoked with " +
                    results.getEntries().size() + " Read so far " + txnRead.get());
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
                            System.out.println(builder);
                            long now = System.currentTimeMillis();
                            long recordInsertedAt = ((ManagedMetadata)entry.getMetadata()).getLastModifiedTime();
                            System.out.println("\n Time since insert: " +
                                (now - recordInsertedAt) + "ms\n");
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

    /**
     * List all stream tags present in the Registry.
     *
     * @return stream tags
     * */
    public Set<String> listStreamTags() {
        Set<String> streamTags = new HashSet<>();

        dynamicProtobufSerializer.getCachedRegistryTable().values().forEach(
            record -> streamTags.addAll(record.getMetadata()
                .getTableOptions().getStreamTagList()));

        System.out.println("\n======================\n");
        System.out.println("Total unique stream tags: " + streamTags.size());
        streamTags.forEach(tag -> System.out.println(tag));
        System.out.println("\n======================\n");

        return streamTags;
    }

    /**
     * List a map of stream tags to table names.
     *
     * @return map of tags to table names in the registry
     */
    public Map<String, List<TableName>> listTagToTableMap() {
        Map<String, List<TableName>> streamTagToTableNames = getTagToTableNamesMap();
        System.out.println("\n======================\n");
        System.out.println("Total unique stream tags: " +
            streamTagToTableNames.keySet().size());
        System.out.println("Stream tags: " + streamTagToTableNames.keySet());
        streamTagToTableNames.forEach(this::printStreamTagMap);
        System.out.println("\n======================\n");
        return streamTagToTableNames;
    }

    /**
     * List all tags for the given table.
     *
     * @param namespace namespace for the table of interest
     * @param table table name of interest
     */
    public Set<String> listTagsForTable(String namespace, String table) {
        Set<String> tags = new HashSet<>();
        TableName tableName = TableName.newBuilder().setNamespace(namespace).setTableName(table).build();

        CorfuRecord<TableDescriptors, TableMetadata> tableRecord =
            dynamicProtobufSerializer.getCachedRegistryTable().get(tableName);
        if (tableRecord != null) {
            tags.addAll(tableRecord.getMetadata().getTableOptions().getStreamTagList());
            System.out.println("\n======================\n");
            System.out.println("table: " + namespace + "$" + table +
                " --- Total Tags = " + tags.size() + " Tags:: " + tags);
            System.out.println("\n======================\n");
        } else {
            log.warn("Invalid namespace {} and table name {}. Review or run operation --listTagsMap" +
                    " for complete map (all tables).", namespace, tableName);
        }
        return tags;
    }

    /**
     * List all tables with a specific stream tag.
     *
     * @param streamTag specific stream tag, if empty or null return all stream tags map
     * @return table names with given 'streamTag'
     */
    public List<TableName> listTablesForTag(@Nonnull String streamTag) {
        if (streamTag == null || streamTag.isEmpty()) {
            log.warn("Stream tag is null or empty. Provide correct --tag <tag> argument.");
            return Collections.EMPTY_LIST;
        }

        Map<String, List<TableName>> streamTagToTableNames = getTagToTableNamesMap();
        System.out.println("\n======================\n");
        printStreamTagMap(streamTag, streamTagToTableNames.get(streamTag));
        System.out.println("\n======================\n");
        return streamTagToTableNames.get(streamTag);
    }

    private Map<String, List<TableName>> getTagToTableNamesMap() {
        Map<String, List<TableName>> streamTagToTableNames = new HashMap<>();

        dynamicProtobufSerializer.getCachedRegistryTable().forEach((tableName, schema) ->
                schema.getMetadata().getTableOptions().getStreamTagList().forEach(tag -> {
                    if (streamTagToTableNames.putIfAbsent(tag, new ArrayList<>(Arrays.asList(tableName))) != null) {
                        streamTagToTableNames.computeIfPresent(tag, (key, tableList) -> {
                            tableList.add(tableName);
                            return tableList;
                        });
                    }
                })
        );

        return streamTagToTableNames;
    }

    private void printStreamTagMap(String tag, List<TableName> tables) {
        String formatMapping = "";
        for (TableName tName : tables) {
            formatMapping += String.format("<%s$%s>, ", tName.getNamespace(), tName.getTableName());
        }
        // Remove last continuation characters for a clean output ', '
        formatMapping = formatMapping.substring(0, formatMapping.length() - 2);
        System.out.println("Tag: " + tag + " --- Total Tables: " + tables.size()
            + " TableNames: " + formatMapping);
    }
}
