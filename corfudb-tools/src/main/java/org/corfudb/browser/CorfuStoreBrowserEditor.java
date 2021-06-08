package org.corfudb.browser;

import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.CorfuStoreMetadata.ProtobufFileName;
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
import org.corfudb.runtime.collections.ManagedTxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ICorfuVersionPolicy;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.serializer.DynamicProtobufSerializer;
import org.corfudb.util.serializer.Serializers;
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
        Serializers.registerSerializer(dynamicProtobufSerializer);
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
        StringBuilder builder;

        CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> table =
            getTable(namespace, tablename);
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
                    System.out.println(builder.toString());
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
            "with ID " + streamUUID.toString() + " has " + tableSize + " " +
                "entries");
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
    public int dropTable(String namespace, String tablename) {
        System.out.println("\n======================\n");
        String fullName = TableRegistry.getFullyQualifiedTableName(namespace, tablename);
        UUID streamUUID = UUID.nameUUIDFromBytes(fullName.getBytes());
        try {
            runtime.getObjectsView().TXBegin();
            CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> table =
                getTable(namespace, tablename);
            int tableSize = table.size();
            System.out.println("Table " + tablename + " in namespace " + namespace
                + "with ID " + streamUUID.toString() + " with " + tableSize +
                "entries will be dropped...");
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
            runtime.getObjectsView().TXBegin();
            CorfuDynamicRecord editedRecord = null;
            CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> table =
                getTable(namespace, tableName);
            if (table.containsKey(dynamicKey)) {
                CorfuDynamicRecord oldRecord = table.get(dynamicKey);

                if (oldRecord == null) {
                    log.warn("Unexpedted Null Value found for key {} in table " +
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
            System.out.println("WARNING: Loading " + numItems + " items of " + itemSize +
                " size in " + batchSize + " batchSized transactions into " +
                namespace + "$" + tablename);
            int itemsRemaining = numItems;
            while (itemsRemaining > 0) {
                ManagedTxnContext tx = store.tx(namespace);
                for (int j = batchSize; j > 0 && itemsRemaining > 0; j--, itemsRemaining--) {
                    ExampleTableName dummyKey = ExampleTableName.newBuilder()
                            .setNamespace(Integer.toString(itemsRemaining))
                            .setTableName(Integer.toString(j)).build();
                    tx.putRecord(table, dummyKey, dummyVal, ManagedMetadata.getDefaultInstance());
                }
                Timestamp address = tx.commit();
                System.out.println("loadTable: Txn at address "
                    + address.getSequence() + " Items  now left " + itemsRemaining);
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
        System.out.println("Listening to updates on Table " + tableName +
            " in namespace " + namespace + " with size " + tableSize + " ID " +
            table.getStreamUUID().toString());

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

        CorfuRecord<CorfuStoreMetadata.TableDescriptors, CorfuStoreMetadata.TableMetadata> record =
            dynamicProtobufSerializer.getCachedRegistryTable().get(tableName);
        if (record != null) {
            tags.addAll(record.getMetadata().getTableOptions().getStreamTagList());
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
            + "TableNames: " + formatMapping);
    }

    /**
     * List all known corfu streams with their names and ids in a cluster.
     *
     * @return stream tags
     * */
    public Set<String> listAllCorfuStreams() {
        Map<UUID, String> allStreamMap = new HashMap<>();
        dynamicProtobufSerializer.getCachedRegistryTable().forEach((key, value) -> {
            String namespace = key.getNamespace();
            String fullTableName = TableRegistry.getFullyQualifiedTableName(key.getNamespace(), key.getTableName());
            UUID streamId = CorfuRuntime.getStreamID(fullTableName);
            allStreamMap.put(streamId, fullTableName);
            allStreamMap.put(CorfuRuntime.getCheckpointStreamIdFromId(streamId), fullTableName + "#chkpt");
            value.getMetadata().getTableOptions().getStreamTagList().forEach(tag -> {
                allStreamMap.put(TableRegistry.getStreamIdForStreamTag(namespace, tag), "stream_tag#" + namespace + "$" + tag);
            });
        });

        List<Map.Entry<UUID, String>> list = new ArrayList<>(allStreamMap.entrySet());
        list.sort(Map.Entry.comparingByValue());

        System.out.println("\n======================\n");
        System.out.println("Total unique stream Ids: "+allStreamMap.size());
        list.forEach(entry -> {
            System.out.println(entry.getKey().toString()+", "+entry.getValue());
        });
        System.out.println("\n======================\n");

        return new HashSet<>(allStreamMap.values());
    }
}
