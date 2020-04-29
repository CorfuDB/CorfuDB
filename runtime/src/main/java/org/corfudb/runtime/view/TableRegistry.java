package org.corfudb.runtime.view;

import com.google.common.reflect.TypeToken;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Message;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableDescriptors;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.CorfuStoreMetadata.TableMetadata;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.PersistedStreamingMap;
import org.corfudb.runtime.collections.StreamManager;
import org.corfudb.runtime.collections.StreamingMap;
import org.corfudb.runtime.collections.StreamingMapDecorator;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ICorfuVersionPolicy;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.ProtobufSerializer;
import org.corfudb.util.serializer.Serializers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Table Registry manages the lifecycle of all the tables in the system.
 * This is a wrapper over the CorfuTable providing transactional operations and accepts only protobuf messages.
 * It accepts a primary key - which is a protobuf message.
 * The payload is a CorfuRecord which comprises of 2 fields - Payload and Metadata. These are protobuf messages as well.
 * The table creation registers the schema used in the table which is further used for serialization. This schema
 * is also used for offline (without access to client protobuf files) browsing and editing.
 * <p>
 * Created by zlokhandwala on 2019-08-10.
 */
@Slf4j
public class TableRegistry {

    /**
     * System Table: To store the table schemas and other options.
     * This information is used to view and edit table using an offline tool without the dependency on the
     * application for the schemas.
     */
    public static final String CORFU_SYSTEM_NAMESPACE = "CorfuSystem";
    public static final String REGISTRY_TABLE_NAME = "RegistryTable";

    /**
     * Connected runtime instance.
     */
    private final CorfuRuntime runtime;

    /**
     * A TableRegistry should just have one stream manager for lifecycle management.
     */
    private StreamManager streamManager;

    /**
     * Stores the schemas of the Key, Value and Metadata.
     * A reference of this map is held by the {@link ProtobufSerializer} to serialize and deserialize the objects.
     */
    private final ConcurrentMap<String, Class<? extends Message>> classMap;

    /**
     * Cache of tables allowing the user to fetch a table by fullyQualified table name without the other options.
     */
    private final ConcurrentMap<String, Table<Message, Message, Message>> tableMap;

    /**
     * Serializer to be used for protobuf messages.
     */
    private final ISerializer protobufSerializer;

    /**
     * This {@link CorfuTable} holds the schemas of the key, payload and metadata for every table created.
     */
    @Getter
    private final CorfuTable<TableName, CorfuRecord<TableDescriptors, TableMetadata>> registryTable;

    public TableRegistry(CorfuRuntime runtime) {
        this.runtime = runtime;
        this.classMap = new ConcurrentHashMap<>();
        this.tableMap = new ConcurrentHashMap<>();
        this.protobufSerializer = new ProtobufSerializer(classMap);
        Serializers.registerSerializer(this.protobufSerializer);
        this.registryTable = this.runtime.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<TableName, CorfuRecord<TableDescriptors, TableMetadata>>>() {
                })
                .setStreamName(getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, REGISTRY_TABLE_NAME))
                .setSerializer(this.protobufSerializer)
                .open();

        // Register the table schemas to schema table.
        addTypeToClassMap(TableName.getDefaultInstance());
        addTypeToClassMap(TableDescriptors.getDefaultInstance());
        addTypeToClassMap(TableMetadata.getDefaultInstance());

        // Register the registry table itself.
        try {
            registerTable(CORFU_SYSTEM_NAMESPACE,
                    REGISTRY_TABLE_NAME,
                    TableName.class,
                    TableDescriptors.class,
                    TableMetadata.class,
                    TableOptions.<TableName, TableDescriptors>builder().build());
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Register a table in the internal Table Registry.
     *
     * @param namespace     Namespace of the table to be registered.
     * @param tableName     Table name of the table to be registered.
     * @param keyClass      Key class.
     * @param payloadClass  Value class.
     * @param metadataClass Metadata class.
     * @param <K>           Type of Key.
     * @param <V>           Type of Value.
     * @param <M>           Type of Metadata.
     * @throws NoSuchMethodException     If this is not a protobuf message.
     * @throws InvocationTargetException If this is not a protobuf message.
     * @throws IllegalAccessException    If this is not a protobuf message.
     */
    private <K extends Message, V extends Message, M extends Message>
    void registerTable(@Nonnull String namespace,
                       @Nonnull String tableName,
                       @Nonnull Class<K> keyClass,
                       @Nonnull Class<V> payloadClass,
                       @Nullable Class<M> metadataClass,
                       @Nonnull final TableOptions<K, V> tableOptions)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        TableName tableNameKey = TableName.newBuilder()
                .setNamespace(namespace)
                .setTableName(tableName)
                .build();

        K defaultKeyMessage = (K) keyClass.getMethod("getDefaultInstance").invoke(null);
        V defaultValueMessage = (V) payloadClass.getMethod("getDefaultInstance").invoke(null);

        TableDescriptors.Builder tableDescriptorsBuilder = TableDescriptors.newBuilder();

        FileDescriptor keyFileDescriptor = defaultKeyMessage.getDescriptorForType().getFile();
        insertAllDependingFileDescriptorProtos(tableDescriptorsBuilder, keyFileDescriptor);
        FileDescriptor valueFileDescriptor = defaultValueMessage.getDescriptorForType().getFile();
        insertAllDependingFileDescriptorProtos(tableDescriptorsBuilder, valueFileDescriptor);

        if (metadataClass != null) {
            M defaultMetadataMessage = (M) metadataClass.getMethod("getDefaultInstance").invoke(null);
            FileDescriptor metaFileDescriptor = defaultMetadataMessage.getDescriptorForType().getFile();
            insertAllDependingFileDescriptorProtos(tableDescriptorsBuilder, metaFileDescriptor);
        }
        TableDescriptors tableDescriptors = tableDescriptorsBuilder.build();

        TableMetadata.Builder metadataBuilder = TableMetadata.newBuilder();
        metadataBuilder.setDiskBased(tableOptions.getPersistentDataPath().isPresent());

        // Schema validation to ensure that there is either proper modification of the schema across open calls.
        // Or no modification to the protobuf files.
        boolean hasSchemaChanged = false;
        CorfuRecord<TableDescriptors, TableMetadata> oldRecord = this.registryTable.get(tableNameKey);
        if (oldRecord != null) {
            if (!oldRecord.getPayload().getFileDescriptorsMap().equals(tableDescriptors.getFileDescriptorsMap())) {
                hasSchemaChanged = true;
                log.error("registerTable: Schema update detected for table "+namespace+" "+ tableName);
                log.debug("registerTable: old schema:"+oldRecord.getPayload().getFileDescriptorsMap());
                log.debug("registerTable: new schema:"+tableDescriptors.getFileDescriptorsMap());
            }
        }
        int numRetries = 9; // Since this is an internal transaction, retry a few times before giving up.
        long finalAddress = Address.NON_ADDRESS;
        while (numRetries-- > 0) {
            try {
                this.runtime.getObjectsView().TXBuild().type(TransactionType.OPTIMISTIC).build().begin();
                if (hasSchemaChanged) {
                    this.registryTable.put(tableNameKey,
                            new CorfuRecord<>(tableDescriptors, metadataBuilder.build()));
                } else {
                    this.registryTable.putIfAbsent(tableNameKey,
                            new CorfuRecord<>(tableDescriptors, metadataBuilder.build()));
                }
                finalAddress = this.runtime.getObjectsView().TXEnd();
            } catch (TransactionAbortedException txAbort) {
                if (numRetries <= 0) {
                    throw txAbort;
                }
                log.info("registerTable: commit failed. Will retry {} times. Cause {}", numRetries, txAbort);
                continue;
            } finally {
                if (finalAddress == Address.NON_ADDRESS) { // Transaction failed or an exception occurred.
                    this.runtime.getObjectsView().TXAbort(); // clear Txn context so thread can be reused.
                }
            }
        }
    }

    /**
     * Inserts the current file descriptor and then performs a depth first search to insert its depending file
     * descriptors into the map in {@link TableDescriptors}.
     *
     * @param tableDescriptorsBuilder Builder instance.
     * @param rootFileDescriptor      File descriptor to be added.
     */
    private void insertAllDependingFileDescriptorProtos(TableDescriptors.Builder tableDescriptorsBuilder,
                                                        FileDescriptor rootFileDescriptor) {
        Deque<FileDescriptor> fileDescriptorStack = new LinkedList<>();
        fileDescriptorStack.push(rootFileDescriptor);

        while (!fileDescriptorStack.isEmpty()) {
            FileDescriptor fileDescriptor = fileDescriptorStack.pop();
            FileDescriptorProto fileDescriptorProto = fileDescriptor.toProto();

            // If the fileDescriptorProto has already been added then continue.
            if (tableDescriptorsBuilder.getFileDescriptorsMap().containsKey(fileDescriptorProto.getName())) {
                continue;
            }

            // Add the fileDescriptorProto into the tableDescriptor map.
            tableDescriptorsBuilder.putFileDescriptors(fileDescriptorProto.getName(), fileDescriptorProto);

            // Add all unvisited dependencies to the deque.
            for (FileDescriptor dependingFileDescriptor : fileDescriptor.getDependencies()) {
                FileDescriptorProto dependingFileDescriptorProto = dependingFileDescriptor.toProto();
                if (!tableDescriptorsBuilder.getFileDescriptorsMap()
                        .containsKey(dependingFileDescriptorProto.getName())) {

                    fileDescriptorStack.push(dependingFileDescriptor);
                }
            }
        }
    }

    /**
     * Gets the type Url of the protobuf descriptor. Used to identify the message during serialization.
     * Note: This is same as used in Any.proto.
     *
     * @param descriptor Descriptor of the protobuf.
     * @return Type url string.
     */
    public static String getTypeUrl(Descriptor descriptor) {
        return "type.googleapis.com/" + descriptor.getFullName();
    }

    /**
     * Fully qualified table name created to produce the stream uuid.
     *
     * @param namespace Namespace of the table.
     * @param tableName Table name of the table.
     * @return Fully qualified table name.
     */
    public static String getFullyQualifiedTableName(String namespace, String tableName) {
        return namespace + "$" + tableName;
    }

    /**
     * Adds the schema to the class map to enable serialization of this table data.
     *
     * @param msg Default message of this protobuf message.
     * @param <T> Type of message.
     */
    private <T extends Message> void addTypeToClassMap(T msg) {
        String typeUrl = getTypeUrl(msg.getDescriptorForType());
        // Register the schemas to schema table.
        if (!classMap.containsKey(typeUrl)) {
            classMap.put(typeUrl, msg.getClass());
        }
    }

    /**
     * Opens a Corfu table with the specified options.
     *
     * @param namespace    Namespace of the table.
     * @param tableName    Name of the table.
     * @param kClass       Key class.
     * @param vClass       Value class.
     * @param mClass       Metadata class.
     * @param tableOptions Table options.
     * @param <K>          Key type.
     * @param <V>          Value type.
     * @param <M>          Metadata type.
     * @return Table instance.
     * @throws NoSuchMethodException     If this is not a protobuf message.
     * @throws InvocationTargetException If this is not a protobuf message.
     * @throws IllegalAccessException    If this is not a protobuf message.
     */
    public <K extends Message, V extends Message, M extends Message>
    Table<K, V, M> openTable(@Nonnull final String namespace,
                             @Nonnull final String tableName,
                             @Nonnull final Class<K> kClass,
                             @Nonnull final Class<V> vClass,
                             @Nullable final Class<M> mClass,
                             @Nonnull final TableOptions<K, V> tableOptions)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        // Register the schemas to schema table.
        K defaultKeyMessage = (K) kClass.getMethod("getDefaultInstance").invoke(null);
        addTypeToClassMap(defaultKeyMessage);

        V defaultValueMessage = (V) vClass.getMethod("getDefaultInstance").invoke(null);
        addTypeToClassMap(defaultValueMessage);

        M defaultMetadataMessage = null;
        if (mClass != null) {
            defaultMetadataMessage = (M) mClass.getMethod("getDefaultInstance").invoke(null);
            addTypeToClassMap(defaultMetadataMessage);
        }

        String fullyQualifiedTableName = getFullyQualifiedTableName(namespace, tableName);
        ICorfuVersionPolicy.VersionPolicy versionPolicy = ICorfuVersionPolicy.DEFAULT;
        Supplier<StreamingMap<K, V>> mapSupplier = () -> new StreamingMapDecorator();
        if (tableOptions.getPersistentDataPath().isPresent()) {
            versionPolicy = ICorfuVersionPolicy.MONOTONIC;
            mapSupplier = () -> new PersistedStreamingMap<>(
                    tableOptions.getPersistentDataPath().get(),
                    PersistedStreamingMap.getPersistedStreamingMapOptions(),
                    protobufSerializer, this.runtime);
        }

        // Open and return table instance.
        Table<K, V, M> table = new Table<>(
                namespace,
                fullyQualifiedTableName,
                defaultValueMessage,
                defaultMetadataMessage,
                this.runtime,
                this.protobufSerializer,
                mapSupplier, versionPolicy);
        tableMap.put(fullyQualifiedTableName, (Table<Message, Message, Message>) table);

        registerTable(namespace, tableName, kClass, vClass, mClass, tableOptions);
        return table;
    }

    /**
     * Get an already opened table. Fetches the table from the cache given only the namespace and table name.
     * Throws a NoSuchElementException if table is not previously opened and not present in cache.
     *
     * @param namespace Namespace of the table.
     * @param tableName Name of the table.
     * @param <K>       Key type.
     * @param <V>       Value type.
     * @param <M>       Metadata type.
     * @return Table instance.
     */
    public <K extends Message, V extends Message, M extends Message>
    Table<K, V, M> getTable(String namespace, String tableName) {
        String fullyQualifiedTableName = getFullyQualifiedTableName(namespace, tableName);
        if (!tableMap.containsKey(fullyQualifiedTableName)) {
            // Table has not been opened, but let's first find out if this table even exists
            // To do so, consult the TableRegistry for an entry which indicates the table exists.
            if (registryTable.containsKey(
                    TableName.newBuilder()
                    .setNamespace(namespace)
                    .setTableName(tableName)
                    .build())
            ) {
                // If table does exist then the caller must use the long form of the openTable()
                // since there are too few arguments to open a table not seen by this runtime.
                throw new IllegalArgumentException("Please provide Key, Value & Metadata schemas to re-open"
                + " this existing table " + tableName + " in namespace " + namespace);
            } else {
                // If the table is completely unheard of return NoSuchElementException
                throw new NoSuchElementException(
                        String.format("No such table found: namespace: %s, tableName: %s",
                        namespace, tableName));
            }
        }
        return (Table<K, V, M>) tableMap.get(fullyQualifiedTableName);
    }

    /**
     * Only clears a table, DOES not delete its file descriptors from metadata.
     * This is because if the purpose of the delete is to upgrade from an old schema to a new schema
     * Then we must first purge all SMR entries of the current (old) format from corfu stream.
     * It is the checkpointer which actually purges via a trim operation.
     * But to even get to the trim, it must be able to read the table, which it can't do if the table's
     * metadata (TableRegistry entry) is also removed here.
     * So what would be nice if we can place a marker indicating that this table is marked
     * for deletion, so that on the next Re-open we will actually force update the table registry entry.
     * For now, just force the openTable to always purge the prior entry assuming that the
     * caller has done the good work of
     * 1. table.clear() using the old version
     * 2. running checkpoint and trim
     * 3. re-open the table with new proto files.
     *
     * @param namespace Namespace of the table.
     * @param tableName Name of the table.
     */
    public void deleteTable(String namespace, String tableName) {
        Table<Message, Message, Message> table = getTable(namespace, tableName);
        table.clear();
    }

    /**
     * Lists all the tables for a namespace.
     *
     * @param namespace Namespace for a table.
     * @return Collection of tables.
     */
    public Collection<TableName> listTables(@Nullable final String namespace) {
        return registryTable.keySet()
                .stream()
                .filter(tableName -> namespace == null || tableName.getNamespace().equals(namespace))
                .collect(Collectors.toList());
    }

    /**
     * Gets the table descriptors for a particular fully qualified table name.
     * This is used for reconstructing a message when the schema is not available.
     *
     * @param tableName Namespace and name of the table.
     * @return Table Descriptor.
     */
    @Nullable
    public TableDescriptors getTableDescriptor(@Nonnull TableName tableName) {
        return Optional.ofNullable(this.registryTable.get(tableName))
                .map(CorfuRecord::getPayload)
                .orElse(null);
    }

    /**
     * Register a stream subscription manager. We want only one of these per runtime.
     */
    public synchronized StreamManager getStreamManager() {
        if (this.streamManager == null) {
            this.streamManager = new StreamManager(runtime);
        }
        return this.streamManager;
    }

    public synchronized void shutdown() {
        if (this.streamManager != null) {
            this.streamManager.shutdown();
        }
    }
}
