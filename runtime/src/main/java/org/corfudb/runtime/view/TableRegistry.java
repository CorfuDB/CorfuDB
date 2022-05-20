package org.corfudb.runtime.view;

import com.google.common.reflect.TypeToken;
import com.google.protobuf.Any;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableDescriptors;
import org.corfudb.runtime.CorfuStoreMetadata.TableMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.CorfuStoreMetadata.ProtobufFileName;
import org.corfudb.runtime.CorfuStoreMetadata.ProtobufFileDescriptor;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.PersistedStreamingMap;
import org.corfudb.runtime.collections.StreamingMap;
import org.corfudb.runtime.collections.StreamingMapDecorator;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TableParameters;
import org.corfudb.runtime.collections.streaming.StreamingManager;
import org.corfudb.runtime.exceptions.SerializerException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ICorfuVersionPolicy;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.ObjectsView.StreamTagInfo;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.ProtobufSerializer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.corfudb.runtime.view.ObjectsView.LOG_REPLICATOR_STREAM_INFO;

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
    public static final String PROTOBUF_DESCRIPTOR_TABLE_NAME = "ProtobufDescriptorTable";

    /**
     * A common prefix for all string based stream tags defined in protobuf.
     */
    private static final String STREAM_TAG_PREFIX = "stream_tag$";

    /**
     * Connected runtime instance.
     */
    private final CorfuRuntime runtime;

    /**
     * A TableRegistry should just have one streaming manager for lifecycle management.
     */
    private volatile StreamingManager streamingManager;

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

    /**
     * To avoid duplicating the protobuf file descriptors that repeat across different tables store all
     * descriptors in a single table indexed by its protobuf file name.
     */
    @Getter
    private final CorfuTable<ProtobufFileName, CorfuRecord<ProtobufFileDescriptor, TableMetadata>> protobufDescriptorTable;

    public TableRegistry(CorfuRuntime runtime) {
        this.runtime = runtime;
        this.tableMap = new ConcurrentHashMap<>();
        ISerializer protoSerializer;
        try {
            // If protobuf serializer is already registered, reference static/global class map so schemas
            // are shared across all runtime's and not overwritten (if multiple runtime's exist).
            // This aims to overcome a current design limitation where the serializers are static and not
            // per runtime (to be changed).
            protoSerializer = runtime.getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE);
        } catch (SerializerException se) {
            // This means the protobuf serializer had not been registered yet.
            protoSerializer = new ProtobufSerializer(new ConcurrentHashMap<>());
            runtime.getSerializers().registerSerializer(protoSerializer);
        }
        this.protobufSerializer = protoSerializer;
        this.registryTable = this.runtime.getObjectsView().build()
            .setTypeToken(new TypeToken<CorfuTable<TableName, CorfuRecord<TableDescriptors, TableMetadata>>>() {
            })
            .setStreamName(getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, REGISTRY_TABLE_NAME))
            .setSerializer(this.protobufSerializer)
            .setStreamTags(LOG_REPLICATOR_STREAM_INFO.getStreamId())
            .open();

        this.protobufDescriptorTable = this.runtime.getObjectsView().build()
            .setTypeToken(new TypeToken<CorfuTable<ProtobufFileName, CorfuRecord<ProtobufFileDescriptor, TableMetadata>>>() {
            })
            .setStreamName(getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, PROTOBUF_DESCRIPTOR_TABLE_NAME))
            .setSerializer(this.protobufSerializer)
            .setStreamTags(LOG_REPLICATOR_STREAM_INFO.getStreamId())
            .open();

        // Register the table schemas to schema table.
        addTypeToClassMap(TableName.getDefaultInstance());
        addTypeToClassMap(TableDescriptors.getDefaultInstance());
        addTypeToClassMap(TableMetadata.getDefaultInstance());
        addTypeToClassMap(ProtobufFileName.getDefaultInstance());
        addTypeToClassMap(ProtobufFileDescriptor.getDefaultInstance());

        // Register the registry table itself.
        try {
            registerTable(CORFU_SYSTEM_NAMESPACE,
                REGISTRY_TABLE_NAME,
                TableName.class,
                TableDescriptors.class,
                TableMetadata.class,
                TableOptions.<TableName, TableDescriptors>builder().build());

            registerTable(CORFU_SYSTEM_NAMESPACE,
                PROTOBUF_DESCRIPTOR_TABLE_NAME,
                ProtobufFileName.class,
                ProtobufFileDescriptor.class,
                TableMetadata.class,
                TableOptions.<ProtobufFileName, ProtobufFileDescriptor>builder().build());
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
                       @Nonnull final TableOptions tableOptions)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        TableName tableNameKey = TableName.newBuilder()
                .setNamespace(namespace)
                .setTableName(tableName)
                .build();

        K defaultKeyMessage = (K) keyClass.getMethod("getDefaultInstance").invoke(null);
        V defaultValueMessage = (V) payloadClass.getMethod("getDefaultInstance").invoke(null);

        Map<ProtobufFileName, CorfuRecord<ProtobufFileDescriptor, TableMetadata>> allDescriptors = new HashMap<>();
        TableDescriptors.Builder tableDescriptorsBuilder = TableDescriptors.newBuilder();
        FileDescriptor keyFileDescriptor = defaultKeyMessage.getDescriptorForType().getFile();
        insertAllDependingFileDescriptorProtos(tableDescriptorsBuilder, keyFileDescriptor, allDescriptors);

        FileDescriptor valueFileDescriptor = defaultValueMessage.getDescriptorForType().getFile();
        insertAllDependingFileDescriptorProtos(tableDescriptorsBuilder, valueFileDescriptor, allDescriptors);

        if (metadataClass != null) {
            M defaultMetadataMessage = (M) metadataClass.getMethod("getDefaultInstance").invoke(null);
            FileDescriptor metaFileDescriptor = defaultMetadataMessage.getDescriptorForType().getFile();
            insertAllDependingFileDescriptorProtos(tableDescriptorsBuilder, metaFileDescriptor, allDescriptors);
            // Add Any for the metadata
            tableDescriptorsBuilder.setMetadata(Any.pack(defaultMetadataMessage));
        }

        // Add the Any for the key and value
        tableDescriptorsBuilder.setKey(Any.pack(defaultKeyMessage))
            .setValue(Any.pack(defaultValueMessage));
        TableDescriptors tableDescriptors = tableDescriptorsBuilder.build();

        // Also capture any TableOptions passed in permanently into the metadata section.
        TableMetadata.Builder metadataBuilder = TableMetadata.newBuilder();
        metadataBuilder.setDiskBased(tableOptions.getPersistentDataPath().isPresent());
        if (tableOptions.getSchemaOptions() == null) {
            metadataBuilder.setTableOptions(CorfuOptions.SchemaOptions.getDefaultInstance());
        } else {
            metadataBuilder.setTableOptions(tableOptions.getSchemaOptions());
        }

        int numRetries = 9; // Since this is an internal transaction, retry a few times before giving up.
        while (numRetries-- > 0) {
            // Schema validation to ensure that there is either proper modification of the schema across open calls.
            // Or no modification to the protobuf files.
            /**
             * Caller is opening a new table with a map of protobufFilename -> protobufFileDescriptor pairs
             *
             *  If no entry in TableRegistry, tt is a new table. Then we insert new protobufs into descriptor table.
             *  Note: we do not replace existing descriptor, as table schema change should be handled by explicit APIs.
             *  (assumption is that protobuf definitions do not change outside of an upgrade/migration scenario)
             *
             *  If entry exists in TableRegistry -> Existing table re-opened:
             *  We won't check equality as the expectation is that schemas do not change outside of upgrade paths.
             */
            if (TransactionalContext.isInTransaction()) {
                throw new IllegalThreadStateException("openTable: Called on an existing transaction");
            }
            try {
                this.runtime.getObjectsView().TXBuild().type(TransactionType.WRITE_AFTER_WRITE).build().begin();
                CorfuRecord<TableDescriptors, TableMetadata> oldRecord = this.registryTable.get(tableNameKey);
                if (oldRecord == null) {
                    this.registryTable.put(tableNameKey,
                        new CorfuRecord<>(tableDescriptors, metadataBuilder.build()));

                    allDescriptors.forEach(this::recordNewSchema);
                }
                this.runtime.getObjectsView().TXEnd();
                break;
            } catch (TransactionAbortedException txAbort) {
                if (numRetries <= 0) {
                    throw txAbort;
                }
                log.info("registerTable: commit failed. Will retry {} times. Cause {}", numRetries, txAbort);
            } finally {
                if (TransactionalContext.isInTransaction()) { // Transaction failed or an exception occurred.
                    this.runtime.getObjectsView().TXAbort(); // clear Txn context so thread can be reused.
                }
            }
        }
    }

    /**
     * For each protobuf filename -> descriptor map, validate if the previous descriptor matches
     * If there is a new schema or an update, record the new update in the table.
     * (Note that oldProto means existing schema previously opened)
     * WARNING: This method MUST be invoked within a transaction.
     *
     * @param protoName - name of the protobuf file
     * @param newProtoFd - descriptor of the protobuf file that is being inserted.
     */
    private void recordNewSchema(ProtobufFileName protoName,
                                    CorfuRecord<ProtobufFileDescriptor, TableMetadata> newProtoFd) {
        final CorfuRecord<ProtobufFileDescriptor, TableMetadata> currentSchema = this.protobufDescriptorTable.get(protoName);
        if (currentSchema == null) {
            // If schema is not present, add to protobufDescriptorTable, otherwise,
            // we assume schema definitions only change between upgrades/migration for which
            // dedicated APIs are available
            this.protobufDescriptorTable.put(protoName, newProtoFd);
        } else {
            if (log.isTraceEnabled() && !protoName.getFileName().startsWith("google/protobuf")
                    && !currentSchema.getPayload().getFileDescriptor()
                    .equals(newProtoFd.getPayload().getFileDescriptor())) {
                log.trace("registerTable: Schema update detected for table {}! " +
                                "Old schema is {}, new schema is {}", protoName,
                        currentSchema.getPayload().getFileDescriptor(),
                        newProtoFd.getPayload().getFileDescriptor());

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
    public static void insertAllDependingFileDescriptorProtos(TableDescriptors.Builder tableDescriptorsBuilder,
                                                              FileDescriptor rootFileDescriptor,
                                                              Map<ProtobufFileName, CorfuRecord<ProtobufFileDescriptor, TableMetadata>>
                                                                      allDescriptors) {
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
            tableDescriptorsBuilder.putFileDescriptors(fileDescriptorProto.getName(),
                    FileDescriptorProto.getDefaultInstance());

            // Add the actual descriptor into a common pool of descriptors to avoid duplication
            ProtobufFileName protoFileName = ProtobufFileName.newBuilder().setFileName(fileDescriptorProto.getName()).build();
            ProtobufFileDescriptor protoFd = ProtobufFileDescriptor.newBuilder().setFileDescriptor(fileDescriptorProto).build();
            allDescriptors.putIfAbsent(protoFileName, new CorfuRecord<>(protoFd, null));

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
     * Fully qualified table name created to produce the stream uuid.
     *
     * @param tableName TableName of the table.
     * @return Fully qualified table name.
     */
    public static String getFullyQualifiedTableName(TableName tableName) {
        return getFullyQualifiedTableName(tableName.getNamespace(), tableName.getTableName());
    }

    /**
     * Return the stream Id for the provided stream tag.
     *
     * @param namespace namespace of the stream
     * @param streamTag stream tag in string
     * @return stream Id in UUID
     */
    public static UUID getStreamIdForStreamTag(String namespace, String streamTag) {
        return CorfuRuntime.getStreamID(STREAM_TAG_PREFIX + namespace + streamTag);
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
        ((ProtobufSerializer)runtime.getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE))
                .getClassMap().put(typeUrl, msg.getClass());
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
                             @Nonnull final TableOptions tableOptions)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        // Register the schemas to schema table.
        if (kClass == null) {
            throw new IllegalArgumentException("Key type cannot be NULL.");
        }
        K defaultKeyMessage = (K) kClass.getMethod("getDefaultInstance").invoke(null);
        addTypeToClassMap(defaultKeyMessage);

        if (vClass == null) {
            throw new IllegalArgumentException("Value type cannot be NULL.");
        }
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

        CorfuOptions.SchemaOptions tableSchemaOptions;
        if (tableOptions.getSchemaOptions() != null) {
            tableSchemaOptions = tableOptions.getSchemaOptions();
        } else {
            tableSchemaOptions = CorfuOptions.SchemaOptions.getDefaultInstance();
        }
        final Set<StreamTagInfo> streamTagInfoForTable = tableSchemaOptions
                .getStreamTagList().stream()
                .map(tag -> new StreamTagInfo(tag, getStreamIdForStreamTag(namespace, tag)))
                .collect(Collectors.toSet());

        final boolean isFederated = tableSchemaOptions.getIsFederated();
        // If table is federated, add a new tagged stream (on which updates to federated tables will be appended for
        // streaming purposes)
        if (isFederated) {
            streamTagInfoForTable.add(LOG_REPLICATOR_STREAM_INFO);
        }

        Set<UUID> streamTagIdsForTable = streamTagInfoForTable
                .stream()
                .map(StreamTagInfo::getStreamId)
                .collect(Collectors.toSet());

        log.info("openTable: opening {}${} with stream tags {}", namespace, tableName, streamTagInfoForTable);

        // Open and return table instance.
        Table<K, V, M> table = new Table<>(
                TableParameters.<K, V, M>builder()
                        .namespace(namespace)
                        .fullyQualifiedTableName(fullyQualifiedTableName)
                        .kClass(kClass)
                        .vClass(vClass)
                        .mClass(mClass)
                        .valueSchema(defaultValueMessage)
                        .metadataSchema(defaultMetadataMessage)
                        .schemaOptions(tableSchemaOptions)
                        .build(),
                this.runtime,
                this.protobufSerializer,
                mapSupplier,
                versionPolicy,
                streamTagIdsForTable);
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
                // If the table is completely unheard of return NoSuchElementException.
                throw new NoSuchElementException(String.format(
                        "No such table found: namespace: %s, tableName: %s", namespace, tableName));
            }
        }
        return (Table<K, V, M>) tableMap.get(fullyQualifiedTableName);
    }

    /**
     * Close a table that is already opened.
     *
     * @param namespace Namespace of the table.
     * @param tableName Name of the table.
     * @throws NoSuchElementException - if the table does not exist.
     */
    public void closeTable(String namespace, String tableName) {
        String fullyQualifiedTableName = getFullyQualifiedTableName(namespace, tableName);
        Table<Message, Message, Message> table = tableMap.get(fullyQualifiedTableName);
        if (table == null) {
            throw new NoSuchElementException("closeTable: Did not find any table "+ fullyQualifiedTableName);
        }
        tableMap.remove(fullyQualifiedTableName);
        ObjectsView.ObjectID oid = new ObjectsView.ObjectID(table.getStreamUUID(), CorfuTable.class);
        Object tableObject = runtime.getObjectsView().getObjectCache().remove(oid);
        if (tableObject == null) {
            throw new NoSuchElementException("closeTable: No object cache entry for "+ fullyQualifiedTableName);
        }
    }

    /**
     * Special method to remove the table metadata from registry without actually touching
     * the table itself.
     *
     * @param namespace Namespace of the table.
     * @param tableName Name of the table.
     * @return number of protobuf files also deleted as a result of this table deletion
     */
    public int deleteTable(String namespace, String tableName) {
        String fullName = getFullyQualifiedTableName(namespace, tableName);
        if (TransactionalContext.isInTransaction()) {
            throw new IllegalStateException("Cannot run deleteTable "+fullName+"from within another transaction");
        }
        if (tableMap.containsKey(fullName)) {
            final Table<Message, Message, Message> messageTable = tableMap.get(fullName);
            tableMap.remove(fullName);
        }
        TableName tableNameProt = TableName.newBuilder().setNamespace(namespace).setTableName(tableName).build();
        runtime.getObjectsView().TXBegin();
        CorfuRecord<TableDescriptors, TableMetadata> tableEntry = registryTable.get(tableNameProt);
        if (tableEntry == null) {
            runtime.getObjectsView().TXEnd();
            throw new NoSuchElementException("deleteTable can't find "+fullName);
        }
        registryTable.delete(tableNameProt);
        Set<String> protoFilesToDelete = new HashSet<>(tableEntry.getPayload().getFileDescriptorsMap().keySet());
        // Do a full registry table scan and prune out any protobuf files also required by some other table(s).
        for (Map.Entry<TableName, CorfuRecord<TableDescriptors, TableMetadata>> entry: registryTable.entrySet()) {
            protoFilesToDelete.removeIf(protFileToDel ->
                    entry.getValue().getPayload().containsFileDescriptors(protFileToDel));
            if (protoFilesToDelete.isEmpty()) {
                break;
            }
        }
        log.warn("deleteTable: {}. Proto files erased {}", fullName, protoFilesToDelete);
        // Safe to wipe out these protobuf files from registry since they are not opened by anyone else.
        protoFilesToDelete.forEach(fileName ->
                protobufDescriptorTable.delete(ProtobufFileName.newBuilder().setFileName(fileName).build()));
        runtime.getObjectsView().TXEnd();
        return protoFilesToDelete.size();
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
     * Lists all tables.
     *
     * @return Collection of tables.
     */
    public Collection<TableName> listTables() {
        return registryTable.keySet();
    }

    /**
     * Lists all tables for a specific stream tag.
     *
     * @return Collection of tables.
     */
    public List<String> listTables(@Nullable final String namespace, @Nullable final String streamTag) {
        return registryTable.entryStream()
                .filter(table -> table.getKey().getNamespace().equals(namespace))
                .filter(table -> table.getValue().getMetadata().getTableOptions().getStreamTagList().contains(streamTag))
                .map(Map.Entry::getKey)
                .map(TableName::getTableName)
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
     * Register a streaming subscription manager as a singleton.
     */
    public synchronized StreamingManager getStreamingManager() {
        if (streamingManager == null) {
            streamingManager = new StreamingManager(runtime);
        }
        return streamingManager;
    }

    /**
     * Shutdown the table register, cleaning up relevant resources.
     */
    public void shutdown() {
        if (streamingManager != null) {
            streamingManager.shutdown();
        }
    }
}
