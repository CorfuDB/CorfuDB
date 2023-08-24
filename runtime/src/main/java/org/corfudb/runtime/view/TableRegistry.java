package org.corfudb.runtime.view;

import com.google.common.reflect.TypeToken;
import com.google.protobuf.Any;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.ProtocolStringList;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.CheckpointWriter;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.ProtobufFileDescriptor;
import org.corfudb.runtime.CorfuStoreMetadata.ProtobufFileName;
import org.corfudb.runtime.CorfuStoreMetadata.TableDescriptors;
import org.corfudb.runtime.CorfuStoreMetadata.TableMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.ICorfuTable;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TableParameters;
import org.corfudb.runtime.collections.streaming.StreamingManager;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.SerializerException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.ObjectsView.StreamTagInfo;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.GitRepositoryState;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.ProtobufSerializer;
import org.corfudb.util.serializer.Serializers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
     * This {@link PersistentCorfuTable} holds the schemas of the key, payload and metadata for every table created.
     */
    @Getter
    private final PersistentCorfuTable<TableName, CorfuRecord<TableDescriptors, TableMetadata>> registryTable;

    /**
     * To avoid duplicating the protobuf file descriptors that repeat across different tables store all
     * descriptors in a single table indexed by its protobuf file name.
     */
    @Getter
    private final PersistentCorfuTable<ProtobufFileName, CorfuRecord<ProtobufFileDescriptor, TableMetadata>> protobufDescriptorTable;

    /**
     * Spawn the local client checkpointer
     */

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
            .setTypeToken(new TypeToken<PersistentCorfuTable<TableName, CorfuRecord<TableDescriptors, TableMetadata>>>() {
            })
            .setStreamName(getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, REGISTRY_TABLE_NAME))
            .setSerializer(this.protobufSerializer)
            .setStreamTags(LOG_REPLICATOR_STREAM_INFO.getStreamId())
            .open();

        this.protobufDescriptorTable = this.runtime.getObjectsView().build()
            .setTypeToken(new TypeToken<PersistentCorfuTable<ProtobufFileName, CorfuRecord<ProtobufFileDescriptor, TableMetadata>>>() {
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
                TableOptions.fromProtoSchema(TableDescriptors.class));

            registerTable(CORFU_SYSTEM_NAMESPACE,
                PROTOBUF_DESCRIPTOR_TABLE_NAME,
                ProtobufFileName.class,
                ProtobufFileDescriptor.class,
                TableMetadata.class,
                TableOptions.fromProtoSchema(ProtobufFileDescriptor.class));
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
    // Exposing for PoC
    public <K extends Message, V extends Message, M extends Message>
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
        TableMetadata tableMetadata = metadataBuilder.build();

        // Since this is an internal transaction, retry a few times before giving up.
        final int minRetryCount = 16;
        // Some clients open a large number of tables in parallel using ForkJoin thread pools
        // greatly increasing the chances of collisions and transaction aborts.
        // So set the number of retries as a factor of the number of cores in the system.
        int numRetries = Math.max(minRetryCount, Runtime.getRuntime().availableProcessors());
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
                this.runtime.getObjectsView().TXBuild()
                        .type(TransactionType.WRITE_AFTER_WRITE)
                        .build()
                        .begin();
                CorfuRecord<TableDescriptors, TableMetadata> oldRecord =
                        this.registryTable.get(tableNameKey);
                CorfuRecord<TableDescriptors, TableMetadata> newRecord =
                        new CorfuRecord<>(tableDescriptors, tableMetadata);
                boolean protoFileChanged = tryUpdateTableSchemas(allDescriptors);

                String fullyQualifiedTableName = getFullyQualifiedTableName(namespace, tableName);
                UUID streamId = CorfuRuntime.getStreamID(fullyQualifiedTableName);

                if (oldRecord == null) {
                    StreamAddressSpace streamAddressSpace = this.runtime.getSequencerView()
                            .getStreamAddressSpace(new StreamAddressRange(streamId, Address.MAX, Address.NON_ADDRESS));
                    if (streamAddressSpace.size() == 0
                            && streamAddressSpace.getTrimMark() != Address.NON_ADDRESS) {
                        log.info("Found trimmed table that is re-opened. Reset table {}", fullyQualifiedTableName);
                        resetTrimmedTable(fullyQualifiedTableName);
                    }
                }
                if (oldRecord == null || protoFileChanged || tableRecordChanged(oldRecord, newRecord)) {
                    this.registryTable.insert(tableNameKey, newRecord);
                }
                this.runtime.getObjectsView().TXEnd();
                break;
            } catch (TransactionAbortedException txAbort) {
                if (txAbort.getAbortCause() == AbortCause.CONFLICT &&
                        txAbort.getConflictStream().equals(protobufDescriptorTable.getCorfuSMRProxy().getStreamID())) {
                    // Updates to protobuf descriptor tables are internal so conflicts hit here
                    // should not count towards the normal retry count.
                    log.info("registerTable {}${} failed due to conflict in protobuf descriptors. Retrying",
                            namespace, tableName);
                    numRetries++;
                    continue;
                }
                if (numRetries <= 0) {
                    log.error("registerTable failed. Retries exhausted. Cause {}", numRetries, txAbort);
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
     * This method in invoked from registerTable when there's a trimmed table which is reopened.
     * To reset the table, we write a HOLE to the stream and a checkpoint. We write this checkpoint
     * with the registerTable transaction's snapshot. Now, when 2 processes attempt to reopen a trimmed
     * table, one of them fails with CONFLICT on registerTable - but both of them might end up writing the
     * checkpoints. Since the checkpoints are both written only at or before the successful registerTable
     * transaction snapshot, the table's state remains as intended.
     *
     * @param fullyQualifiedTableName get the fullyQualified name of a table
     */
    private void resetTrimmedTable(String fullyQualifiedTableName) {
        if (!TransactionalContext.isInTransaction()) {
            throw new IllegalStateException(
                    "resetTrimmedTable cannot be invoked outside a transaction.");
        }

        UUID streamId = CorfuRuntime.getStreamID(fullyQualifiedTableName);
        PersistentCorfuTable<TableName, CorfuRecord<TableDescriptors, TableMetadata>> corfuTable =
                this.runtime.getObjectsView().build()
                        .setTypeToken(new TypeToken<PersistentCorfuTable<TableName, CorfuRecord<TableDescriptors, TableMetadata>>>() {

                        })
                        .setStreamName(fullyQualifiedTableName)
                        .setSerializer(Serializers.JSON)
                        .open();

        CheckpointWriter<ICorfuTable<?,?>> cpw =
                new CheckpointWriter<>(runtime, streamId, "resetTrimmedTable", corfuTable);
        cpw.forceNoOpEntry();
        cpw.startCheckpoint(TransactionalContext.getCurrentContext().getSnapshotTimestamp());
        cpw.finishCheckpoint();
        log.info("Finished resetting trimmed table {}", fullyQualifiedTableName);
    }

    /**
     * A helper method for comparing CorfuRecord. If the new record is different from old record, the entry
     * of registry table should be updated accordingly.
     *
     * @param oldRecord The record already present in registry table.
     * @param newRecord The new record that is being registered.
     * @return True if the two records are different. Otherwise, return false.
     */
    private boolean tableRecordChanged(@Nonnull CorfuRecord<TableDescriptors, TableMetadata> oldRecord,
                                       @Nonnull CorfuRecord<TableDescriptors, TableMetadata> newRecord) {
        // Both record should have the same table name as TableName is the key of registry table.
        TableName tableName = newRecord.getMetadata().getTableName();
        TableDescriptors oldDescriptors = oldRecord.getPayload();
        TableDescriptors newDescriptors = newRecord.getPayload();
        CorfuOptions.SchemaOptions oldOptions = oldRecord.getMetadata().getTableOptions();
        CorfuOptions.SchemaOptions newOptions = newRecord.getMetadata().getTableOptions();

        // Compare TableDescriptors including the types of Key, Value and Metadata.
        if (!oldDescriptors.getKey().getTypeUrl().equals(newDescriptors.getKey().getTypeUrl()) ||
            !oldDescriptors.getValue().getTypeUrl().equals(newDescriptors.getValue().getTypeUrl()) ||
            !oldDescriptors.getMetadata().getTypeUrl().equals(newDescriptors.getMetadata().getTypeUrl())) {
            log.debug("The record of {} will be updated as TableDescriptors was changed", tableName);
            return true;
        }

        // Compare the primitive types in SchemaOptions
        if (oldOptions.getSecondaryKeyDeprecated() != newOptions.getSecondaryKeyDeprecated() ||
            oldOptions.getVersion() != newOptions.getVersion() ||
            oldOptions.getRequiresBackupSupport() != newOptions.getRequiresBackupSupport() ||
            oldOptions.getIsFederated() != newOptions.getIsFederated() ||
            oldOptions.getStreamTagCount() != newOptions.getStreamTagCount() ||
            oldOptions.getSecondaryKeyCount() != newOptions.getSecondaryKeyCount()) {
            log.debug("The record of {} will be updated as SchemaOptions was changed", tableName);
            return true;
        }


        ProtocolStringList oldStreamTagList = oldOptions.getStreamTagList();
        ProtocolStringList newStreamTagList = newOptions.getStreamTagList();
        // Only needs to check in one direction as their sizes have been compared.
        if (!oldStreamTagList.containsAll(newStreamTagList)) {
            log.debug("The record of {} will be updated as stream tags were changed", tableName);
            return true;
        }

        List<CorfuOptions.SecondaryIndex> oldSecondaryIndices = oldOptions.getSecondaryKeyList();
        List<CorfuOptions.SecondaryIndex> newSecondaryIndices = newOptions.getSecondaryKeyList();
        // Only needs to check in one direction as their sizes have been compared.
        if (!oldSecondaryIndices.containsAll(newSecondaryIndices)) {
            log.debug("The record of {} will be updated as secondary keys were changed", tableName);
            return true;
        }

        return false;
    }

    /**
     * For each protobuf filename -> descriptor map, validate if the previous descriptor matches
     * If there is a new schema or an update, record the new update in the table.
     * (Note that oldProto means existing schema previously opened)
     * WARNING: This method MUST be invoked within a transaction.
     *
     * @param allTableDescriptors  - A map of all the names of the protobuf files with their
     *                               protobuf file descriptors.
     * @return true if a schema change was detected and new schema was updated, false otherwise
     */
    private boolean tryUpdateTableSchemas(Map<ProtobufFileName,
                                          CorfuRecord<ProtobufFileDescriptor, TableMetadata>> allTableDescriptors) {
        boolean schemaChangeDetected = false;
        for (Map.Entry<ProtobufFileName, CorfuRecord<ProtobufFileDescriptor, TableMetadata>> e :
                allTableDescriptors.entrySet()) {
            ProtobufFileName protoName = e.getKey();
            CorfuRecord<ProtobufFileDescriptor, TableMetadata> newProtoFd = e.getValue();
            final CorfuRecord<ProtobufFileDescriptor, TableMetadata> currentSchema =
                    this.protobufDescriptorTable.get(protoName);
            /** Known bug: Protobuf FileDescriptor maps are not deterministically constructed **
            if (log.isTraceEnabled() && currentSchema != null &&
                    !protoName.getFileName().startsWith("google/protobuf") &&
                    !currentSchema.getPayload().getFileDescriptor()
                    .equals(newProtoFd.getPayload().getFileDescriptor())) {
                log.trace("registerTable: Schema update detected for table {}! " +
                                "Old schema is {}, new schema is {}", protoName,
                        currentSchema.getPayload().getFileDescriptor(),
                        newProtoFd.getPayload().getFileDescriptor());
            } Uncomment and run once the above bug is either fixed or has a workaround */
            if (currentSchema != null &&
                    currentSchema.getPayload().getVersion() == newProtoFd.getPayload().getVersion()) {
                continue; // old schema is same as the new schema, avoid doing an expensive I/O
            } // else this process is running a new code version, conservatively update the schemas
            schemaChangeDetected = true;
            this.protobufDescriptorTable.insert(protoName, newProtoFd);
            if (currentSchema != null) {
                log.info("Schema change in {}: {} -> {}", protoName,
                        currentSchema.getPayload().getVersion(), newProtoFd.getPayload().getVersion());
                log.debug("Old Descriptor {}", currentSchema.getPayload().getFileDescriptor());
                log.debug("New Descriptor {}", newProtoFd.getPayload().getFileDescriptor());
            }
        }
        return schemaChangeDetected;
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

            // Version the protobuf file so that we can detect if there is a schema change
            // this is needed to overcome the bug where protobuf file descriptor maps
            // are not deterministically constructed. So we can tell if we are coming up after
            // a fresh upgrade, we conservatively record the git repo version in each proto file
            // and update it if this version were to be different.
            long corfuCodeVersion = GitRepositoryState.getCorfuSourceCodeVersion();
            ProtobufFileName protoFileName = ProtobufFileName.newBuilder()
                    .setFileName(fileDescriptorProto.getName()).build();
            ProtobufFileDescriptor protoFd = ProtobufFileDescriptor.newBuilder()
                    .setFileDescriptor(fileDescriptorProto)
                    .setVersion(corfuCodeVersion)
                    .build();
            // Add the actual descriptor into a common pool of descriptors to avoid duplication
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
        if (namespace == null || namespace.isEmpty()) {
            return tableName;
        }
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
        return CorfuRuntime.getStreamID(getStreamTagFullStreamName(namespace, streamTag));
    }

    /**
     * Return fully qualified stream name for the given tag
     *
     * @param namespace namespace of this stream tag
     * @param streamTag the tag without the namespace
     * @return the fully qualified stream name of this stream tag
     */
    public static String getStreamTagFullStreamName(String namespace, String streamTag) {
        return STREAM_TAG_PREFIX + namespace + streamTag;
    }

    /**
     * Adds the schema to the class map to enable serialization of this table data.
     * This method is exposed as public purely for those tables whose value schema
     * has changed under the hood from one type to another without changing the table name.
     * So those tables can be accessed without hitting a serialization exception once the
     * old type is added to the Serializer's known type map.
     *
     * @param msg Default message of this protobuf message.
     * @param <T> Type of message.
     */
    public <T extends Message> void addTypeToClassMap(T msg) {
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

        // persistentDataPath is deprecated and needs to be removed.
        CorfuOptions.PersistenceOptions persistenceOptions =
                tableOptions.getPersistenceOptions();
        if (tableOptions.getPersistentDataPath().isPresent()) {
            persistenceOptions = tableOptions.getPersistenceOptions().toBuilder().setDataPath(
                    tableOptions.getPersistentDataPath().get().toString()).build();
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

        if (tableSchemaOptions.hasReplicationGroup()) {
            streamTagInfoForTable.add(ObjectsView.getLogicalGroupStreamTagInfo(
                tableSchemaOptions.getReplicationGroup().getClientName()));
        }

        Set<UUID> streamTagIdsForTable = streamTagInfoForTable
                .stream()
                .map(StreamTagInfo::getStreamId)
                .collect(Collectors.toSet());

        log.info(CorfuRuntime.LOG_NOT_IMPORTANT, "openTable: opening {}${} with stream tags {}", namespace, tableName, streamTagInfoForTable);

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
                        .persistenceOptions(persistenceOptions)
                        .secondaryIndexesDisabled(tableOptions.isSecondaryIndexesDisabled())
                        .build(),
                this.runtime,
                this.protobufSerializer,
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
     * Allow underlying objects of this table to be garbage collected
     * while still retaining the metadata of the table.
     *
     * @param namespace Namespace of the table.
     * @param tableName Name of the table.
     * @throws NoSuchElementException - if the table does not exist.
     */
    public void freeTableData(String namespace, String tableName) {
        String fullyQualifiedTableName = getFullyQualifiedTableName(namespace, tableName);
        Table<Message, Message, Message> table = tableMap.get(fullyQualifiedTableName);
        if (table == null) {
            throw new NoSuchElementException("freeTableData: Did not find any table "+ fullyQualifiedTableName);
        }
        table.resetTableData(runtime);
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
        table.clearAll();
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
     * Returns all the tables that are already opened by the runtime
     *
     * @return List of opened Tables
     */
    public List<Table<Message, Message, Message>> getAllOpenTables() {
        List<Table<Message, Message, Message>> allTables = new ArrayList<>(this.tableMap.values());
        try {
            allTables.add(wrapInternalTable(REGISTRY_TABLE_NAME, TableName.class,
                    TableDescriptors.class, TableMetadata.class, TableOptions.<TableName, TableDescriptors>builder().build()));
            allTables.add(wrapInternalTable(PROTOBUF_DESCRIPTOR_TABLE_NAME, ProtobufFileName.class,
                    ProtobufFileDescriptor.class, TableMetadata.class, TableOptions.<TableName, TableDescriptors>builder().build()));
        } catch (Exception e) {
            log.warn("Unable to wrap into Table object due to {}. StackTrace: {}", e.getMessage(), e.getStackTrace());
        }
        return allTables;
    }

    private <K extends Message, V extends Message, M extends Message>
    Table<Message, Message, Message> wrapInternalTable(@Nonnull String tableName,
                                                       @Nonnull Class<K> kClass,
                                                       @Nonnull Class<V> vClass,
                                                       @Nullable Class<M> mClass,
                                                       @Nonnull final TableOptions tableOptions)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        // persistentDataPath is deprecated and needs to be removed.
        CorfuOptions.PersistenceOptions persistenceOptions =
                tableOptions.getPersistenceOptions();
        if (tableOptions.getPersistentDataPath().isPresent()) {
            persistenceOptions = tableOptions.getPersistenceOptions().toBuilder().setDataPath(
                    tableOptions.getPersistentDataPath().get().toString()).build();
        }

        CorfuOptions.SchemaOptions tableSchemaOptions;
        if (tableOptions.getSchemaOptions() != null) {
            tableSchemaOptions = tableOptions.getSchemaOptions();
        } else {
            tableSchemaOptions = CorfuOptions.SchemaOptions.getDefaultInstance();
        }
        V defaultValueMessage = (V) vClass.getMethod("getDefaultInstance").invoke(null);
        M defaultMetadataMessage = (M) mClass.getMethod("getDefaultInstance").invoke(null);

        return (Table<Message, Message, Message>) new Table<K, V, M>(
                TableParameters.<K, V, M>builder()
                        .namespace(CORFU_SYSTEM_NAMESPACE)
                        .fullyQualifiedTableName(getFullyQualifiedTableName(CORFU_SYSTEM_NAMESPACE, tableName))
                        .kClass(kClass)
                        .vClass(vClass)
                        .mClass(mClass)
                        .valueSchema(defaultValueMessage)
                        .metadataSchema(defaultMetadataMessage)
                        .schemaOptions(tableSchemaOptions)
                        .persistenceOptions(persistenceOptions)
                        .secondaryIndexesDisabled(tableOptions.isSecondaryIndexesDisabled())
                        .build(),
                this.runtime,
                this.protobufSerializer,
                new HashSet<>(Collections.singletonList(LOG_REPLICATOR_STREAM_INFO.getStreamId())));
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
