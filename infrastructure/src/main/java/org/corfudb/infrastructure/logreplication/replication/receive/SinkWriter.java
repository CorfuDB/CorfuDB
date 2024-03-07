package org.corfudb.infrastructure.logreplication.replication.receive;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableDescriptors;
import org.corfudb.runtime.CorfuStoreMetadata.TableMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.serializer.ISerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.corfudb.util.serializer.ProtobufSerializer.PROTOBUF_SERIALIZER_CODE;

/**
 * A parent class for Sink side StreamsSnapshotWriter and LogEntryWriter, which contains some common
 * utility methods that could be used in both snapshot sync and log entry sync.
 */
@Slf4j
public abstract class SinkWriter {

    // Configuration for LR in Source / Sink cluster.
    final LogReplicationConfig config;

    final ISerializer protobufSerializer;

    final LogReplicationMetadataManager logReplicationMetadataManager;


    // Limit the initialization of this class only to its children classes.
    SinkWriter(LogReplicationConfig config, LogReplicationMetadataManager logReplicationMetadataManager) {
        this.logReplicationMetadataManager = logReplicationMetadataManager;
        this.config = config;
        // The CorfuRuntime in LogReplicationConfigManager is generally used to get the config fields from registry
        // table, and the protobufSerializer is guaranteed to be registered before initializing SinkWriter.
        this.protobufSerializer = config.getConfigManager().getConfigRuntime()
                .getSerializers().getSerializer(PROTOBUF_SERIALIZER_CODE);
    }

    /**
     * Drop Source side registry table entries whose is_federated flag differs from records in Sink side registry.
     * For entries to be applied, add their serialization info before applying, because the registry table will be read
     * subsequently but in log replication table entries are sent to Sink in form of OpaqueEntry,
     *
     * @param smrEntries List of SMREntry for registry table
     * @return A list of new SMR entries with their serialization info added.
     */
    List<SMREntry> filterRegistryTableEntries(List<SMREntry> smrEntries) {
        List<SMREntry> corfuSMREntries = new ArrayList<>();

        for (SMREntry smrEntry : smrEntries) {
            // Get serialized form of arguments for registry table. They were sent in OpaqueEntry and
            // need to be deserialized using ProtobufSerializer
            Object[] objs = smrEntry.getSMRArguments();
            ByteBuf keyBuf = Unpooled.wrappedBuffer((byte[]) objs[0]);
            TableName tableName = (TableName) protobufSerializer.deserialize(keyBuf, null);

            ByteBuf valueBuf = Unpooled.wrappedBuffer((byte[]) objs[1]);
            CorfuRecord<TableDescriptors, TableMetadata> record =
                    (CorfuRecord<TableDescriptors, TableMetadata>) protobufSerializer.deserialize(valueBuf, null);

            UUID streamId = CorfuRuntime.getStreamID(TableRegistry.getFullyQualifiedTableName(tableName));
            if (ignoreEntryForRegistryTable(streamId, record)) {
                log.info("Ignoring registry table's record for {}", tableName);
            } else {
                log.info("Registry table will be updated, key = {}", tableName);
                SMREntry newValidEntry = new SMREntry("put", new Object[] {tableName, record}, protobufSerializer);
                // Serialize the entries back such that they could be applied in following steps
                ByteBuf byteBuf = Unpooled.buffer();
                newValidEntry.serialize(byteBuf);
                corfuSMREntries.add(newValidEntry);
            }
        }

        return corfuSMREntries;
    }

    /**
     * Check if a stream id belongs to list of replicated streams to drop in LogReplicationConfig. If so, its entries
     * should be ignored by SnapshotWriter and LogEntryWriter.
     *
     * @param streamId ID of the stream whose entries are being applied by LR
     * @return True if the entries should be ignored.
     */
    boolean ignoreEntriesForStream(UUID streamId) {
        return config.getReplicatedStreamsToDrop().contains(streamId);
    }

    /**
     * Check if the given stream belongs to list of replicated streams to drop, or the deserialized entry sent by Source has
     * is_federated = false. The record should not be applied if either of the conditions established.
     *
     * @param streamId stream ID of the registry table entry.
     * @param record CorfuRecord of the registry table entry.
     * @return True if the entry should be ignored.
     */
    boolean ignoreEntryForRegistryTable(UUID streamId, CorfuRecord<TableDescriptors, TableMetadata> record) {

        return config.getReplicatedStreamsToDrop().contains(streamId) ||
                !record.getMetadata().getTableOptions().getIsFederated();
    }
}
