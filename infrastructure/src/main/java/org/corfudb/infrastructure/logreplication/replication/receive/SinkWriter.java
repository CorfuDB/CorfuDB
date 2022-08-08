package org.corfudb.infrastructure.logreplication.replication.receive;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuStoreMetadata.TableDescriptors;
import org.corfudb.runtime.CorfuStoreMetadata.TableMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.util.serializer.ISerializer;

import java.util.ArrayList;
import java.util.List;

import static org.corfudb.util.serializer.ProtobufSerializer.PROTOBUF_SERIALIZER_CODE;

/**
 * A parent class for Sink side StreamsSnapshotWriter and LogEntryWriter, which contains some common
 * utility methods that could be used in both snapshot sync and log entry sync.
 */
@Slf4j
public abstract class SinkWriter {

    final ISerializer protobufSerializer;

    final LogReplicationMetadataManager logReplicationMetadataManager;


    // Limit the initialization of this class only to its children classes.
    SinkWriter(LogReplicationConfigManager configManager, LogReplicationMetadataManager logReplicationMetadataManager) {
        this.logReplicationMetadataManager = logReplicationMetadataManager;
        // The CorfuRuntime in LogReplicationConfigManager is generally used to get the config fields from registry
        // table, and the protobufSerializer is guaranteed to be registered before initializing SinkWriter.
        this.protobufSerializer = configManager.getConfigRuntime().getSerializers().getSerializer(PROTOBUF_SERIALIZER_CODE);
    }

    /**
     * In Log Replication table entries are sent to Sink in form of OpaqueEntry. As the registry table will be read
     * subsequently, new entries need to add their serialization info before applying, which is lost if the OpaqueEntry
     * is written directly.
     *
     * @param smrEntries List of SMREntry for registry table
     * @return A list of new SMR entries with their serialization info added.
     */
    List<SMREntry> addSerializationInfo(List<SMREntry> smrEntries) {
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

            log.info("Registry table will be updated, key = {}", tableName);
            SMREntry newValidEntry = new SMREntry("put", new Object[] {tableName, record}, protobufSerializer);
            // Serialize the entries back such that they could be applied in following steps
            ByteBuf byteBuf = Unpooled.buffer();
            newValidEntry.serialize(byteBuf);
            corfuSMREntries.add(newValidEntry);
        }

        return corfuSMREntries;
    }
}
