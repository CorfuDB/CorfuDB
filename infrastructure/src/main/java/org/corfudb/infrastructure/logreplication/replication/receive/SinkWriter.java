package org.corfudb.infrastructure.logreplication.replication.receive;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableDescriptors;
import org.corfudb.runtime.CorfuStoreMetadata.TableMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.ICorfuTable;
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
    // A CorfuRuntime passed down from LogReplicationSinkManager.
    final CorfuRuntime rt;

    // Registry table for verifying replicated entries from Source side.
    final ICorfuTable<TableName, CorfuRecord<TableDescriptors, TableMetadata>> registryTable;

    // Limit the initialization of this class only to its children classes.
    SinkWriter(CorfuRuntime rt) {
        this.rt = rt;
        // Instantiate TableRegistry and register serializers in advance to avoid opening
        // registry table in log entry writer's transaction
        this.registryTable = rt.getTableRegistry().getRegistryTable();
    }

    /**
     * In Log Replication table entries are sent to Sink in form of OpaqueEntry. Registry table's entries need
     * to be deserialized in advance to avoid overwriting records that already existed in Sink's registry table.
     * In addition, as the registry table must be read subsequently, new entries need to add their serialization
     * info before applying, which is lost if the OpaqueEntry is written directly.
     *
     *
     * @param smrEntries List of SMREntry for registry table
     * @return This method will filter out the entries already exist in Sink side registry table, and return a list
     *         of new entries with their serialization info added.
     */
    List<SMREntry> fetchNewEntries(List<SMREntry> smrEntries) {
        List<SMREntry> newEntries = new ArrayList<>();
        ISerializer protobufSerializer = rt.getSerializers().getSerializer(PROTOBUF_SERIALIZER_CODE);

        for (SMREntry smrEntry : smrEntries) {
            // Get serialized form of arguments for registry table. They were sent in OpaqueEntry and
            // need to be deserialized using ProtobufSerializer
            Object[] objs = smrEntry.getSMRArguments();
            ByteBuf keyBuf = Unpooled.wrappedBuffer((byte[]) objs[0]);
            TableName tableName =
                    (TableName) rt.getSerializers()
                            .getSerializer(PROTOBUF_SERIALIZER_CODE).deserialize(keyBuf, null);

            ByteBuf valueBuf = Unpooled.wrappedBuffer((byte[]) objs[1]);
            CorfuRecord<TableDescriptors, TableMetadata> record =
                    (CorfuRecord<TableDescriptors, TableMetadata>) protobufSerializer.deserialize(valueBuf, null);

            if (!registryTable.containsKey(tableName)) {
                log.info("Updating registry table, key = {}", tableName);
                SMREntry newEntry = new SMREntry("put", new Object[] {tableName, record}, protobufSerializer);
                // Serialize the entries back such that they could be applied in following steps
                ByteBuf byteBuf = Unpooled.buffer();
                newEntry.serialize(byteBuf);
                newEntries.add(newEntry);
            }
        }

        return newEntries;
    }
}
