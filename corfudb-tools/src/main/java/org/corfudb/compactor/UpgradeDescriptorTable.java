package org.corfudb.compactor;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.ProtobufFileDescriptor;
import org.corfudb.runtime.CorfuStoreMetadata.ProtobufFileName;
import org.corfudb.runtime.CorfuStoreMetadata.TableDescriptors;
import org.corfudb.runtime.CorfuStoreMetadata.TableMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.SerializerException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.ProtobufSerializer;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.corfudb.runtime.view.TableRegistry.getTypeUrl;

@Slf4j
public class UpgradeDescriptorTable {

    CorfuRuntime corfuRuntime;

    UpgradeDescriptorTable(CorfuRuntime corfuRuntime) {
        this.corfuRuntime = corfuRuntime;
    }
    /**
     * Create a protobuf serializer.
     *
     * @return Protobuf Serializer.
     */
    private ISerializer createProtobufSerializer() {
        ConcurrentMap<String, Class<? extends Message>> classMap = new ConcurrentHashMap<>();

        // Register the schemas of TableName, TableDescriptors, TableMetadata, ProtobufFilename/Descriptor
        // to be able to understand registry table.
        classMap.put(getTypeUrl(TableName.getDescriptor()), TableName.class);
        classMap.put(getTypeUrl(TableDescriptors.getDescriptor()),
                TableDescriptors.class);
        classMap.put(getTypeUrl(TableMetadata.getDescriptor()),
                TableMetadata.class);
        classMap.put(getTypeUrl(ProtobufFileName.getDescriptor()),
                ProtobufFileName.class);
        classMap.put(getTypeUrl(ProtobufFileDescriptor.getDescriptor()),
                ProtobufFileDescriptor.class);
        return new ProtobufSerializer(classMap);
    }

    /**
     * Populate the ProtobufDescriptorTable using the RegistryTable.
     * Enables backward compatibility in case of data migration.
     */
    public void syncProtobufDescriptorTable() {

        log.info("Running syncProtobufDescriptorTable ...");
        // Create or get a protobuf serializer to read the table registry.
        try {
            corfuRuntime.getSerializers().getSerializer(ProtobufSerializer.PROTOBUF_SERIALIZER_CODE);
        } catch (SerializerException se) {
            // This means the protobuf serializer had not been registered yet.
            ISerializer protobufSerializer = createProtobufSerializer();
            corfuRuntime.getSerializers().registerSerializer(protobufSerializer);
        }

        int numRetries = 9;
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        while (true) {
            try (TxnContext tx = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                PersistentCorfuTable<TableName, CorfuRecord<TableDescriptors, TableMetadata>>
                        registryTable = corfuRuntime.getTableRegistry().getRegistryTable();
                PersistentCorfuTable<ProtobufFileName, CorfuRecord<ProtobufFileDescriptor, TableMetadata>>
                        descriptorTable = corfuRuntime.getTableRegistry().getProtobufDescriptorTable();

                Set<TableName> allTableNames = registryTable.keySet();
                for (TableName tableName : allTableNames) {
                    populateDescriptorTable(tableName, registryTable, descriptorTable);
                }
                tx.commit();
                log.info("syncProtobufDescriptorTable: completed!");
                break;
            } catch (TransactionAbortedException txAbort) {
                if (numRetries-- <= 0) {
                    throw txAbort;
                }
                log.info("syncProtobufDescriptorTable: commit failed. " +
                        "Will retry {} times. Cause {}", numRetries, txAbort);
            }
        }
    }

    private void populateDescriptorTable(TableName tableName,
                                         PersistentCorfuTable<TableName, CorfuRecord<TableDescriptors, TableMetadata>> registryTable,
                                         PersistentCorfuTable<ProtobufFileName, CorfuRecord<ProtobufFileDescriptor, TableMetadata>> descriptorTable) {

        CorfuRecord<TableDescriptors, TableMetadata> registryRecord = registryTable.get(tableName);
        TableDescriptors.Builder tableDescriptorsBuilder = TableDescriptors.newBuilder();
        registryRecord.getPayload().getFileDescriptorsMap().forEach(
                (protoName, fileDescriptorProto) -> {
                    // populate ProtobufDescriptorTable
                    ProtobufFileName fileName = ProtobufFileName
                            .newBuilder().setFileName(protoName).build();
                    ProtobufFileDescriptor fileDescriptor = ProtobufFileDescriptor
                            .newBuilder().setFileDescriptor(fileDescriptorProto).build();

                    // TODO(Zach): replaced putIfAbsent
                    CorfuRecord<ProtobufFileDescriptor, TableMetadata> corfuRecord =
                            descriptorTable.get(fileName);

                    if (corfuRecord == null) {
                        descriptorTable.insert(fileName, new CorfuRecord<>(fileDescriptor, null));
                        log.info("Add proto file {}, fileDescriptor {} to ProtobufDescriptorTable",
                                fileName, fileDescriptor.getFileDescriptor());
                    }
                    // construct a new tableDescriptorsMap using default FileDescriptorProto instances
                    tableDescriptorsBuilder.putFileDescriptors(protoName,
                            fileDescriptorProto.getDefaultInstanceForType());
                });

        tableDescriptorsBuilder.setKey(registryRecord.getPayload().getKey());
        tableDescriptorsBuilder.setValue(registryRecord.getPayload().getValue());
        tableDescriptorsBuilder.setMetadata(registryRecord.getPayload().getMetadata());

        // clean up FileDescriptorsMap inside RegistryTable to optimize memory
        TableDescriptors tableDescriptors = tableDescriptorsBuilder.build();
        registryTable.insert(tableName, new CorfuRecord<>(tableDescriptors, registryRecord.getMetadata()));

        log.info("Cleaned up an entry in RegistryTable: {}${}",
                tableName.getNamespace(), tableName.getTableName());
    }
}
