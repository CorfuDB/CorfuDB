package org.corfudb.browser;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import lombok.Getter;
import org.apache.commons.io.FileUtils;
import org.corfudb.infrastructure.log.LogFormat;
import org.corfudb.infrastructure.log.LogFormat.LogEntry;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.MultiSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.*;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.serializer.DynamicProtobufSerializer;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.log.StreamLogFiles.*;

class CorfuTableDescriptor {
    @Getter
    private final UUID streamID;
    @Getter
    private final UUID checkpointID;

    public CorfuTableDescriptor(String namespace, String tableName) {
        String name = TableRegistry.getFullyQualifiedTableName(namespace, tableName);
        streamID = CorfuRuntime.getStreamID(name);
        checkpointID = CorfuRuntime.getCheckpointStreamIdFromId(streamID);
    }

    public boolean belongsToStream(LogData data) {
        if (data.containsStream(streamID) || data.containsStream(checkpointID)) return true;
        return false;
    }
}

@SuppressWarnings("checkstyle:printLine")
public class CorfuOfflineBrowserEditor implements CorfuBrowserEditorCommands {
    private final Path logDir;
    private DynamicProtobufSerializer dynamicProtobufSerializer;

    private final CorfuTableDescriptor registryTableDsc;

    private final CorfuTableDescriptor protobufDescTableDsc;

    /**
     * Creates a CorfuOfflineBrowser linked to an existing log directory.
     * @param offlineDbDir Path to the database directory.
     */
    public CorfuOfflineBrowserEditor(String offlineDbDir) throws UncheckedIOException {

        logDir = Paths.get(offlineDbDir);
        registryTableDsc = new CorfuTableDescriptor(TableRegistry.CORFU_SYSTEM_NAMESPACE,
                TableRegistry.REGISTRY_TABLE_NAME);
        protobufDescTableDsc = new CorfuTableDescriptor(TableRegistry.CORFU_SYSTEM_NAMESPACE,
                TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME);
    }

    public boolean processLogData(LogData data,
                                  CorfuTableDescriptor table,
                                  CorfuRuntime runtimeSerializer,
                                  List<LogEntryOrdering> tableEntries) {

        if (!table.belongsToStream(data)) return false;

        if (data.getType() == DataType.HOLE) {
            System.out.println("DEBUG: HOLE Found");
            return true;
        }

        Object modifiedData = data.getPayload(runtimeSerializer);
        List<SMREntry> smrUpdates = new ArrayList<>();
        long snapshotAddress = 0;
        if (modifiedData instanceof CheckpointEntry) {
            snapshotAddress = Long.decode(((CheckpointEntry)modifiedData).
                    getDict().
                    get(CheckpointEntry.CheckpointDictKey.SNAPSHOT_ADDRESS));
            MultiSMREntry smrEntries = ((CheckpointEntry) modifiedData).
                    getSmrEntries(false, runtimeSerializer);
            if (smrEntries != null) smrUpdates = smrEntries.getUpdates();
        } else if (modifiedData instanceof MultiObjectSMREntry) {
            smrUpdates = ((MultiObjectSMREntry) modifiedData).getSMRUpdates(table.getStreamID());
        }

        for (SMREntry smrUpdate : smrUpdates) {
            LogEntryOrdering entry = (modifiedData instanceof CheckpointEntry) ?
                    new LogEntryOrdering(smrUpdate, snapshotAddress) :
                    new LogEntryOrdering(smrUpdate, smrUpdate.getGlobalAddress());
            tableEntries.add(entry);
        }
        return true;
    }

    public void addRecordsToConcurrentMap(List<LogEntryOrdering> tableEntries, ConcurrentMap cachedTable) {

        tableEntries.sort(new LogEntryComparator());
        while (tableEntries.size() != 0) {

            LogEntryOrdering tableEntry = tableEntries.remove(0);
            Object[] smrUpdateArg = ((SMREntry) tableEntry.getObj()).getSMRArguments();

            String smrMethod = ((SMREntry) tableEntry.getObj()).getSMRMethod();
            switch (smrMethod) {
                case "put":
                    cachedTable.put(smrUpdateArg[0], smrUpdateArg[1]);
                    break;
                case "remove":
                    cachedTable.remove(smrUpdateArg[0]);
                    break;
                case "clear":
                    cachedTable.clear();
                    break;
                default:
                    System.out.println("DEBUG: Operation Unknown");
            }
        }
    }

    private void updateSerializer(CorfuRuntime runtimeSerializer, ConcurrentMap registryTable, ConcurrentMap protobufTable) {
        dynamicProtobufSerializer = new DynamicProtobufSerializer(registryTable, protobufTable);
        runtimeSerializer.getSerializers().registerSerializer(dynamicProtobufSerializer);
    }

    /**
     * Fetches the table from the given namespace
     * @param namespace
     * @param tableName
     * @return ConcurrentMap
     */
    public ConcurrentMap getTableData(String namespace, String tableName) {

        CorfuTableDescriptor table = new CorfuTableDescriptor(namespace, tableName);

        ConcurrentMap cachedRegistryTable = new ConcurrentHashMap<>();
        ConcurrentMap cachedProtobufDescriptorTable = new ConcurrentHashMap<>();
        ConcurrentMap cachedTable = new ConcurrentHashMap<>();

        List<LogEntryOrdering> registryTableEntries = new ArrayList<>();
        List<LogEntryOrdering> protobufDescriptorTableEntries = new ArrayList<>();
        List<LogEntryOrdering> tableEntries = new ArrayList<>();

        CorfuRuntime serializerCS = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build());
        serializerCS.getSerializers().registerSerializer(DynamicProtobufSerializer.createProtobufSerializer());

        CorfuRuntime serializerTable = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build());
        serializerTable.getSerializers().registerSerializer(DynamicProtobufSerializer.createProtobufSerializer());

        String[] extension = {"log"};
        File dir = logDir.toFile();
        Collection<File> files = FileUtils.listFiles(dir, extension, true);

        for (File file : files) {
            try (FileChannel fileChannel = FileChannel.open(file.toPath())) {

                fileChannel.position(0);
                parseHeader(null, fileChannel, file.getAbsolutePath());

                while (fileChannel.position() < fileChannel.size() - 14) {

                    LogFormat.Metadata metadata = StreamLogFiles.parseMetadata(null, fileChannel, file.getAbsolutePath());
                    LogEntry entry = StreamLogFiles.parseEntry(null, fileChannel, metadata, file.getAbsolutePath());

                    if (metadata != null && entry != null) {
                        LogData data = StreamLogFiles.getLogData(entry);
                        boolean registry = processLogData(data,
                                registryTableDsc,
                                serializerCS,
                                registryTableEntries);
                        boolean protobuf = processLogData(data,
                                protobufDescTableDsc,
                                serializerCS,
                                protobufDescriptorTableEntries);
                        if (registry || protobuf) {
                            addRecordsToConcurrentMap(registryTableEntries, cachedRegistryTable);
                            addRecordsToConcurrentMap(protobufDescriptorTableEntries, cachedProtobufDescriptorTable);
                            updateSerializer(serializerTable, cachedRegistryTable, cachedProtobufDescriptorTable);
                        }
                        processLogData(data, table, serializerTable, tableEntries);
                    }
                }
                System.out.println("Finished processing file: " + file.getAbsolutePath());

            } catch (IOException e) {
                throw new IllegalStateException("Invalid header: " + file.getAbsolutePath(), e);
            }
        }

        addRecordsToConcurrentMap(tableEntries, cachedTable);
        return cachedTable;
    }

    /**
     * Prints the payload and metadata in the given table
     * @param namespace - the namespace where the table belongs
     * @param tableName - table name without the namespace
     * @return - number of entries in the table
     */
    @Override
    public int printTable(String namespace, String tableName) {

        if (namespace.equals(TableRegistry.CORFU_SYSTEM_NAMESPACE)
                && tableName.equals(TableRegistry.REGISTRY_TABLE_NAME)) return printTableRegistry();

        if (namespace.equals(TableRegistry.CORFU_SYSTEM_NAMESPACE)
                && tableName.equals(TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME)) return printAllProtoDescriptors();

        ConcurrentMap<CorfuDynamicKey, CorfuDynamicRecord> cachedTable = getTableData(namespace, tableName);
        for (CorfuDynamicKey key: cachedTable.keySet()) {
            System.out.println("Key: ");
            System.out.println(key.getKey());
            System.out.println("Value: ");
            System.out.println(cachedTable.get(key).getPayload());
        }
        return cachedTable.size();
    }

    private int printTableRegistry() {

        getTableData(TableRegistry.CORFU_SYSTEM_NAMESPACE, TableRegistry.REGISTRY_TABLE_NAME);
        for (Map.Entry<CorfuStoreMetadata.TableName,
                CorfuRecord<CorfuStoreMetadata.TableDescriptors, CorfuStoreMetadata.TableMetadata>> entry :
                dynamicProtobufSerializer.getCachedRegistryTable().entrySet()) {
            try {
                StringBuilder builder = new StringBuilder("\nKey:\n").append(JsonFormat.printer().print(entry.getKey()));
                System.out.println(builder);
            } catch (Exception e) {
                System.out.println("Unable to print tableName of this registry table key " + entry.getKey());
            }
            try {
                StringBuilder builder = new StringBuilder();
                builder.append("\nkeyType = \"" + entry.getValue().getPayload().getKey().getTypeUrl() + "\"");
                builder.append("\npayloadType = \"" + entry.getValue().getPayload().getValue().getTypeUrl() + "\"");
                builder.append("\nmetadataType = \"" + entry.getValue().getPayload().getMetadata().getTypeUrl() + "\"");
                builder.append("\nProtobuf Source Files: \"" +
                        entry.getValue().getPayload().getFileDescriptorsMap().keySet()
                );
                System.out.println(builder);
            } catch (Exception e) {
                System.out.println("Unable to extract payload fields from registry table key " + entry.getKey());
            }

            try {
                StringBuilder builder = new StringBuilder("\nMetadata:\n")
                        .append(JsonFormat.printer().print(entry.getValue().getMetadata()));
                System.out.println(builder);
            } catch (Exception e) {
                System.out.println("Unable to print metadata section of registry table");
            }
        }

        return dynamicProtobufSerializer.getCachedRegistryTable().size();
    }

    /**
     * List all tables in CorfuStore
     * @param namespace - the namespace where the table belongs
     * @return - number of tables in this namespace
     */
    @Override
    public int listTables(String namespace) {
        int numTables = 0;
        System.out.println("\n=====Tables=======\n");
        for (CorfuStoreMetadata.TableName tableName : listTablesInNamespace(namespace)) {
            System.out.println("Table: " + tableName.getTableName());
            System.out.println("Namespace: " + tableName.getNamespace());
            numTables++;
        }
        System.out.println("\n======================\n");
        return numTables;
    }

    public List<CorfuStoreMetadata.TableName> listTablesInNamespace(String namespace) {

        getTableData(TableRegistry.CORFU_SYSTEM_NAMESPACE, TableRegistry.REGISTRY_TABLE_NAME);
        return dynamicProtobufSerializer.getCachedRegistryTable().keySet()
                .stream()
                .filter(tableName -> namespace == null || tableName.getNamespace().equals(namespace))
                .collect(Collectors.toList());
    }

    /**
     * Print information about a specific table in CorfuStore
     * @param namespace - the namespace where the table belongs
     * @param tableName - table name without the namespace
     * @return - number of entries in the table
     */
    @Override
    public int printTableInfo(String namespace, String tableName) {
        System.out.println("\n======================\n");
        CorfuTableDescriptor tableDsc = new CorfuTableDescriptor(namespace, tableName);
        ConcurrentMap<CorfuDynamicKey, CorfuDynamicRecord> table = getTableData(namespace, tableName);
        int tableSize = table.size();
        System.out.println("Table " + tableName + " in namespace " + namespace +
                " with ID " + tableDsc.getStreamID() + " has " + tableSize + " entries");
        System.out.println("\n======================\n");
        return tableSize;
    }

    /**
     * Helper to analyze all the protobufs used in this cluster
     */
    @Override
    public int printAllProtoDescriptors() {

        getTableData(TableRegistry.CORFU_SYSTEM_NAMESPACE, TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME);
        System.out.println("=========PROTOBUF FILE NAMES===========");
        for (CorfuStoreMetadata.ProtobufFileName protoFileName :
                dynamicProtobufSerializer.getCachedProtobufDescriptorTable().keySet()) {
            try {
                System.out.println(JsonFormat.printer().print(protoFileName));
            } catch (InvalidProtocolBufferException e) {
                System.out.println("Unable to print protobuf for key " + protoFileName + e);
            }
        }
        System.out.println("=========PROTOBUF FILE DESCRIPTORS ===========");
        for (CorfuStoreMetadata.ProtobufFileName protoFileName :
                dynamicProtobufSerializer.getCachedProtobufDescriptorTable().keySet()) {
            try {
                System.out.println(JsonFormat.printer().print(protoFileName));
                System.out.println(JsonFormat.printer().print(
                        dynamicProtobufSerializer.getCachedProtobufDescriptorTable()
                                .get(protoFileName).getPayload())
                );
            } catch (InvalidProtocolBufferException e) {
                System.out.println("Unable to print protobuf for key " + protoFileName + e);
            }
        }
        return dynamicProtobufSerializer.getCachedProtobufDescriptorTable().keySet().size();
    }

    @Override
    public EnumMap<IMetadata.LogUnitMetadataType, Object> printMetadataMap(long address) {
        return null;
    }

    @Override
    public CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> getTable(String namespace, String tableName) {
        return null;
    }

    @Override
    public int clearTable(String namespace, String tableName) {
        return -1;
    }

    @Override
    public CorfuDynamicRecord addRecord(String namespace, String tableName, String newKey, String newValue, String newMetadata) {
        return null;
    }

    @Override
    public CorfuDynamicRecord editRecord(String namespace, String tableName, String keyToEdit, String newRecord) {
        return null;
    }

    @Override
    public int deleteRecordsFromFile(String namespace, String tableName, String pathToKeysFile, int batchSize) {
        return 0;
    }

    @Override
    public int deleteRecords(String namespace, String tableName, List<String> keysToDelete, int batchSize) {
        return 0;
    }

    @Override
    public int loadTable(String namespace, String tableName, int numItems, int batchSize, int itemSize) {
        return 0;
    }

    @Override
    public int listenOnTable(String namespace, String tableName, int stopAfter) {
        return 0;
    }

    @Override
    public Set<String> listStreamTags() {
        return null;
    }

    @Override
    public Map<String, List<CorfuStoreMetadata.TableName>> listTagToTableMap() {
        return null;
    }

    @Override
    public Set<String> listTagsForTable(String namespace, String table) {
        return null;
    }

    @Override
    public List<CorfuStoreMetadata.TableName> listTablesForTag(@Nonnull String streamTag) {
        return null;
    }
}

/**
 * Wrapper class for LogEntry objects with an address
 */
class LogEntryOrdering {
    org.corfudb.protocols.logprotocol.LogEntry obj;
    long ordering; //address is stored here

    public LogEntryOrdering(org.corfudb.protocols.logprotocol.LogEntry obj, long ordering) {
        this.obj = obj;
        this.ordering = ordering;
    }

    public org.corfudb.protocols.logprotocol.LogEntry getObj() {
        return obj;
    }

    public void setObj(org.corfudb.protocols.logprotocol.LogEntry obj) {
        this.obj = obj;
    }

    public long getOrdering() {
        return ordering;
    }

    public void setOrdering(long ordering) {
        this.ordering = ordering;
    }
}

/**
 * Comparator class for LogEntry objects
 */
class LogEntryComparator implements Comparator<LogEntryOrdering> {
    @Override
    public int compare(LogEntryOrdering a, LogEntryOrdering b) {
        return Long.compare(a.getOrdering(), b.getOrdering());
    }
}