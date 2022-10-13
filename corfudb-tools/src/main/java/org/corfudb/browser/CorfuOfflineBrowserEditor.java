package org.corfudb.browser;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import lombok.Getter;
import org.apache.commons.io.FileUtils;
import org.corfudb.infrastructure.log.LogFormat;
import org.corfudb.infrastructure.log.LogFormat.LogEntry;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.MultiSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.serializer.DynamicProtobufSerializer;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.Set;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.log.StreamLogFiles.parseHeader;
import static org.corfudb.infrastructure.log.StreamLogFiles.parseMetadata;
import static org.corfudb.infrastructure.log.StreamLogFiles.parseEntry;
import static org.corfudb.infrastructure.log.StreamLogFiles.getLogData;

import static org.corfudb.browser.CorfuStoreBrowserEditor.printTableRegistry;
import static org.corfudb.browser.CorfuStoreBrowserEditor.printKey;
import static org.corfudb.browser.CorfuStoreBrowserEditor.printMetadata;
import static org.corfudb.browser.CorfuStoreBrowserEditor.printPayload;

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

        if (!table.belongsToStream(data)) {
            return false;
        }

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
            if (smrEntries != null) {
                smrUpdates = smrEntries.getUpdates();
            }
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

                    LogFormat.Metadata metadata = parseMetadata(null, fileChannel, file.getAbsolutePath());
                    LogEntry entry = parseEntry(null, fileChannel, metadata, file.getAbsolutePath());

                    if (metadata != null && entry != null) {
                        LogData data = getLogData(entry);
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
                && tableName.equals(TableRegistry.REGISTRY_TABLE_NAME)) {
            getTableData(namespace, tableName);
            return printTableRegistry(dynamicProtobufSerializer);
        }

        if (namespace.equals(TableRegistry.CORFU_SYSTEM_NAMESPACE)
                && tableName.equals(TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME)) {
            getTableData(namespace, tableName);
            return printAllProtoDescriptors();
        }

        ConcurrentMap<CorfuDynamicKey, CorfuDynamicRecord> cachedTable = getTableData(namespace, tableName);
        Set<Map.Entry<CorfuDynamicKey, CorfuDynamicRecord>> entries = cachedTable.entrySet();

        for (Map.Entry<CorfuDynamicKey, CorfuDynamicRecord> entry : entries) {
            printKey(entry);
            printPayload(entry);
            printMetadata(entry);
        }
        return cachedTable.size();
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
        CorfuTableDescriptor tableDsc = new CorfuTableDescriptor(namespace, tableName);
        ConcurrentMap<CorfuDynamicKey, CorfuDynamicRecord> table = getTableData(namespace, tableName);
        int tableSize = table.size();
        System.out.println("Table " + tableName + " in namespace " + namespace +
                " with ID " + tableDsc.getStreamID() + " has " + tableSize + " entries");
        return tableSize;
    }

    /**
     * Helper to analyze all the protobufs used in this cluster
     */
    @Override
    public int printAllProtoDescriptors() {

        getTableData(TableRegistry.CORFU_SYSTEM_NAMESPACE, TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME);
        String errMessage = "Unable to print protobuf for key ";
        System.out.println("=========PROTOBUF FILE NAMES===========");
        for (CorfuStoreMetadata.ProtobufFileName protoFileName :
                dynamicProtobufSerializer.getCachedProtobufDescriptorTable().keySet()) {
            try {
                System.out.println(JsonFormat.printer().print(protoFileName));
            } catch (InvalidProtocolBufferException e) {
                System.out.println(errMessage + protoFileName + e);
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
                System.out.println(errMessage + protoFileName + e);
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
            return data.containsStream(streamID) || data.containsStream(checkpointID);
        }
    }
}


