package org.corfudb.browser;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import lombok.Getter;
import lombok.Setter;
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
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.collections.ICorfuTable;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.TableRegistry.FullyQualifiedTableName;
import org.corfudb.util.serializer.DynamicProtobufSerializer;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static org.corfudb.browser.CorfuStoreBrowserEditor.printKey;
import static org.corfudb.browser.CorfuStoreBrowserEditor.printMetadata;
import static org.corfudb.browser.CorfuStoreBrowserEditor.printPayload;
import static org.corfudb.browser.CorfuStoreBrowserEditor.printTableRegistry;
import static org.corfudb.infrastructure.log.Segment.parseEntry;
import static org.corfudb.infrastructure.log.Segment.parseHeader;
import static org.corfudb.infrastructure.log.Segment.parseMetadata;
import static org.corfudb.infrastructure.log.SegmentUtils.getLogData;

@SuppressWarnings("checkstyle:printLine")
public class CorfuOfflineBrowserEditor implements CorfuBrowserEditorCommands {
    private final Path logDir;
    private final CorfuRuntime runtimeSerializer;
    private DynamicProtobufSerializer dynamicProtobufSerializer;
    private final RegistryTable registryTable;
    private final ProtoBufTable protoBufTable;

    private final CorfuTableDescriptor registryTableDsc;

    private final CorfuTableDescriptor protobufDescTableDsc;

    /**
     * Creates a CorfuOfflineBrowser linked to an existing log directory.
     * Builds the registry table and protobuf table for deserialization of log entries.
     * @param offlineDbDir Path to the database directory.
     */
    public CorfuOfflineBrowserEditor(String offlineDbDir) throws UncheckedIOException {

        logDir = Paths.get(offlineDbDir);
        registryTableDsc = new CorfuTableDescriptor(TableRegistry.CORFU_SYSTEM_NAMESPACE,
                TableRegistry.REGISTRY_TABLE_NAME);
        protobufDescTableDsc = new CorfuTableDescriptor(TableRegistry.CORFU_SYSTEM_NAMESPACE,
                TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME);
        registryTable = new RegistryTable();
        protoBufTable = new ProtoBufTable();
        runtimeSerializer = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build());
        runtimeSerializer.getSerializers().registerSerializer(DynamicProtobufSerializer.createProtobufSerializer());
        buildRegistryTableProtobufTable();
    }

    /**
     * Builds the Registry table and Protobuf Table.
     */
    private void buildRegistryTableProtobufTable() {

        List<CorfuTableDescriptor> tableDsc = Arrays.asList(registryTableDsc, protobufDescTableDsc);
        List<CachedTable> tables = Arrays.asList(registryTable, protoBufTable);

        parseLogsFiles(tableDsc, tables);
        updateSerializer(runtimeSerializer, registryTable.getTrimmedTable(), protoBufTable.getTrimmedTable());
    }

    /**
     * Adds the transaction to the in-memory CachedTable.
     * @param smrUpdate Transaction.
     * @param address Address of the Log Entry.
     * @param table In-Memory table.
     */
    private void addTransactionToTable(SMREntry smrUpdate, long address, CachedTable table) {

        Object[] smrUpdateArg = smrUpdate.getSMRArguments();
        String smrMethod = smrUpdate.getSMRMethod();

        switch (smrMethod) {
            case "put":
                table.insert(smrUpdateArg[0], smrUpdateArg[1], address);
                break;
            case "remove":
                table.insert(smrUpdateArg[0], null, address);
                break;
            case "clear":
                table.clear(address);
                break;
            default:
                System.out.println("DEBUG: Operation Unknown");
        }
    }

    /**
     * Processes the LogData for Transactions.
     * @param data LogData.
     * @param tableDsc Corfu Table Descriptor.
     * @param table In-Memory table.
     */
    private void processLogData(LogData data, CorfuTableDescriptor tableDsc, CachedTable table) {

        Object modifiedData = data.getPayload(runtimeSerializer);
        List<SMREntry> smrUpdates = new ArrayList<>();
        long snapshotAddress = 0;

        if (modifiedData instanceof CheckpointEntry) {
            snapshotAddress = Long.decode(((CheckpointEntry)modifiedData).
                    getDict().
                    get(CheckpointEntry.CheckpointDictKey.SNAPSHOT_ADDRESS));
            MultiSMREntry smrEntries = ((CheckpointEntry) modifiedData).getSmrEntries();
            if (smrEntries != null) {
                smrUpdates = smrEntries.getUpdates();
            }
        } else if (modifiedData instanceof MultiObjectSMREntry) {
            smrUpdates = ((MultiObjectSMREntry) modifiedData).getSMRUpdates(tableDsc.getStreamID());
        }

        for (SMREntry smrUpdate : smrUpdates) {
            if (modifiedData instanceof CheckpointEntry) {
                addTransactionToTable(smrUpdate, snapshotAddress, table);
            } else {
                addTransactionToTable(smrUpdate, smrUpdate.getGlobalAddress(), table);
            }
        }
    }

    /**
     * Parses the log files and builds the in-memory tables.
     * @param tableDsc list of table Descriptors.
     * @param tables list of in-memory tables for list of table Descriptors.
     */
    private void parseLogsFiles(List<CorfuTableDescriptor> tableDsc, List<CachedTable> tables) {

        String[] extension = {"log"};
        File dir = logDir.toFile();
        Collection<File> files = FileUtils.listFiles(dir, extension, true);

        for (File file : files) {
            try (FileChannel fileChannel = FileChannel.open(file.toPath())) {

                fileChannel.position(0);
                parseHeader(fileChannel, file.getAbsolutePath());

                while (fileChannel.position() < fileChannel.size() - 14) {

                    LogFormat.Metadata metadata = parseMetadata(fileChannel, file.getAbsolutePath());
                    LogEntry entry = parseEntry(fileChannel, metadata, file.getAbsolutePath());

                    if (metadata != null && entry != null) {
                        LogData data = getLogData(entry);
                        for (int index = 0; index < tableDsc.size(); ++index) {
                            if (tableDsc.get(index).belongsToStream(data) && data.getType() != DataType.HOLE) {
                                processLogData(data, tableDsc.get(index), tables.get(index));
                            }
                        }
                    }
                }
                System.out.println("Finished processing file: " + file.getAbsolutePath());

            } catch (IOException e) {
                throw new IllegalStateException("Invalid header: " + file.getAbsolutePath(), e);
            }
        }
    }

    private void updateSerializer(CorfuRuntime runtimeSerializer, ConcurrentMap registryTable, ConcurrentMap protobufTable) {
        dynamicProtobufSerializer = new DynamicProtobufSerializer(registryTable, protobufTable);
        runtimeSerializer.getSerializers().registerSerializer(dynamicProtobufSerializer);
    }

    /**
     * Builds the given table.
     * @param namespace
     * @param tableName
     * @return ConcurrentMap
     */
    public ConcurrentMap getTableData(String namespace, String tableName) {

        CorfuTableDescriptor tableDsc = new CorfuTableDescriptor(namespace, tableName);
        Table table = new Table();

        parseLogsFiles(Arrays.asList(tableDsc), Arrays.asList(table));
        return table.getTrimmedTable();
    }

    /**
     * Prints the payload and metadata in the given table
     * @param namespace
     * @param tableName
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
     * @param namespace
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
    public ICorfuTable<CorfuDynamicKey, CorfuDynamicRecord> getTable(String namespace, String tableName) {
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

    class CorfuTableDescriptor {
        @Getter
        private final UUID streamID;
        @Getter
        private final UUID checkpointID;
        private final String namespace;
        private final String tableName;

        public CorfuTableDescriptor(String namespace, String tableName) {
            this.namespace = namespace;
            this.tableName = tableName;
            streamID = FullyQualifiedTableName.streamId(namespace, tableName).getId();
            checkpointID = CorfuRuntime.getCheckpointStreamIdFromId(streamID);
        }

        public boolean belongsToStream(LogData data) {
            return data.containsStream(streamID) || data.containsStream(checkpointID);
        }

        public boolean belongsToStream(String namespace, String tableName) {
            return this.namespace.equals(namespace) && this.tableName.equals(tableName);
        }
    }

    class OrderedObject {
        @Getter
        private Object update;
        @Getter
        private long order;

        public OrderedObject(Object smrObject, long address) {
            this.update = smrObject;
            this.order = address;
        }
    }

    class CachedTable {
        private ConcurrentMap<Object, OrderedObject> table;
        @Setter
        @Getter
        private long clearTableTxn;

        public void insert(Object key, Object value, long address) {
            if (table.containsKey(key) && (table.get(key)).getOrder() > address) {
                return;
            }
            table.put(key, new OrderedObject(value, address));
        }

        public ConcurrentMap getTrimmedTable() {
            return new ConcurrentHashMap();
        }

        public void clear(long address) {
            clearTableTxn = Math.max(address, clearTableTxn);
        }
    }

    class RegistryTable extends CachedTable {
        private ConcurrentMap<CorfuStoreMetadata.TableName, OrderedObject> table;

        public RegistryTable() {
            this.table = new ConcurrentHashMap<>();
            setClearTableTxn(-1);
        }

        public void insert(Object key, Object value, long address) {
            CorfuStoreMetadata.TableName newKey = (CorfuStoreMetadata.TableName) key;
            if (table.containsKey(newKey) && (table.get(newKey)).getOrder() > address) {
                return;
            }
            table.put(newKey, new OrderedObject(value, address));
        }

        public ConcurrentMap getTrimmedTable() {

            ConcurrentMap trimmedTable = new ConcurrentHashMap<>();
            Set<Map.Entry<CorfuStoreMetadata.TableName, OrderedObject>> entries = table.entrySet();
            for (Map.Entry<CorfuStoreMetadata.TableName, OrderedObject> entry : entries) {
                OrderedObject value = entry.getValue();
                if (value.getOrder() > getClearTableTxn() && value.getUpdate() != null) {
                    trimmedTable.put(entry.getKey(), value.getUpdate());
                }
            }
            return trimmedTable;
        }
    }

    class ProtoBufTable extends CachedTable {
        private ConcurrentMap<CorfuStoreMetadata.ProtobufFileName, OrderedObject> table;

        public ProtoBufTable() {
            this.table = new ConcurrentHashMap<>();
            setClearTableTxn(-1);
        }

        public void insert(Object key, Object value, long address) {
            CorfuStoreMetadata.ProtobufFileName newKey = (CorfuStoreMetadata.ProtobufFileName) key;
            if (table.containsKey(newKey) && (table.get(newKey)).getOrder() > address) {
                return;
            }
            table.put(newKey, new OrderedObject(value, address));
        }

        public ConcurrentMap getTrimmedTable() {

            ConcurrentMap trimmedTable = new ConcurrentHashMap<>();
            Set<Map.Entry<CorfuStoreMetadata.ProtobufFileName, OrderedObject>> entries = table.entrySet();
            for (Map.Entry<CorfuStoreMetadata.ProtobufFileName, OrderedObject> entry : entries) {
                OrderedObject value = entry.getValue();
                if (value.getOrder() > getClearTableTxn() && value.getUpdate() != null) {
                    trimmedTable.put(entry.getKey(), value.getUpdate());
                }
            }
            return trimmedTable;
        }
    }

    class Table extends CachedTable {
        private ConcurrentMap<CorfuDynamicKey, OrderedObject> table;

        public Table() {
            this.table = new ConcurrentHashMap<>();
            setClearTableTxn(-1);
        }

        public void insert(Object key, Object value, long address) {
            CorfuDynamicKey newKey = (CorfuDynamicKey) key;
            if (table.containsKey(newKey) && (table.get(newKey)).getOrder() > address) {
                return;
            }
            table.put(newKey, new OrderedObject(value, address));
        }

        public ConcurrentMap getTrimmedTable() {

            ConcurrentMap trimmedTable = new ConcurrentHashMap<>();
            Set<Map.Entry<CorfuDynamicKey, OrderedObject>> entries = table.entrySet();
            for (Map.Entry<CorfuDynamicKey, OrderedObject> entry : entries) {
                OrderedObject value = entry.getValue();
                if (value.getOrder() > getClearTableTxn() && value.getUpdate() != null) {
                    trimmedTable.put(entry.getKey(), value.getUpdate());
                }
            }
            return trimmedTable;
        }
    }
}
