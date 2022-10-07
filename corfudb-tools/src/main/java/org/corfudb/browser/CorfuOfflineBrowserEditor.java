package org.corfudb.browser;

import com.google.common.collect.Iterables;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.java.Log;
import org.apache.commons.io.FileUtils;
import org.apache.tools.ant.types.selectors.SelectSelector;
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
import java.util.stream.Stream;

import static org.corfudb.browser.CorfuStoreBrowserEditor.*;
import static org.corfudb.infrastructure.log.StreamLogFiles.*;


@SuppressWarnings("checkstyle:printLine")
public class CorfuOfflineBrowserEditor implements CorfuBrowserEditorCommands {
    private final Path logDir;
    private DynamicProtobufSerializer dynamicProtobufSerializer;

    /**
     * Creates a CorfuOfflineBrowser linked to an existing log directory.
     * @param offlineDbDir Path to the database directory.
     */
    public CorfuOfflineBrowserEditor(String offlineDbDir) throws UncheckedIOException {
        logDir = Paths.get(offlineDbDir);
        buildRegistryProtoDescriptors();
    }

    /**
     * Opens all log files one by one, accesses and prints log entry data for each Corfu log file.
     */
    public void buildRegistryProtoDescriptors() {
        CorfuRuntime runtimeWithOnlyProtoSerializer = CorfuRuntime
                .fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build());
        runtimeWithOnlyProtoSerializer.getSerializers()
                .registerSerializer(DynamicProtobufSerializer.createProtobufSerializer());

        System.out.println("Analyzing log information:");

        String[] extension = {"log"};
        File dir = logDir.toFile();
        Collection<File> files = FileUtils.listFiles(dir, extension, true);

        String registryTableName = TableRegistry.getFullyQualifiedTableName(TableRegistry.CORFU_SYSTEM_NAMESPACE, TableRegistry.REGISTRY_TABLE_NAME);
        UUID registryTableStreamId = CorfuRuntime.getStreamID(registryTableName);
        UUID registryTableCheckpointStream = CorfuRuntime.getCheckpointStreamIdFromId(registryTableStreamId);

        String protobufDescriptorTableName = TableRegistry.getFullyQualifiedTableName(TableRegistry.CORFU_SYSTEM_NAMESPACE, TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME);
        UUID protobufDescriptorStreamId = CorfuRuntime.getStreamID(protobufDescriptorTableName);
        UUID protobufDescriptorCheckpointStream = CorfuRuntime.getCheckpointStreamIdFromId(protobufDescriptorStreamId);

        List<LogEntryOrdering> tempRegistryTableEntries = new ArrayList<>();
        List<LogEntryOrdering> tempProtobufDescriptorTableEntries = new ArrayList<>();

        ConcurrentMap cachedRegistryTable = new ConcurrentHashMap<>();
        ConcurrentMap cachedProtobufDescriptorTable = new ConcurrentHashMap<>();

        for (File file : files) {
            try (FileChannel fileChannel = FileChannel.open(file.toPath())) {
                fileChannel.position(0);
                LogFormat.LogHeader header = parseHeader(null, fileChannel, file.getAbsolutePath());

                while (fileChannel.position() < fileChannel.size() - 14) {
                    LogFormat.Metadata metadata = StreamLogFiles.parseMetadata(null, fileChannel, file.getAbsolutePath());
                    LogEntry entry = StreamLogFiles.parseEntry(null, fileChannel, metadata, file.getAbsolutePath());

                    if (metadata != null && entry != null) {
                        LogData data = StreamLogFiles.getLogData(entry);
                        processLogData(data, registryTableStreamId, registryTableCheckpointStream, runtimeWithOnlyProtoSerializer, cachedRegistryTable, tempRegistryTableEntries);
                        processLogData(data, protobufDescriptorStreamId, protobufDescriptorCheckpointStream, runtimeWithOnlyProtoSerializer, cachedProtobufDescriptorTable, tempProtobufDescriptorTableEntries);
                    }
                }
                System.out.println("Finished processing file: " + file.getAbsolutePath());

            } catch (IOException e) {
                throw new IllegalStateException("Invalid header: " + file.getAbsolutePath(), e);
            }
        }

        dynamicProtobufSerializer = new DynamicProtobufSerializer(cachedRegistryTable, cachedProtobufDescriptorTable);
        printAllProtoDescriptors();

        System.out.println("Finished analyzing log information.");
    }

    public void processLogData(LogData data,
                               UUID tableStreamID,
                               UUID tableCheckPointStream,
                               CorfuRuntime runtimeWithOnlyProtoSerializer,
                               ConcurrentMap cachedTable,
                               List<LogEntryOrdering> tableEntries) {

        if (data.containsStream(tableStreamID) || data.containsStream(tableCheckPointStream)) {
            if(data.getType() == DataType.DATA) {
                Object modifiedData = data.getPayload(runtimeWithOnlyProtoSerializer);
                List<SMREntry> smrUpdates = new ArrayList<>();
                long snapshotAddress = 0;
                if (modifiedData instanceof CheckpointEntry) {
                    snapshotAddress = Long.decode(((CheckpointEntry)modifiedData).getDict().get(CheckpointEntry.CheckpointDictKey.SNAPSHOT_ADDRESS));
                    MultiSMREntry smrEntries = ((CheckpointEntry) modifiedData).getSmrEntries(false, runtimeWithOnlyProtoSerializer);
                    if (smrEntries != null) smrUpdates = smrEntries.getUpdates();
                } else if (modifiedData instanceof MultiObjectSMREntry) {
                    smrUpdates = ((MultiObjectSMREntry) modifiedData).getSMRUpdates(tableStreamID);
                }

                for (SMREntry smrUpdate : smrUpdates) {
                    LogEntryOrdering entry = (modifiedData instanceof CheckpointEntry) ? new LogEntryOrdering(smrUpdate, snapshotAddress) : new LogEntryOrdering(smrUpdate, smrUpdate.getGlobalAddress());
                    tableEntries.add(entry);
                }
                tableEntries.sort(new LogEntryComparator());

                int tableSize = tableEntries.size();
                for (int i = 0; i < tableSize; ++i) {
                    addRecordToConcurrentMap(tableEntries, cachedTable);
                }

            } else if (data.getType() == DataType.HOLE) {
                System.out.println("Hole found.");
            }
        }
    }

    public void addRecordToConcurrentMap(List<LogEntryOrdering> tableEntries, ConcurrentMap cachedTable) {
        LogEntryOrdering tableEntry = tableEntries.remove(0);
        Object[] smrUpdateArg = ((SMREntry) tableEntry.getObj()).getSMRArguments();
        Object smrUpdateTable;
        Object smrUpdateCorfuRecord;

        String smrMethod = ((SMREntry) tableEntry.getObj()).getSMRMethod();
        switch (smrMethod) {
            case "put":
                smrUpdateCorfuRecord = smrUpdateArg[1];
                smrUpdateTable = smrUpdateArg[0];
                cachedTable.put(smrUpdateTable, smrUpdateCorfuRecord);
                break;
            case "remove":
                smrUpdateTable = smrUpdateArg[0];
                cachedTable.remove(smrUpdateTable);
                break;
            case "clear":
                cachedTable.clear();
                break;
            default:
                System.out.println("Operation Unknown");
        }
    }

    public Object callback(Object smrUpdateTable) {
        if(smrUpdateTable instanceof CorfuStoreMetadata.TableName) {
            return ((CorfuStoreMetadata.TableName) smrUpdateTable);
        } else if(smrUpdateTable instanceof CorfuStoreMetadata.ProtobufFileName) {
            return ((CorfuStoreMetadata.ProtobufFileName) smrUpdateTable);
        }
        return null;
    }

    @Override
    public EnumMap<IMetadata.LogUnitMetadataType, Object> printMetadataMap(long address) {
        return null;
    }

    /**
     * Fetches the table from the given namespace
     * @param namespace Namespace of the table
     * @param tableName Name of the table
     * @return CorfuTable
     */
    @Override
    public CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> getTable(String namespace, String tableName) {
        return null;
    }

    public ConcurrentMap<CorfuDynamicKey, CorfuDynamicRecord> getTableData(String namespace, String tablename) {
        String genericTableName = TableRegistry.getFullyQualifiedTableName(namespace, tablename);
        UUID genericTableStreamId = CorfuRuntime.getStreamID(genericTableName);
        UUID genericTableCheckpointStream = CorfuRuntime.getCheckpointStreamIdFromId(genericTableStreamId);

        CorfuRuntime runtimeWithOnlyProtoSerializer = CorfuRuntime
                .fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build());
        runtimeWithOnlyProtoSerializer.getSerializers()
                .registerSerializer(dynamicProtobufSerializer);

        String[] extension = {"log"};
        File dir = logDir.toFile();
        Collection<File> files = FileUtils.listFiles(dir, extension, true);
        List<LogEntryOrdering> genericTableEntries = new ArrayList<>();
        ConcurrentMap<CorfuDynamicKey, CorfuDynamicRecord> genericCachedTable = new ConcurrentHashMap<>();

        for (File file : files) {
            try (FileChannel fileChannel = FileChannel.open(file.toPath())) {
                fileChannel.position(0);
                LogFormat.LogHeader header = parseHeader(null, fileChannel, file.getAbsolutePath());

                while (fileChannel.position() < fileChannel.size() - 14) {

                    LogFormat.Metadata metadata = StreamLogFiles.parseMetadata(null, fileChannel, file.getAbsolutePath());
                    LogEntry entry = StreamLogFiles.parseEntry(null, fileChannel, metadata, file.getAbsolutePath());

                    if (metadata != null && entry != null){
                        LogData data = StreamLogFiles.getLogData(entry);
                        processLogData(data, genericTableStreamId, genericTableCheckpointStream, runtimeWithOnlyProtoSerializer, genericCachedTable, genericTableEntries);
                    }
                }
                System.out.println("Finished processing file: " + file.getAbsolutePath());

            } catch (IOException e) {
                throw new IllegalStateException("Invalid header: " + file.getAbsolutePath(), e);
            }
        }

        return genericCachedTable;
    }

    /**
     * Prints the payload and metadata in the given table
     * @param namespace - the namespace where the table belongs
     * @param tablename - table name without the namespace
     * @return - number of entries in the table
     */
    @Override
    public int printTable(String namespace, String tablename) {

        ConcurrentMap<CorfuDynamicKey, CorfuDynamicRecord> cachedTable = getTableData(namespace, tablename);
        for (CorfuDynamicKey key: cachedTable.keySet()) {
            System.out.println("Key: ");
            System.out.println(key.getKey());
            System.out.println("Value: ");
            System.out.println(cachedTable.get(key).getPayload());
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
        System.out.println("\n======================\n");
        return numTables;
    }

    public List<CorfuStoreMetadata.TableName> listTablesInNamespace(String namespace) {
        return dynamicProtobufSerializer.getCachedRegistryTable().keySet()
                .stream()
                .filter(tableName -> namespace == null || tableName.getNamespace().equals(namespace))
                .collect(Collectors.toList());
    }

    /**
     * Print information about a specific table in CorfuStore
     * @param namespace - the namespace where the table belongs
     * @param tablename - table name without the namespace
     * @return - number of entries in the table
     */
    @Override
    public int printTableInfo(String namespace, String tablename) {
        System.out.println("\n======================\n");
        String fullName = TableRegistry.getFullyQualifiedTableName(namespace, tablename);
        UUID streamUUID = UUID.nameUUIDFromBytes(fullName.getBytes());
        ConcurrentMap<CorfuDynamicKey, CorfuDynamicRecord> table = getTableData(namespace, tablename);
        int tableSize = table.size();
        System.out.println("Table " + tablename + " in namespace " + namespace +
                " with ID " + streamUUID.toString() + " has " + tableSize + " entries");
        System.out.println("\n======================\n");
        return tableSize;
    }

    /**
     * Helper to analyze all the protobufs used in this cluster
     */
    @Override
    public int printAllProtoDescriptors() {
        int numProtoFiles = -1;
        System.out.println("=========PROTOBUF FILE NAMES===========");
        for (CorfuStoreMetadata.ProtobufFileName protoFileName :
                dynamicProtobufSerializer.getCachedProtobufDescriptorTable().keySet()) {
            try {
                System.out.println(JsonFormat.printer().print(protoFileName));
            } catch (InvalidProtocolBufferException e) {
                System.out.println("Unable to print protobuf for key " + protoFileName + e);
            }
            numProtoFiles++;
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
        return numProtoFiles;
    }

    @Override
    public int clearTable(String namespace, String tablename) {
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