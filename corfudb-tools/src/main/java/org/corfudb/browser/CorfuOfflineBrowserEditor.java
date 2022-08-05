package org.corfudb.browser;

import com.google.common.collect.Iterables;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.apache.commons.io.FileUtils;
import org.corfudb.infrastructure.log.LogFormat;
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
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.serializer.DynamicProtobufSerializer;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.corfudb.infrastructure.log.LogFormat.LogEntry;

import static org.corfudb.infrastructure.log.StreamLogFiles.*;


@SuppressWarnings("checkstyle:printLine")
public class CorfuOfflineBrowserEditor implements CorfuBrowserEditorCommands {
    private final Path logDir;
    // make dynamic protobuf serializer final later
    private DynamicProtobufSerializer dynamicProtobufSerializer;
    private final String QUOTE = "\"";

    public CorfuOfflineBrowserEditor(String offlineDbDir) {
        logDir = Paths.get(offlineDbDir, "log");
        System.out.println("Analyzing database located at :"+logDir);

        // prints header information for each of the corfu log files
        printLogEntryData();

        /**
         * write methods that populate the cachedRegistryTable, cacheProtobufDescriptorTable,
         * the fdProtoMap and the messageFdProtoMap to replace the nulls below for the
         * new DynamicProtobufSerializer constructor
         */

        // System.out.println(listTables("CorfuSystem"));
        //dynamicProtobufSerializer = new DynamicProtobufSerializer(null, null);

        // testing printAllProtoDescriptors
        //System.out.println(printAllProtoDescriptors());
    }

    /**
     * Opens all log files one by one, accesses and prints log entry data for each Corfu log file.
     */
    //@param String namespace, String tableName
    public void printLogEntryData() {
        // Add the following lines when the printLogEntryData() method begins...
        CorfuRuntime runtimeWithOnlyProtoSerializer = CorfuRuntime
                .fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build());
        runtimeWithOnlyProtoSerializer.getSerializers()
                .registerSerializer(DynamicProtobufSerializer.createProtobufSerializer());

        System.out.println("Analyzing log information:");

        String[] extension = {"log"};
        File dir = logDir.toFile();
        Collection<File> files = FileUtils.listFiles(dir, extension, true);

        // get the UUIDs of the streams of interest
        String registryTableName = TableRegistry.getFullyQualifiedTableName(TableRegistry.CORFU_SYSTEM_NAMESPACE, TableRegistry.REGISTRY_TABLE_NAME);
        UUID registryTableStreamId = CorfuRuntime.getStreamID(registryTableName);

        String protobufDescriptorTableName = TableRegistry.getFullyQualifiedTableName(TableRegistry.CORFU_SYSTEM_NAMESPACE, TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME);
        UUID protobufDescriptorStreamId = CorfuRuntime.getStreamID(protobufDescriptorTableName);

        UUID registryTableCheckpointStream = CorfuRuntime.getCheckpointStreamIdFromId(registryTableStreamId);
        UUID protobufDescriptorCheckpointStream = CorfuRuntime.getCheckpointStreamIdFromId(protobufDescriptorStreamId);

        // temporary ArrayLists to store the log entries in causal order before they are put into the ConcurrentMap
        ArrayList<SMREntry> registryTableEntries = new ArrayList<>();
        ArrayList<SMREntry> protobufDescriptorTableEntries = new ArrayList<>();

        // concurrent hashmaps registry table and protobufDescriptor tables for materialization
        ConcurrentMap cachedRegistryTable = new ConcurrentHashMap();
        ConcurrentMap cachedProtobufDescriptorTable = new ConcurrentHashMap();

        // tracking the highest globalAddress seen among the specified table's entries
        long rtEntryGlobalAddress = -1;
        long pdtEntryGlobalAddress = -1;

        for (File file : files) {
            try (FileChannel fileChannel = FileChannel.open(file.toPath())) {
                // set the file channel's position back to 0
                fileChannel.position(0);
                //long pos = fileChannel.size();

                // parse header
                LogFormat.LogHeader header = parseHeader(null, fileChannel, file.getAbsolutePath());
                //System.out.println(header);

                // iterate through the file
                // make sure that fileChannel.size() - fileChannel.position() > 14 to prevent
                // actualMetaDataSize < METADATA_SIZE, which would create an exception
                while (fileChannel.size() - fileChannel.position() > 14) {
                    //long channelOffset = fileChannel.position();

                    // parse metadata and entry
                    LogFormat.Metadata metadata = StreamLogFiles.parseMetadata(null, fileChannel, file.getAbsolutePath());
                    //System.out.println(entry);
                    LogEntry entry = StreamLogFiles.parseEntry(null, fileChannel, metadata, file.getAbsolutePath());
                    //System.out.println(entry);

                    //System.out.println(channelOffset);
                    if(metadata != null && entry != null) {
                        // convert the LogEntry to LogData to access getPayload
                        LogData data = StreamLogFiles.getLogData(entry);
                        //System.out.println(data.getData());
                        //Object modifiedData = data.getPayload(null);

                        // filter the data
                        // if it belongs to the CorfuSystem$RegistryTable or CorfuSystem$ProtobufDescriptorTable or its checkpoint streams process it
                        //                        if(data.containsStream(registryTableStreamId) || data.containsStream(protobufDescriptorStreamId)
                        //                                || data.containsStream(registryTableCheckpointStream) || data.containsStream(protobufDescriptorCheckpointStream)) {
                        if(data.containsStream(registryTableStreamId)
                                || data.containsStream(registryTableCheckpointStream)) {
                            // call get payload to decompress and deserialize data
                            if(data.getType() == DataType.DATA) {
                                Object modifiedData = data.getPayload(runtimeWithOnlyProtoSerializer);
                                //System.out.println(modifiedData);

                                if(modifiedData instanceof CheckpointEntry) {
                                    long snapshotAddress = Long.decode(((CheckpointEntry)modifiedData).getDict().get(CheckpointEntry.CheckpointDictKey.SNAPSHOT_ADDRESS));
                                    //System.out.println(snapshotAddress);

                                    MultiSMREntry smrEntries = ((CheckpointEntry) modifiedData).getSmrEntries(false, runtimeWithOnlyProtoSerializer);
                                    //System.out.println("SMR Entries: " + smrEntries);
                                    List<SMREntry> smrUpdates = null;
                                    if(smrEntries != null) {
                                        smrUpdates = smrEntries.getUpdates();
                                        //System.out.println("SMR Updates: " + smrUpdates);
                                        for (int i = 0; i < smrUpdates.size(); i++) {
                                            Object[] smrUpdateArg = smrUpdates.get(i).getSMRArguments();
                                            Object smrUpdateTable = smrUpdateArg[0];
                                            Object smrUpdateCorfuRecord = smrUpdateArg[1];

                                            CorfuStoreMetadata.TableName corfuRecordTableName = ((CorfuStoreMetadata.TableName) smrUpdateTable);
                                            //System.out.println(corfuRecordTableName);

                                            CorfuRecord corfuRecord = (CorfuRecord) smrUpdateCorfuRecord;
                                            //System.out.println(corfuRecord);

                                            cachedRegistryTable.put(corfuRecordTableName, corfuRecord);

                                            registryTableEntries.add(smrUpdates.get(i));
                                        }
                                    }
                                }
                            }

                            else if(data.getType() == DataType.HOLE) {
                                System.out.println("Hole found.");
                            }

                        }

                        if(data.containsStream(protobufDescriptorStreamId)
                                || data.containsStream(protobufDescriptorCheckpointStream)) {
                            // call get payload to decompress and deserialize data
                            if(data.getType() == DataType.DATA) {
                                Object modifiedData = data.getPayload(runtimeWithOnlyProtoSerializer);
                                //System.out.println(modifiedData);

                                if(modifiedData instanceof CheckpointEntry) {
                                    long snapshotAddress = Long.decode(((CheckpointEntry)modifiedData).getDict().get(CheckpointEntry.CheckpointDictKey.SNAPSHOT_ADDRESS));
                                    //System.out.println(snapshotAddress);

                                    MultiSMREntry smrEntries = ((CheckpointEntry) modifiedData).getSmrEntries(false, runtimeWithOnlyProtoSerializer);
                                    //System.out.println("SMR Entries: " + smrEntries);
                                    List<SMREntry> smrUpdates = null;
                                    if(smrEntries != null) {
                                        smrUpdates = smrEntries.getUpdates();
                                        //System.out.println("SMR Updates: " + smrUpdates);
                                        for (int i = 0; i < smrUpdates.size(); i++) {
                                            Object[] smrUpdateArg = smrUpdates.get(i).getSMRArguments();
                                            Object smrUpdateTable = smrUpdateArg[0];
                                            Object smrUpdateCorfuRecord = smrUpdateArg[1];

                                            CorfuStoreMetadata.ProtobufFileName corfuRecordTableName = ((CorfuStoreMetadata.ProtobufFileName) smrUpdateTable);
                                            //String corfuRecordTableName = ((CorfuStoreMetadata.ProtobufFileName) smrUpdateTable).getFileName();
                                            //System.out.println(corfuRecordTableName);

                                            CorfuRecord corfuRecord = (CorfuRecord) smrUpdateCorfuRecord;
                                            //System.out.println(corfuRecord);

                                            cachedProtobufDescriptorTable.put(corfuRecordTableName, corfuRecord);

                                            protobufDescriptorTableEntries.add(smrUpdates.get(i));
                                        }
                                    }

                                    /**
                                    List<SMREntry> smrUpdates = ((MultiObjectSMREntry) modifiedData).getSMRUpdates(protobufDescriptorCheckpointStream);
                                    //System.out.println("SMR Updates: " + smrUpdates);

                                    for (int i = 0; i < smrUpdates.size(); i++) {
                                        Object[] smrUpdateArg = smrUpdates.get(i).getSMRArguments();
                                        Object smrUpdateTable = smrUpdateArg[0];
                                        Object smrUpdateCorfuRecord = smrUpdateArg[1];

                                        String corfuRecordTableName = ((CorfuStoreMetadata.TableName) smrUpdateTable).getTableName();
                                        //System.out.println(corfuRecordTableName);

                                        CorfuRecord corfuRecord = (CorfuRecord) smrUpdateCorfuRecord;
                                        //System.out.println(corfuRecord);

                                        cachedProtobufDescriptorTable.put(corfuRecordTableName, corfuRecord);

                                        protobufDescriptorTableEntries.add(smrUpdates.get(i));
                                    }
                                    */
                                }
                            }

                            else if(data.getType() == DataType.HOLE) {
                                System.out.println("Hole found.");
                            }
                        }
                    }


                }
                System.out.println("Finished processing file: " + file.getAbsolutePath().toString());

            } catch (IOException e) {
                throw new IllegalStateException("Invalid header: " + file.getAbsolutePath(), e);
            }
        }

        //System.out.println(registryTableEntries);
        //System.out.println(protobufDescriptorTableEntries);

        //System.out.println(cachedRegistryTable);
        dynamicProtobufSerializer = new DynamicProtobufSerializer(cachedRegistryTable, cachedProtobufDescriptorTable);
        listTables("nsx");
        
        System.out.println("Finished analyzing log information.");
    }

    /**
    public void processEntryData(LogEntry entry) {
        // if the LogEntry object is inside of the CorfuSystem$RegistryTable
        // or CorfuSystem$ProtobufDescriptorTable
        // or its checkpoint streams, process it

        UUID registryTableCheckpointStream = CorfuRuntime.getCheckpointStreamIdFromId(registryTableStreamId);
        UUID protobufDescriptorCheckpointStream = CorfuRuntime.getCheckpointStreamIdFromId(protobufDescriptorStreamId);




        // The following 3 tables are what we care about in the browser for most operations
        String registryTableName = TableRegistry.getFullyQualifiedTableName(TableRegistry.CORFU_SYSTEM_NAMESPACE, TableRegistry.REGISTRY_TABLE_NAME);
        UUID registryTableStreamId = CorfuRuntime.getStreamID(registryTableName);

        String protobufDescriptorTableName = TableRegistry.getFullyQualifiedTableName(TableRegistry.CORFU_SYSTEM_NAMESPACE, TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME);
        UUID protobufDescriptorStreamId = CorfuRuntime.getStreamID(protobufDescriptorTableName);

        // Depending on the operation the following stream may not may not be given as input...
        String browsedTableName = TableRegistry.getFullyQualifiedTableName(givenNamespace, givenTableName);
        UUID browsedTableStreamId = CorfuRuntime.getStreamID(browsedTableName);
        UUID browsedTableCheckpointStreamId = CorfuRuntime.getCheckpointStreamIdFromId(browsedTableStreamId);
    }

     */

    @Override
    public EnumMap<IMetadata.LogUnitMetadataType, Object> printMetadataMap(long address) {
        return null;
    }

    @Override
    public CorfuTable<CorfuDynamicKey, CorfuDynamicRecord> getTable(String namespace, String tableName) {
        return null;
    }

    /**
     * List all tables in CorfuStore
     * @param namespace - the namespace where the table belongs
     * @return - number of tables in this namespace
     */
    @Override
    public int listTables(String namespace)
    {
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

    @Override
    public int printTableInfo(String namespace, String tablename) {
        return 0;
    }

    @Override
    public int printAllProtoDescriptors() {
        return 200;
    }

    @Override
    public int clearTable(String namespace, String tablename) {
        return 0;
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
class AgeComparator implements Comparator<CheckpointEntry> {
    @Override
    public int compare(CheckpointEntry a, CheckpointEntry b) {
        return a. < b.age ? -1 : a.age == b.age ? 0 : 1;
    }

    Comparator<LogEntry> compareByAddressOnly = (LogEntry r1, LogEntry r2) -> {
        long r1Order = r1 instanceOf CheckpointEntry ? r1.getSnapshotAddress(): r1.getGlobalAddress();
        long r2Order = r2 instanceOf CheckpointEntry ? r2.getSnapshotAddress(): r2.getGlobalAddress();
        if (r1Order == r2Order) return 0;
        if (r1Order > r2Order) return 1;
        return -1;
    };

Collections.sort(myArrayList, compareByAddressOnly);
}
*/
