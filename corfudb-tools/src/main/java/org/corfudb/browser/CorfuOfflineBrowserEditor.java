package org.corfudb.browser;

import org.apache.commons.io.FileUtils;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.log.LogFormat;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.serializer.DynamicProtobufSerializer;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static com.github.luben.zstd.Zstd.decompress;
import static org.corfudb.infrastructure.log.StreamLogFiles.parseHeader;
import static org.corfudb.infrastructure.log.StreamLogFiles.parseMetadata;
import static org.corfudb.infrastructure.log.StreamLogFiles.parseEntry;
import org.corfudb.infrastructure.log.LogFormat.LogEntry;


@SuppressWarnings("checkstyle:printLine")
public class CorfuOfflineBrowserEditor implements CorfuBrowserEditorCommands {
    private final Path logDir;
    private final DynamicProtobufSerializer dynamicProtobufSerializer;
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
        dynamicProtobufSerializer = new DynamicProtobufSerializer(null, null);

        // testing printAllProtoDescriptors
        System.out.println(printAllProtoDescriptors());
    }

    /**
     * Opens all log files one by one, accesses and prints log entry data for each Corfu log file.
     */
    public void printLogEntryData() {
        System.out.println("Printing log entry data information:");

        String[] extension = {"log"};
        File dir = logDir.toFile();
        Collection<File> files = FileUtils.listFiles(dir, extension, true);

        for (File file : files) {
            try (FileChannel fileChannel = FileChannel.open(file.toPath())) {
                while (fileChannel.size() - fileChannel.position() > 0) {
                    //long channelOffset = fileChannel.position();

                    // parse, print header
                    LogFormat.LogHeader header = parseHeader(null, fileChannel, file.getAbsolutePath());
                    System.out.println(header);

                    // parse metadata, then pass it in parse entry, and print entry
                    LogFormat.Metadata metadata = StreamLogFiles.parseMetadata(null, fileChannel, file.getAbsolutePath());
                    LogEntry entry = StreamLogFiles.parseEntry(null, fileChannel, metadata, file.getAbsolutePath());
                    System.out.println(entry);

                    // iterate over each stream in an entry and filter them
                    // if it belongs to the RegistryTable or ProtobufDescriptorTable
                    // filter them by printing
                    String registryTableName = TableRegistry.getFullyQualifiedTableName(TableRegistry.CORFU_SYSTEM_NAMESPACE, TableRegistry.REGISTRY_TABLE_NAME);
                    UUID registryTableStreamId = CorfuRuntime.getStreamID(registryTableName);

                    String protobufDescriptorTableName = TableRegistry.getFullyQualifiedTableName(TableRegistry.CORFU_SYSTEM_NAMESPACE, TableRegistry.PROTOBUF_DESCRIPTOR_TABLE_NAME);
                    UUID protobufDescriptorStreamId = CorfuRuntime.getStreamID(protobufDescriptorTableName);

                    UUID registryTableCheckpointStream = CorfuRuntime.getCheckpointStreamIdFromId(registryTableStreamId);
                    UUID protobufDescriptorCheckpointStream = CorfuRuntime.getCheckpointStreamIdFromId(protobufDescriptorStreamId);

                    if(entry != null) {
                        for(int i = 0; i < entry.getStreamsCount(); i ++) {
                            if(entry.getStreams(i).equals(registryTableStreamId) || entry.getStreams(i).equals(protobufDescriptorStreamId)
                            || entry.getStreams(i).equals(registryTableCheckpointStream) || entry.getStreams(i).equals(protobufDescriptorCheckpointStream)) {
                                // decompress the entry

                                // deserialize the entry
                                System.out.println(entry);
                            }
                        }
                    }



                }


            } catch (IOException e) {
                throw new IllegalStateException("Invalid header: " + file.getAbsolutePath(), e);
            }

        }
    }

    /*
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

    @Override
    public int printTable(String namespace, String tablename) {
        return 0;
    }

    @Override
    public int listTables(String namespace) {
        return 100;
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
