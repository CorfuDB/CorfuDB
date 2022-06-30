package org.corfudb.browser;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.io.FileUtils;
import org.corfudb.infrastructure.log.LogFormat;
import org.corfudb.infrastructure.log.LogMetadata;
import org.corfudb.infrastructure.log.StreamLogDataStore;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuDynamicRecord;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.infrastructure.log.LogFormat.LogHeader;
import org.corfudb.infrastructure.log.LogFormat.LogEntry;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

// i'm guessing that bc StreamLogFiles has this code in the same package, it can access it directly
import static org.corfudb.infrastructure.IServerRouter.log;
import static org.corfudb.infrastructure.log.StreamLogFiles.METADATA_SIZE;

@SuppressWarnings("checkstyle:printLine")
public class CorfuOfflineBrowserEditor implements CorfuBrowserEditorCommands {
    private final Path logDir;
    public CorfuOfflineBrowserEditor(String offlineDbDir) {
        logDir = Paths.get(offlineDbDir, "log");
        System.out.println("Analyzing database located at :"+logDir);

        // prints header information for each of the corfu log files
        printHeader();

        // System.out.println(listTables("CorfuSystem"));

        // testing printAllProtoDescriptors
        System.out.println(printAllProtoDescriptors());
    }

    /**
     * Opens all log files one by one, and prints the header information for each Corfu log file.
     */
    public void printHeader() {
        System.out.println("Printing header information:");

        String[] extension = {"log"};
        File dir = logDir.toFile();

        Collection<File> files = FileUtils.listFiles(dir, extension, true);

        for (File file : files) {
            LogFormat.LogHeader header;

            try (FileChannel fileChannel = FileChannel.open(file.toPath())) {
                header = parseHeader(fileChannel, file.getAbsolutePath());

                System.out.println(header);

            } catch (IOException e) {
                throw new IllegalStateException("Invalid header: " + file.getAbsolutePath(), e);
            }

        }
    }

    /**
     * Parse the logfile header, or create it, or recreate it if it was
     * partially written.
     *
     * @param channel file channel
     * @return log header
     * @throws IOException IO exception
     */
    public LogHeader parseHeader(FileChannel channel, String segmentFile) throws IOException {
        LogFormat.Metadata metadata = parseMetadata(channel, segmentFile);

        if (metadata == null) {
            // Partial write on the metadata for the header
            // Rewind the channel position to the beginning of the file
            channel.position(0);
            return null;
        }

        // print metadata info
        //System.out.println(metadata);

        ByteBuffer buffer = getPayloadForMetadata(channel, metadata);
        if (buffer == null) {
            // partial write on the header payload
            // Rewind the channel position to the beginning of the file
            channel.position(0);
            return null;
        }

        // print Stream LogEntry data
        //LogFormat.LogEntry entry;
        //entry = LogFormat.LogEntry.parseFrom(buffer.array());
        //System.out.println(entry);


        if (StreamLogFiles.Checksum.getChecksum(buffer.array()) != metadata.getPayloadChecksum()) {
            String errorMessage = getDataCorruptionErrorMessage("Invalid metadata checksum",
                    channel, segmentFile
            );
            throw new DataCorruptionException(errorMessage);
        }

        LogHeader header;

        try {
            header = LogHeader.parseFrom(buffer.array());
        } catch (InvalidProtocolBufferException e) {
            String errorMessage = getDataCorruptionErrorMessage("Invalid header",
                    channel, segmentFile
            );
            throw new DataCorruptionException(errorMessage, e);
        }

        return header;
    }

    /**
     * Read a payload given metadata.
     *
     * @param fileChannel channel to read the payload from
     * @param metadata    the metadata that is written before the payload
     * @return ByteBuffer for the payload
     * @throws IOException IO exception
     */
    public ByteBuffer getPayloadForMetadata(FileChannel fileChannel, LogFormat.Metadata metadata) throws IOException {
        if (fileChannel.size() - fileChannel.position() < metadata.getLength()) {
            return null;
        }

        ByteBuffer buf = ByteBuffer.allocate(metadata.getLength());
        fileChannel.read(buf);
        buf.flip();
        return buf;
    }

    /**
     * Parse the metadata field. This method should only be called
     * when a metadata field is expected.
     *
     * @param fileChannel the channel to read from
     * @return metadata field of null if it was partially written.
     * @throws IOException IO exception
     */
    public LogFormat.Metadata parseMetadata(FileChannel fileChannel, String segmentFile) throws IOException {
        long actualMetaDataSize = fileChannel.size() - fileChannel.position();
        if (actualMetaDataSize < METADATA_SIZE) {
            log.warn("Metadata has wrong size. Actual size: {}, expected: {}",
                    actualMetaDataSize, METADATA_SIZE
            );
            return null;
        }

        ByteBuffer buf = ByteBuffer.allocate(METADATA_SIZE);
        fileChannel.read(buf);
        buf.flip();

        LogFormat.Metadata metadata;

        try {
            metadata = LogFormat.Metadata.parseFrom(buf.array());
        } catch (InvalidProtocolBufferException e) {
            String errorMessage = getDataCorruptionErrorMessage("Can't parse metadata",
                    fileChannel, segmentFile
            );
            throw new DataCorruptionException(errorMessage, e);
        }

        if (metadata.getLengthChecksum() != StreamLogFiles.Checksum.getChecksum(metadata.getLength())) {
            String errorMessage = getDataCorruptionErrorMessage("Metadata: invalid length checksum",
                    fileChannel, segmentFile
            );
            throw new DataCorruptionException(errorMessage);
        }

        return metadata;
    }
    private LogMetadata logMetadata;

    public String getDataCorruptionErrorMessage(
            String message, FileChannel fileChannel, String segmentFile) throws IOException {
        return message +
                ". Segment File: " + segmentFile +
                ". File size: " + fileChannel.size() +
                ". File position: " + fileChannel.position() +
                ". Global tail: " + logMetadata.getGlobalTail() +

                // left this out, what is the importance of a tail segment?
                // ". Tail segment: " + dataStore.getTailSegment() +

                ". Stream tails size: " + logMetadata.getStreamTails().size();
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
