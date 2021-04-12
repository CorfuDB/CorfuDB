package org.corfudb.runtime;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.FileUtils;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.stream.OpaqueStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Provides Corfu native backup support.
 *
 * Backup a selective set of tables specified by stream id or UFO table option.
 *
 * Steps:
 * 1. Open the selective set of tables as OpaqueStreams
 * 2. Write each table to individual temporary file
 * 3. Merge temporary backup files into a single .tar file under a user-given path
 */
@Slf4j
public class Backup {

    // The path of backup tar file
    private final String filePath;

    // The path of a temporary directory under which table's backup files are stored
    private final String backupTempDirPath;

    // The stream IDs of tables which are backed up
    private final List<UUID> streamIDs;

    // The snapshot address to back up
    private long timestamp;

    // The Corfu Runtime which is performing the back up
    private final CorfuRuntime runtime;

    // All tables in Corfu Db
    private List<UUID> allTablesInDb;

    /**
     * Backup files of tables are temporarily stored under BACKUP_TEMP_DIR. They are deleted after backup finishes.
     */
    private static final String BACKUP_TEMP_DIR_PREFIX = "corfu_backup_";

    /**
     * @param filePath      - the filePath where the generated backup tar file will be placed
     * @param streamIDs     - the stream IDs of tables which are backed up
     * @param runtime       - the runtime which is performing the back up
     * @throws IOException  - when failed to create the temp directory
     */
    public Backup(String filePath, List<UUID> streamIDs, CorfuRuntime runtime) throws IOException {
        this.filePath = filePath;
        this.backupTempDirPath = Files.createTempDirectory(BACKUP_TEMP_DIR_PREFIX).toString();
        this.streamIDs = streamIDs;
        this.runtime = runtime;
    }

    /**
     * The difference from the constructor above is that this one will automatically discover
     * tables which require back up from their tags.
     *
     * @param filePath      - the filePath where the generated backup tar file will be placed
     * @param runtime       - the runtime which is performing the back up
     * @throws IOException  - when failed to create the temp directory
     */
    public Backup(String filePath, CorfuRuntime runtime) throws IOException {
        this.filePath = filePath;
        this.backupTempDirPath = Files.createTempDirectory(BACKUP_TEMP_DIR_PREFIX).toString();
        this.runtime = runtime;
        this.streamIDs = getTaggedTables();
    }

    /**
     * Start the backup process
     *
     * @throws IOException
     */
    public void start() throws IOException {
        if (streamIDs == null) {
            log.warn("streamIDs is a null variable! back up aborted!");
            return;
        }

        this.timestamp = runtime.getAddressSpaceView().getLogTail();

        try {
            backup();
            generateTarFile();
        } catch (IOException e) {
            log.error("failed to backup tables: {}", streamIDs);
            throw e;
        } finally {
            cleanup();
        }
        log.info("backup completed");
    }

    /**
     * Check if table exists in Corfu Db
     *
     * @param streamId   the stream id of the table which is being checked
     * @return           true if table exists
     */
    private boolean tableExists(UUID streamId) {
        if (allTablesInDb == null) {
            allTablesInDb = runtime.getTableRegistry().listTables()
                    .stream()
                    .map(tableName -> CorfuRuntime.getStreamID(TableRegistry.getFullyQualifiedTableName(tableName)))
                    .collect(Collectors.toList());
        }
        return allTablesInDb.contains(streamId);
    }

    /**
     * All temp backupTable files will be placed under BACKUP_DIR_PATH directory.
     *
     * @throws IOException
     */
    private void backup() throws IOException {
        if (streamIDs.isEmpty()) {
            log.warn("back up is called with empty streamIDs!");
            return;
        }

        for (UUID streamId : streamIDs) {
            if (!tableExists(streamId)) {
                log.warn("cannot back up a non-existent table: {}", streamId);
                continue;
            }
            String fileName = backupTempDirPath + File.separator + streamId;
            backupTable(fileName, streamId);
        }
        log.info("successfully backed up {} tables to {} directory", streamIDs.size(), backupTempDirPath);
    }

    /**
     * Back up a single table
     *
     * If the log is trimmed at timestamp, the backupTable will fail.
     * If the table has no data to be backed up, it will create a file with empty contents.
     *
     * @param fileName   - the name of the backup file
     * @param uuid       - the uuid of the table which is being backed up
     * @throws IOException
     */
    private void backupTable(String fileName, UUID uuid) throws IOException {
        try (FileOutputStream fileOutput = new FileOutputStream(fileName)) {
            StreamOptions options = StreamOptions.builder()
                    .ignoreTrimmed(false)
                    .cacheEntries(false)
                    .build();
            Stream<OpaqueEntry> stream = (new OpaqueStream(runtime.getStreamsView().get(uuid, options))).streamUpTo(timestamp);

            writeTableToFile(fileOutput, stream, uuid);
        } catch (IOException e) {
            log.error("failed to back up table {} to file {}", uuid, fileName);
            throw e;
        } catch (TrimmedException e) {
            log.error("failed to back up tables as log was trimmed after back up starts.");
            throw e;
        }

        log.info("{} table is backed up and stored to temp file {}", uuid, fileName);
    }

    private void writeTableToFile(FileOutputStream fileOutput, Stream<OpaqueEntry> stream, UUID uuid) throws IOException {
        Iterator<OpaqueEntry> iterator = stream.iterator();
        while (iterator.hasNext()) {
            OpaqueEntry lastEntry = iterator.next();
            List<SMREntry> smrEntries = lastEntry.getEntries().get(uuid);
            if (smrEntries != null) {
                Map<UUID, List<SMREntry>> map = new HashMap<>();
                map.put(uuid, smrEntries);
                OpaqueEntry newOpaqueEntry = new OpaqueEntry(lastEntry.getVersion(), map);
                OpaqueEntry.write(fileOutput, newOpaqueEntry);
            }
        }
        fileOutput.flush();
    }

    /**
     * All generated files under tmp directory will be composed into one tar file
     *
     * @throws IOException
     */
    private void generateTarFile() throws IOException {
        File folder = new File(backupTempDirPath);
        File[] srcFiles = folder.listFiles();
        if (srcFiles == null) {
            log.debug("no backup file found under directory {}", backupTempDirPath);
            return;
        }

        try (FileOutputStream fileOutput = new FileOutputStream(filePath);
             TarArchiveOutputStream tarOutput = new TarArchiveOutputStream(fileOutput)) {
            for (File srcFile : srcFiles) {
                addToTarFile(srcFile, tarOutput);
            }
        } catch (IOException e) {
            log.error("failed to generate a backup tar file {}", filePath);
            throw e;
        }

        log.info("backup tar file is generated at {}", filePath);
    }

    /**
     * Add the table backup file to the backup tar file which contains all tables
     *
     * @param tableFile    - the table backup file
     * @param tarOutput    - the backup tar file which contains all tables
     * @throws IOException
     */
    private void addToTarFile(File tableFile,TarArchiveOutputStream tarOutput) throws IOException {
        try (FileInputStream fileInput = new FileInputStream(tableFile)) {
            TarArchiveEntry tarEntry = new TarArchiveEntry(tableFile);
            tarEntry.setName(tableFile.getName());
            tarOutput.putArchiveEntry(tarEntry);

            int count;
            byte[] buf = new byte[1024];
            while ((count = fileInput.read(buf, 0, 1024)) != -1) {
                tarOutput.write(buf, 0, count);
            }
            tarOutput.closeArchiveEntry();
        } catch (IOException e) {
            log.error("failed to add table backup file {} to tar file", tableFile.getName());
            throw e;
        }
    }

    /**
     * Find tables which have the requires_backup_support tag set
     *
     * @return  List<UUID>   - a list of UUIDs for all the tables which require backup support
     */
    private List<UUID> getTaggedTables() {
        TableRegistry tableRegistry = runtime.getTableRegistry();
        List<UUID> tables = tableRegistry
                .listTables()
                .stream()
                .filter(tableName -> tableRegistry
                        .getRegistryTable()
                        .get(tableName)
                        .getMetadata()
                        .getTableOptions()
                        .getRequiresBackupSupport())
                .map(tableName -> CorfuRuntime
                        .getStreamID(TableRegistry.getFullyQualifiedTableName(tableName)))
                .collect(Collectors.toList());
        log.info("{} tables need to be backed up: {}", tables.size(), tables);

        return tables;
    }

    /**
     * Cleanup the table backup files under the backupDir directory.
     */
    private void cleanup() {
        try {
            FileUtils.deleteDirectory(new File(backupTempDirPath));
        } catch (IOException e) {
            log.error("failed to clean up the temporary backup directory {}", backupTempDirPath);
        }
    }
}
