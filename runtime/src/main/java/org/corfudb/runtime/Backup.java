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
import java.nio.file.Path;
import java.nio.file.Paths;
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
     * Discover and back up all tables, or tables with requires_backup_support tag
     *
     * @param filePath          - the filePath where the generated backup tar file will be placed
     * @param runtime           - the runtime which is performing the back up
     * @param taggedTablesOnly  - if true, back up tables which has requires_backup_support tag set;
     *                            if false, back up all UFO tables
     */
    public Backup(String filePath, CorfuRuntime runtime, boolean taggedTablesOnly) throws IOException {
        this.filePath = filePath;
        this.backupTempDirPath = Files.createTempDirectory(BACKUP_TEMP_DIR_PREFIX).toString();
        this.runtime = runtime;
        if (taggedTablesOnly) {
            this.streamIDs = getTaggedTables();
        } else {
            this.streamIDs = getAllTables();
        }
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

        long startTime = System.currentTimeMillis();
        Map<UUID, String> streamIdToTableNameMap = getStreamIdToTableNameMap();
        for (UUID streamId : streamIDs) {
            if (!tableExists(streamId)) {
                log.warn("cannot back up a non-existent table stream id {} table name {}",
                        streamId, streamIdToTableNameMap.get(streamId));
                continue;
            }

            // temporary backup file's name format: uuid.namespace$tableName
            Path filePath = Paths.get(backupTempDirPath)
                    .resolve(streamId + "." + streamIdToTableNameMap.get(streamId));
            backupTable(filePath, streamId);
        }
        long elapsedTime = System.currentTimeMillis() - startTime;

        log.info("successfully backed up {} tables to {} directory, elapsed time {}ms",
                streamIDs.size(), backupTempDirPath, elapsedTime);
    }

    /**
     * Back up a single table
     *
     * If the log is trimmed at timestamp, the backupTable will fail.
     * If the table has no data to be backed up, it will create a file with empty contents.
     *
     * @param filePath   - the path of the backup file
     * @param uuid       - the uuid of the table which is being backed up
     * @throws IOException
     */
    private void backupTable(Path filePath, UUID uuid) throws IOException {
        long startTime = System.currentTimeMillis();
        BackupTableStats backupTableStats;

        try (FileOutputStream fileOutput = new FileOutputStream(filePath.toString())) {
            StreamOptions options = StreamOptions.builder()
                    .ignoreTrimmed(false)
                    .cacheEntries(false)
                    .build();
            Stream<OpaqueEntry> stream = (new OpaqueStream(runtime.getStreamsView().get(uuid, options))).streamUpTo(timestamp);

            backupTableStats = writeTableToFile(fileOutput, stream, uuid);
        } catch (IOException e) {
            log.error("failed to back up table {} to file {}", uuid, filePath);
            throw e;
        } catch (TrimmedException e) {
            log.error("failed to back up tables as log was trimmed after back up starts.");
            throw e;
        }

        long elapsedTime = System.currentTimeMillis() - startTime;

        log.info("{} entries (size: {} bytes, elapsed time: {} ms) saved to temp file {}",
                backupTableStats.getNumOfEntries(), backupTableStats.getTableSize(), elapsedTime, filePath);
    }

    private BackupTableStats writeTableToFile(FileOutputStream fileOutput, Stream<OpaqueEntry> stream, UUID uuid) throws IOException {
        Iterator<OpaqueEntry> iterator = stream.iterator();
        int numOfEntries = 0;
        int tableSize = 0;
        while (iterator.hasNext()) {
            numOfEntries++;
            OpaqueEntry lastEntry = iterator.next();
            List<SMREntry> smrEntries = lastEntry.getEntries().get(uuid);
            if (smrEntries != null) {
                Map<UUID, List<SMREntry>> map = new HashMap<>();
                map.put(uuid, smrEntries);
                OpaqueEntry newOpaqueEntry = new OpaqueEntry(lastEntry.getVersion(), map);
                tableSize = OpaqueEntry.write(fileOutput, newOpaqueEntry);
            }
        }
        fileOutput.flush();
        return new BackupTableStats(numOfEntries, tableSize);
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
            // truncate file names if too long
            tarOutput.setLongFileMode(TarArchiveOutputStream.LONGFILE_TRUNCATE);
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
     * Get UUIDs of tables which have the requires_backup_support tag set
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
        log.info("{} tables need to be backed up.", tables.size());

        return tables;
    }

    /**
     * Get UUIDs of all registered UFO tables
     *
     * @return  List<UUID>   - a list of UUIDs for all registered UFO tables
     */
    private List<UUID> getAllTables() {
        return runtime.getTableRegistry().listTables().stream()
                .map(table -> CorfuRuntime.getStreamID(
                        TableRegistry.getFullyQualifiedTableName(table.getNamespace(), table.getTableName())))
                .collect(Collectors.toList());
    }

    /**
     * Get a map which maps stream ID to TableName
     *
     * @return  Map<UUID, String>   - a map: streamId -> fully qualified table name
     */
    private Map<UUID, String> getStreamIdToTableNameMap() {
        Map<UUID, String> streamIdToTableNameMap = new HashMap<>();
        runtime.getTableRegistry().listTables().forEach(tableName -> {
                String name = TableRegistry.getFullyQualifiedTableName(tableName);
                streamIdToTableNameMap.put(CorfuRuntime.getStreamID(name), name);
        });
        return streamIdToTableNameMap;
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

    private static class BackupTableStats {

        private final int numOfEntries;
        private final int tableSize;

        BackupTableStats(int numOfEntries, int tableSize) {
            this.numOfEntries = numOfEntries;
            this.tableSize = tableSize;
        }

        public int getNumOfEntries() {
            return numOfEntries;
        }

        public int getTableSize() {
            return tableSize;
        }
    }
}