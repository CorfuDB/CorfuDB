package org.corfudb.runtime;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.io.FileUtils;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.exceptions.BackupRestoreException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.TableRegistry.FullyQualifiedTableName;
import org.corfudb.runtime.view.stream.OpaqueStream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Provides Corfu native backup support.
 * <p>
 * Backup a selective set of tables specified by stream id or UFO table option.
 * <p>
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
    private String backupTempDirPath;

    // The stream IDs of tables which are backed up
    private final List<UUID> streamsToBackUp = new ArrayList<>();

    // The snapshot address to back up
    private long timestamp;

    // The Corfu Runtime which is performing the backup
    private final CorfuRuntime runtime;

    // The tables belonging to this namespace will be backed up
    private final String namespace;

    // Whether to back up only tagged tables or all tables
    private final boolean taggedTables;

    /**
     * Backup files of tables are temporarily stored under BACKUP_TEMP_DIR. They are deleted after backup finishes.
     */
    private static final String BACKUP_TEMP_DIR_PREFIX = "corfu_backup_";

    /**
     * Discover and back up all tables
     *
     * @param filePath          - the filePath where the generated backup tar file will be placed
     * @param runtime           - the runtime which is performing the backup
     * @param taggedTables      - if true, back up only tagged tables; if false, back up all tables
     * @param namespace         - tables belonging to this namespace will be backed up; if null, back up all namespaces
     */
    public Backup(@Nonnull String filePath,
                  @Nonnull CorfuRuntime runtime,
                  boolean taggedTables,
                  @Nullable String namespace) {

        this.filePath = filePath;
        this.runtime = runtime;
        this.taggedTables = taggedTables;
        this.namespace = namespace;
    }

    /**
     * Start the backup process
     *
     * @throws IOException io exception
     */
    public void start() throws IOException {
        log.info("started corfu backup");

        streamsToBackUp.addAll(runtime.getTableRegistry()
                .getRegistryTable()
                .entryStream()
                .filter(this::filterTable)
                .map(table -> FullyQualifiedTableName.streamId(table.getKey()).getId())
                .collect(Collectors.toList()));

        if (streamsToBackUp.isEmpty()) {
            log.warn("back up is called with empty streamIDs!");
        }

        log.info("Preparing to back up {} tables: {}", streamsToBackUp.size(), streamsToBackUp);

        this.timestamp = runtime.getAddressSpaceView().getLogTail();

        try {
            // The cleanup() in finally block is not guaranteed to have
            // been run in previous backups if there was OOM
            cleanup();
            backup();
            generateTarFile();
        } catch (Exception e) {
            throw new BackupRestoreException("failed to backup tables " + streamsToBackUp, e);
        } finally {
            cleanup();
        }
        log.info("backup completed");
    }

    /**
     * A predicate checking if the given table should be backed up based on the given configuration
     *
     * @param table a given table entry from RegistryTable
     * @return if the table should be backed up
     */
    private boolean filterTable(Map.Entry<CorfuStoreMetadata.TableName,
            CorfuRecord<CorfuStoreMetadata.TableDescriptors, CorfuStoreMetadata.TableMetadata>> table) {
        return (namespace == null || table.getKey().getNamespace().equals(namespace)) &&
                (!taggedTables || table.getValue().getMetadata().getTableOptions().getRequiresBackupSupport());
    }

    /**
     * All temp backupTable files will be placed under BACKUP_DIR_PATH directory.
     *
     * @throws IOException io exception
     */
    private void backup() throws IOException {
        long startTime = System.currentTimeMillis();

        this.backupTempDirPath = Files.createTempDirectory(BACKUP_TEMP_DIR_PREFIX).toString();
        Map<UUID, String> streamIdToTableNameMap = getStreamIdToTableNameMap();
        for (UUID streamId : streamsToBackUp) {
            // temporary backup file's name format: uuid.namespace$tableName
            Path filePath = Paths.get(backupTempDirPath)
                    .resolve(streamId + "." + streamIdToTableNameMap.get(streamId));
            backupTable(filePath, streamId);
        }
        long elapsedTime = System.currentTimeMillis() - startTime;

        log.info("successfully backed up {} tables to {} directory, elapsed time {}ms",
                streamsToBackUp.size(), backupTempDirPath, elapsedTime);
    }

    /**
     * Back up a single table
     * <p>
     * If the log is trimmed at timestamp, the backupTable will fail.
     * If the table has no data to be backed up, it will create a file with empty contents.
     *
     * @param filePath   - the path of the backup file
     * @param uuid       - the uuid of the table which is being backed up
     * @throws IOException io exception
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

        log.info("{} SMREntry (size: {} bytes, elapsed time: {} ms) saved to temp file {}",
                backupTableStats.getNumOfEntries(), backupTableStats.getTableSize(), elapsedTime, filePath);
    }

    private BackupTableStats writeTableToFile(FileOutputStream fileOutput, Stream<OpaqueEntry> stream, UUID uuid) throws IOException {
        Iterator<OpaqueEntry> iterator = stream.iterator();
        int numOfEntries = 0;
        int tableSize = 0;
        while (iterator.hasNext()) {
            OpaqueEntry lastEntry = iterator.next();
            List<SMREntry> smrEntries = lastEntry.getEntries().get(uuid);
            if (smrEntries != null) {
                numOfEntries += smrEntries.size();
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
     * @throws IOException io exception
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
     * @throws IOException io exception
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
     * Get a map which maps stream ID to TableName
     *
     * @return  Map<UUID, String>   - a map: streamId -> fully qualified table name
     */
    private Map<UUID, String> getStreamIdToTableNameMap() {
        Map<UUID, String> streamIdToTableNameMap = new HashMap<>();
        runtime.getTableRegistry().listTables().forEach(tableName -> {
                FullyQualifiedTableName name = FullyQualifiedTableName.build(tableName);
                streamIdToTableNameMap.put(name.toStreamId().getId(), name.toFqdn());
        });
        return streamIdToTableNameMap;
    }

    /**
     * Delete all temp backup directories under the system temp directory.
     */
    private void cleanup() {
        File tmpdir = new File(System.getProperty("java.io.tmpdir"));
        File[] backupDirs = tmpdir.listFiles(file -> file.getName().contains(BACKUP_TEMP_DIR_PREFIX));
        if (backupDirs != null) {
            for (File file : backupDirs) {
                try {
                    FileUtils.deleteDirectory(file);
                    log.info("removed temporary backup directory {}", file.getAbsolutePath());
                } catch (IOException e) {
                    log.error("failed to delete the temporary backup directory {}", file.getAbsolutePath());
                }
            }
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