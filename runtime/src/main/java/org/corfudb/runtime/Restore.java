package org.corfudb.runtime;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.corfudb.protocols.logprotocol.MultiSMREntry;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.BackupRestoreException;
import org.corfudb.runtime.view.CacheOption;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.serializer.Serializers;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.corfudb.runtime.view.TableRegistry.getFullyQualifiedTableName;

/**
 * Provides Corfu native restore support. Works together with Backup.
 *
 * Restore all tables in the given backup .tar file which is generated by Corfu Backup.
 *
 * Steps:
 * 1. Open the given .tar file, obtain a set of backup files for tables
 * 2. Restore tables by committing transactions using the OpaqueEntries from table backup files
 */
@Slf4j
public class Restore {

    // The path of backup tar file
    private final String filePath;

    // The path of a temporary directory under which the unpacked table's backup files are stored
    private String restoreTempDirPath;

    // The filename of each table's backup file, format: uuid.namespace$tableName.
    private List<String> tableBackups;

    private CorfuRuntime rt;

    // The Corfu Store associated with the runtime
    private CorfuStore corfuStore;

    // Restore mode. Refer to class definition for details
    private RestoreMode restoreMode;

    // Cache the mapping from table uuid to requires_backup_support in RegistryTable
    private Map<UUID, Boolean> tableTagged = new HashMap<>();

    //
    private DistributedCheckpointerHelper cpHelper;

    // List of tables ignored during clean up and restore
    private static final List<String> IGNORED_CORFU_SYSTEM_TABLES = Arrays.asList(
            CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME,
            CompactorMetadataTables.COMPACTION_CONTROLS_TABLE
    );

    /**
     * Unpacked files from backup tar file are stored under RESTORE_TEMP_DIR. They are deleted after restore finishes.
     */
    private static final String RESTORE_TEMP_DIR_PREFIX = "corfu_restore_";

    /***
     * @param filePath      - the path of backup tar file
     * @param runtime       - the runtime which is performing the restore
     * @throws IOException when failed to create the temp directory
     */
    public Restore(String filePath, CorfuRuntime runtime, RestoreMode restoreMode) throws IOException {
        this.filePath = filePath;
        this.tableBackups = new ArrayList<>();
        this.rt = runtime;
        this.corfuStore = new CorfuStore(runtime);
        this.restoreMode = restoreMode;
    }

    /**
     * Start the restore process
     *
     * @throws IOException
     */
    public void start() throws IOException {
        log.info("started corfu restore");
        try {
            // The cleanup() in finally block is not guaranteed to have
            // been run in previous restore if there was OOM
            disableCompaction();
            cleanup();
            openTarFile();
            verify();
            restore();
        } catch (Exception e) {
            log.error("failed to run restore.", e);
            throw new BackupRestoreException("failed to restore from backup file " + filePath, e);
        } finally {
            enableCompaction();
            cleanup();
        }
        log.info("restore completed");
    }

    @VisibleForTesting
    protected void disableCompaction() throws Exception {
        log.info("Disabling compaction...");
        if (cpHelper == null) {
            try {
                cpHelper = new DistributedCheckpointerHelper(corfuStore);
            } catch (Exception e) {
                log.error("Failed to obtain a DistributedCheckpointerHelper.", e);
                throw e;
            }
        }

        cpHelper.disableCompactionWithWait();
    }

    private void enableCompaction() {
        log.info("Enabling compaction...");
        cpHelper.enableCompaction();
    }

    @VisibleForTesting
    protected void restore() throws IOException {
        if (restoreMode == RestoreMode.FULL) {
            clearAllExceptIgnoredTables();
        }

        long startTime = System.currentTimeMillis();
        for (String tableBackup : tableBackups) {
            log.info("start restoring table {}", tableBackup);
            String fullyQualifiedTableName = tableBackup.split("\\.")[1];
            String namespace = fullyQualifiedTableName.split("\\$")[0];
            String tableName = fullyQualifiedTableName.split("\\$")[1];
            if (namespace.equals(CORFU_SYSTEM_NAMESPACE) &&
                    IGNORED_CORFU_SYSTEM_TABLES.contains(tableName)) {
                log.info("Skip restoring table {} which is part of IGNORED_TABLES", tableBackup);
                continue;
            }
            if (restoreMode == RestoreMode.PARTIAL_TAGGED && !isTableTagged(tableBackup)) {
                log.info("skip restoring table {} since it doesn't have requires_backup_support tag", tableBackup);
                continue;
            }

            UUID streamId = UUID.fromString(tableBackup.split("\\.")[0]);
            try {
                Path tableBackupPath = Paths.get(restoreTempDirPath).resolve(tableBackup);
                restoreTable(tableBackupPath, streamId);
            } catch (IOException e) {
                log.error("failed to restore table {} from temp file {}", streamId, tableBackup);
                throw e;
            }
        }
        long elapsedTime = System.currentTimeMillis() - startTime;
        log.info("successfully restored {} tables to, elapsed time {}ms",
                tableBackups.size(), elapsedTime);
    }

    /**
     * Restore a single table
     *
     * @param filePath   - the path of the temp backup file
     * @param streamId   - the stream ID of the table which is to be restored
     * @throws IOException
     */
    private void restoreTable(Path filePath, UUID streamId) throws IOException {
        long startTime = System.currentTimeMillis();

        try (FileInputStream fileInput = new FileInputStream(filePath.toString())) {

            // Clear table before restore
            if (restoreMode == RestoreMode.PARTIAL || restoreMode == RestoreMode.PARTIAL_TAGGED) {
                SMREntry entry = new SMREntry("clear", new Array[0], Serializers.PRIMITIVE);
                nonCachedAppendSMREntries(streamId, entry);
            }

            StreamBatchWriter sbw = new StreamBatchWriter(rt.getParameters().getRestoreBatchSize(),
                    rt.getParameters().getMaxWriteSize(), streamId);
            while (fileInput.available() > 0) {
                OpaqueEntry opaqueEntry = OpaqueEntry.read(fileInput);
                List<SMREntry> smrEntries = opaqueEntry.getEntries().get(streamId);
                if (smrEntries == null || smrEntries.isEmpty()) {
                    continue;
                }

                sbw.batchWrite(smrEntries);
            }
            sbw.shutdown();

            long elapsedTime = System.currentTimeMillis() - startTime;

            log.info("completed restore of table {} with {} numEntries, total size {} byte(s), elapsed time {}ms",
                    streamId, sbw.getTotalNumSMREntries(), sbw.getTotalWriteSize(), elapsedTime);
        } catch (FileNotFoundException e) {
            log.error("restoreTable can not find file {}", filePath);
            throw e;
        }
    }

    /**
     * Check if the table has requires_backup_tag. Return true if it's RegistryTable.
     */
    private boolean isTableTagged(String tableBackup) {
        // tableBackup name format: uuid.namespace$tableName
        UUID uuid = UUID.fromString(tableBackup.substring(0, tableBackup.indexOf(".")));

        if (!tableTagged.isEmpty()) {
            // tableTagged is read from RegistryTable which should contain all tables
            Preconditions.checkState(tableTagged.containsKey(uuid));
            return tableTagged.get(uuid);
        }

        String[] strings = tableBackup.substring(tableBackup.indexOf(".")+1).split("\\$");
        if (Objects.equals(strings[0], TableRegistry.CORFU_SYSTEM_NAMESPACE) &&
                Objects.equals(strings[1], TableRegistry.REGISTRY_TABLE_NAME)) {
            return true;
        }

        // Populate tableTagged map
        // The reason we don't read the tag by getRegistryTable().get(tableName)
        // is that, the tableName extracted from tableBackup is not guaranteed
        // to be a full name. 'TarArchiveOutputStream.LONGFILE_TRUNCATE' is used
        // to generate the backup files which truncates the table names which are
        // too long. The uuid as the first part of the file name is always complete
        // since it has fixed length and is smaller than the truncate limit (100).
        try {
            rt.getTableRegistry()
                    .getRegistryTable()
                    .entryStream()
                    .sequential()
                    .forEach(entry -> {
                        String tableName = getFullyQualifiedTableName(entry.getKey().getNamespace(),
                                entry.getKey().getTableName());
                        UUID streamId = CorfuRuntime.getStreamID(tableName);
                        Boolean tagged = entry.getValue().getMetadata().hasTableOptions() &&
                                entry.getValue().getMetadata().getTableOptions().getRequiresBackupSupport();
                        log.info("table name is {}, uuid is {}, has backup restore tag {}", tableName, streamId, tagged);

                        tableTagged.put(streamId, tagged);
                    });
            log.info("finished caching backup tag information");
        } catch (Exception ex) {
            log.error("failed to populate the tableTagged map!", ex);
            throw ex;
        }

        return tableTagged.get(uuid);
    }

    /**
     * Open the backup tar file and save the table backups to tableDir directory
     */
    @VisibleForTesting
    protected void openTarFile() throws IOException {
        this.restoreTempDirPath = Files.createTempDirectory(RESTORE_TEMP_DIR_PREFIX).toString();
        try (FileInputStream fileInput = new FileInputStream(filePath);
             TarArchiveInputStream tarInput = new TarArchiveInputStream(fileInput)) {
            getTablesFromTarFile(tarInput);
        } catch (IOException e) {
            log.error("failed to get tables from tar file {}", filePath);
            throw e;
        }
    }

    private void getTablesFromTarFile(TarArchiveInputStream tarInput) throws IOException {
        int count;
        byte[] buf = new byte[1024];
        TarArchiveEntry entry;
        while ((entry = tarInput.getNextTarEntry()) != null) {
            tableBackups.add(entry.getName());

            String tablePath = restoreTempDirPath + File.separator + entry.getName();
            try (FileOutputStream fos = new FileOutputStream(tablePath)) {
                while ((count = tarInput.read(buf, 0, 1024)) != -1) {
                    fos.write(buf, 0, count);
                }
            }
        }

        // Move the RegistryTable to the beginning of the list
        String registryTableName = getFullyQualifiedTableName(
                TableRegistry.CORFU_SYSTEM_NAMESPACE, TableRegistry.REGISTRY_TABLE_NAME);
        UUID registryTableUUID = CorfuRuntime.getStreamID(registryTableName);
        // format: uuid.namespace$tableName
        String backupEntry = registryTableUUID + "." + registryTableName;
        int index = tableBackups.indexOf(backupEntry);
        if (index != -1) {
            Collections.swap(tableBackups, 0, index);
        }
    }

    /**
     * Some verification logic (TBD) such as
     * - Compare the user provided streamIds and names of table backups under tmp directory, or some metadata file
     * - Checksum
     */
    private void verify() {
        // TODO: verification logic to be implemented
    }

    /**
     * Delete all temp restore directories under the system temp directory.
     */
    private void cleanup() {
        File tmpdir = new File(System.getProperty("java.io.tmpdir"));
        File[] restoreDirs = tmpdir.listFiles(file -> file.getName().contains(RESTORE_TEMP_DIR_PREFIX));
        if (restoreDirs != null) {
            for (File file : restoreDirs) {
                try {
                    FileUtils.deleteDirectory(file);
                    log.info("removed temporary backup directory {}", file.getAbsolutePath());
                } catch (IOException e) {
                    log.error("failed to delete the temporary backup directory {}", file.getAbsolutePath());
                }
            }
        }
    }

    private void clearAllExceptIgnoredTables() {
        TxnContext txn = corfuStore.txn(TableRegistry.CORFU_SYSTEM_NAMESPACE);

        corfuStore.getRuntime().getTableRegistry().listTables().forEach(tableName -> {
            if (tableName.getNamespace().equals(CORFU_SYSTEM_NAMESPACE) && IGNORED_CORFU_SYSTEM_TABLES.contains(tableName.getTableName())) {
                return;
            }
            String name = getFullyQualifiedTableName(tableName);
            UUID streamId = CorfuRuntime.getStreamID(name);
            SMREntry entry = new SMREntry("clear", new Array[0], Serializers.PRIMITIVE);
            txn.logUpdate(streamId, entry);
        });
        txn.commit();
        log.info("Cleared all tables.");
    }

    private void nonCachedAppendSMREntries(UUID streamId, SMREntry... smrEntries) {
        MultiSMREntry multiSMREntry = new MultiSMREntry();
        multiSMREntry.addTo(Arrays.asList(smrEntries));
        rt.getStreamsView().append(multiSMREntry, null, CacheOption.WRITE_AROUND, streamId);
    }

    public enum RestoreMode {
        /*
         Restore all tables in the given backup file. Clean up ALL tables before restore.
         */
        FULL,

        /*
         Restore all tables in given backup file. Clean up ONLY tables that are to be restored.
         */
        PARTIAL,

        /*
         Restore tables which have requires_backup_support tag. Clean up ONLY tables that are to be restored.
         This mode supports restoring a subset of (tagged) tables from a full backup file.
         */
        PARTIAL_TAGGED
    }

    class StreamBatchWriter {

        private final int maxBatchSize;
        private final int maxWriteSize;
        private final UUID streamId;

        @Getter
        private int totalNumSMREntries = 0;
        @Getter
        private int totalWriteSize = 0;

        private final List<SMREntry> buffer = new ArrayList<>();
        private int bufferWriteSize = 0;

        StreamBatchWriter(int maxBatchSize, int maxWriteSize, UUID streamId) {
            this.maxBatchSize = maxBatchSize;
            this.maxWriteSize = maxWriteSize;
            this.streamId = streamId;
        }

        public void batchWrite(List<SMREntry> smrEntries) {
            if (smrEntries.isEmpty()) {
                return;
            }

            for (SMREntry smrEntry : smrEntries) {
                if (buffer.size() == maxBatchSize || bufferWriteSize + smrEntry.getSerializedSize() > maxWriteSize) {
                    flushBuffer();
                }
                buffer.add(smrEntry);
                bufferWriteSize += smrEntry.getSerializedSize();
            }
        }

        public void shutdown() {
            flushBuffer();
        }

        private void flushBuffer() {
            totalNumSMREntries += buffer.size();
            totalWriteSize += bufferWriteSize;

            nonCachedAppendSMREntries(streamId, buffer.toArray(new SMREntry[0]));

            bufferWriteSize = 0;
            buffer.clear();
        }

    }
}
