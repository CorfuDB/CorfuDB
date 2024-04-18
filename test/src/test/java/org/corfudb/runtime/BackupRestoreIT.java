package org.corfudb.runtime;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.lang.RandomStringUtils;
import org.corfudb.integration.AbstractIT;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.exceptions.BackupRestoreException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.proto.RpcCommon;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.TableRegistry.FullyQualifiedTableName;
import org.corfudb.test.SampleSchema;
import org.corfudb.test.SampleSchema.Uuid;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.runtime.CompactorMetadataTables.COMPACTION_CONTROLS_TABLE;
import static org.corfudb.runtime.CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.corfudb.runtime.view.TableRegistry.FQ_PROTO_DESC_TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertThrows;


/**
 * Test the Corfu native backup and restore functionalities. Overall the tests bring up two CorfuServers,
 * one used as source server which is backed up, and the other as destination server which restores the data
 * using the backup file generated from the source server.
 */
@Slf4j
public class BackupRestoreIT extends AbstractIT {

    public static final int numEntries = 123;
    public static final int valSize = 2000;
    public static final int numTables = 10;
    private static final String NAMESPACE = "test_namespace";
    private static final String ANOTHER_NAMESPACE = "another_test_namespace";
    private static final String EMPTY_NAMESPACE = "";
    private static final String backupTable = "test_table";
    private static final int longNameRepeat = 15;

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 9000;
    private static final int WRITER_PORT = DEFAULT_PORT + 1;
    private static final String SOURCE_ENDPOINT = DEFAULT_HOST + ":" + DEFAULT_PORT;
    private static final String DESTINATION_ENDPOINT = DEFAULT_HOST + ":" + WRITER_PORT;
    private static final String BACKUP_TEMP_DIR_PREFIX = "corfu_backup_";
    private static final String RESTORE_TEMP_DIR_PREFIX = "corfu_restore_";

    // Log path of source server
    static final private String LOG_PATH1 = getCorfuServerLogPath(DEFAULT_HOST, DEFAULT_PORT);

    // Location where the backup tar file is stored
     static final private String BACKUP_TAR_FILE_PATH = new File(LOG_PATH1).getParent() + File.separator + "backup.tar";

    // Connect to sourceServer to generate data
    private CorfuRuntime srcDataRuntime = null;

    // Connect to sourceServer to backup data
    private CorfuRuntime backupRuntime = null;

    // Connect to destinationServer to restore data
    private CorfuRuntime restoreRuntime = null;

    // Connect to destinationServer to verify data
    private CorfuRuntime destDataRuntime = null;

    private SampleSchema.Uuid uuidKey = null;

    /**
     * Setup Test Environment
     *
     * - Two independent Corfu Servers (source and destination)
     * - Four Corfu Runtimes connected to Corfu Servers
     */
    private void setupEnv() throws IOException {
        // Source Corfu Server (data will be written to this server)
        new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(DEFAULT_PORT)
                .setSingle(true)
                .runServer();

        // Destination Corfu Server (data will be replicated into this server)
        new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(WRITER_PORT)
                .setSingle(true)
                .runServer();

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();

        srcDataRuntime = CorfuRuntime
                .fromParameters(params)
                .parseConfigurationString(SOURCE_ENDPOINT)
                .connect();

        backupRuntime = CorfuRuntime
                .fromParameters(params)
                .parseConfigurationString(SOURCE_ENDPOINT)
                .connect();

        restoreRuntime = CorfuRuntime
                .fromParameters(params)
                .parseConfigurationString(DESTINATION_ENDPOINT)
                .connect();

        destDataRuntime = CorfuRuntime
                .fromParameters(params)
                .parseConfigurationString(DESTINATION_ENDPOINT)
                .connect();
    }

    /**
     * Shutdown all Corfu Runtimes
     */
    private void cleanEnv() {
        if (srcDataRuntime != null)
            srcDataRuntime.shutdown();

        if (backupRuntime != null)
            backupRuntime.shutdown();

        if (restoreRuntime != null)
            restoreRuntime.shutdown();

        if (destDataRuntime != null)
            destDataRuntime.shutdown();
    }

    /**
     * Generate a list of tableNames
     *
     * @param numTables     the number of table name to generate
     * @return tableNames   a list of String representing table names
     */
    private List<String> getTableNames(int numTables) {
        List<String> tableNames = new ArrayList<>();
        for (int i = 0; i < numTables; i++) {
            tableNames.add(backupTable + "_" + i);
        }
        return tableNames;
    }

    private List<String> getLongTableNames(int numTables) {
        StringBuilder longPrefix = new StringBuilder();
        // generate a 150-character long prefix
        for (int i = 0; i < longNameRepeat; i++) {
            longPrefix.append(backupTable);
        }

        List<String> tableNames = new ArrayList<>();
        for (int i = 0; i < numTables; i++) {
            tableNames.add(longPrefix + "_" + i);
        }
        return tableNames;
    }

    /**
     * Open a simple table using the tableName on the given Corfu Store.
     * - Key type is Uuid
     * - Value type is EventInfo
     * - Metadata type is UUid
     *
     * @param corfuStore    the Corfu Store at which new table is opened
     * @param tableName     the name of table to open
     */
    private Table<Uuid, SampleSchema.EventInfo, Uuid>
    openTableWithoutBackupTag(CorfuStore corfuStore, String namespace, String tableName) throws Exception {
        return corfuStore.openTable(namespace,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.EventInfo.class,
                SampleSchema.Uuid.class,
                TableOptions.fromProtoSchema(SampleSchema.EventInfo.class));
    }

    /**
     * Open a simple table using the tableName on the given Corfu Store.
     * Payload 'SampleTableAMsg' has requires_backup_support set to true.
     * - Key type is Uuid
     * - Value type is SampleTableAMsg
     * - Metadata type is UUid
     *
     * @param corfuStore    the Corfu Store at which new table is opened
     * @param tableName     the name of table to open
     */
    private Table<SampleSchema.Uuid, SampleSchema.SampleTableAMsg, SampleSchema.Uuid>
    openTableWithBackupTag(CorfuStore corfuStore, String namespace, String tableName) throws Exception {
        return corfuStore.openTable(namespace,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.SampleTableAMsg.class,
                SampleSchema.Uuid.class,
                TableOptions.fromProtoSchema(SampleSchema.SampleTableAMsg.class));
    }

    /**
     * Generate random EventInfo entries and save into the given Corfu DataStore.
     *
     * @param dataStore     the data store used
     * @param tableName     the table which generated entries are added to
     * */
    private void generateData(CorfuStore dataStore, String namespace, String tableName, boolean hasBackupTag) throws Exception {
        Table table = hasBackupTag ? openTableWithBackupTag(dataStore, namespace, tableName) :
                openTableWithoutBackupTag(dataStore, namespace, tableName);

        TxnContext txn = dataStore.txn(namespace);
        for (int i = 0; i < numEntries; i++) {
            uuidKey = SampleSchema.Uuid.newBuilder()
                    .setMsb(i)
                    .setLsb(i)
                    .build();

            String name = RandomStringUtils.random(valSize, true, true);
            if (hasBackupTag) {
                SampleSchema.SampleTableAMsg payload = SampleSchema.SampleTableAMsg.newBuilder().setPayload(name).build();
                txn.putRecord(table, uuidKey, payload, uuidKey);
            } else {
                SampleSchema.EventInfo eventInfo = SampleSchema.EventInfo.newBuilder().setName(name).build();
                txn.putRecord(table, uuidKey, eventInfo, uuidKey);
            }
        }
        txn.commit();
    }

    /**
     * Compare the entries inside two CorfuStore tables
     *
     * @param corfuStore1   the first corfuStore
     * @param tableName1    the table which is compared in the first corfuStore
     * @param corfuStore2   the second corfuStore
     * @param tableName2    the table in the second corfuStore which is compared with tableName1
     */
    private void compareCorfuStoreTables(CorfuStore corfuStore1, String namespace,
                                         String tableName1, CorfuStore corfuStore2, String tableName2) {
        TxnContext aTxn = corfuStore1.txn(namespace);
        List<CorfuStoreEntry<Uuid, SampleSchema.SampleTableAMsg, Uuid>> aValueSet = new ArrayList<>();
        for (int i = 0; i < numEntries; i++) {
            uuidKey = SampleSchema.Uuid.newBuilder()
                    .setMsb(i)
                    .setLsb(i)
                    .build();
            aValueSet.add(aTxn.getRecord(tableName1, uuidKey));
        }
        aTxn.close();

        TxnContext bTxn = corfuStore2.txn(namespace);
        List<CorfuStoreEntry<Uuid, SampleSchema.SampleTableAMsg, Uuid>> bValueSet = new ArrayList<>();
        for (int i = 0; i < numEntries; i++) {
            uuidKey = SampleSchema.Uuid.newBuilder()
                    .setMsb(i)
                    .setLsb(i)
                    .build();
            bValueSet.add(bTxn.getRecord(tableName2, uuidKey));
        }
        bTxn.close();

        // Check if values are the same
        for (int i = 0; i < numEntries; i++) {
            assertThat(aValueSet.get(i)).isEqualTo(bValueSet.get(i));
        }
    }

    /**
     * Get a list of stream Ids from the backup tar file
     *
     * @param tarFile     the path of the backup tar file
     * @return            stream Ids whose corresponding tables are included in the tar file
     */
    private List<UUID> getStreamIdsFromTarFile(String tarFile) throws IOException {
        List<UUID> streamIDs = new ArrayList<>();
        FileInputStream fileInput = new FileInputStream(tarFile);
        TarArchiveInputStream tarInput = new TarArchiveInputStream(fileInput);
        TarArchiveEntry entry;
        while ((entry = tarInput.getNextTarEntry()) != null) {
            String streamId = entry.getName().substring(0, entry.getName().indexOf("."));
            streamIDs.add(UUID.fromString(streamId));
        }
        return streamIDs;
    }

    /**
     * An end-to-end Backup and Restore test for multiple tables specified by stream ids
     *
     * 1. Open multiple tables and generate random entries.
     * 2. Backup a list of tables and obtain a tar file.
     * 3. Use the tar file to restore tables.
     * 4. Compare the table contents before and after the backup/restore.
     */
    @Test
    public void backupRestoreMultipleTablesTest() throws Exception {

        // Set up the test environment
        setupEnv();

        // Create Corfu Store to add entries into server
        CorfuStore srcDataCorfuStore = new CorfuStore(srcDataRuntime);
        CorfuStore destDataCorfuStore = new CorfuStore(destDataRuntime);

        List<String> tableNames = getTableNames(numTables);

        // Generate random entries and save into sourceServer
        for (String tableName : tableNames) {
            generateData(srcDataCorfuStore, NAMESPACE, tableName, false);
        }

        // Obtain the corresponding streamIDs for the tables in sourceServer
        List<UUID> streamIDs = new ArrayList<>();
        for (String tableName : tableNames) {
            streamIDs.add(FullyQualifiedTableName.streamId(NAMESPACE, tableName).getId());
        }

        // Backup
        Backup backup = new Backup(BACKUP_TAR_FILE_PATH, backupRuntime, false, null);
        backup.start();

        // Verify that backup tar file exists
        File backupTarFile = new File(BACKUP_TAR_FILE_PATH);
        assertThat(backupTarFile).exists();

        // Add pre-existing data into restore server
        // Restore should clean up the pre-existing data before actual restoring
        for (String tableName : tableNames) {
            generateData(destDataCorfuStore, NAMESPACE, tableName, true);
        }
        long preRestoreEntryCnt = destDataCorfuStore.getRuntime().getStreamsView().get(streamIDs.get(0)).stream().count();

        // Restore using backup files
        Restore restore = new Restore(BACKUP_TAR_FILE_PATH, restoreRuntime, Restore.RestoreMode.PARTIAL, null);
        restore.start();

        // Compare data entries in CorfuStore before and after the Backup/Restore
        for (String tableName : tableNames) {
            openTableWithoutBackupTag(destDataCorfuStore, NAMESPACE, tableName);
            compareCorfuStoreTables(srcDataCorfuStore, NAMESPACE, tableName, destDataCorfuStore, tableName);
        }

        long postRestoreEntryCnt = destDataCorfuStore.getRuntime().getStreamsView().get(streamIDs.get(0)).stream().count();
        // New updates are 1 (clear) + N (batched restore writes)
        assertThat(postRestoreEntryCnt - preRestoreEntryCnt - 1).isEqualTo(
                (long)Math.ceil((1.0 * numEntries) / destDataCorfuStore.getRuntime().getParameters().getRestoreBatchSize()));

        // Close servers and runtime before exiting
        cleanEnv();
    }

    /**
     * An end-to-end Backup and Restore test for multiple tables which have requires_backup_support tag
     */
    @Test
    public void backupRestoreTaggedTablesTest() throws Exception {

        // Set up the test environment
        setupEnv();

        // Create Corfu Store to add entries into server
        CorfuStore srcDataCorfuStore = new CorfuStore(srcDataRuntime);
        CorfuStore destDataCorfuStore = new CorfuStore(destDataRuntime);

        List<String> tableNames = getTableNames(numTables);

        // Generate random entries and save into sourceServer
        for (String tableName : tableNames) {
            generateData(srcDataCorfuStore, NAMESPACE, tableName, true);
        }

        // Backup
        Backup backup = new Backup(BACKUP_TAR_FILE_PATH, backupRuntime, true, null);
        backup.start();

        // Verify that backup tar file exists
        File backupTarFile = new File(BACKUP_TAR_FILE_PATH);
        assertThat(backupTarFile).exists();

        // Restore using backup files
        Restore restore = new Restore(BACKUP_TAR_FILE_PATH, restoreRuntime, Restore.RestoreMode.PARTIAL, null);
        restore.start();

        // Compare data entries in CorfuStore before and after the Backup/Restore
        for (String tableName : tableNames) {
            openTableWithBackupTag(destDataCorfuStore, NAMESPACE, tableName);
            compareCorfuStoreTables(srcDataCorfuStore, NAMESPACE, tableName, destDataCorfuStore, tableName);
        }

        // Close servers and runtime before exiting
        cleanEnv();
    }

    /**
     * Test if only tables with requires_backup_support tag are backed up
     */
    @Test
    public void backupTablesSelectedByTagsTest() throws Exception {

        // Set up the test environment
        setupEnv();

        // Create Corfu Store to add entries into server
        CorfuStore srcDataCorfuStore = new CorfuStore(srcDataRuntime);

        List<String> tableNames = getTableNames(numTables);

        // Generate random entries and save into sourceServer
        // Set half of the tables with backup tag, and the other half without backup tag
        int i = 0;
        for (String tableName : tableNames) {
            if (i++ % 2 == 0) {
                // set requires_backup_support
                generateData(srcDataCorfuStore, NAMESPACE, tableName, true);
            } else {
                generateData(srcDataCorfuStore, NAMESPACE, tableName, false);
            }
        }

        // Obtain the corresponding streamIDs for the tables in sourceServer
        List<UUID> streamIDs = new ArrayList<>();
        for (String tableName : tableNames) {
            streamIDs.add(FullyQualifiedTableName.streamId(NAMESPACE, tableName).getId());
        }

        // Backup
        Backup backup = new Backup(BACKUP_TAR_FILE_PATH, backupRuntime, true, null);
        backup.start();

        // Verify that backup tar file exists
        File backupTarFile = new File(BACKUP_TAR_FILE_PATH);
        assertThat(backupTarFile).exists();

        // Verify that only a selective set of tables are backed up
        List<UUID> backupStreamIDs = getStreamIdsFromTarFile(BACKUP_TAR_FILE_PATH);
        for (i = 0; i < streamIDs.size(); i++) {
            if (i % 2 == 0) {
                // tables with requires_backup_support tag
                assertThat(streamIDs.get(i)).isIn(backupStreamIDs);
            } else {
                // tables without requires_backup_support tag
                assertThat(streamIDs.get(i)).isNotIn(backupStreamIDs);
            }
        }

        // Close servers and runtime before exiting
        cleanEnv();
    }

    /**
     * Test if only tables with correct namespace are backed up
     */
    @Test
    public void backupTablesSelectedByNamespaceTest() throws Exception {

        // Set up the test environment
        setupEnv();

        // Create Corfu Store to add entries into server
        CorfuStore srcDataCorfuStore = new CorfuStore(srcDataRuntime);

        List<String> tableNames = getTableNames(numTables);
        List<UUID> streamIDs = new ArrayList<>();

        // Generate random entries and save into sourceServer.
        // 1/3 of the tables belong to NAMESPACE,
        // another 1/3 belong to ANOTHER_NAMESPACE,
        // and the last 1/3 belong to EMPTY_NAMESPACE.
        int i = 0;
        for (String tableName : tableNames) {
            String namespace;
            if (i % 3 == 0) {
                namespace = NAMESPACE;
            } else if (i % 3 == 1) {
                namespace = ANOTHER_NAMESPACE;
            } else {
                namespace = EMPTY_NAMESPACE;
            }
            i++;

            generateData(srcDataCorfuStore, namespace, tableName, false);
            streamIDs.add(FullyQualifiedTableName.streamId(namespace, tableName).getId());
        }

        // Back up tables belonging to NAMESPACE, regardless of the tag
        Backup backup = new Backup(BACKUP_TAR_FILE_PATH, backupRuntime, false, NAMESPACE);
        backup.start();

        // Verify that only NAMESPACE tables are backed up
        List<UUID> backupStreamIDs = getStreamIdsFromTarFile(BACKUP_TAR_FILE_PATH);
        for (i = 0; i < streamIDs.size(); i++) {
            if (i % 3 == 0) {
                // tables with requires_backup_support tag
                assertThat(streamIDs.get(i)).isIn(backupStreamIDs);
            } else {
                // tables without requires_backup_support tag
                assertThat(streamIDs.get(i)).isNotIn(backupStreamIDs);
            }
        }

        // Delete backup file
        File backupTarFile = new File(BACKUP_TAR_FILE_PATH);
        backupTarFile.delete();

        // Back up tables belonging to EMPTY_NAMESPACE, regardless of the tag
        backup = new Backup(BACKUP_TAR_FILE_PATH, backupRuntime, false, EMPTY_NAMESPACE);
        backup.start();

        // Verify that only EMPTY_NAMESPACE tables are backed up
        backupStreamIDs = getStreamIdsFromTarFile(BACKUP_TAR_FILE_PATH);
        for (i = 0; i < streamIDs.size(); i++) {
            if (i % 3 == 2) {
                // tables with requires_backup_support tag
                assertThat(streamIDs.get(i)).isIn(backupStreamIDs);
            } else {
                // tables without requires_backup_support tag
                assertThat(streamIDs.get(i)).isNotIn(backupStreamIDs);
            }
        }

        // Close servers and runtime before exiting
        cleanEnv();
    }

    /**
     * Test if only tables with correct namespace and tag are backed up
     */
    @Test
    public void backupTablesSelectedByBothNamespaceAndTagTest() throws Exception {

        // Set up the test environment
        setupEnv();

        // Create Corfu Store to add entries into server
        CorfuStore srcDataCorfuStore = new CorfuStore(srcDataRuntime);

        List<String> tableNames = getTableNames(numTables);
        List<UUID> streamIDs = new ArrayList<>();

        // Generate random entries and save into sourceServer.
        // 1/3 of the tables belong to NAMESPACE, without the tag,
        // another 1/3 belong to ANOTHER_NAMESPACE, with the tag,
        // and the last 1/3 belong to EMPTY_NAMESPACE, without the tag.
        int i = 0;
        for (String tableName : tableNames) {
            String namespace;
            boolean taggedTables;
            if (i % 3 == 0) {
                namespace = NAMESPACE;
                taggedTables = false;
            } else if (i % 3 == 1) {
                namespace = ANOTHER_NAMESPACE;
                taggedTables = true;
            } else {
                namespace = EMPTY_NAMESPACE;
                taggedTables = false;
            }
            i++;

            generateData(srcDataCorfuStore, namespace, tableName, taggedTables);
            streamIDs.add(FullyQualifiedTableName.streamId(namespace, tableName).getId());
        }

        // Back up tables belonging to ANOTHER_NAMESPACE and has the tag
        Backup backup = new Backup(BACKUP_TAR_FILE_PATH, backupRuntime, true, ANOTHER_NAMESPACE);
        backup.start();

        // Verify that only NAMESPACE tables are backed up
        List<UUID> backupStreamIDs = getStreamIdsFromTarFile(BACKUP_TAR_FILE_PATH);
        for (i = 0; i < streamIDs.size(); i++) {
            if (i % 3 == 1) {
                // tables with requires_backup_support tag
                assertThat(streamIDs.get(i)).isIn(backupStreamIDs);
            } else {
                // tables without requires_backup_support tag
                assertThat(streamIDs.get(i)).isNotIn(backupStreamIDs);
            }
        }

        // Delete backup file
        File backupTarFile = new File(BACKUP_TAR_FILE_PATH);
        backupTarFile.delete();

        // Back up tables belonging to NAMESPACE, and has the tag
        backup = new Backup(BACKUP_TAR_FILE_PATH, backupRuntime, true, NAMESPACE);
        backup.start();

        // Verify that only EMPTY_NAMESPACE tables are backed up
        backupStreamIDs = getStreamIdsFromTarFile(BACKUP_TAR_FILE_PATH);
        assertThat(backupStreamIDs).isEmpty();

        // Close servers and runtime before exiting
        cleanEnv();
    }

    /**
     * Test backing up an existent but empty table
     */
    @Test
    public void backupRestoreEmptyTableTest() throws Exception {

        // Set up the test environment
        setupEnv();

        // Create Corfu Store to add entries into server
        CorfuStore srcDataCorfuStore = new CorfuStore(srcDataRuntime);
        CorfuStore destDataCorfuStore = new CorfuStore(destDataRuntime);

        List<String> tableNames = getTableNames(numTables);

        // Generate random entries and save into sourceServer
        for (String tableName : tableNames) {
            generateData(srcDataCorfuStore, NAMESPACE, tableName, false);
        }

        // Obtain the corresponding streamIDs for the tables in sourceServer
        List<UUID> streamIDs = new ArrayList<>();
        for (String tableName : tableNames) {
            streamIDs.add(FullyQualifiedTableName.streamId(NAMESPACE, tableName).getId());
        }

        String emptyTableName = "emptyTableName";
        UUID emptyTableUuid = FullyQualifiedTableName.streamId(NAMESPACE, emptyTableName).getId();
        streamIDs.add(emptyTableUuid);
        openTableWithoutBackupTag(srcDataCorfuStore, NAMESPACE, emptyTableName);

        // Add two CorfuSystem tables
        streamIDs.add(TableRegistry.FQ_REGISTRY_TABLE_NAME.toStreamId().getId());
        streamIDs.add(FQ_PROTO_DESC_TABLE_NAME.toStreamId().getId());

        // Backup all tables
        Backup backup = new Backup(BACKUP_TAR_FILE_PATH, backupRuntime, false, null);
        backup.start();

        // Verify that backup tar file exists
        File backupTarFile = new File(BACKUP_TAR_FILE_PATH);
        assertThat(backupTarFile).exists();

        List<UUID> backupStreamIds = getStreamIdsFromTarFile(BACKUP_TAR_FILE_PATH);
        assertThat(streamIDs.size()).isEqualTo(backupStreamIds.size());
        assertThat(streamIDs).containsAll(backupStreamIds);

        // Restore using backup files
        Restore restore = new Restore(BACKUP_TAR_FILE_PATH, restoreRuntime, Restore.RestoreMode.PARTIAL, null);
        restore.start();

        // Compare data entries in CorfuStore before and after the Backup/Restore
        openTableWithoutBackupTag(destDataCorfuStore, NAMESPACE, emptyTableName);
        compareCorfuStoreTables(srcDataCorfuStore, NAMESPACE, emptyTableName, destDataCorfuStore, emptyTableName);

        // Close servers and runtime before exiting
        cleanEnv();
    }

    /**
     * Test FileNotFoundException is thrown when backup TAR file is removed before restore
     */
    @Test
    public void backupTarFileNotFoundTest() throws Exception {

        // Set up the test environment
        setupEnv();

        // Create Corfu Store to add entries into server
        CorfuStore srcDataCorfuStore = new CorfuStore(srcDataRuntime);

        List<String> tableNames = getTableNames(numTables);

        // Generate random entries and save into sourceServer
        int i = 0;
        for (String tableName : tableNames) {
            if (i++ % 2 == 0) {
                // Only these tables are non-empty
                generateData(srcDataCorfuStore, NAMESPACE, tableName, false);
            }
        }

        // Backup all tables
        Backup backup = new Backup(BACKUP_TAR_FILE_PATH, backupRuntime, false, null);
        backup.start();

        // Verify that backup tar file exists
        File backupTarFile = new File(BACKUP_TAR_FILE_PATH);
        assertThat(backupTarFile).exists();

        // Delete backup tar file before restore
        backupTarFile.delete();

        // Restore using backup files
        Restore restore = new Restore(BACKUP_TAR_FILE_PATH, restoreRuntime, Restore.RestoreMode.PARTIAL, null);
        Exception e = assertThrows(BackupRestoreException.class, restore::start);
        assertThat(e.getCause().getClass()).isEqualTo(FileNotFoundException.class);

        // Close servers and runtime before exiting
        cleanEnv();
    }

    /**
     * Test trimming log before backup starts
     */
    @Test
    public void backupAfterTrimTest() throws Exception {

        // Set up the test environment
        setupEnv();

        // Create Corfu Store to add entries into server
        CorfuStore srcDataCorfuStore = new CorfuStore(srcDataRuntime);

        List<String> tableNames = getTableNames(numTables);

        // Generate random entries and save into sourceServer
        int i = 0;
        for (String tableName : tableNames) {
            if (i++ % 2 == 0) {
                // Only these tables are non-empty
                generateData(srcDataCorfuStore, NAMESPACE, tableName, false);
            }
        }

        // Trim the log
        Token token = new Token(0, srcDataRuntime.getAddressSpaceView().getLogTail());
        srcDataRuntime.getAddressSpaceView().prefixTrim(token);

        // Backup all tables
        Backup backup = new Backup(BACKUP_TAR_FILE_PATH, backupRuntime, false, null);

        Exception ex = assertThrows(TransactionAbortedException.class, backup::start);
        assertThat(ex.getCause().getClass()).isEqualTo(TrimmedException.class);

        // Verify that backup tar file does not exist
        File backupTarFile = new File(BACKUP_TAR_FILE_PATH);
        assertThat(backupTarFile).doesNotExist();

        // Close servers and runtime before exiting
        cleanEnv();

    }

    /**
     * Test full backup and restore
     */
    @Test
    public void backupRestoreAllTablesTest() throws Exception {
        // Set up the test environment
        setupEnv();

        // Create Corfu Store to add entries into server
        CorfuStore srcDataCorfuStore = new CorfuStore(srcDataRuntime);
        CorfuStore destDataCorfuStore = new CorfuStore(destDataRuntime);

        List<String> tableNames = getTableNames(numTables);

        // Generate random entries and save into sourceServer
        // Set half of the tables with backup tag, and the other half without backup tag
        int i = 0;
        for (String tableName : tableNames) {
            if (i++ % 2 == 0) {
                // set requires_backup_support
                generateData(srcDataCorfuStore, NAMESPACE, tableName, true);
            } else {
                generateData(srcDataCorfuStore, NAMESPACE, tableName, false);
            }
        }

        // Backup all tables
        Backup backup = new Backup(BACKUP_TAR_FILE_PATH, backupRuntime, false, null);
        backup.start();

        // Verify that backup tar file exists
        File backupTarFile = new File(BACKUP_TAR_FILE_PATH);
        assertThat(backupTarFile).exists();

        // Generate pre-existing data and save into destServer
        for (String tableName : tableNames) {
            generateData(destDataCorfuStore, NAMESPACE, tableName, true);
        }

        // Restore using backup files
        Restore restore = new Restore(BACKUP_TAR_FILE_PATH, restoreRuntime, Restore.RestoreMode.FULL, null);
        restore.start();

        // Compare data entries in CorfuStore before and after the Backup/Restore
        for (String tableName : tableNames) {
            openTableWithoutBackupTag(destDataCorfuStore, NAMESPACE, tableName);
            compareCorfuStoreTables(srcDataCorfuStore, NAMESPACE, tableName, destDataCorfuStore, tableName);
        }

        Collection<CorfuStoreMetadata.TableName> allTablesBeforeBackup =
                srcDataRuntime.getTableRegistry().listTables();
        Collection<CorfuStoreMetadata.TableName> allTablesAfterRestore =
                destDataRuntime.getTableRegistry().listTables();

        assertThat(allTablesBeforeBackup).containsAll(allTablesAfterRestore);
        assertThat(allTablesAfterRestore).containsAll(allTablesBeforeBackup);

        Table<StringKey, CheckpointingStatus, Message> compactionManagerTable = destDataCorfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                COMPACTION_MANAGER_TABLE_NAME,
                StringKey.class,
                CheckpointingStatus.class,
                null,
                TableOptions.fromProtoSchema(CheckpointingStatus.class));

        Table<StringKey, RpcCommon.TokenMsg, Message> compactionControlsTable = destDataCorfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                COMPACTION_CONTROLS_TABLE,
                StringKey.class,
                RpcCommon.TokenMsg.class,
                null,
                TableOptions.fromProtoSchema(RpcCommon.TokenMsg.class));

        try (TxnContext txn = destDataCorfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CheckpointingStatus managerStatus = txn.getRecord(compactionManagerTable,
                    CompactorMetadataTables.COMPACTION_MANAGER_KEY).getPayload();
            assertThat(managerStatus).isNull();
            assertThat(txn.getRecord(compactionControlsTable,
                    CompactorMetadataTables.DISABLE_COMPACTION).getPayload()).isNull();
            txn.commit();
        }

        // Close servers and runtime before exiting
        cleanEnv();
    }

    /**
     * Tests if compaction is disabled till end of restore()
     *
     * @throws Exception
     */
    @Test
    public void backupRestoreDisableCompactionTest() throws Exception {
        // Set up the test environment
        setupEnv();

        // Create Corfu Store to add entries into server
        CorfuStore srcDataCorfuStore = new CorfuStore(srcDataRuntime);
        CorfuStore destDataCorfuStore = new CorfuStore(destDataRuntime);

        List<String> tableNames = getTableNames(numTables);

        // Generate random entries and save into sourceServer
        // Set half of the tables with backup tag, and the other half without backup tag
        int i = 0;
        for (String tableName : tableNames) {
            if (i++ % 2 == 0) {
                // set requires_backup_support
                generateData(srcDataCorfuStore, NAMESPACE, tableName, true);
            } else {
                generateData(srcDataCorfuStore, NAMESPACE, tableName, false);
            }
        }

        // Backup all tables
        Backup backup = new Backup(BACKUP_TAR_FILE_PATH, backupRuntime, false, null);
        backup.start();

        // Verify that backup tar file exists
        File backupTarFile = new File(BACKUP_TAR_FILE_PATH);
        assertThat(backupTarFile).exists();

        // Generate pre-existing data and save into destServer
        for (String tableName : tableNames) {
            generateData(destDataCorfuStore, NAMESPACE, tableName, true);
        }

        // Restore using backup files
        Restore restore = new Restore(BACKUP_TAR_FILE_PATH, restoreRuntime, Restore.RestoreMode.FULL, null);
        restore.disableCompaction();
        restore.openTarFile();
        restore.restore();

        //Do not call enableCompaction() here as we want to test if compaction is disabled after restore()
        Table<StringKey, RpcCommon.TokenMsg, Message> compactionControlsTable = destDataCorfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                COMPACTION_CONTROLS_TABLE,
                StringKey.class,
                RpcCommon.TokenMsg.class,
                null,
                TableOptions.fromProtoSchema(RpcCommon.TokenMsg.class));

        try (TxnContext txn = destDataCorfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            assertThat(txn.getRecord(compactionControlsTable,
                    CompactorMetadataTables.DISABLE_COMPACTION).getPayload()).isNotNull();
            txn.commit();
        }

        // Close servers and runtime before exiting
        cleanEnv();
    }

    @Test
    public void cleanupTempDirsBeforeBackupRestoreTest() throws Exception{
        // Set up the test environment
        setupEnv();

        // Simulate existing directories that were not cleaned up in previous runs
        File dir1 = Files.createTempDirectory(BACKUP_TEMP_DIR_PREFIX).toFile();
        File dir2 = Files.createTempDirectory(BACKUP_TEMP_DIR_PREFIX).toFile();

        // Backup all tables
        Backup backup = new Backup(BACKUP_TAR_FILE_PATH, backupRuntime, false, null);
        backup.start();

        assertThat(dir1).doesNotExist();
        assertThat(dir2).doesNotExist();

        // Simulate existing directories that were not cleaned up in previous runs
        dir1 = Files.createTempDirectory(RESTORE_TEMP_DIR_PREFIX).toFile();
        dir2 = Files.createTempDirectory(RESTORE_TEMP_DIR_PREFIX).toFile();

        // Restore using backup files
        Restore restore = new Restore(BACKUP_TAR_FILE_PATH, restoreRuntime, Restore.RestoreMode.PARTIAL, null);
        restore.start();

        assertThat(dir1).doesNotExist();
        assertThat(dir2).doesNotExist();
    }

    /**
     * Back up all tables and test restoring tagged tables from it
     */
    @Test
    public void restoreTaggedTablesFromFullBackupTest() throws Exception {

        // Set up the test environment
        setupEnv();

        // Create Corfu Store to add entries into server
        CorfuStore srcDataCorfuStore = new CorfuStore(srcDataRuntime);
        CorfuStore destDataCorfuStore = new CorfuStore(destDataRuntime);

        List<String> tableNames = getLongTableNames(numTables);

        // Generate random entries and save into sourceServer
        // Set half of the tables with backup tag, and the other half without backup tag
        int i = 0;
        for (String tableName : tableNames) {
            // set requires_backup_support if 'i' is even
            generateData(srcDataCorfuStore, NAMESPACE, tableName, i++ % 2 == 0);
        }

        // Backup all tables
        Backup backup = new Backup(BACKUP_TAR_FILE_PATH, backupRuntime, false, null);
        backup.start();

        // Restore using backup files
        Restore restore = new Restore(BACKUP_TAR_FILE_PATH, restoreRuntime, Restore.RestoreMode.PARTIAL_TAGGED, null);
        restore.start();

        // Compare data entries in CorfuStore before and after the Backup/Restore
        i = 0;
        for (String tableName : tableNames) {
            if (i++ % 2 == 0) {
                // tables that have requires_backup_support tag
                openTableWithBackupTag(destDataCorfuStore, NAMESPACE, tableName);
                compareCorfuStoreTables(srcDataCorfuStore, NAMESPACE, tableName, destDataCorfuStore, tableName);
            } else {
                // tables that don't have requires_backup_support tag are not restored and should remain empty
                openTableWithoutBackupTag(destDataCorfuStore, NAMESPACE, tableName);
                assertThat(destDataCorfuStore.getTable(NAMESPACE, tableName).count()).isZero();
            }
        }

        // Close servers and runtime before exiting
        cleanEnv();
    }

    /**
     * Back up all tables and test restore by namespace
     */
    @Test
    public void restoreANamespaceFromFullBackupTest() throws Exception {

        // Set up the test environment
        setupEnv();

        // Create Corfu Store to add entries into server
        CorfuStore srcDataCorfuStore = new CorfuStore(srcDataRuntime);
        CorfuStore destDataCorfuStore = new CorfuStore(destDataRuntime);

        List<String> tableNames = getLongTableNames(numTables);

        // Generate random entries and save into sourceServer
        // Set half of the tables with backup tag, and the other half without backup tag
        int i = 0;
        for (String tableName : tableNames) {
            if (i++ % 2 == 0) {
                generateData(srcDataCorfuStore, NAMESPACE, tableName, false);
            } else {
                generateData(srcDataCorfuStore, ANOTHER_NAMESPACE, tableName, false);
            }

        }

        // Backup all tables
        Backup backup = new Backup(BACKUP_TAR_FILE_PATH, backupRuntime, false, null);
        backup.start();

        // Add pre-existing data into restore server
        // Restore should clean up the pre-existing data before actual restoring
        i = 0;
        for (String tableName : tableNames) {
            if (i++ % 2 == 0) {
                generateData(destDataCorfuStore, NAMESPACE, tableName, false);
            } else {
                generateData(destDataCorfuStore, ANOTHER_NAMESPACE, tableName, false);
            }
        }

        // Restore using backup files
        Restore restore = new Restore(BACKUP_TAR_FILE_PATH, restoreRuntime, Restore.RestoreMode.PARTIAL_NAMESPACE, ANOTHER_NAMESPACE);
        restore.start();

        // Compare data entries in CorfuStore before and after the Backup/Restore
        i = 0;
        for (String tableName : tableNames) {
            if (i++ % 2 == 0) {
                // Tables in other namespaces should not be affected
                openTableWithoutBackupTag(destDataCorfuStore, NAMESPACE, tableName);
                assertThat(destDataCorfuStore.getTable(NAMESPACE, tableName).count()).isEqualTo(numEntries);
            } else {
                // Only tables in ANOTHER_NAMESPACE are restored
                openTableWithoutBackupTag(destDataCorfuStore, ANOTHER_NAMESPACE, tableName);
                compareCorfuStoreTables(srcDataCorfuStore, ANOTHER_NAMESPACE, tableName, destDataCorfuStore, tableName);
            }
        }

        // Close servers and runtime before exiting
        cleanEnv();
    }
}
