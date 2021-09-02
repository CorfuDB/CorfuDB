package org.corfudb.runtime;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.test.SampleSchema;
import org.corfudb.test.SampleSchema.Uuid;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;


/**
 * Test the Corfu native backup and restore functionalities. Overall the tests bring up two CorfuServers,
 * one used as source server which is backed up, and the other as destination server which restores the data
 * using the backup file generated from the source server.
 */
@Slf4j
public class BackupRestoreIT extends AbstractIT {

    final static public int numEntries = 100;
    final static public int valSize = 2000;
    static final public int numTables = 5;
    static final private String NAMESPACE = "test_namespace";
    static final private String backupTable = "test_table";

    static final private String DEFAULT_HOST = "localhost";
    static final private int DEFAULT_PORT = 9000;
    private static final int WRITER_PORT = DEFAULT_PORT + 1;
    private static final String SOURCE_ENDPOINT = DEFAULT_HOST + ":" + DEFAULT_PORT;
    private static final String DESTINATION_ENDPOINT = DEFAULT_HOST + ":" + WRITER_PORT;

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
                .setTransactionLogging(true)
                .parseConfigurationString(SOURCE_ENDPOINT)
                .connect();

        backupRuntime = CorfuRuntime
                .fromParameters(params)
                .setTransactionLogging(true)
                .parseConfigurationString(SOURCE_ENDPOINT)
                .connect();

        restoreRuntime = CorfuRuntime
                .fromParameters(params)
                .setTransactionLogging(true)
                .parseConfigurationString(DESTINATION_ENDPOINT)
                .connect();

        destDataRuntime = CorfuRuntime
                .fromParameters(params)
                .setTransactionLogging(true)
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
    openTableWithoutBackupTag(CorfuStore corfuStore, String tableName) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        return corfuStore.openTable(NAMESPACE,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.EventInfo.class,
                SampleSchema.Uuid.class,
                TableOptions.builder().build());
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
    openTableWithBackupTag(CorfuStore corfuStore, String tableName) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        return corfuStore.openTable(NAMESPACE,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.SampleTableAMsg.class,
                SampleSchema.Uuid.class,
                TableOptions.builder().build());
    }

    /**
     * Generate random EventInfo entries and save into the given Corfu DataStore.
     *
     * @param dataStore     the data store used
     * @param tableName     the table which generated entries are added to
     * */
    private void generateData(CorfuStore dataStore, String tableName, boolean hasBackupTag) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Table table = hasBackupTag ? openTableWithBackupTag(dataStore, tableName) :
                openTableWithoutBackupTag(dataStore, tableName);

        TxnContext txn = dataStore.txn(NAMESPACE);
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
    private void compareCorfuStoreTables(CorfuStore corfuStore1, String tableName1, CorfuStore corfuStore2, String tableName2) {
        TxnContext aTxn = corfuStore1.txn(NAMESPACE);
        List<CorfuStoreEntry<Uuid, SampleSchema.SampleTableAMsg, Uuid>> aValueSet = new ArrayList<>();
        for (int i = 0; i < numEntries; i++) {
            uuidKey = SampleSchema.Uuid.newBuilder()
                    .setMsb(i)
                    .setLsb(i)
                    .build();
            aValueSet.add(aTxn.getRecord(tableName1, uuidKey));
        }
        aTxn.close();

        TxnContext bTxn = corfuStore2.txn(NAMESPACE);
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
    public void backupRestoreMultipleTablesTest() throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {

        // Set up the test environment
        setupEnv();

        // Create Corfu Store to add entries into server
        CorfuStore srcDataCorfuStore = new CorfuStore(srcDataRuntime);
        CorfuStore destDataCorfuStore = new CorfuStore(destDataRuntime);

        List<String> tableNames = getTableNames(numTables);

        // Generate random entries and save into sourceServer
        for (String tableName : tableNames) {
            generateData(srcDataCorfuStore, tableName, false);
        }

        // Obtain the corresponding streamIDs for the tables in sourceServer
        List<UUID> streamIDs = new ArrayList<>();
        for (String tableName : tableNames) {
            streamIDs.add(CorfuRuntime.getStreamID(TableRegistry.getFullyQualifiedTableName(NAMESPACE, tableName)));
        }

        // Backup
        Backup backup = new Backup(BACKUP_TAR_FILE_PATH, streamIDs, backupRuntime);
        backup.start();

        // Verify that backup tar file exists
        File backupTarFile = new File(BACKUP_TAR_FILE_PATH);
        assertThat(backupTarFile).exists();

        // Add pre-existing data into restore server
        // Restore should clean up the pre-existing data before actual restoring
        for (String tableName : tableNames) {
            generateData(destDataCorfuStore, tableName, true);
        }

        // Restore using backup files
        Restore restore = new Restore(BACKUP_TAR_FILE_PATH, restoreRuntime, Restore.RestoreMode.PARTIAL);
        restore.start();

        // Compare data entries in CorfuStore before and after the Backup/Restore
        for (String tableName : tableNames) {
            openTableWithoutBackupTag(destDataCorfuStore, tableName);
            compareCorfuStoreTables(srcDataCorfuStore, tableName, destDataCorfuStore, tableName);
        }

        // Close servers and runtime before exiting
        cleanEnv();
    }

    /**
     * An end-to-end Backup and Restore test for multiple tables which have requires_backup_support tag
     */
    @Test
    public void backupRestoreTaggedTablesTest() throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {

        // Set up the test environment
        setupEnv();

        // Create Corfu Store to add entries into server
        CorfuStore srcDataCorfuStore = new CorfuStore(srcDataRuntime);
        CorfuStore destDataCorfuStore = new CorfuStore(destDataRuntime);

        List<String> tableNames = getTableNames(numTables);

        // Generate random entries and save into sourceServer
        for (String tableName : tableNames) {
            generateData(srcDataCorfuStore, tableName, true);
        }

        // Backup
        Backup backup = new Backup(BACKUP_TAR_FILE_PATH, backupRuntime, true);
        backup.start();

        // Verify that backup tar file exists
        File backupTarFile = new File(BACKUP_TAR_FILE_PATH);
        assertThat(backupTarFile).exists();

        // Restore using backup files
        Restore restore = new Restore(BACKUP_TAR_FILE_PATH, restoreRuntime, Restore.RestoreMode.PARTIAL);
        restore.start();

        // Compare data entries in CorfuStore before and after the Backup/Restore
        for (String tableName : tableNames) {
            openTableWithBackupTag(destDataCorfuStore, tableName);
            compareCorfuStoreTables(srcDataCorfuStore, tableName, destDataCorfuStore, tableName);
        }

        // Close servers and runtime before exiting
        cleanEnv();
    }

    /**
     * Test if only user-specified tables are backed up
     */
    @Test
    public void backupTablesSelectedByStreamIdsTest() throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {

        // Set up the test environment
        setupEnv();

        // Create Corfu Store to add entries into server
        CorfuStore srcDataCorfuStore = new CorfuStore(srcDataRuntime);

        List<String> tableNames = getTableNames(numTables);

        // Generate random entries and save into sourceServer
        for (String tableName : tableNames) {
            // Tables without requires_backup_tag should be backed up if stream ids are provided in Backup constructor
            generateData(srcDataCorfuStore, tableName, false);
        }

        // Obtain the corresponding streamIDs for the tables in sourceServer
        List<UUID> streamIDs = new ArrayList<>();
        int i = 0;
        for (String tableName : tableNames) {
            // Only back up a half of all tables
            if (i++ % 2 == 0) {
                streamIDs.add(CorfuRuntime.getStreamID(TableRegistry.getFullyQualifiedTableName(NAMESPACE, tableName)));
            }
        }

        // Backup
        Backup backup = new Backup(BACKUP_TAR_FILE_PATH, streamIDs, backupRuntime);
        backup.start();

        // Verify that backup tar file exists
        File backupTarFile = new File(BACKUP_TAR_FILE_PATH);
        assertThat(backupTarFile).exists();

        // Verify that only a selective set of tables are backed up
        List<UUID> backupStreamIDs = getStreamIdsFromTarFile(BACKUP_TAR_FILE_PATH);
        assertThat(streamIDs.size()).isEqualTo(backupStreamIDs.size());
        assertThat(streamIDs).containsAll(backupStreamIDs);

        // Close servers and runtime before exiting
        cleanEnv();
    }

    /**
     * Test if only tables with requires_backup_support tag are backed up
     */
    @Test
    public void backupTablesSelectedByTagsTest() throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {

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
                generateData(srcDataCorfuStore, tableName, true);
            } else {
                generateData(srcDataCorfuStore, tableName, false);
            }
        }

        // Obtain the corresponding streamIDs for the tables in sourceServer
        List<UUID> streamIDs = new ArrayList<>();
        for (String tableName : tableNames) {
            streamIDs.add(CorfuRuntime.getStreamID(TableRegistry.getFullyQualifiedTableName(NAMESPACE, tableName)));
        }

        // Backup
        Backup backup = new Backup(BACKUP_TAR_FILE_PATH, backupRuntime, true);
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
     * Test backing up a non-existent table
     */
    @Test
    public void backupNonExistentTableTest() throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {

        // Set up the test environment
        setupEnv();

        // Create Corfu Store to add entries into server
        CorfuStore srcDataCorfuStore = new CorfuStore(srcDataRuntime);

        List<String> tableNames = getTableNames(numTables);

        // Generate random entries and save into sourceServer
        for (String tableName : tableNames) {
            generateData(srcDataCorfuStore, tableName, false);
        }

        String nonExistentTableName = "nonExistentTableName";
        UUID nonExistentTableUuid = CorfuRuntime.getStreamID(TableRegistry.getFullyQualifiedTableName(NAMESPACE, nonExistentTableName));

        // Backup
        Backup backup = new Backup(BACKUP_TAR_FILE_PATH, Arrays.asList(nonExistentTableUuid), backupRuntime);
        backup.start();

        // Verify that backup tar file exists
        File backupTarFile = new File(BACKUP_TAR_FILE_PATH);
        assertThat(backupTarFile).exists();

        List<UUID> backupStreamIds = getStreamIdsFromTarFile(BACKUP_TAR_FILE_PATH);
        assertThat(backupStreamIds).isEmpty();

        // Close servers and runtime before exiting
        cleanEnv();
    }

    /**
     * Test backing up an existent but empty table
     */
    @Test
    public void backupRestoreEmptyTableTest() throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {

        // Set up the test environment
        setupEnv();

        // Create Corfu Store to add entries into server
        CorfuStore srcDataCorfuStore = new CorfuStore(srcDataRuntime);
        CorfuStore destDataCorfuStore = new CorfuStore(destDataRuntime);

        List<String> tableNames = getTableNames(numTables);

        // Generate random entries and save into sourceServer
        for (String tableName : tableNames) {
            generateData(srcDataCorfuStore, tableName, false);
        }

        // Obtain the corresponding streamIDs for the tables in sourceServer
        List<UUID> streamIDs = new ArrayList<>();
        for (String tableName : tableNames) {
            streamIDs.add(CorfuRuntime.getStreamID(TableRegistry.getFullyQualifiedTableName(NAMESPACE, tableName)));
        }

        String emptyTableName = "emptyTableName";
        UUID emptyTableUuid = CorfuRuntime.getStreamID(TableRegistry.getFullyQualifiedTableName(NAMESPACE, emptyTableName));
        streamIDs.add(emptyTableUuid);
        openTableWithoutBackupTag(srcDataCorfuStore, emptyTableName);

        // Backup
        Backup backup = new Backup(BACKUP_TAR_FILE_PATH, streamIDs, backupRuntime);
        backup.start();

        // Verify that backup tar file exists
        File backupTarFile = new File(BACKUP_TAR_FILE_PATH);
        assertThat(backupTarFile).exists();

        List<UUID> backupStreamIds = getStreamIdsFromTarFile(BACKUP_TAR_FILE_PATH);
        assertThat(streamIDs.size()).isEqualTo(backupStreamIds.size());
        assertThat(streamIDs).containsAll(backupStreamIds);

        // Restore using backup files
        Restore restore = new Restore(BACKUP_TAR_FILE_PATH, restoreRuntime, Restore.RestoreMode.PARTIAL);
        restore.start();

        // Compare data entries in CorfuStore before and after the Backup/Restore
        openTableWithoutBackupTag(destDataCorfuStore, emptyTableName);
        compareCorfuStoreTables(srcDataCorfuStore, emptyTableName, destDataCorfuStore, emptyTableName);

        // Close servers and runtime before exiting
        cleanEnv();
    }

    /**
     * Test FileNotFoundException is thrown when backup TAR file is removed before restore
     */
    @Test
    public void backupTarFileNotFoundTest() throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {

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
                generateData(srcDataCorfuStore, tableName, false);
            }
        }

        // Obtain the corresponding streamIDs for the tables in sourceServer
        List<UUID> streamIDs = new ArrayList<>();
        for (String tableName : tableNames) {
            streamIDs.add(CorfuRuntime.getStreamID(TableRegistry.getFullyQualifiedTableName(NAMESPACE, tableName)));
        }

        // Backup
        Backup backup = new Backup(BACKUP_TAR_FILE_PATH, streamIDs, backupRuntime);
        backup.start();

        // Verify that backup tar file exists
        File backupTarFile = new File(BACKUP_TAR_FILE_PATH);
        assertThat(backupTarFile).exists();

        // Delete backup tar file before restore
        backupTarFile.delete();

        // Restore using backup files
        Restore restore = new Restore(BACKUP_TAR_FILE_PATH, restoreRuntime, Restore.RestoreMode.PARTIAL);
        assertThrows(FileNotFoundException.class, restore::start);

        // Close servers and runtime before exiting
        cleanEnv();
    }

    /**
     * Test trimming log before backup starts
     */
    @Test
    public void backupAfterTrimTest() throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {

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
                generateData(srcDataCorfuStore, tableName, false);
            }
        }

        // Obtain the corresponding streamIDs for the tables in sourceServer
        List<UUID> streamIDs = new ArrayList<>();
        for (String tableName : tableNames) {
            streamIDs.add(CorfuRuntime.getStreamID(TableRegistry.getFullyQualifiedTableName(NAMESPACE, tableName)));
        }

        // Trim the log
        Token token = new Token(0, srcDataRuntime.getAddressSpaceView().getLogTail());
        srcDataRuntime.getAddressSpaceView().prefixTrim(token);

        // Backup
        Backup backup = new Backup(BACKUP_TAR_FILE_PATH, streamIDs, backupRuntime);

        Exception ex = assertThrows(TransactionAbortedException.class, backup::start);
        assertThat(ex.getCause().getClass()).isEqualTo(TrimmedException.class);

        // Verify that backup tar file exists
        File backupTarFile = new File(BACKUP_TAR_FILE_PATH);
        assertThat(backupTarFile).doesNotExist();

        // Close servers and runtime before exiting
        cleanEnv();

    }

    /**
     * Test full backup and restore
     */
    @Test
    public void backupRestoreAllTablesTest() throws
            IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
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
                generateData(srcDataCorfuStore, tableName, true);
            } else {
                generateData(srcDataCorfuStore, tableName, false);
            }
        }

        // Backup
        Backup backup = new Backup(BACKUP_TAR_FILE_PATH, backupRuntime, false);
        backup.start();

        // Verify that backup tar file exists
        File backupTarFile = new File(BACKUP_TAR_FILE_PATH);
        assertThat(backupTarFile).exists();

        // Generate pre-existing data and save into destServer
        for (String tableName : tableNames) {
            generateData(destDataCorfuStore, tableName, true);
        }

        // Restore using backup files
        Restore restore = new Restore(BACKUP_TAR_FILE_PATH, restoreRuntime, Restore.RestoreMode.FULL);
        restore.start();

        // Compare data entries in CorfuStore before and after the Backup/Restore
        for (String tableName : tableNames) {
            openTableWithoutBackupTag(destDataCorfuStore, tableName);
            compareCorfuStoreTables(srcDataCorfuStore, tableName, destDataCorfuStore, tableName);
        }

        Collection<CorfuStoreMetadata.TableName> allTablesBeforeBackup =
                srcDataRuntime.getTableRegistry().listTables();
        Collection<CorfuStoreMetadata.TableName> allTablesAfterRestore =
                destDataRuntime.getTableRegistry().listTables();

        assertThat(allTablesBeforeBackup).containsAll(allTablesAfterRestore);
        assertThat(allTablesAfterRestore).containsAll(allTablesBeforeBackup);

        // Close servers and runtime before exiting
        cleanEnv();
    }
}
