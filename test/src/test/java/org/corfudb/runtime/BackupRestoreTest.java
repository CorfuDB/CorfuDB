package org.corfudb.runtime;
import static org.assertj.core.api.Assertions.assertThat;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.RandomStringUtils;
import org.corfudb.integration.AbstractIT;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Query;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxBuilder;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.test.SampleSchema;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Set;
import java.util.UUID;
import org.corfudb.test.SampleSchema.Uuid;

@Slf4j
// Open a table and generate a few entries
// Use backupTable API to generate a file.log
// Use restoreTable API to read the log file and generate opaque entries
// Check the opaque entries are the same with the opaque entries in the original database.

public class BackupRestoreTest extends AbstractIT {
    final static public int numEntries = 40000;
    final static public int valSize = 50000;
    static final String NAMESPACE = "test_namespace";
    static final String backupFileName = "test_backup_file";
    static final String backupTable = "backup_table";
    static final String restoreTable = "restore_table";
    static final String DEFAULT_HOST = "localhost";
    static final String LOG_PATH1 = "/Users/maxi/corfu1";
    static final String LOG_PATH2 = "/Users/maxi/corfu2";

    static final int DEFAULT_PORT = 9000;
    private static final int WRITER_PORT = DEFAULT_PORT + 1;
    private static final String SOURCE_ENDPOINT = DEFAULT_HOST + ":" + DEFAULT_PORT;
    private static final String DESTINATION_ENDPOINT = DEFAULT_HOST + ":" + WRITER_PORT;

    private Process sourceServer;
    private Process destinationServer;

    Table<SampleSchema.Uuid, SampleSchema.EventInfo, SampleSchema.Uuid> table1;
    Table<SampleSchema.Uuid, SampleSchema.EventInfo, SampleSchema.Uuid> table2;
    SampleSchema.Uuid uuidKey = null;

    void generateData(CorfuStore dataStore, String nameSpace, String tableName) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {

        SampleSchema.EventInfo eventInfo;
        table1 = dataStore.openTable(NAMESPACE,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.EventInfo.class,
                SampleSchema.Uuid.class,
                TableOptions.builder().build());

        String name = RandomStringUtils.random(valSize, true, true);

        for (int i = 0 ; i < numEntries; i++) {
            uuidKey = SampleSchema.Uuid.newBuilder()
                    .setMsb(i)
                    .setLsb(i)
                    .build();
            TxBuilder tx = dataStore.tx(NAMESPACE);
            eventInfo = SampleSchema.EventInfo.newBuilder().setName(name).build();
            tx.update(tableName, uuidKey, eventInfo, uuidKey).commit();
        }
    }

    @Test
    public void backupEntryTest() throws IOException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        String logPath = LOG_PATH1 + "/corfu/log";
        Runtime.getRuntime().exec("rm -rf " + logPath);

        logPath = LOG_PATH2 + "/corfu/log";
        Runtime.getRuntime().exec("rm -rf " + logPath);

        sourceServer = new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(DEFAULT_PORT)
                .setLogPath(LOG_PATH1)
                .setSingle(true)
                .runServer();

        // Destination Corfu Server (data will be replicated into this server)
        destinationServer = new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(WRITER_PORT)
                .setLogPath(LOG_PATH2)
                .setSingle(true)
                .runServer();

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();

        CorfuRuntime dataRuntime = CorfuRuntime.fromParameters(params).setTransactionLogging(true).parseConfigurationString(SOURCE_ENDPOINT).connect();
        CorfuRuntime backupRuntime = CorfuRuntime.fromParameters(params).setTransactionLogging(true).parseConfigurationString(SOURCE_ENDPOINT).connect();
        CorfuRuntime restoreRuntime = CorfuRuntime.fromParameters(params).setTransactionLogging(true).parseConfigurationString(DESTINATION_ENDPOINT).connect();
        CorfuRuntime restoreDataRuntime = CorfuRuntime.fromParameters(params).setTransactionLogging(true).parseConfigurationString(DESTINATION_ENDPOINT).connect();

        CorfuStore dataCorfuStore = new CorfuStore(dataRuntime);
        CorfuStore restoreCorfuStore = new CorfuStore(restoreRuntime);
        CorfuStore restoreDataCorfuStore = new CorfuStore(restoreDataRuntime);

        long time0 = System.currentTimeMillis();
        generateData(dataCorfuStore, NAMESPACE, backupTable);
        long time1 = System.currentTimeMillis();
        System.out.print("\nGenerated Data " + numEntries + " used " + (time1 - time0));

        String fullName = TableRegistry.getFullyQualifiedTableName(NAMESPACE, backupTable);
        UUID srcUuid = CorfuRuntime.getStreamID(fullName);

        time0 = System.currentTimeMillis();
        Backup.backupTable(backupFileName, srcUuid, backupRuntime, backupRuntime.getAddressSpaceView().getLogTail());
        time1 = System.currentTimeMillis();
        System.out.print("\nbackup takes " + (time1 - time0));

        fullName = TableRegistry.getFullyQualifiedTableName(NAMESPACE, restoreTable);
        UUID uuid = CorfuRuntime.getStreamID(fullName);

        Restore.restoreTable(backupFileName, uuid, srcUuid, restoreCorfuStore);
        long time2 = System.currentTimeMillis();
        System.out.print("\nrestore takes " + (time2 - time1));


        /*
        Query q1 = dataCorfuStore.query(NAMESPACE);

        table2 = restoreDataCorfuStore.openTable(NAMESPACE,
                restoreTable,
                SampleSchema.Uuid.class,
                SampleSchema.Uuid.class,
                SampleSchema.Uuid.class,
                TableOptions.builder().build());

        Query q2 = restoreDataCorfuStore.query(NAMESPACE);

        Set<Uuid> aSet = q1.keySet(backupTable, null);
        Set<Uuid> bSet = q2.keySet(restoreTable, null);


        assertThat(aSet.containsAll(bSet));
        assertThat(bSet.containsAll(aSet));

        for (int i = 0; i < numEntries; i++) {
            uuidKey = SampleSchema.Uuid.newBuilder()
                    .setMsb(i)
                    .setLsb(i)
                    .build();
            CorfuRecord<SampleSchema.Uuid, SampleSchema.Uuid> rd1 = dataCorfuStore.query(NAMESPACE).getRecord(backupTable, uuidKey);
            CorfuRecord<SampleSchema.Uuid, SampleSchema.Uuid> rd2 = restoreDataCorfuStore.query(NAMESPACE).getRecord(restoreTable, uuidKey);
            assertThat(rd1).isEqualTo(rd2);
        }*/
    }
}
