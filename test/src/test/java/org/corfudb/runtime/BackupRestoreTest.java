package org.corfudb.runtime;
import static org.assertj.core.api.Assertions.assertThat;

import lombok.extern.slf4j.Slf4j;
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

public class BackupRestoreTest extends AbstractViewTest {
    final static public int numEntries = 10;
    static final String NAMESPACE = "test_namespace";
    static final String backupFileName = "test_backup_file";
    static final String backupTable = "backup_table";
    static final String restoreTable = "restore_table";

    Table<SampleSchema.Uuid, SampleSchema.Uuid, SampleSchema.Uuid> table1;
    Table<SampleSchema.Uuid, SampleSchema.Uuid, SampleSchema.Uuid> table2;
    SampleSchema.Uuid uuidKey = null;

    void generateData(CorfuStore dataStore, String nameSpace, String tableName) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {

        table1 = dataStore.openTable(NAMESPACE,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.Uuid.class,
                SampleSchema.Uuid.class,
                TableOptions.builder().build());

        for (int i = 0 ; i < numEntries; i++) {
            uuidKey = SampleSchema.Uuid.newBuilder()
                    .setMsb(i)
                    .setLsb(i)
                    .build();

            TxBuilder tx = dataStore.tx(NAMESPACE);
            tx.create(tableName, uuidKey, uuidKey, uuidKey).commit();
        }
    }

    @Test
    public void backupEntryTest() throws IOException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        CorfuRuntime dataRuntime = getDefaultRuntime().setTransactionLogging(true);
        CorfuRuntime backupRuntime = getNewRuntime(getDefaultNode()).setTransactionLogging(true).connect();
        CorfuRuntime restoreRuntime = getNewRuntime(getDefaultNode()).setTransactionLogging(true).connect();
        CorfuRuntime restoreDataRuntime = getNewRuntime(getDefaultNode()).setTransactionLogging(true).connect();

        CorfuStore dataCorfuStore = new CorfuStore(dataRuntime);
        CorfuStore restoreCorfuStore = new CorfuStore(restoreRuntime);
        CorfuStore restoreDataCorfuStore = new CorfuStore(restoreDataRuntime);

        generateData(dataCorfuStore, NAMESPACE, backupTable);

        String fullName = TableRegistry.getFullyQualifiedTableName(NAMESPACE, backupTable);
        UUID srcUuid = CorfuRuntime.getStreamID(fullName);

        Backup.backupTable(backupFileName, srcUuid, backupRuntime, backupRuntime.getAddressSpaceView().getLogTail());


        fullName = TableRegistry.getFullyQualifiedTableName(NAMESPACE, restoreTable);
        UUID uuid = CorfuRuntime.getStreamID(fullName);

        Restore.restoreTable(backupFileName, uuid, srcUuid, restoreCorfuStore);

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
        }
    }
}
