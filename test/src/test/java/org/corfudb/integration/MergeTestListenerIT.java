package org.corfudb.integration;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.SessionManager;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplicationLogicalGroupClient;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.test.SampleSchema;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.integration.LogReplicationAbstractIT.NAMESPACE;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATION_STATUS_TABLE_NAME;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class MergeTestListenerIT extends CorfuReplicationMultiSourceSinkIT {

    /**
     * Static fields to facilitate the test whose names are self-explanatory.
     */

    private static final int NUM_WRITES = 500;


    private static final String SAMPLE_CLIENT_NAME = "SAMPLE_CLIENT";

    private static final String GROUP_A = "groupA";


    private static final String STREAMING_TAG = "tag_one";

    public static final String LOCAL_TABLE_PREFIX = "Local_Table00";

    public static final String GROUPA_TABLE_PREFIX = "GroupA_Table00";

    public static final String MERGED_TABLE_NAME = "MERGED_TABLE";

    private static final int SLEEP_INTERVAL = 5;

    private static final int SOURCE_INDEX = 0;

    private static final int SINK2_INDEX = 1;

    private static final int SINK3_INDEX = 2;

    private static final int numSource = 1;

    private static final int numSink = 3;

    private final String defaultClusterId = UUID.randomUUID().toString();
    private final String testClientName = "lr_test_client";

    /**
     * Note that the following lists are used for storing the references of tables opened on Source or Sink side.
     * They do not provide guarantees to guard against from which side the tables are actually opened.
     */

    // A list of tables open on Source side whose logical group is GROUP_A
    private final List<Table<Sample.StringKey, SampleSchema.SampleGroupMsgA, Message>> srcTablesGroupA = new ArrayList<>();

    // A list of tables open on Sink side whose logical group is GROUP_A
    private final List<Table<Sample.StringKey, SampleSchema.SampleGroupMsgA, Message>> sinkTablesGroupA = new ArrayList<>();

    // A list of local tables open on Sink side
    private final List<Table<Sample.StringKey, SampleSchema.SampleGroupMsgA, Message>> sinkTablesLocal = new ArrayList<>();

    /**
     * Tables for this class.
     */

    Table<LogReplication.LogReplicationSession, LogReplication.ReplicationStatus, Message> replicationStatusTable;

    Table<Sample.StringKey, SampleSchema.SampleMergedTable, Message> mergedTable;


    /**
     * Initialize the 1-Source & 3-Sink topology and open the replication status table on Source side.
     */
    @Before
    public void setUp() throws Exception {
        setUp(numSource, numSink, DefaultClusterManager.TP_MIXED_MODEL_THREE_SINK);
        openLogReplicationStatusTable();
    }

    @Test
    public void testForTestingMergeListener() throws Exception {
        // Register client and add group destinations
        CorfuRuntime clientRuntime = getClientRuntime();
        LogReplicationLogicalGroupClient logicalGroupClient =
                new LogReplicationLogicalGroupClient(clientRuntime, SAMPLE_CLIENT_NAME);
        logicalGroupClient.setDestinations(GROUP_A,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK2_INDEX)));

        // Open tables on both side
        int numTables = 2;
        // Source
        openGroupATable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupA);
        // Sink
        openGroupATable(numTables, sinkCorfuStores.get(SINK2_INDEX), sinkTablesGroupA);
        openLocalTable(numTables, sinkCorfuStores.get(SINK2_INDEX), sinkTablesLocal);
        openMergedTable(sinkCorfuStores.get(SINK2_INDEX));

        // Write data to Source side tables
        writeDataOnSource(0, NUM_WRITES);

        // Start log replication for all sessions
        startReplicationServers();

        // Verify all the sessions' snapshot sync completed
        verifySessionInLogEntrySyncState(SINK2_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());

        // Verify tables' content on Sink side
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);

        // Write data to Sink side tables
        writeDataOnSinkGroupA(NUM_WRITES, NUM_WRITES);
        writeDataOnSinkLocal(NUM_WRITES, 2*NUM_WRITES);

        // Subscribe MergeTestStreamListener on Sink side
        MergeTestListener listenerSink2 = new MergeTestListener(sinkCorfuStores.get(SINK2_INDEX)
                , NAMESPACE, numTables,
                numTables);
        sinkCorfuStores.get(SINK2_INDEX).subscribeLogReplicationListener(listenerSink2,
                NAMESPACE, STREAMING_TAG, sinkCorfuStores.get(SINK2_INDEX));

        // Verify tables' content on Sink side
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), 2 * NUM_WRITES, sinkTablesGroupA);
        verifyMergedTableData(sinkCorfuStores.get(SINK2_INDEX), 3 * NUM_WRITES,
               Collections.singletonList(mergedTable));

        // Write data to Sink side tables
        writeDataOnSinkGroupA(2 * NUM_WRITES, NUM_WRITES);
        writeDataOnSinkLocal(2 * NUM_WRITES, 2 * NUM_WRITES);

        // Verify tables' content on Sink side
        verifyMergedTableData(sinkCorfuStores.get(SINK2_INDEX), 4 * NUM_WRITES,
                Collections.singletonList(mergedTable));
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), 3 * NUM_WRITES, sinkTablesGroupA);

        // Add GROUP_A to Sink3 and verify the GROUP_A tables' data is successfully replicated to Sink2 and Sink3.
        logicalGroupClient.addDestinations(GROUP_A,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX)));
        List<Table<Sample.StringKey, SampleSchema.SampleGroupMsgA, Message>> sinkTablesGroupAOnSink3 = new ArrayList<>();
        openGroupATable(numTables, sinkCorfuStores.get(SINK3_INDEX), sinkTablesGroupAOnSink3);
        writeDataOnSource(NUM_WRITES, 3 * NUM_WRITES);
        verifyGroupATableData(sinkCorfuStores.get(SINK3_INDEX), 4 * NUM_WRITES, sinkTablesGroupAOnSink3);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), 4 * NUM_WRITES, sinkTablesGroupA);
        verifyMergedTableData(sinkCorfuStores.get(SINK2_INDEX), 4 * NUM_WRITES,
                Collections.singletonList(mergedTable));

        // Remove GROUP_A from Sink3 and verify GROUP_A tables' data no longer replicated to Sink3
        logicalGroupClient.removeDestinations(GROUP_A,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX)));
        TimeUnit.SECONDS.sleep(SLEEP_INTERVAL);
        writeDataOnSource(4 * NUM_WRITES,  NUM_WRITES);
        writeDataOnSinkLocal(4 * NUM_WRITES, 2 * NUM_WRITES);
        verifyGroupATableData(sinkCorfuStores.get(SINK3_INDEX), 4 * NUM_WRITES, sinkTablesGroupAOnSink3);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), 5 * NUM_WRITES, sinkTablesGroupA);
        verifyMergedTableData(sinkCorfuStores.get(SINK2_INDEX), 6 * NUM_WRITES,
                Collections.singletonList(mergedTable));

        // Delete data from local tables on Sink side
        deleteDataOnSinkLocal( 5 * NUM_WRITES, NUM_WRITES);

        // Delete data from GROUP_A tables on Source side
        deleteDataOnSource(0, 5 * NUM_WRITES);

        // Verify GROUP_A Sink side tables' content has been deleted
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), 0,
                sinkTablesGroupA);

        // Verify Merged tables' content has been deleted
        verifyMergedTableData(sinkCorfuStores.get(SINK2_INDEX), 0,
                Collections.singletonList(mergedTable));
    }

    /**
     * Get the client runtime that connects to Source cluster node.
     *
     * @return CorfuRuntime for client
     */
    private CorfuRuntime getClientRuntime() {
        return CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build())
                .parseConfigurationString(DEFAULT_HOST + ":" + DEFAULT_PORT)
                .connect();
    }

    /**
     * Open GROUP_A table using the given CorfuStore and add it to the list.
     *
     * @param num Number of GROUP_A table to be opened.
     * @param corfuStore CorfuStore that will open the tables.
     * @param tableList List of tables to which the newly opened tables should be added.
     */
    private void openGroupATable(int num, CorfuStore corfuStore,
                                    List<Table<Sample.StringKey, SampleSchema.SampleGroupMsgA, Message>> tableList)
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        for (int i = 1; i <= num; i++) {
            String tableName = GROUPA_TABLE_PREFIX + i;
            Table<Sample.StringKey, SampleSchema.SampleGroupMsgA, Message> groupATable = corfuStore.openTable(
                    NAMESPACE,
                    tableName,
                    Sample.StringKey.class,
                    SampleSchema.SampleGroupMsgA.class,
                    null,
                    TableOptions.fromProtoSchema(SampleSchema.SampleGroupMsgA.class)
            );
            tableList.add(groupATable);
        }
    }

    /**
     * Open local table using the given CorfuStore and add it to the list.
     *
     * @param num Number of local table to be opened.
     * @param corfuStore CorfuStore that will open the tables.
     * @param tableList List of tables to which the newly opened tables should be added.
     */
    private void openLocalTable(int num, CorfuStore corfuStore,
                                 List<Table<Sample.StringKey, SampleSchema.SampleGroupMsgA, Message>> tableList)
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        for (int i = 1; i <= num; i++) {
            String tableName = LOCAL_TABLE_PREFIX + i;
            Table<Sample.StringKey, SampleSchema.SampleGroupMsgA, Message> localTable = corfuStore.openTable(
                    NAMESPACE,
                    tableName,
                    Sample.StringKey.class,
                    SampleSchema.SampleGroupMsgA.class,
                    null,
                    TableOptions.fromProtoSchema(SampleSchema.SampleGroupMsgA.class)
            );
            tableList.add(localTable);
        }
    }


    /**
     * Open merged table using the given CorfuStore.
     *
     * @param corfuStore CorfuStore that will open the tables.
     */
    private void openMergedTable(CorfuStore corfuStore) throws
            InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        mergedTable = corfuStore.openTable(
                NAMESPACE,
                MERGED_TABLE_NAME,
                Sample.StringKey.class,
                SampleSchema.SampleMergedTable.class,
                null,
                TableOptions.fromProtoSchema(SampleSchema.SampleMergedTable.class)
        );
    }

    /**
     * Write data to all the Source side tables uniformly. Note that if any table is supposed to be written
     * particularly, please handle it separately in the test.
     *
     * @param startNum  Start number that will be written to Source side tables.
     * @param numWrites Number of writes added to Source side tables.
     */
    private void writeDataOnSource(int startNum, int numWrites) {
        for (int i = startNum; i < startNum + numWrites; i++) {
            SampleSchema.SampleGroupMsgA groupATablePayload = SampleSchema.SampleGroupMsgA.newBuilder().setPayload(String.valueOf(i)).build();

            try (TxnContext txn = sourceCorfuStores.get(SOURCE_INDEX).txn(NAMESPACE)) {
                for (int j = 0; j < srcTablesGroupA.size(); j++) {
                    Sample.StringKey key = Sample.StringKey.newBuilder().setKey(Integer.toString(i)).build();
                    txn.putRecord(srcTablesGroupA.get(j), key, groupATablePayload, null);
                }
                txn.commit();
            }
        }
    }

    /**
     * Delete data from all the Source side tables uniformly.
     *
     * @param startNum  Start number that will be deleted from Source side tables.
     * @param numWrites Number of writes deleted from Source side tables.
     */
    private void deleteDataOnSource(int startNum, int numWrites) {
        for (int i = startNum; i < startNum + numWrites; i++) {
            try (TxnContext txn = sourceCorfuStores.get(SOURCE_INDEX).txn(NAMESPACE)) {
                for (int j = 0; j < srcTablesGroupA.size(); j++) {
                    Sample.StringKey key = Sample.StringKey.newBuilder().setKey(Integer.toString(i)).build();
                    txn.delete(srcTablesGroupA.get(j), key);
                }
                txn.commit();
            }
        }
    }

    /**
     * Write data to the Sink side GROUP_A tables.
     *
     * @param startNum  Start number that will be written to Sink side GROUP_A tables.
     * @param numWrites Number of writes added to Sink side GROUP_A tables.
     */
    private void writeDataOnSinkGroupA(int startNum, int numWrites) {
        for (int i = startNum; i < startNum + numWrites; i++) {
            SampleSchema.SampleGroupMsgA groupATablePayload = SampleSchema.SampleGroupMsgA.newBuilder().setPayload(String.valueOf(i)).build();

            try (TxnContext txn = sinkCorfuStores.get(SINK2_INDEX).txn(NAMESPACE)) {
                for (int j = 0; j < sinkTablesGroupA.size(); j++) {
                    Sample.StringKey key = Sample.StringKey.newBuilder().setKey(Integer.toString(i)).build();
                    txn.putRecord(sinkTablesGroupA.get(j), key, groupATablePayload, null);
                }
                txn.commit();
            }
        }
    }

    /**
     * Write data to the Sink side local tables.
     *
     * @param startNum  Start number that will be written to Sink side local tables.
     * @param numWrites Number of writes added to Sink side local tables.
     */
    private void writeDataOnSinkLocal(int startNum, int numWrites) {
        for (int i = startNum; i < startNum + numWrites; i++) {
            SampleSchema.SampleGroupMsgA localTablePayload = SampleSchema.SampleGroupMsgA.newBuilder().setPayload(String.valueOf(i)).build();

            try (TxnContext txn = sinkCorfuStores.get(SINK2_INDEX).txn(NAMESPACE)) {
                for (int j = 0; j < sinkTablesLocal.size(); j++) {
                    Sample.StringKey key = Sample.StringKey.newBuilder().setKey(Integer.toString(i)).build();
                    txn.putRecord(sinkTablesLocal.get(j), key, localTablePayload, null);
                }
                txn.commit();
            }
        }
    }

    /**
     * Delete data from the Sink side local tables.
     *
     * @param startNum  Start number that will be deleted from Sink side local tables.
     * @param numWrites Number of writes deleted from Sink side local tables.
     */
    private void deleteDataOnSinkLocal(int startNum, int numWrites) {
        for (int i = startNum; i < startNum + numWrites; i++) {
            try (TxnContext txn = sinkCorfuStores.get(SINK2_INDEX).txn(NAMESPACE)) {
                for (int j = 0; j < sinkTablesLocal.size(); j++) {
                    Sample.StringKey key = Sample.StringKey.newBuilder().setKey(Integer.toString(i)).build();
                        txn.delete(sinkTablesLocal.get(j), key);
                }
                txn.commit();
            }

        }
    }

    /**
     * Verify the replication session with the given Sink is already in log entry sync status.
     *
     * @param sinkIndex  Sink index that indicates which Sink should be verified.
     * @param subscriber Replication subscriber that is supposed to be verified.
     */
    private void verifySessionInLogEntrySyncState(int sinkIndex, LogReplication.ReplicationSubscriber subscriber) {
        LogReplication.LogReplicationSession session = LogReplication.LogReplicationSession.newBuilder()
                .setSourceClusterId(DefaultClusterConfig.getSourceClusterIds().get(SOURCE_INDEX))
                .setSinkClusterId(DefaultClusterConfig.getSinkClusterIds().get(sinkIndex))
                .setSubscriber(subscriber)
                .build();

        LogReplication.ReplicationStatus status = null;

        while (status == null || !status.getSourceStatus().getReplicationInfo().getSyncType().equals(LogReplication.SyncType.LOG_ENTRY)
                || !status.getSourceStatus().getReplicationInfo().getSnapshotSyncInfo().getStatus()
                .equals(LogReplication.SyncStatus.COMPLETED)) {
            try (TxnContext txn = sourceCorfuStores.get(SOURCE_INDEX).txn(LogReplicationMetadataManager.NAMESPACE)) {
                status = (LogReplication.ReplicationStatus) txn.getRecord(REPLICATION_STATUS_TABLE_NAME, session).getPayload();
                txn.commit();
            }
        }

        // Snapshot sync should have completed and log entry sync is ongoing
        assertThat(status.getSourceStatus().getReplicationInfo().getSyncType()).isEqualTo(LogReplication.SyncType.LOG_ENTRY);
        assertThat(status.getSourceStatus().getReplicationInfo().getStatus())
                .isEqualTo(LogReplication.SyncStatus.ONGOING);

        assertThat(status.getSourceStatus().getReplicationInfo().getSnapshotSyncInfo().getType())
                .isEqualTo(LogReplication.SnapshotSyncInfo.SnapshotSyncType.DEFAULT);
        assertThat(status.getSourceStatus().getReplicationInfo().getSnapshotSyncInfo().getStatus())
                .isEqualTo(LogReplication.SyncStatus.COMPLETED);
    }

    /**
     * Verify the count and actual content of a list of GROUP_A tables.
     *
     * @param corfuStore CorfuStore that the given tables were opened from.
     * @param expectedConsecutiveWrites Each table is expected to have a size of expectedConsecutiveWrites, and its
     *                                  content should be from 0 to expectedConsecutiveWrites (exclusive)
     * @param tables A list of federated tables to be verified.
     */
    private void verifyGroupATableData(CorfuStore corfuStore, int expectedConsecutiveWrites,
                                       List<Table<Sample.StringKey, SampleSchema.SampleGroupMsgA, Message>> tables) {
        tables.forEach(table -> {
            log.info("Verify Data on Table {}", table.getFullyQualifiedTableName());

            // Wait until data is fully replicated
            while (table.count() != expectedConsecutiveWrites) {
                // Block until expected number of entries is reached
                log.debug("table count: {}, expectedConsecutiveWrites: {}", table.count(), expectedConsecutiveWrites);
            }

            log.info("Number updates on table {} :: {} ", table.getFullyQualifiedTableName(), expectedConsecutiveWrites);

            // Verify data is present in Sink Site
            assertThat(table.count()).isEqualTo(expectedConsecutiveWrites);

            for (int i = 0; i < (expectedConsecutiveWrites); i++) {
                try (TxnContext tx = corfuStore.txn(table.getNamespace())) {
                    assertThat(tx.getRecord(table,
                            Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build()).getPayload().getPayload())
                            .isEqualTo(String.valueOf(i));
                    tx.commit();
                }
            }
        });
    }

    /**
     * Verify the count and actual content of merged tables.
     *
     * @param corfuStore CorfuStore that the given tables were opened from.
     * @param expectedConsecutiveWrites Each table is expected to have a size of expectedConsecutiveWrites, and its
     *                                  content should be from 0 to expectedConsecutiveWrites (exclusive)
     */
    private void verifyMergedTableData(CorfuStore corfuStore, int expectedConsecutiveWrites,
                                       List<Table<Sample.StringKey, SampleSchema.SampleMergedTable, Message>> tables) {
        tables.forEach(table -> {
            log.info("Verify Data on Table {}", table.getFullyQualifiedTableName());
            // Wait until data is fully replicated
            while (table.count() != expectedConsecutiveWrites) {
                // Block until expected number of entries is reached
                log.debug("table count: {}, expectedConsecutiveWrites: {}", table.count(), expectedConsecutiveWrites);
            }

            log.info("Number updates on table {} :: {} ", table.getFullyQualifiedTableName(), expectedConsecutiveWrites);

            // Verify data is present in Sink Site
            assertThat(table.count()).isEqualTo(expectedConsecutiveWrites);

            for (int i = 0; i < (expectedConsecutiveWrites); i++) {
                try (TxnContext tx = corfuStore.txn(table.getNamespace())) {
                    assertThat(tx.getRecord(table,
                            Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build()).getPayload().getPayload())
                            .isEqualTo(String.valueOf(i));
                    tx.commit();
                }
            }
        });
    }

    private LogReplication.LogReplicationSession getTestSession() {
        LogReplication.ReplicationSubscriber subscriber = LogReplication.ReplicationSubscriber.newBuilder().setClientName(testClientName)
                .setModel(LogReplication.ReplicationModel.LOGICAL_GROUPS)
                .build();
        return LogReplication.LogReplicationSession.newBuilder().setSourceClusterId(defaultClusterId)
                .setSinkClusterId(defaultClusterId).setSubscriber(subscriber)
                .build();
    }

    private LogReplication.ReplicationStatus getTestReplicationStatus(boolean dataConsistent) {
        return LogReplication.ReplicationStatus.newBuilder()
                .setSinkStatus(LogReplication.SinkReplicationStatus.newBuilder().setDataConsistent(dataConsistent))
                .build();
    }

    private void openLogReplicationStatusTable() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        replicationStatusTable = sourceCorfuStores.get(SOURCE_INDEX).openTable(CORFU_SYSTEM_NAMESPACE,
                REPLICATION_STATUS_TABLE_NAME, LogReplication.LogReplicationSession.class,
                LogReplication.ReplicationStatus.class, null,
                TableOptions.fromProtoSchema(LogReplication.ReplicationStatus.class));

        try (TxnContext tx = sourceCorfuStores.get(SOURCE_INDEX).txn(CORFU_SYSTEM_NAMESPACE)) {
            LogReplication.LogReplicationSession key = getTestSession();
            LogReplication.ReplicationStatus val = getTestReplicationStatus(true);
            tx.putRecord(replicationStatusTable, key, val, null);
            tx.commit();
        }
    }
}
