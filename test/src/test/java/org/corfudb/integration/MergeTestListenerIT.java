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

    private static final int SOURCE_INDEX = 0;

    private static final int SINK2_INDEX = 1;

    private static final int SINK3_INDEX = 2;

    private static final int numSource = 1;

    private static final int numSink = 3;

    private final String defaultClusterId = UUID.randomUUID().toString();
    private final String testClientName = "lr_test_client";

    Table<LogReplication.LogReplicationSession, LogReplication.ReplicationStatus, Message> replicationStatusTable;

    Table<Sample.StringKey, SampleSchema.SampleMergedTable, Message> mergedTable;

    /**
     * Note that the following lists are used for storing the references of tables opened on Source or Sink side.
     * They do not provide guarantees to guard against from which side the tables are actually opened.
     */



    // A list of tables open on Source side whose logical group is GROUP_A
    private final List<Table<Sample.StringKey, SampleSchema.SampleGroupMsgA, Message>> srcTablesGroupA = new ArrayList<>();

    // A list of tables open on Sink side whose logical group is GROUP_A
    private final List<Table<Sample.StringKey, SampleSchema.SampleGroupMsgA, Message>> sinkTablesGroupA = new ArrayList<>();

    // A list of tables open on Sink side whose logical group is GROUP_B
    private final List<Table<Sample.StringKey, SampleSchema.SampleGroupMsgA, Message>> sinkLocalTables = new ArrayList<>();


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
        openLocalTable(numTables, sinkCorfuStores.get(SINK2_INDEX), sinkLocalTables);
        openMergedTable(sinkCorfuStores.get(SINK2_INDEX));

        writeDataOnSource(0, NUM_WRITES);

        startReplicationServers();

        verifySessionInLogEntrySyncState(SINK2_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);

        writeDataOnSink(NUM_WRITES, NUM_WRITES);

        MergeTestListener listenerSink2 = new MergeTestListener(sinkCorfuStores.get(SINK2_INDEX)
                , NAMESPACE, numTables,
                numTables);

        // Subscribe the listeners on the sink side
        sinkCorfuStores.get(SINK2_INDEX).subscribeLogReplicationListener(listenerSink2,
                NAMESPACE, STREAMING_TAG, sinkCorfuStores.get(SINK2_INDEX));

        // Verify the listeners get all the update
        verifyMergedTableData(sinkCorfuStores.get(SINK2_INDEX), 2 * NUM_WRITES,
                        Collections.singletonList(mergedTable));

        writeDataOnSource(2*NUM_WRITES, NUM_WRITES);

        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), 3 * NUM_WRITES, sinkTablesGroupA);
        verifyMergedTableData(sinkCorfuStores.get(SINK2_INDEX), 3 * NUM_WRITES,
                Collections.singletonList(mergedTable));

        writeDataOnSink(3*NUM_WRITES, NUM_WRITES);
        verifyMergedTableData(sinkCorfuStores.get(SINK2_INDEX), 4 * NUM_WRITES,
                Collections.singletonList(mergedTable));
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), 4 * NUM_WRITES, sinkTablesGroupA);

        logicalGroupClient.addDestinations(GROUP_A,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX)));
        List<Table<Sample.StringKey, SampleSchema.SampleGroupMsgA, Message>> sinkTablesGroupAOnSink3 = new ArrayList<>();
        openGroupATable(numTables, sinkCorfuStores.get(SINK3_INDEX), sinkTablesGroupAOnSink3);

        writeDataOnSource(NUM_WRITES, NUM_WRITES);

        verifyGroupATableData(sinkCorfuStores.get(SINK3_INDEX), 3 * NUM_WRITES, sinkTablesGroupAOnSink3);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), 4 * NUM_WRITES, sinkTablesGroupA);
        verifyMergedTableData(sinkCorfuStores.get(SINK2_INDEX), 4 * NUM_WRITES,
                Collections.singletonList(mergedTable));

        logicalGroupClient.removeDestinations(GROUP_A,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX)));

        writeDataOnSource(3*NUM_WRITES, NUM_WRITES);

        verifyGroupATableData(sinkCorfuStores.get(SINK3_INDEX), 3 * NUM_WRITES, sinkTablesGroupAOnSink3);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), 4 * NUM_WRITES, sinkTablesGroupA);
        verifyMergedTableData(sinkCorfuStores.get(SINK2_INDEX), 4 * NUM_WRITES,
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

    private void writeDataOnSink(int startNum, int numWrites) {
        for (int i = startNum; i < startNum + numWrites; i++) {
            SampleSchema.SampleGroupMsgA groupATablePayload = SampleSchema.SampleGroupMsgA.newBuilder().setPayload(String.valueOf(i)).build();
            SampleSchema.SampleGroupMsgA localTablePayload = SampleSchema.SampleGroupMsgA.newBuilder().setPayload(String.valueOf(i)).build();

            try (TxnContext txn = sinkCorfuStores.get(SINK2_INDEX).txn(NAMESPACE)) {
                for (int j = 0; j < sinkTablesGroupA.size(); j++) {
                    Sample.StringKey key = Sample.StringKey.newBuilder().setKey(Integer.toString(i)).build();
                    txn.putRecord(sinkTablesGroupA.get(j), key, groupATablePayload, null);
                }

                for (int j = 0; j < sinkLocalTables.size(); j++) {
                    Sample.StringKey key = Sample.StringKey.newBuilder().setKey(Integer.toString(i)).build();
                    txn.putRecord(sinkLocalTables.get(j), key, localTablePayload, null);
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
