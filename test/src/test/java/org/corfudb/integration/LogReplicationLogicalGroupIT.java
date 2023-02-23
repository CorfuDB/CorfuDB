package org.corfudb.integration;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.SessionManager;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultSnapshotSyncPlugin;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatus;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.SyncStatus;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.SyncType;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.infrastructure.logreplication.proto.Sample.StringKey;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.integration.LogReplicationAbstractIT.SnapshotSyncPluginListener;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.ExampleSchemas.SnapshotSyncPluginValue;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.LogReplication.ReplicationSubscriber;
import org.corfudb.runtime.LogReplicationLogicalGroupClient;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.test.SampleSchema.SampleGroupMsgA;
import org.corfudb.test.SampleSchema.SampleGroupMsgB;
import org.corfudb.test.SampleSchema.ValueFieldTagOne;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.REPLICATION_STATUS_TABLE_NAME;
import static org.junit.Assert.fail;


@Slf4j
@SuppressWarnings("checkstyle:magicnumber")
public class LogReplicationLogicalGroupIT extends CorfuReplicationMultiSourceSinkIT {

    private static final int NUM_WRITES = 500;

    private static final String SAMPLE_CLIENT_NAME = "SAMPLE_CLIENT";

    private static final String GROUP_A = "groupA";

    private static final String GROUP_B = "groupB";

    private static final String FEDERATED_TABLE_PREFIX = "Federated_Table00";

    private static final String GROUPA_TABLE_PREFIX = "GroupA_Table00";

    private static final String GROUPB_TABLE_PREFIX = "GroupB_Table00";

    private static final int SOURCE_INDEX = 0;

    private static final int SINK1_INDEX = 0;

    private static final int SINK2_INDEX = 1;

    private static final int SINK3_INDEX = 2;

    private int numSource = 1;

    private int numSink = 3;

    private final List<Table<StringKey, ValueFieldTagOne, Message>> srcFederatedTables = new ArrayList<>();

    private final List<Table<StringKey, ValueFieldTagOne, Message>> sinkFederatedTables = new ArrayList<>();

    private final List<Table<StringKey, SampleGroupMsgA, Message>> srcTablesGroupA = new ArrayList<>();

    private final List<Table<StringKey, SampleGroupMsgA, Message>> sinkTablesGroupA = new ArrayList<>();

    private final List<Table<StringKey, SampleGroupMsgB, Message>> srcTablesGroupB = new ArrayList<>();

    private final List<Table<StringKey, SampleGroupMsgB, Message>> sinkTablesGroupB = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        setUp(numSource, numSink, DefaultClusterManager.TP_MIXED_MODEL);
        openLogReplicationStatusTable();
    }

    @Test
    public void testLogicalGroupReplicationEndToEnd() throws Exception {
        // Register client and add group destinations
        CorfuRuntime clientRuntime = getClientRuntime();
        LogReplicationLogicalGroupClient logicalGroupClient =
                new LogReplicationLogicalGroupClient(clientRuntime, SAMPLE_CLIENT_NAME);
        logicalGroupClient.addDestination(GROUP_A, DefaultClusterConfig.getSinkClusterIds().get(SINK2_INDEX));
        logicalGroupClient.addDestination(GROUP_B, DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX));

        // Open tables for replication on Source side
        int numTables = 2;
        openFederatedTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcFederatedTables);
        openGroupATable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupA);
        openGroupBTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupB);
        openFederatedTable(numTables, sinkCorfuStores.get(SINK1_INDEX), sinkFederatedTables);
        openGroupATable(numTables, sinkCorfuStores.get(SINK2_INDEX), sinkTablesGroupA);
        openGroupBTable(numTables, sinkCorfuStores.get(SINK3_INDEX), sinkTablesGroupB);

        // Write data to Source side tables
        writeDataOnSource(0, NUM_WRITES);

        // Start log replication for all sessions
        startReplicationServers();

        // Verify all the sessions' snapshot sync completed
        verifyInLogEntrySyncState(SINK1_INDEX, SessionManager.getDefaultSubscriber());
        // TODO: should be replaced by real client name after Sink session creation workflow is introduced
        verifyInLogEntrySyncState(SINK2_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());
        verifyInLogEntrySyncState(SINK3_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());

        // Verify tables' content on Sink side
        verifyFederatedTableDataOnSink(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableDataOnSink(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
        verifyGroupBTableDataOnSink(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);

        // Write more data for log entry sync
        writeDataOnSource(NUM_WRITES, NUM_WRITES);

        // Verify tables' content on Sink side
        int targetWrites = 2 * NUM_WRITES;
        verifyFederatedTableDataOnSink(sinkCorfuStores.get(SINK1_INDEX), targetWrites, sinkFederatedTables);
        verifyGroupATableDataOnSink(sinkCorfuStores.get(SINK2_INDEX), targetWrites, sinkTablesGroupA);
        verifyGroupBTableDataOnSink(sinkCorfuStores.get(SINK3_INDEX), targetWrites, sinkTablesGroupB);
    }

    /**
     * This test has the similar workflow as testNewStreamsInLogEntrySync() in
     * {@link LogReplicationDynamicStreamIT}, with Logical Group feature.
     */
    @Test
    public void testNewStreamsInLogEntrySync() throws Exception {
        clientInitialization();

        int numTables = 2;
        openFederatedTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcFederatedTables);
        openGroupATable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupA);
        openFederatedTable(numTables, sinkCorfuStores.get(SINK1_INDEX), sinkFederatedTables);
        openGroupATable(numTables, sinkCorfuStores.get(SINK2_INDEX), sinkTablesGroupA);

        for (int i = 0; i < NUM_WRITES; i++) {
            StringKey key = StringKey.newBuilder().setKey(String.valueOf(i)).build();
            ValueFieldTagOne federatedTablePayload = ValueFieldTagOne.newBuilder().setPayload(String.valueOf(i)).build();
            SampleGroupMsgA groupATablePayload = SampleGroupMsgA.newBuilder().setPayload(String.valueOf(i)).build();

            try (TxnContext txn = sourceCorfuStores.get(SOURCE_INDEX).txn(NAMESPACE)) {
                for (int j = 0; j < srcFederatedTables.size(); j++) {
                    txn.putRecord(srcFederatedTables.get(j), key, federatedTablePayload, null);
                }
                for (int j = 0; j < srcTablesGroupA.size(); j++) {
                    txn.putRecord(srcTablesGroupA.get(j), key, groupATablePayload, null);
                }
                txn.commit();
            }
        }

        startReplicationServers();

        verifyFederatedTableDataOnSink(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableDataOnSink(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);

        verifyInLogEntrySyncState(SINK1_INDEX, SessionManager.getDefaultSubscriber());
        verifyInLogEntrySyncState(SINK2_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());

        openGroupBTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupB);

        for (int i = 0; i < NUM_WRITES; i++) {
            StringKey key = StringKey.newBuilder().setKey(String.valueOf(i)).build();
            SampleGroupMsgB groupBTablePayload = SampleGroupMsgB.newBuilder().setPayload(String.valueOf(i)).build();

            try (TxnContext txn = sourceCorfuStores.get(SOURCE_INDEX).txn(NAMESPACE)) {
                for (int j = 0; j < srcTablesGroupB.size(); j++) {
                    txn.putRecord(srcTablesGroupB.get(j), key, groupBTablePayload, null);
                }
                txn.commit();
            }
        }

        openGroupBTable(numTables, sinkCorfuStores.get(SINK3_INDEX), sinkTablesGroupB);
        verifyGroupBTableDataOnSink(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);
    }

    /**
     * This test has the same workflow as testSinkLocalWritesClearing() in
     * {@link LogReplicationDynamicStreamIT}, with Logical Group feature.
     */
    @Test
    public void testLocalWrite() throws Exception {
        // Set up the runtimes to each server and the model
        setUp(numSource, numSink, DefaultClusterManager.TP_MIXED_MODEL);
        // Register client
        clientInitialization();

        // Open tables on sources and sinks
        openTablesOnSource(true, true, false);
        openTablesOnSink(true, true, false);

        // Write data on Sources
        for (int i = 0; i < NUM_WRITES; i++) {
            Sample.StringKey key = Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build();
            ValueFieldTagOne federatedTablePayload = ValueFieldTagOne.newBuilder().setPayload(String.valueOf(i)).build();
            SampleGroupMsgA groupATablePayload = SampleGroupMsgA.newBuilder().setPayload(String.valueOf(i)).build();
            try (TxnContext txn = sourceCorfuStores.get(0).txn(NAMESPACE)) {
                txn.putRecord(srcFederatedTables.get(0), key, federatedTablePayload, null);
                txn.putRecord(srcTablesGroupA.get(0), key, groupATablePayload, null);
                txn.commit();
            }
        }

        // Write local data on Sinks
        for (int i = NUM_WRITES; i < NUM_WRITES + NUM_WRITES/2 ; i++) {
            Sample.StringKey key = Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build();
            ValueFieldTagOne federatedTablePayload = ValueFieldTagOne.newBuilder().setPayload(String.valueOf(i)).build();
            SampleGroupMsgA groupATablePayload = SampleGroupMsgA.newBuilder().setPayload(String.valueOf(i)).build();
            try (TxnContext txn = sinkCorfuStores.get(0).txn(NAMESPACE)) {
                txn.putRecord(sinkFederatedTables.get(0), key, federatedTablePayload, null);
                txn.putRecord(sinkTablesGroupA.get(0), key, groupATablePayload, null);
                txn.commit();
            }
        }

        // Open table on sink
        openTablesOnSink(false, false, true);

        // Write local data on sink
        for (int i = 0; i < NUM_WRITES; i++) {
            Sample.StringKey key = Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build();
            SampleGroupMsgB groupBTablePayload = SampleGroupMsgB.newBuilder().setPayload(String.valueOf(i)).build();
            try (TxnContext txn = sinkCorfuStores.get(0).txn(NAMESPACE)) {
                txn.putRecord(sinkTablesGroupB.get(0), key, groupBTablePayload, null);
                txn.commit();
            }
        }

        // Start the LR
        startReplicationServers();

        // Verify that the data is replicated
        verifyFederatedTableDataOnSink(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableDataOnSink(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
        // Verify that the local data is overwritten after replication
        verifyGroupBTableDataOnSink(sinkCorfuStores.get(SINK3_INDEX), 0, sinkTablesGroupB);

        // Verify that the local data is overwritten after replication
        for (int i = NUM_WRITES; i < NUM_WRITES + NUM_WRITES/2 ; i++) {
            Sample.StringKey key = Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build();
            try (TxnContext txn = sinkCorfuStores.get(0).txn(NAMESPACE)) {
                assertThat(txn.getRecord(sinkFederatedTables.get(0), key).getPayload()).isNull();
                assertThat(txn.getRecord(sinkTablesGroupA.get(0), key).getPayload()).isNull();
                txn.commit();
            }
        }
    }

    /**
     * This test has the similar workflow as testSnapshotAndLogEntrySync() in
     * {@link LogReplicationDynamicStreamIT}, with Logical Group feature.
     */
    @Test
    public void testSourceNewTablesReplication() throws Exception {
        // Open tables on source
        openTablesOnSource(true, true, true);

        //Initialize Client and set the destination
        clientInitialization();

        // Write data to tables opened on each source cluster
        writeDataOnSource(0, NUM_WRITES);

        // Assert that data is in the source
        for (int i = 0; i < NUM_WRITES; i++) {
            Sample.StringKey key = Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build();
            try (TxnContext txn = sourceCorfuStores.get(0).txn(NAMESPACE)) {
                assertThat(txn.getRecord(srcFederatedTables.get(0), key).getPayload()).isNotNull();
                assertThat(txn.getRecord(srcTablesGroupA.get(0), key).getPayload()).isNotNull();
                assertThat(txn.getRecord(srcTablesGroupB.get(0), key).getPayload()).isNotNull();
                txn.commit();
            }
        }

        // Start the replication
        startReplicationServers();

        // Open tables on sink
        openTablesOnSink(true, true, true);

        while (sinkFederatedTables.get(0).count() != NUM_WRITES) {
            // Block until expected number of entries is reached
        }
        while (sinkTablesGroupA.get(0).count() != NUM_WRITES) {
            // Block until expected number of entries is reached
        }
        while (sinkTablesGroupB.get(0).count() != NUM_WRITES) {
            // Block until expected number of entries is reached
        }

        // Assert that data is in the sink
        for (int i = 0; i < NUM_WRITES; i++) {
            Sample.StringKey key = Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build();
            try (TxnContext txn = sinkCorfuStores.get(0).txn(NAMESPACE)) {
                assertThat(txn.getRecord(sinkFederatedTables.get(0), key).getPayload()).isNotNull();
                assertThat(txn.getRecord(sinkTablesGroupA.get(0), key).getPayload());
                assertThat(txn.getRecord(sinkTablesGroupB.get(0), key).getPayload());
                txn.commit();
            }
        }

        writeDataOnSource(NUM_WRITES, NUM_WRITES);

        // Assert that data is in the source
        for (int i = 0; i < 2 * NUM_WRITES; i++) {
            Sample.StringKey key = Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build();
            try (TxnContext txn = sourceCorfuStores.get(0).txn(NAMESPACE)) {
                assertThat(txn.getRecord(srcFederatedTables.get(0), key).getPayload()).isNotNull();
                assertThat(txn.getRecord(srcTablesGroupA.get(0), key).getPayload());
                assertThat(txn.getRecord(srcTablesGroupB.get(0), key).getPayload());
                txn.commit();
            }
        }

        // Wait until data is fully replicated
        while (sinkFederatedTables.get(0).count() != 2 * NUM_WRITES) {
            // Block until expected number of entries is reached
        }
        while (sinkTablesGroupA.get(0).count() != 2 * NUM_WRITES) {
            // Block until expected number of entries is reached
        }
        while (sinkTablesGroupB.get(0).count() != 2 * NUM_WRITES) {
            // Block until expected number of entries is reached
        }

        assertThat(sinkFederatedTables.get(0).count()).isEqualTo(2 * NUM_WRITES);
        assertThat(sinkTablesGroupA.get(0).count()).isEqualTo(2 * NUM_WRITES);
        assertThat(sinkTablesGroupB.get(0).count()).isEqualTo(2 * NUM_WRITES);

        int targetWrites = 2 * NUM_WRITES;

        verifyFederatedTableDataOnSink(sinkCorfuStores.get(SINK1_INDEX), targetWrites, sinkFederatedTables);
        verifyGroupATableDataOnSink(sinkCorfuStores.get(SINK2_INDEX), targetWrites, sinkTablesGroupA);
        verifyGroupBTableDataOnSink(sinkCorfuStores.get(SINK3_INDEX), targetWrites, sinkTablesGroupB);


    }

    @Test
    public void testSinkSideStreaming() {
        // TODO
    }

    @Test
    public void testGroupDestinationChange() throws Exception {
        // Register client and add group destinations
        CorfuRuntime clientRuntime = getClientRuntime();
        LogReplicationLogicalGroupClient logicalGroupClient =
                new LogReplicationLogicalGroupClient(clientRuntime, SAMPLE_CLIENT_NAME);
        logicalGroupClient.addDestination(GROUP_A, DefaultClusterConfig.getSinkClusterIds().get(SINK2_INDEX));
        logicalGroupClient.addDestination(GROUP_B, DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX));

        // Open tables for replication on Source side
        int numTables = 2;
        openFederatedTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcFederatedTables);
        openGroupATable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupA);
        openGroupBTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupB);
        openFederatedTable(numTables, sinkCorfuStores.get(SINK1_INDEX), sinkFederatedTables);
        openGroupATable(numTables, sinkCorfuStores.get(SINK2_INDEX), sinkTablesGroupA);
        openGroupBTable(numTables, sinkCorfuStores.get(SINK3_INDEX), sinkTablesGroupB);

        // Subscribe snapshot plugin listener on each Sink cluster, Sink1 will only have 1 snapshot sync while Sink2
        // and Sink3 will have 2 snapshot sync for each, where 1 snapshot sync will update the listener twice.
        CountDownLatch latchSink1 = new CountDownLatch(2);
        SnapshotSyncPluginListener snapshotSyncPluginListenerSink1 = new SnapshotSyncPluginListener(latchSink1);
        subscribeToSnapshotSyncPluginTable(sinkCorfuStores.get(SINK1_INDEX), snapshotSyncPluginListenerSink1);

        CountDownLatch latchSink2 = new CountDownLatch(4);
        SnapshotSyncPluginListener snapshotSyncPluginListenerSink2 = new SnapshotSyncPluginListener(latchSink2);
        subscribeToSnapshotSyncPluginTable(sinkCorfuStores.get(SINK2_INDEX), snapshotSyncPluginListenerSink2);

        CountDownLatch latchSink3 = new CountDownLatch(4);
        SnapshotSyncPluginListener snapshotSyncPluginListenerSink3 = new SnapshotSyncPluginListener(latchSink3);
        subscribeToSnapshotSyncPluginTable(sinkCorfuStores.get(SINK3_INDEX), snapshotSyncPluginListenerSink3);

        // Write data to Source side tables
        writeDataOnSource(0, NUM_WRITES);

        // Start log replication for all sessions
        startReplicationServers();

        // Verify all the sessions' snapshot sync completed
        verifyInLogEntrySyncState(SINK1_INDEX, SessionManager.getDefaultSubscriber());
        // TODO: should be replaced by real client name after Sink session creation workflow is introduced
        verifyInLogEntrySyncState(SINK2_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());
        verifyInLogEntrySyncState(SINK3_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());

        // Verify tables' content on Sink side
        verifyFederatedTableDataOnSink(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableDataOnSink(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
        verifyGroupBTableDataOnSink(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);

        // Add groupA to Sink3 and verify the groupA tables' data is successfully replicated to Sink3.
        logicalGroupClient.addDestination(GROUP_A, DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX));
        List<Table<StringKey, SampleGroupMsgA, Message>> sinkTablesGroupAOnSink3 = new ArrayList<>();
        openGroupATable(numTables, sinkCorfuStores.get(SINK3_INDEX), sinkTablesGroupAOnSink3);
        verifyGroupATableDataOnSink(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupAOnSink3);

        // Write more data for log entry sync
        writeDataOnSource(NUM_WRITES, NUM_WRITES);

        // Verify tables' content on Sink side
        int targetWrites = 2 * NUM_WRITES;
        verifyFederatedTableDataOnSink(sinkCorfuStores.get(SINK1_INDEX), targetWrites, sinkFederatedTables);
        verifyGroupATableDataOnSink(sinkCorfuStores.get(SINK2_INDEX), targetWrites, sinkTablesGroupA);
        verifyGroupATableDataOnSink(sinkCorfuStores.get(SINK3_INDEX), targetWrites, sinkTablesGroupAOnSink3);
        verifyGroupBTableDataOnSink(sinkCorfuStores.get(SINK3_INDEX), targetWrites, sinkTablesGroupB);

        // Add groupB to Sink2 and verify the groupB tables' data is successfully replicated to Sink2.
        logicalGroupClient.addDestination(GROUP_B, DefaultClusterConfig.getSinkClusterIds().get(SINK2_INDEX));
        List<Table<StringKey, SampleGroupMsgB, Message>> sinkTablesGroupBOnSink2 = new ArrayList<>();
        openGroupBTable(numTables, sinkCorfuStores.get(SINK2_INDEX), sinkTablesGroupBOnSink2);
        verifyGroupBTableDataOnSink(sinkCorfuStores.get(SINK2_INDEX), targetWrites, sinkTablesGroupBOnSink2);

        // Check Sink clusters snapshot plugin listeners received expected updates.
        latchSink1.await();
        latchSink2.await();
        latchSink3.await();
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
     * Open replication status table on each Sink for verify replication status.
     */
    private void openLogReplicationStatusTable() throws Exception {
        for (int i = 0; i < numSource; i++) {
            sourceCorfuStores.get(i).openTable(
                    LogReplicationMetadataManager.NAMESPACE,
                    REPLICATION_STATUS_TABLE_NAME,
                    LogReplicationSession.class,
                    ReplicationStatus.class,
                    null,
                    TableOptions.fromProtoSchema(ReplicationStatus.class)
            );
        }
    }

    private void openFederatedTable(int num, CorfuStore corfuStore,
                                    List<Table<StringKey, ValueFieldTagOne, Message>> tableList)
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        for (int i = 1; i <= num; i++) {
            String tableName = FEDERATED_TABLE_PREFIX + i;
            Table<StringKey, ValueFieldTagOne, Message> federatedTable = corfuStore.openTable(
                    NAMESPACE,
                    tableName,
                    StringKey.class,
                    ValueFieldTagOne.class,
                    null,
                    TableOptions.fromProtoSchema(ValueFieldTagOne.class)
            );
            tableList.add(federatedTable);
        }
    }

    private void openGroupATable(int num, CorfuStore corfuStore,
                                 List<Table<StringKey, SampleGroupMsgA, Message>> tableList)
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        for (int i = 1; i <= num; i++) {
            String tableName = GROUPA_TABLE_PREFIX + i;
            Table<StringKey, SampleGroupMsgA, Message> groupATable = corfuStore.openTable(
                    NAMESPACE,
                    tableName,
                    StringKey.class,
                    SampleGroupMsgA.class,
                    null,
                    TableOptions.fromProtoSchema(SampleGroupMsgA.class)
            );
            tableList.add(groupATable);
        }
    }

    private void openGroupBTable(int num, CorfuStore corfuStore,
                                 List<Table<StringKey, SampleGroupMsgB, Message>> tableList)
            throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        for (int i = 1; i <= num; i++) {
            String tableName = GROUPB_TABLE_PREFIX + i;
            Table<StringKey, SampleGroupMsgB, Message> groupBTable = corfuStore.openTable(
                    NAMESPACE,
                    tableName,
                    StringKey.class,
                    SampleGroupMsgB.class,
                    null,
                    TableOptions.fromProtoSchema(SampleGroupMsgB.class)
            );
            tableList.add(groupBTable);
        }
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
            StringKey key = StringKey.newBuilder().setKey(String.valueOf(i)).build();
            ValueFieldTagOne federatedTablePayload = ValueFieldTagOne.newBuilder().setPayload(String.valueOf(i)).build();
            SampleGroupMsgA groupATablePayload = SampleGroupMsgA.newBuilder().setPayload(String.valueOf(i)).build();
            SampleGroupMsgB groupBTablePayload = SampleGroupMsgB.newBuilder().setPayload(String.valueOf(i)).build();

            try (TxnContext txn = sourceCorfuStores.get(SOURCE_INDEX).txn(NAMESPACE)) {
                for (int j = 0; j < srcFederatedTables.size(); j++) {
                    txn.putRecord(srcFederatedTables.get(j), key, federatedTablePayload, null);
                }
                for (int j = 0; j < srcTablesGroupA.size(); j++) {
                    txn.putRecord(srcTablesGroupA.get(j), key, groupATablePayload, null);
                }
                for (int j = 0; j < srcTablesGroupB.size(); j++) {
                    txn.putRecord(srcTablesGroupB.get(j), key, groupBTablePayload, null);
                }
                txn.commit();
            }
        }
    }

    /**
     * Verify the given Sink is already in log entry sync status.
     *
     * @param sinkIndex  Sink index that indicates which Sink should be verified.
     * @param subscriber Replication subscriber that is supposed to be verifeid.
     * @throws InterruptedException InterruptedException
     */
    private void verifyInLogEntrySyncState(int sinkIndex, ReplicationSubscriber subscriber) throws InterruptedException {
        LogReplicationSession session = LogReplicationSession.newBuilder()
                .setSourceClusterId(new DefaultClusterConfig().getSourceClusterIds().get(SOURCE_INDEX))
                .setSinkClusterId(new DefaultClusterConfig().getSinkClusterIds().get(sinkIndex))
                .setSubscriber(subscriber)
                .build();

        ReplicationStatus status = null;

        while (status == null || !status.getSourceStatus().getReplicationInfo().getSyncType().equals(SyncType.LOG_ENTRY)
                || !status.getSourceStatus().getReplicationInfo().getSnapshotSyncInfo().getStatus()
                .equals(SyncStatus.COMPLETED)) {
            TimeUnit.SECONDS.sleep(1);
            try (TxnContext txn = sourceCorfuStores.get(SOURCE_INDEX).txn(LogReplicationMetadataManager.NAMESPACE)) {
                status = (ReplicationStatus) txn.getRecord(REPLICATION_STATUS_TABLE_NAME, session).getPayload();
                txn.commit();
            }
        }

        // Snapshot sync should have completed and log entry sync is ongoing
        assertThat(status.getSourceStatus().getReplicationInfo().getSyncType()).isEqualTo(SyncType.LOG_ENTRY);
        assertThat(status.getSourceStatus().getReplicationInfo().getStatus())
                .isEqualTo(SyncStatus.ONGOING);

        assertThat(status.getSourceStatus().getReplicationInfo().getSnapshotSyncInfo().getType())
                .isEqualTo(LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType.DEFAULT);
        assertThat(status.getSourceStatus().getReplicationInfo().getSnapshotSyncInfo().getStatus())
                .isEqualTo(SyncStatus.COMPLETED);
    }

    private void verifyFederatedTableDataOnSink(CorfuStore corfuStoreSink, int expectedConsecutiveWrites,
                                  List<Table<StringKey, ValueFieldTagOne, Message>> sinkTables) {
        sinkTables.forEach(table -> {
            log.info("Verify Data on Sink's Table {}", table.getFullyQualifiedTableName());

            // Wait until data is fully replicated
            while (table.count() != expectedConsecutiveWrites) {
                // Block until expected number of entries is reached
            }

            log.info("Number updates on Sink Map {} :: {} ", table.getFullyQualifiedTableName(), expectedConsecutiveWrites);

            // Verify data is present in Sink Site
            assertThat(table.count()).isEqualTo(expectedConsecutiveWrites);

            for (int i = 0; i < (expectedConsecutiveWrites); i++) {
                try (TxnContext tx = corfuStoreSink.txn(table.getNamespace())) {
                    assertThat(tx.getRecord(table,
                            Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build()).getPayload().getPayload())
                            .isEqualTo(String.valueOf(i));
                    tx.commit();
                }
            }
        });
    }

    private void verifyGroupATableDataOnSink(CorfuStore corfuStoreSink, int expectedConsecutiveWrites,
                                                List<Table<StringKey, SampleGroupMsgA, Message>> sinkTables) {
        sinkTables.forEach(table -> {
            log.info("Verify Data on Sink's Table {}", table.getFullyQualifiedTableName());

            // Wait until data is fully replicated
            while (table.count() != expectedConsecutiveWrites) {
                // Block until expected number of entries is reached
            }

            log.info("Number updates on Sink Map {} :: {} ", table.getFullyQualifiedTableName(), expectedConsecutiveWrites);

            // Verify data is present in Sink Site
            assertThat(table.count()).isEqualTo(expectedConsecutiveWrites);

            for (int i = 0; i < (expectedConsecutiveWrites); i++) {
                try (TxnContext tx = corfuStoreSink.txn(table.getNamespace())) {
                    assertThat(tx.getRecord(table,
                            Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build()).getPayload().getPayload())
                            .isEqualTo(String.valueOf(i));
                    tx.commit();
                }
            }
        });
    }

    private void verifyGroupBTableDataOnSink(CorfuStore corfuStoreSink, int expectedConsecutiveWrites,
                                             List<Table<StringKey, SampleGroupMsgB, Message>> sinkTables) {
        sinkTables.forEach(table -> {
            log.info("Verify Data on Sink's Table {}", table.getFullyQualifiedTableName());

            // Wait until data is fully replicated
            while (table.count() != expectedConsecutiveWrites) {
                // Block until expected number of entries is reached
            }

            log.info("Number updates on Sink Map {} :: {} ", table.getFullyQualifiedTableName(), expectedConsecutiveWrites);

            // Verify data is present in Sink Site
            assertThat(table.count()).isEqualTo(expectedConsecutiveWrites);

            for (int i = 0; i < (expectedConsecutiveWrites); i++) {
                try (TxnContext tx = corfuStoreSink.txn(table.getNamespace())) {
                    assertThat(tx.getRecord(table,
                            Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build()).getPayload().getPayload())
                            .isEqualTo(String.valueOf(i));
                    tx.commit();
                }
            }
        });
    }

    private void subscribeToSnapshotSyncPluginTable(CorfuStore sinkCorfuStore, SnapshotSyncPluginListener listener) {
        try {
            sinkCorfuStore.openTable(DefaultSnapshotSyncPlugin.NAMESPACE,
                    DefaultSnapshotSyncPlugin.TABLE_NAME, ExampleSchemas.Uuid.class, SnapshotSyncPluginValue.class,
                    SnapshotSyncPluginValue.class, TableOptions.fromProtoSchema(SnapshotSyncPluginValue.class));
            sinkCorfuStore.subscribeListener(listener, DefaultSnapshotSyncPlugin.NAMESPACE, DefaultSnapshotSyncPlugin.TAG);
        } catch (Exception e) {
            fail("Exception while attempting to subscribe to snapshot sync plugin table");
        }
    }

    /**
     * A helper method for the testSourceNewTablesReplication(). It opens the federated table,
     * table for group A, table for group B on the Source.
     */
    private void openTablesOnSource(boolean federated, boolean groupA, boolean groupB) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        Table<Sample.StringKey, ValueFieldTagOne, Message> federatedTable;
        Table<Sample.StringKey, SampleGroupMsgA, Message> tableGroupA;
        Table<Sample.StringKey, SampleGroupMsgB, Message> tableGroupB;
        CorfuStore srcCorfuStore = sourceCorfuStores.get(0);
        int federatedTableIndex = 0;
        int groupATableIndex = 1;
        int groupBTableIndex = 2;

        if (federated) {
            federatedTable = srcCorfuStore.openTable(NAMESPACE, tableNames.get(federatedTableIndex),
                    Sample.StringKey.class, ValueFieldTagOne.class,
                    null, TableOptions.fromProtoSchema(ValueFieldTagOne.class));
            srcFederatedTables.add(federatedTable);
        }

        if (groupA) {
            tableGroupA = srcCorfuStore.openTable(NAMESPACE, tableNames.get(groupATableIndex),
                    Sample.StringKey.class, SampleGroupMsgA.class,
                    null, TableOptions.fromProtoSchema(SampleGroupMsgA.class));
            srcTablesGroupA.add(tableGroupA);
        }

        if (groupB) {
            tableGroupB = srcCorfuStore.openTable(NAMESPACE, tableNames.get(groupBTableIndex),
                    Sample.StringKey.class, SampleGroupMsgB.class,
                    null, TableOptions.fromProtoSchema(SampleGroupMsgB.class));
            srcTablesGroupB.add(tableGroupB);
        }
    }

    /**
     * A helper method for the testSourceNewTablesReplication(). It opens the federated table,
     * table for group A, table for group B on the sink.
     */
    private void openTablesOnSink(boolean federated, boolean groupA, boolean groupB) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        Table<Sample.StringKey, ValueFieldTagOne, Message> federatedTable;
        Table<Sample.StringKey, SampleGroupMsgA, Message> tableGroupA;
        Table<Sample.StringKey, SampleGroupMsgB, Message> tableGroupB;

        int federatedTableIndex = 0;
        int groupATableIndex = 1;
        int groupBTableIndex = 2;

        if (federated) {
            federatedTable = sinkCorfuStores.get(federatedTableIndex).openTable(NAMESPACE, tableNames.get(federatedTableIndex),
                    Sample.StringKey.class, ValueFieldTagOne.class,
                    null, TableOptions.fromProtoSchema(ValueFieldTagOne.class));
            sinkFederatedTables.add(federatedTable);
        }

        if (groupA) {
            tableGroupA = sinkCorfuStores.get(groupATableIndex).openTable(NAMESPACE, tableNames.get(groupATableIndex),
                    Sample.StringKey.class, SampleGroupMsgA.class,
                    null, TableOptions.fromProtoSchema(SampleGroupMsgA.class));
            sinkTablesGroupA.add(tableGroupA);
        }

        if (groupB) {
            tableGroupB = sinkCorfuStores.get(groupBTableIndex).openTable(NAMESPACE, tableNames.get(groupBTableIndex),
                    Sample.StringKey.class, SampleGroupMsgB.class,
                    null, TableOptions.fromProtoSchema(SampleGroupMsgB.class));
            sinkTablesGroupB.add(tableGroupB);
        }
    }

    /**
     * A helper method for testSourceNewTablesReplication(). It initializes a client, then add the destinations
     * to logical group A and B.
     */
    private void clientInitialization() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        CorfuRuntime clientRuntime = getClientRuntime();
        LogReplicationLogicalGroupClient logicalGroupClient =
                new LogReplicationLogicalGroupClient(clientRuntime, SAMPLE_CLIENT_NAME);
        logicalGroupClient.addDestination(GROUP_A, DefaultClusterConfig.getSinkClusterIds().get(1));
        logicalGroupClient.addDestination(GROUP_A, DefaultClusterConfig.getSinkClusterIds().get(2));
        logicalGroupClient.addDestination(GROUP_B, DefaultClusterConfig.getSinkClusterIds().get(2));
    }
}
