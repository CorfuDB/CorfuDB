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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.REPLICATION_STATUS_TABLE_NAME;
import static org.corfudb.integration.LogReplicationAbstractIT.NAMESPACE;
import static org.corfudb.integration.LogReplicationAbstractIT.checkpointAndTrimCorfuStore;
import static org.junit.Assert.fail;


@Slf4j
@SuppressWarnings("checkstyle:magicnumber")
public class LogReplicationLogicalGroupIT extends CorfuReplicationMultiSourceSinkIT {

    private static final int NUM_WRITES = 500;

    private static final String SAMPLE_CLIENT_NAME = "SAMPLE_CLIENT";

    private static final String GROUP_A = "groupA";

    private static final String GROUP_B = "groupB";

    private static final String STREAMING_TAG = "tag_one";

    private static final String FEDERATED_TABLE_PREFIX = "Federated_Table00";

    private static final String GROUPA_TABLE_PREFIX = "GroupA_Table00";

    private static final String GROUPB_TABLE_PREFIX = "GroupB_Table00";

    private static final int SLEEP_INTERVAL = 3;

    private static final int SOURCE_INDEX = 0;

    private static final int SINK1_INDEX = 0;

    private static final int SINK2_INDEX = 1;

    private static final int SINK3_INDEX = 2;

    private static final int numSource = 1;

    private static final int numSink = 3;

    private final List<Table<StringKey, ValueFieldTagOne, Message>> srcFederatedTables = new ArrayList<>();

    private final List<Table<StringKey, ValueFieldTagOne, Message>> sinkFederatedTables = new ArrayList<>();

    private final List<Table<StringKey, SampleGroupMsgA, Message>> srcTablesGroupA = new ArrayList<>();

    private final List<Table<StringKey, SampleGroupMsgA, Message>> sinkTablesGroupA = new ArrayList<>();

    private final List<Table<StringKey, SampleGroupMsgB, Message>> srcTablesGroupB = new ArrayList<>();

    private final List<Table<StringKey, SampleGroupMsgB, Message>> sinkTablesGroupB = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        setUp(numSource, numSink, DefaultClusterManager.TP_MIXED_MODEL_THREE_SINK);
        openLogReplicationStatusTable();
    }

    @Test
    public void testLogicalGroupReplicationEndToEnd() throws Exception {
        // Register client and add group destinations
        CorfuRuntime clientRuntime = getClientRuntime();
        LogReplicationLogicalGroupClient logicalGroupClient =
                new LogReplicationLogicalGroupClient(clientRuntime, SAMPLE_CLIENT_NAME);
        logicalGroupClient.setDestinations(GROUP_A,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK2_INDEX)));
        logicalGroupClient.setDestinations(GROUP_B,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX)));

        // Open tables for replication on both side
        int numTables = 2;
        // Source side
        openFederatedTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcFederatedTables);
        openGroupATable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupA);
        openGroupBTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupB);
        // Sink side
        openFederatedTable(numTables, sinkCorfuStores.get(SINK1_INDEX), sinkFederatedTables);
        openGroupATable(numTables, sinkCorfuStores.get(SINK2_INDEX), sinkTablesGroupA);
        openGroupBTable(numTables, sinkCorfuStores.get(SINK3_INDEX), sinkTablesGroupB);

        // Write data to Source side tables
        writeDataOnSource(0, NUM_WRITES);

        // Start log replication for all sessions
        startReplicationServers();

        // Verify all the sessions' snapshot sync completed
        verifySessionInLogEntrySyncState(SINK1_INDEX, SessionManager.getDefaultSubscriber());
        // TODO: should be replaced by real client name after Sink session creation workflow is introduced
        verifySessionInLogEntrySyncState(SINK2_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());
        verifySessionInLogEntrySyncState(SINK3_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());

        // Verify tables' content on Sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);

        // Write more data for log entry sync
        writeDataOnSource(NUM_WRITES, NUM_WRITES);

        // Verify tables' content on Sink side
        int targetWrites = 2 * NUM_WRITES;
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), targetWrites, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), targetWrites, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), targetWrites, sinkTablesGroupB);
    }

    @Test
    public void testNoGroupSetupBeforeLogReplication() throws Exception {
        // Register client
        CorfuRuntime clientRuntime = getClientRuntime();
        LogReplicationLogicalGroupClient logicalGroupClient =
                new LogReplicationLogicalGroupClient(clientRuntime, SAMPLE_CLIENT_NAME);

        // Open tables on both side
        int numTables = 2;
        // Source side
        openFederatedTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcFederatedTables);
        openGroupATable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupA);
        openGroupBTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupB);
        // Sink side
        openFederatedTable(numTables, sinkCorfuStores.get(SINK1_INDEX), sinkFederatedTables);
        openGroupATable(numTables, sinkCorfuStores.get(SINK2_INDEX), sinkTablesGroupA);
        openGroupBTable(numTables, sinkCorfuStores.get(SINK3_INDEX), sinkTablesGroupB);

        // Write data to Source side tables
        writeDataOnSource(0, NUM_WRITES);

        // Start log replication
        startReplicationServers();

        // Verify all the sessions' snapshot sync completed
        verifySessionInLogEntrySyncState(SINK1_INDEX, SessionManager.getDefaultSubscriber());
        // TODO: should be replaced by real client name after Sink session creation workflow is introduced
        verifySessionInLogEntrySyncState(SINK2_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());
        verifySessionInLogEntrySyncState(SINK3_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());

        // Verify tables' content on Sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), 0, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), 0, sinkTablesGroupB);

        // Add group destination now and verify data replicated successfully
        logicalGroupClient.setDestinations(GROUP_A,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK2_INDEX)));
        logicalGroupClient.setDestinations(GROUP_B,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX)));

        // Verify tables' content on Sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);
    }

    @Test
    public void testSameClientDuplicateRegistration() throws Exception {
        // Register client
        CorfuRuntime clientRuntime = getClientRuntime();
        LogReplicationLogicalGroupClient logicalGroupClient =
                new LogReplicationLogicalGroupClient(clientRuntime, SAMPLE_CLIENT_NAME);
        LogReplicationLogicalGroupClient duplicateClient =
                new LogReplicationLogicalGroupClient(clientRuntime, SAMPLE_CLIENT_NAME);
        logicalGroupClient.setDestinations(GROUP_A,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK2_INDEX)));
        duplicateClient.setDestinations(GROUP_B,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX)));

        // Open tables for replication on both side
        int numTables = 2;
        // Source side
        openFederatedTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcFederatedTables);
        openGroupATable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupA);
        openGroupBTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupB);
        // Sink side
        openFederatedTable(numTables, sinkCorfuStores.get(SINK1_INDEX), sinkFederatedTables);
        openGroupATable(numTables, sinkCorfuStores.get(SINK2_INDEX), sinkTablesGroupA);
        openGroupBTable(numTables, sinkCorfuStores.get(SINK3_INDEX), sinkTablesGroupB);

        // Write data to Source side tables
        writeDataOnSource(0, NUM_WRITES);

        // Start log replication for all sessions
        startReplicationServers();

        // Verify all the sessions' snapshot sync completed
        verifySessionInLogEntrySyncState(SINK1_INDEX, SessionManager.getDefaultSubscriber());
        // TODO: should be replaced by real client name after Sink session creation workflow is introduced
        verifySessionInLogEntrySyncState(SINK2_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());
        verifySessionInLogEntrySyncState(SINK3_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());

        // Verify tables' content on Sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);

        // Write more data for log entry sync
        writeDataOnSource(NUM_WRITES, NUM_WRITES);

        // Verify tables' content on Sink side
        int targetWrites = 2 * NUM_WRITES;
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), targetWrites, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), targetWrites, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), targetWrites, sinkTablesGroupB);
    }

    @Test
    public void testGroupAddedWithNoTableOpened() throws Exception {
        // Register client and add group destinations
        CorfuRuntime clientRuntime = getClientRuntime();
        LogReplicationLogicalGroupClient logicalGroupClient =
                new LogReplicationLogicalGroupClient(clientRuntime, SAMPLE_CLIENT_NAME);
        logicalGroupClient.setDestinations(GROUP_A,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK2_INDEX)));
        logicalGroupClient.setDestinations(GROUP_B,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX)));

        // Open tables for replication on both side
        int numTables = 2;
        // Source side
        openFederatedTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcFederatedTables);
        openGroupBTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupB);
        // Sink side
        openFederatedTable(numTables, sinkCorfuStores.get(SINK1_INDEX), sinkFederatedTables);
        openGroupATable(numTables, sinkCorfuStores.get(SINK2_INDEX), sinkTablesGroupA);
        openGroupBTable(numTables, sinkCorfuStores.get(SINK3_INDEX), sinkTablesGroupB);

        // Write to Source side tables
        for (int i = 0; i < NUM_WRITES; i++) {
            StringKey key = StringKey.newBuilder().setKey(String.valueOf(i)).build();
            ValueFieldTagOne federatedTablePayload = ValueFieldTagOne.newBuilder().setPayload(String.valueOf(i)).build();
            SampleGroupMsgB groupBTablePayload = SampleGroupMsgB.newBuilder().setPayload(String.valueOf(i)).build();

            try (TxnContext txn = sourceCorfuStores.get(SOURCE_INDEX).txn(NAMESPACE)) {
                for (Table<StringKey, ValueFieldTagOne, Message> srcFederatedTable : srcFederatedTables) {
                    txn.putRecord(srcFederatedTable, key, federatedTablePayload, null);
                }
                for (Table<StringKey, SampleGroupMsgB, Message> stringKeySampleGroupMsgBMessageTable : srcTablesGroupB) {
                    txn.putRecord(stringKeySampleGroupMsgBMessageTable, key, groupBTablePayload, null);
                }
                txn.commit();
            }
        }

        // Start log replication
        startReplicationServers();

        // Verify all the sessions' snapshot sync completed
        verifySessionInLogEntrySyncState(SINK1_INDEX, SessionManager.getDefaultSubscriber());
        // TODO: should be replaced by real client name after Sink session creation workflow is introduced
        verifySessionInLogEntrySyncState(SINK2_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());
        verifySessionInLogEntrySyncState(SINK3_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());

        // Verify tables' content on Sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), 0, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);

        // Write to Source groupA tables
        openGroupATable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupA);
        for (int i = 0; i < NUM_WRITES; i++) {
            StringKey key = StringKey.newBuilder().setKey(String.valueOf(i)).build();
            SampleGroupMsgA groupATablePayload = SampleGroupMsgA.newBuilder().setPayload(String.valueOf(i)).build();

            try (TxnContext txn = sourceCorfuStores.get(SOURCE_INDEX).txn(NAMESPACE)) {
                for (Table<StringKey, SampleGroupMsgA, Message> stringKeySampleGroupMsgAMessageTable : srcTablesGroupA) {
                    txn.putRecord(stringKeySampleGroupMsgAMessageTable, key, groupATablePayload, null);
                }
                txn.commit();
            }
        }

        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
    }

    /**
     * This test has the similar workflow as testNewStreamsInLogEntrySync() in
     * {@link LogReplicationDynamicStreamIT}, with Logical Group feature.
     *
     * This IT tests if the log replication is successfully replicated when one
     * of the table (table Group B in this IT) is opened during log entry sync status.
     */
    @Test
    public void testNewStreamsInLogEntrySync() throws Exception {
        // Register client and add group destinations
        CorfuRuntime clientRuntime = getClientRuntime();
        LogReplicationLogicalGroupClient logicalGroupClient =
                new LogReplicationLogicalGroupClient(clientRuntime, SAMPLE_CLIENT_NAME);
        logicalGroupClient.setDestinations(GROUP_A,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK2_INDEX)));
        logicalGroupClient.setDestinations(GROUP_B,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX)));

        // Open federated table and table group A
        int numTables = 2;
        openFederatedTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcFederatedTables);
        openGroupATable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupA);
        openFederatedTable(numTables, sinkCorfuStores.get(SINK1_INDEX), sinkFederatedTables);
        openGroupATable(numTables, sinkCorfuStores.get(SINK2_INDEX), sinkTablesGroupA);

        // Write data to federated and table group A
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

        // Start log replication
        startReplicationServers();

        // Verify the state of the sync
        verifySessionInLogEntrySyncState(SINK1_INDEX, SessionManager.getDefaultSubscriber());
        verifySessionInLogEntrySyncState(SINK2_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());
        verifySessionInLogEntrySyncState(SINK3_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());

        // Verify if the data is in the sink
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);

        // Open source table group B
        openGroupBTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupB);

        // Write data to table group B
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

        // Open sink table group B
        openGroupBTable(numTables, sinkCorfuStores.get(SINK3_INDEX), sinkTablesGroupB);

        // Verify the data in source table B is successfully replicated to sink table B
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);
    }

    /**
     * This test has the same workflow as testSinkLocalWritesClearing() in
     * {@link LogReplicationDynamicStreamIT}, with Logical Group feature.
     * This IT tests the local write data on the sinks side is overwritten
     * after the log replication.
     *
     * In this IT, sinkFederatedTable, sinkTablesGroupA,
     * and sinkTablesGroupB have NUM_WRITES/2 local data before log replication.
     *
     * sinkFederatedTables have NUM_WRITES data on source, so it has NUM_WRITES data
     * after log replication. Same applies to sinkTablesGroupA.
     * Since sinkTablesGroupB has no data on source, it has no data after log replication.
     */
    @Test
    public void testSinkLocalWritesClearing() throws Exception {
        // Register client and add group destinations
        CorfuRuntime clientRuntime = getClientRuntime();
        LogReplicationLogicalGroupClient logicalGroupClient =
                new LogReplicationLogicalGroupClient(clientRuntime, SAMPLE_CLIENT_NAME);
        logicalGroupClient.setDestinations(GROUP_A,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK2_INDEX)));
        logicalGroupClient.setDestinations(GROUP_B,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX)));


        // Open Federated Table, Group A Table on sources and sinks
        int numTables = 2;
        openFederatedTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcFederatedTables);
        openGroupATable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupA);
        openFederatedTable(numTables, sinkCorfuStores.get(SINK1_INDEX), sinkFederatedTables);
        openGroupATable(numTables, sinkCorfuStores.get(SINK2_INDEX), sinkTablesGroupA);

        // Write data to Federated Table, Group A Table on Sources
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

        // Open table Group B on sink
        openGroupBTable(numTables, sinkCorfuStores.get(SINK3_INDEX), sinkTablesGroupB);

        // Write local data to table Group B on sink
        writeDataOnSink(NUM_WRITES, NUM_WRITES/2);

        // Start the log replication
        startReplicationServers();

        // Verify the status of sync
        verifySessionInLogEntrySyncState(SINK1_INDEX, SessionManager.getDefaultSubscriber());
        verifySessionInLogEntrySyncState(SINK2_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());
        verifySessionInLogEntrySyncState(SINK3_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());

        // Verify that the data is replicated
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
        // Verify that the local data is overwritten after replication(Table Group B does not have data on Source)
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), 0, sinkTablesGroupB);

        // Verify that the local data is overwritten after replication
        for (int i = NUM_WRITES; i < NUM_WRITES + NUM_WRITES/2 ; i++) {
            Sample.StringKey key = Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build();
            try (TxnContext txn = sinkCorfuStores.get(SINK1_INDEX).txn(NAMESPACE)) {
                assertThat(txn.getRecord(sinkFederatedTables.get(SINK1_INDEX), key).getPayload()).isNull();
                txn.commit();
            }
        }

        for (int i = NUM_WRITES; i < NUM_WRITES + NUM_WRITES/2 ; i++) {
            Sample.StringKey key = Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build();
            try (TxnContext txn = sinkCorfuStores.get(SINK2_INDEX).txn(NAMESPACE)) {
                assertThat(txn.getRecord(sinkFederatedTables.get(SINK2_INDEX), key).getPayload()).isNull();
                txn.commit();
            }
        }
    }

    /**
     * This test has the similar workflow as testSnapshotAndLogEntrySync() in
     * {@link LogReplicationDynamicStreamIT}, with Logical Group feature.
     *
     * This IT tests the correctness of log replication if
     * only source tables(srcFederatedTables, srcTablesGroupA,
     * srcTablesGroupB) are opened before log replication.
     */
    @Test
    public void testSourceNewTablesReplication() throws Exception {
        // Register client and add group destinations
        CorfuRuntime clientRuntime = getClientRuntime();
        LogReplicationLogicalGroupClient logicalGroupClient =
                new LogReplicationLogicalGroupClient(clientRuntime, SAMPLE_CLIENT_NAME);
        logicalGroupClient.setDestinations(GROUP_A,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK2_INDEX)));
        logicalGroupClient.setDestinations(GROUP_B,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX)));

        // Open tables for replication on Source side
        int numTables = 2;
        openFederatedTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcFederatedTables);
        openGroupATable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupA);
        openGroupBTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupB);

        // Write data to tables opened on each source cluster
        writeDataOnSource(0, NUM_WRITES);

        // Start the replication
        startReplicationServers();

        // Verify all the sessions' snapshot sync completed
        verifySessionInLogEntrySyncState(SINK1_INDEX, SessionManager.getDefaultSubscriber());
        verifySessionInLogEntrySyncState(SINK2_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());
        verifySessionInLogEntrySyncState(SINK3_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());

        // Open tables on sink
        openFederatedTable(numTables, sinkCorfuStores.get(SINK1_INDEX), sinkFederatedTables);
        openGroupATable(numTables, sinkCorfuStores.get(SINK2_INDEX), sinkTablesGroupA);
        openGroupBTable(numTables, sinkCorfuStores.get(SINK3_INDEX), sinkTablesGroupB);

        // Verify the data is in table on the sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);

        // Write data on sources
        writeDataOnSource(NUM_WRITES, NUM_WRITES);

        // Verify all the sessions' snapshot sync completed
        verifySessionInLogEntrySyncState(SINK1_INDEX, SessionManager.getDefaultSubscriber());
        verifySessionInLogEntrySyncState(SINK2_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());
        verifySessionInLogEntrySyncState(SINK3_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());

        // Verify the data is in the table on the sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), 2 * NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), 2 * NUM_WRITES, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), 2 * NUM_WRITES, sinkTablesGroupB);
    }

    @Test
    public void testGroupDestinationChange() throws Exception {
        // Register client and add group destinations
        CorfuRuntime clientRuntime = getClientRuntime();
        LogReplicationLogicalGroupClient logicalGroupClient =
                new LogReplicationLogicalGroupClient(clientRuntime, SAMPLE_CLIENT_NAME);
        logicalGroupClient.setDestinations(GROUP_A,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK2_INDEX)));
        logicalGroupClient.setDestinations(GROUP_B,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX)));

        // Open tables for replication on both side
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
        verifySessionInLogEntrySyncState(SINK1_INDEX, SessionManager.getDefaultSubscriber());
        // TODO: should be replaced by real client name after Sink session creation workflow is introduced
        verifySessionInLogEntrySyncState(SINK2_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());
        verifySessionInLogEntrySyncState(SINK3_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());

        // Verify tables' content on Sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);

        // Add groupA to Sink3 and verify the groupA tables' data is successfully replicated to Sink3.
        logicalGroupClient.addDestinations(GROUP_A,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX)));
        List<Table<StringKey, SampleGroupMsgA, Message>> sinkTablesGroupAOnSink3 = new ArrayList<>();
        openGroupATable(numTables, sinkCorfuStores.get(SINK3_INDEX), sinkTablesGroupAOnSink3);
        verifyGroupATableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupAOnSink3);

        // Write more data for log entry sync
        writeDataOnSource(NUM_WRITES, NUM_WRITES);

        // Verify tables' content on Sink side
        int targetWrites = 2 * NUM_WRITES;
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), targetWrites, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), targetWrites, sinkTablesGroupA);
        verifyGroupATableData(sinkCorfuStores.get(SINK3_INDEX), targetWrites, sinkTablesGroupAOnSink3);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), targetWrites, sinkTablesGroupB);

        // Add groupB to Sink2 and verify the groupB tables' data is successfully replicated to Sink2.
        logicalGroupClient.addDestinations(GROUP_B,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK2_INDEX)));
        List<Table<StringKey, SampleGroupMsgB, Message>> sinkTablesGroupBOnSink2 = new ArrayList<>();
        openGroupBTable(numTables, sinkCorfuStores.get(SINK2_INDEX), sinkTablesGroupBOnSink2);
        verifyGroupBTableData(sinkCorfuStores.get(SINK2_INDEX), targetWrites, sinkTablesGroupBOnSink2);

        // Check Sink clusters snapshot plugin listeners received expected updates.
        latchSink1.await();
        latchSink2.await();
        latchSink3.await();

        // Remove groupB from Sink3 and verify groupB tables' data no longer replicated to Sink3
        logicalGroupClient.removeDestinations(GROUP_B,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX)));
        // Pause the test thread for a while to let the destination removal and new config take effect.
        TimeUnit.SECONDS.sleep(SLEEP_INTERVAL);

        for (int i = targetWrites; i < targetWrites + NUM_WRITES; i++) {
            StringKey key = StringKey.newBuilder().setKey(String.valueOf(i)).build();
            SampleGroupMsgB groupBTablePayload = SampleGroupMsgB.newBuilder().setPayload(String.valueOf(i)).build();

            try (TxnContext txn = sourceCorfuStores.get(SOURCE_INDEX).txn(NAMESPACE)) {
                for (Table<StringKey, SampleGroupMsgB, Message> table : srcTablesGroupB) {
                    txn.putRecord(table, key, groupBTablePayload, null);
                }
                txn.commit();
            }
        }

        // groupB is only targeting Sink2 now, so the incremental data should be replicated to Sink2 only.
        verifyGroupBTableData(sinkCorfuStores.get(SINK2_INDEX),
                targetWrites + NUM_WRITES, sinkTablesGroupBOnSink2);
        
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), targetWrites, sinkTablesGroupB);
    }

    @Test
    public void testGroupDestinationRemovedAndAddedBack() throws Exception {
        // Register client and add group destinations
        CorfuRuntime clientRuntime = getClientRuntime();
        LogReplicationLogicalGroupClient logicalGroupClient =
                new LogReplicationLogicalGroupClient(clientRuntime, SAMPLE_CLIENT_NAME);
        logicalGroupClient.setDestinations(GROUP_A,
                Arrays.asList(
                        DefaultClusterConfig.getSinkClusterIds().get(SINK2_INDEX),
                        DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX)));
        logicalGroupClient.setDestinations(GROUP_B,
                Arrays.asList(
                        DefaultClusterConfig.getSinkClusterIds().get(SINK2_INDEX),
                        DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX)));

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

        // Open tables for replication on both side
        int numTables = 3;
        openFederatedTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcFederatedTables);
        openGroupATable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupA);
        openGroupBTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupB);
        //
        openFederatedTable(numTables, sinkCorfuStores.get(SINK1_INDEX), sinkFederatedTables);
        openGroupATable(numTables, sinkCorfuStores.get(SINK2_INDEX), sinkTablesGroupA);
        openGroupBTable(numTables, sinkCorfuStores.get(SINK3_INDEX), sinkTablesGroupB);

        // Write data to Source side tables
        writeDataOnSource(0, NUM_WRITES);

        // Start log replication for all sessions
        startReplicationServers();

        // Verify all the sessions' snapshot sync completed
        verifySessionInLogEntrySyncState(SINK1_INDEX, SessionManager.getDefaultSubscriber());
        // TODO: should be replaced by real client name after Sink session creation workflow is introduced
        verifySessionInLogEntrySyncState(SINK2_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());
        verifySessionInLogEntrySyncState(SINK3_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());

        List<Table<StringKey, SampleGroupMsgA, Message>> sinkTablesGroupAOnSink3 = new ArrayList<>();
        openGroupATable(numTables, sinkCorfuStores.get(SINK3_INDEX), sinkTablesGroupAOnSink3);
        List<Table<StringKey, SampleGroupMsgB, Message>> sinkTablesGroupBOnSink2 = new ArrayList<>();
        openGroupBTable(numTables, sinkCorfuStores.get(SINK2_INDEX), sinkTablesGroupBOnSink2);

        // Verify tables' content on Sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
        verifyGroupATableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupAOnSink3);
        verifyGroupBTableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupBOnSink2);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);

        logicalGroupClient.removeDestinations(GROUP_A,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX)));
        logicalGroupClient.removeDestinations(GROUP_B,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK2_INDEX)));

        // Pause the test thread for a while to let the destination removal and new config take effect.
        TimeUnit.SECONDS.sleep(SLEEP_INTERVAL);

        int targetWrites = 2 * NUM_WRITES;
        for (int i = NUM_WRITES; i < targetWrites; i++) {
            StringKey key = StringKey.newBuilder().setKey(String.valueOf(i)).build();
            SampleGroupMsgA groupATablePayload = SampleGroupMsgA.newBuilder().setPayload(String.valueOf(i)).build();
            SampleGroupMsgB groupBTablePayload = SampleGroupMsgB.newBuilder().setPayload(String.valueOf(i)).build();

            try (TxnContext txn = sourceCorfuStores.get(SOURCE_INDEX).txn(NAMESPACE)) {
                for (Table<StringKey, SampleGroupMsgA, Message> table : srcTablesGroupA) {
                    txn.putRecord(table, key, groupATablePayload, null);
                }
                for (Table<StringKey, SampleGroupMsgB, Message> table : srcTablesGroupB) {
                    txn.putRecord(table, key, groupBTablePayload, null);
                }
                txn.commit();
            }
        }

        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), targetWrites, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), targetWrites, sinkTablesGroupB);
        verifyGroupATableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupAOnSink3);
        verifyGroupBTableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupBOnSink2);

        logicalGroupClient.addDestinations(GROUP_A,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX)));
        logicalGroupClient.addDestinations(GROUP_B,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK2_INDEX)));
        verifyGroupATableData(sinkCorfuStores.get(SINK3_INDEX), targetWrites, sinkTablesGroupAOnSink3);
        verifyGroupBTableData(sinkCorfuStores.get(SINK2_INDEX), targetWrites, sinkTablesGroupBOnSink2);

        // Check Sink clusters snapshot plugin listeners received expected updates.
        latchSink1.await();
        latchSink2.await();
        latchSink3.await();
    }

    @Test
    public void testCheckpointTrimSourceBetweenSnapshotSync() throws Exception {
        // Register client and add group destinations
        CorfuRuntime clientRuntime = getClientRuntime();
        LogReplicationLogicalGroupClient logicalGroupClient =
                new LogReplicationLogicalGroupClient(clientRuntime, SAMPLE_CLIENT_NAME);
        logicalGroupClient.setDestinations(GROUP_A,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK2_INDEX)));
        logicalGroupClient.setDestinations(GROUP_B,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX)));

        // Open tables for replication on both side
        int numTables = 1;
        // Source side
        openFederatedTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcFederatedTables);
        openGroupATable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupA);
        openGroupBTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupB);
        // Sink side
        openFederatedTable(numTables, sinkCorfuStores.get(SINK1_INDEX), sinkFederatedTables);
        openGroupATable(numTables, sinkCorfuStores.get(SINK2_INDEX), sinkTablesGroupA);
        openGroupBTable(numTables, sinkCorfuStores.get(SINK3_INDEX), sinkTablesGroupB);

        // Subscribe snapshot plugin listener on each Sink cluster, Sink1 will only have 1 snapshot sync while Sink2
        // and Sink3 will have 2 snapshot sync for each, where 1 snapshot sync will update the listener twice.
        CountDownLatch latchSink1 = new CountDownLatch(4);
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
        verifySessionInLogEntrySyncState(SINK1_INDEX, SessionManager.getDefaultSubscriber());
        // TODO: should be replaced by real client name after Sink session creation workflow is introduced
        verifySessionInLogEntrySyncState(SINK2_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());
        verifySessionInLogEntrySyncState(SINK3_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());

        // Verify tables' content on Sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);


        // Stop Source replication server
        stopReplicationServer(sourceReplicationPorts.get(SOURCE_INDEX), sourceReplicationServers.get(SOURCE_INDEX));
        sourceReplicationServers.clear();

        // Checkpoint & trim on the Sink such that shadow streams are trimmed
        checkpointAndTrimCorfuStore(createRuntimeWithCache(sinkEndpoints.get(SINK1_INDEX)));
        checkpointAndTrimCorfuStore(createRuntimeWithCache(sinkEndpoints.get(SINK2_INDEX)));
        checkpointAndTrimCorfuStore(createRuntimeWithCache(sinkEndpoints.get(SINK3_INDEX)));

        // Write more data on Source
        int targetWrites = NUM_WRITES * 2;
        writeDataOnSource(NUM_WRITES, NUM_WRITES);

        // Verify new data exists on Source side
        verifyFederatedTableData(sourceCorfuStores.get(SOURCE_INDEX), targetWrites, srcFederatedTables);
        verifyGroupATableData(sourceCorfuStores.get(SOURCE_INDEX), targetWrites, srcTablesGroupA);
        verifyGroupBTableData(sourceCorfuStores.get(SOURCE_INDEX), targetWrites, srcTablesGroupB);

        // Verify new data does not exist on Sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);

        // Checkpoint & trim on Source side to enforce a snapshot sync on restart for all the 3 sessions
        checkpointAndTrimCorfuStore(createRuntimeWithCache(sourceEndpoints.get(SOURCE_INDEX)));
        sourceReplicationServers.add(runReplicationServer(sourceReplicationPorts.get(SOURCE_INDEX), pluginConfigFilePath));
        Table<ExampleSchemas.ClusterUuidMsg, ExampleSchemas.ClusterUuidMsg, ExampleSchemas.ClusterUuidMsg> configTable =
                sourceCorfuStores.get(SOURCE_INDEX).openTable(
                        DefaultClusterManager.CONFIG_NAMESPACE, DefaultClusterManager.CONFIG_TABLE_NAME,
                        ExampleSchemas.ClusterUuidMsg.class, ExampleSchemas.ClusterUuidMsg.class, ExampleSchemas.ClusterUuidMsg.class,
                        TableOptions.fromProtoSchema(ExampleSchemas.ClusterUuidMsg.class)
                );
        try (TxnContext txn = sourceCorfuStores.get(SOURCE_INDEX).txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.TP_MIXED_MODEL_THREE_SINK,
                    DefaultClusterManager.TP_MIXED_MODEL_THREE_SINK, DefaultClusterManager.TP_MIXED_MODEL_THREE_SINK);
            txn.commit();
        }

        // Verify new data replicated to Sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), targetWrites, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), targetWrites, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), targetWrites, sinkTablesGroupB);

        latchSink1.await();
        latchSink2.await();
        latchSink3.await();
    }

    @Test
    public void testCheckpointTrimSinkDuringLogEntrySync() throws Exception {
        // Register client and add group destinations
        CorfuRuntime clientRuntime = getClientRuntime();
        LogReplicationLogicalGroupClient logicalGroupClient =
                new LogReplicationLogicalGroupClient(clientRuntime, SAMPLE_CLIENT_NAME);
        logicalGroupClient.setDestinations(GROUP_A,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK2_INDEX)));
        logicalGroupClient.setDestinations(GROUP_B,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX)));

        // Open tables for replication on both side
        int numTables = 1;
        // Source side
        openFederatedTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcFederatedTables);
        openGroupATable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupA);
        openGroupBTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupB);
        // Sink side
        openFederatedTable(numTables, sinkCorfuStores.get(SINK1_INDEX), sinkFederatedTables);
        openGroupATable(numTables, sinkCorfuStores.get(SINK2_INDEX), sinkTablesGroupA);
        openGroupBTable(numTables, sinkCorfuStores.get(SINK3_INDEX), sinkTablesGroupB);

        // Write data to Source side tables
        writeDataOnSource(0, NUM_WRITES);

        // Start log replication for all sessions
        startReplicationServers();

        // Verify all the sessions' snapshot sync completed
        verifySessionInLogEntrySyncState(SINK1_INDEX, SessionManager.getDefaultSubscriber());
        // TODO: should be replaced by real client name after Sink session creation workflow is introduced
        verifySessionInLogEntrySyncState(SINK2_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());
        verifySessionInLogEntrySyncState(SINK3_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());

        // Verify tables' content on Sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);

        // Stop Source replication server
        stopReplicationServer(sourceReplicationPorts.get(SOURCE_INDEX), sourceReplicationServers.get(SOURCE_INDEX));
        sourceReplicationServers.clear();

        // Checkpoint & trim on the Sink such that shadow streams are trimmed
        checkpointAndTrimCorfuStore(createRuntimeWithCache(sinkEndpoints.get(SINK1_INDEX)));
        checkpointAndTrimCorfuStore(createRuntimeWithCache(sinkEndpoints.get(SINK2_INDEX)));
        checkpointAndTrimCorfuStore(createRuntimeWithCache(sinkEndpoints.get(SINK3_INDEX)));

        // Write more data on Source
        int targetWrites = NUM_WRITES * 2;
        writeDataOnSource(NUM_WRITES, NUM_WRITES);

        // Verify new data exists on Source side
        verifyFederatedTableData(sourceCorfuStores.get(SOURCE_INDEX), targetWrites, srcFederatedTables);
        verifyGroupATableData(sourceCorfuStores.get(SOURCE_INDEX), targetWrites, srcTablesGroupA);
        verifyGroupBTableData(sourceCorfuStores.get(SOURCE_INDEX), targetWrites, srcTablesGroupB);

        // Verify new data does not exist on Sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);

        // Restart Source to enforce re-negotiations
        sourceReplicationServers.add(runReplicationServer(sourceReplicationPorts.get(SOURCE_INDEX), pluginConfigFilePath));
        Table<ExampleSchemas.ClusterUuidMsg, ExampleSchemas.ClusterUuidMsg, ExampleSchemas.ClusterUuidMsg> configTable =
                sourceCorfuStores.get(SOURCE_INDEX).openTable(
                        DefaultClusterManager.CONFIG_NAMESPACE, DefaultClusterManager.CONFIG_TABLE_NAME,
                        ExampleSchemas.ClusterUuidMsg.class, ExampleSchemas.ClusterUuidMsg.class, ExampleSchemas.ClusterUuidMsg.class,
                        TableOptions.fromProtoSchema(ExampleSchemas.ClusterUuidMsg.class)
                );
        try (TxnContext txn = sourceCorfuStores.get(SOURCE_INDEX).txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.TP_MIXED_MODEL_THREE_SINK,
                    DefaultClusterManager.TP_MIXED_MODEL_THREE_SINK, DefaultClusterManager.TP_MIXED_MODEL_THREE_SINK);
            txn.commit();
        }

        // Verify new data replicated to Sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), targetWrites, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), targetWrites, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), targetWrites, sinkTablesGroupB);
    }

    @Test
    public void testLogicalGroupSinkRemoved() throws Exception {
        // Register client and add group destinations
        CorfuRuntime clientRuntime = getClientRuntime();
        LogReplicationLogicalGroupClient logicalGroupClient =
                new LogReplicationLogicalGroupClient(clientRuntime, SAMPLE_CLIENT_NAME);
        logicalGroupClient.setDestinations(GROUP_A,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK2_INDEX)));
        logicalGroupClient.setDestinations(GROUP_B,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX)));

        // Open tables for replication on both side
        int numTables = 2;
        // Source side
        openFederatedTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcFederatedTables);
        openGroupATable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupA);
        openGroupBTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupB);
        // Sink side
        openFederatedTable(numTables, sinkCorfuStores.get(SINK1_INDEX), sinkFederatedTables);
        openGroupATable(numTables, sinkCorfuStores.get(SINK2_INDEX), sinkTablesGroupA);
        openGroupBTable(numTables, sinkCorfuStores.get(SINK3_INDEX), sinkTablesGroupB);

        // Write data to Source side tables
        writeDataOnSource(0, NUM_WRITES);

        // Start log replication for all sessions
        startReplicationServers();

        // Verify all the sessions' snapshot sync completed
        verifySessionInLogEntrySyncState(SINK1_INDEX, SessionManager.getDefaultSubscriber());
        // TODO: should be replaced by real client name after Sink session creation workflow is introduced
        verifySessionInLogEntrySyncState(SINK2_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());
        verifySessionInLogEntrySyncState(SINK3_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());

        // Verify tables' content on Sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);

        // Write more data for log entry sync
        writeDataOnSource(NUM_WRITES, NUM_WRITES);

        // Verify tables' content on Sink side
        int targetWrites = 2 * NUM_WRITES;
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), targetWrites, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), targetWrites, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), targetWrites, sinkTablesGroupB);

        Table<ExampleSchemas.ClusterUuidMsg, ExampleSchemas.ClusterUuidMsg, ExampleSchemas.ClusterUuidMsg> configTable =
                sourceCorfuStores.get(SOURCE_INDEX).getTable(DefaultClusterManager.CONFIG_NAMESPACE,
                        DefaultClusterManager.CONFIG_TABLE_NAME);
        try (TxnContext txn = sourceCorfuStores.get(SOURCE_INDEX).txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.OP_TWO_SINK_MIXED,
                    DefaultClusterManager.OP_TWO_SINK_MIXED, DefaultClusterManager.OP_TWO_SINK_MIXED);
            txn.commit();
        }

        TimeUnit.SECONDS.sleep(SLEEP_INTERVAL);


        // Write more data for log entry sync
        writeDataOnSource(targetWrites, NUM_WRITES);
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), targetWrites + NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), targetWrites + NUM_WRITES, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), targetWrites, sinkTablesGroupB);
    }


    /**
     * This IT has the similar workflow as
     * {@link org.corfudb.integration.LogReplicationDynamicStreamIT#testSinkStreamingLogEntrySync()},
     * with LOGICAL_GROUP replication model.
     *
     * (1) Perform Snapshot sync
     * (2) After snapshot sync, open new tables on source side and sink side
     * (3) Initiate testing stream listeners and subscribe to tag_one on Sink
     * (4) Write some entries to the tables on Source
     * (5) Verify testing stream listeners get all the update
     *
     */
    @Test
    public void testSinkStreamingLogEntrySync() throws Exception {
        // Register client and add group destinations
        CorfuRuntime clientRuntime = getClientRuntime();
        LogReplicationLogicalGroupClient logicalGroupClient =
                new LogReplicationLogicalGroupClient(clientRuntime, SAMPLE_CLIENT_NAME);
        logicalGroupClient.setDestinations(GROUP_A,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK2_INDEX)));
        logicalGroupClient.setDestinations(GROUP_B,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX)));

        // Open tables for replication on Source side
        int numTables = 2;
        openFederatedTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcFederatedTables);
        openGroupATable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupA);
        openGroupBTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupB);
        openFederatedTable(numTables, sinkCorfuStores.get(SINK1_INDEX), sinkFederatedTables);
        openGroupATable(numTables, sinkCorfuStores.get(SINK2_INDEX), sinkTablesGroupA);
        openGroupBTable(numTables, sinkCorfuStores.get(SINK3_INDEX), sinkTablesGroupB);

        // Write data to tables on source side
        writeDataOnSource(0, NUM_WRITES);

        // Start log replication
        startReplicationServers();

        // Verify all the sessions' snapshot sync completed
        verifySessionInLogEntrySyncState(SINK1_INDEX, SessionManager.getDefaultSubscriber());
        verifySessionInLogEntrySyncState(SINK2_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());
        verifySessionInLogEntrySyncState(SINK3_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());

        // Verify tables' content on Sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);

        // Initialize tableName UUID's sets and tableName's lists for stream listener
        Set<UUID> tableNameForFederatedForLogEntry = new HashSet<>();
        List<String> tableNameSubscribeForFederatedForLogEntry = new ArrayList<>();
        Set<UUID> tableNameForGroupAForLogEntry = new HashSet<>();
        List<String> tableNameSubscribeForGroupAForLogEntry = new ArrayList<>();
        Set<UUID> tableNameForGroupBForLogEntry = new HashSet<>();
        List<String> tableNameSubscribeForGroupBForLogEntry = new ArrayList<>();

        // Open new tables on source side and sink side, and add tableName to the above data structures
        for (int i = numTables + 1; i <= 2 * numTables; i++) {
            String tableName = FEDERATED_TABLE_PREFIX + i;
            Table<StringKey, ValueFieldTagOne, Message> federatedTable = sourceCorfuStores.get(SOURCE_INDEX).openTable(
                    NAMESPACE,
                    tableName,
                    StringKey.class,
                    ValueFieldTagOne.class,
                    null,
                    TableOptions.fromProtoSchema(ValueFieldTagOne.class)
            );
            tableNameForFederatedForLogEntry.add(CorfuRuntime.getStreamID(federatedTable.getFullyQualifiedTableName()));
            tableNameSubscribeForFederatedForLogEntry.add(tableName);
            srcFederatedTables.add(federatedTable);
        }

        for (int i = numTables + 1; i <= 2 * numTables; i++) {
            String tableName = FEDERATED_TABLE_PREFIX + i;
            Table<StringKey, ValueFieldTagOne, Message> federatedTable = sinkCorfuStores.get(SINK1_INDEX).openTable(
                    NAMESPACE,
                    tableName,
                    StringKey.class,
                    ValueFieldTagOne.class,
                    null,
                    TableOptions.fromProtoSchema(ValueFieldTagOne.class)
            );
            sinkFederatedTables.add(federatedTable);
        }

        for (int i = numTables + 1; i <= 2 * numTables; i++) {
            String tableName = GROUPA_TABLE_PREFIX + i;
            Table<StringKey, SampleGroupMsgA, Message> groupATable = sourceCorfuStores.get(SOURCE_INDEX).openTable(
                    NAMESPACE,
                    tableName,
                    StringKey.class,
                    SampleGroupMsgA.class,
                    null,
                    TableOptions.fromProtoSchema(SampleGroupMsgA.class)
            );
            tableNameForGroupAForLogEntry.add(CorfuRuntime.getStreamID(groupATable.getFullyQualifiedTableName()));
            tableNameSubscribeForGroupAForLogEntry.add(tableName);
            srcTablesGroupA.add(groupATable);
        }

        for (int i = numTables + 1; i <= 2 * numTables; i++) {
            String tableName = GROUPA_TABLE_PREFIX + i;
            Table<StringKey, SampleGroupMsgA, Message> groupATable = sinkCorfuStores.get(SINK2_INDEX).openTable(
                    NAMESPACE,
                    tableName,
                    StringKey.class,
                    SampleGroupMsgA.class,
                    null,
                    TableOptions.fromProtoSchema(SampleGroupMsgA.class)
            );
            sinkTablesGroupA.add(groupATable);
        }

        for (int i = numTables + 1; i <= 2 * numTables; i++) {
            String tableName = GROUPB_TABLE_PREFIX + i;
            Table<StringKey, SampleGroupMsgB, Message> groupBTable = sourceCorfuStores.get(SOURCE_INDEX).openTable(
                    NAMESPACE,
                    tableName,
                    StringKey.class,
                    SampleGroupMsgB.class,
                    null,
                    TableOptions.fromProtoSchema(SampleGroupMsgB.class)
            );
            tableNameForGroupBForLogEntry.add(CorfuRuntime.getStreamID(groupBTable.getFullyQualifiedTableName()));
            tableNameSubscribeForGroupBForLogEntry.add(tableName);
            srcTablesGroupB.add(groupBTable);
        }

        for (int i = numTables + 1; i <= 2 * numTables; i++) {
            String tableName = GROUPB_TABLE_PREFIX + i;
            Table<StringKey, SampleGroupMsgB, Message> groupBTable = sinkCorfuStores.get(SINK3_INDEX).openTable(
                    NAMESPACE,
                    tableName,
                    StringKey.class,
                    SampleGroupMsgB.class,
                    null,
                    TableOptions.fromProtoSchema(SampleGroupMsgB.class)
            );
            sinkTablesGroupB.add(groupBTable);
        }

        // Initialize testing stream listeners
        int expectedMessageSizeForLogEntry = NUM_WRITES * numTables;
        CountDownLatch streamingSinkSnapshotCompletionForFederatedLogEntry = new CountDownLatch(NUM_WRITES);
        CountDownLatch numTxLatchForFederatedLogEntry = new CountDownLatch(NUM_WRITES);
        CountDownLatch streamingSinkSnapshotCompletionForGroupALogEntry = new CountDownLatch(NUM_WRITES);
        CountDownLatch numTxLatchForGroupALogEntry = new CountDownLatch(NUM_WRITES);
        CountDownLatch streamingSinkSnapshotCompletionForGroupBLogEntry = new CountDownLatch(NUM_WRITES);
        CountDownLatch numTxLatchForGroupBLogEntry = new CountDownLatch(NUM_WRITES);
        LogReplicationAbstractIT.StreamingSinkListener listenerForFederatedLogEntry =
                new LogReplicationAbstractIT.StreamingSinkListener(streamingSinkSnapshotCompletionForFederatedLogEntry,
                        numTxLatchForFederatedLogEntry, tableNameForFederatedForLogEntry);
        LogReplicationAbstractIT.StreamingSinkListener listenerForGroupALogEntry =
                new LogReplicationAbstractIT.StreamingSinkListener(streamingSinkSnapshotCompletionForGroupALogEntry,
                        numTxLatchForGroupALogEntry, tableNameForGroupAForLogEntry);
        LogReplicationAbstractIT.StreamingSinkListener listenerForGroupBLogEntry =
                new LogReplicationAbstractIT.StreamingSinkListener(streamingSinkSnapshotCompletionForGroupBLogEntry,
                        numTxLatchForGroupBLogEntry, tableNameForGroupBForLogEntry);

        // Subscribe the listeners on the sink side
        sinkCorfuStores.get(SINK1_INDEX).subscribeListener(listenerForFederatedLogEntry, NAMESPACE, STREAMING_TAG,
                tableNameSubscribeForFederatedForLogEntry);
        sinkCorfuStores.get(SINK2_INDEX).subscribeListener(listenerForGroupALogEntry, NAMESPACE, STREAMING_TAG,
                tableNameSubscribeForGroupAForLogEntry);
        sinkCorfuStores.get(SINK3_INDEX).subscribeListener(listenerForGroupBLogEntry, NAMESPACE, STREAMING_TAG,
                tableNameSubscribeForGroupBForLogEntry);

        // Write new entries to the tables
        for (int i = 0; i < NUM_WRITES; i++) {
            StringKey key = StringKey.newBuilder().setKey(String.valueOf(i)).build();
            ValueFieldTagOne federatedTablePayload =
                    ValueFieldTagOne.newBuilder().setPayload(String.valueOf(i)).build();
            SampleGroupMsgA groupATablePayload = SampleGroupMsgA.newBuilder().setPayload(String.valueOf(i)).build();
            SampleGroupMsgB groupBTablePayload = SampleGroupMsgB.newBuilder().setPayload(String.valueOf(i)).build();

            try (TxnContext txn = sourceCorfuStores.get(SOURCE_INDEX).txn(NAMESPACE)) {
                for (int j = 2; j < srcFederatedTables.size(); j++) {
                    txn.putRecord(srcFederatedTables.get(j), key, federatedTablePayload, null);
                }
                for (int j = 2; j < srcTablesGroupA.size(); j++) {
                    txn.putRecord(srcTablesGroupA.get(j), key, groupATablePayload, null);
                }
                for (int j = 2; j < srcTablesGroupB.size(); j++) {
                    txn.putRecord(srcTablesGroupB.get(j), key, groupBTablePayload, null);
                }
                txn.commit();
            }
        }

        // Verify tables' content on Sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);

        // Wait until listeners get all the update
        streamingSinkSnapshotCompletionForFederatedLogEntry.await();
        streamingSinkSnapshotCompletionForGroupALogEntry.await();
        streamingSinkSnapshotCompletionForGroupBLogEntry.await();

        numTxLatchForFederatedLogEntry.await();
        numTxLatchForGroupALogEntry.await();
        numTxLatchForGroupBLogEntry.await();

        // Verify the listeners get all the update
        assertThat(listenerForFederatedLogEntry.messages).hasSize(expectedMessageSizeForLogEntry);
        assertThat(listenerForGroupALogEntry.messages).hasSize(expectedMessageSizeForLogEntry);
        assertThat(listenerForGroupBLogEntry.messages).hasSize(expectedMessageSizeForLogEntry);
    }

    /**
     * This IT has the similar workflow as
     * {@link org.corfudb.integration.LogReplicationDynamicStreamIT#testSinkStreamingSnapshotSync()},
     * with LOGICAL_GROUP replication model.
     *
     * (1) Open tables with tag_one on Source and Sink
     * (2) Initiate testing stream listeners and subscribe to tag_one on Sink
     * (3) Write some entries to the tables on Source
     * (4) Start log replication snapshot sync and verify the test listener received all the updates
     *
     */
    @Test
    public void testSinkStreamingSnapshotSync() throws Exception {
        // Register client and add group destinations
        CorfuRuntime clientRuntime = getClientRuntime();
        LogReplicationLogicalGroupClient logicalGroupClient =
                new LogReplicationLogicalGroupClient(clientRuntime, SAMPLE_CLIENT_NAME);
        logicalGroupClient.setDestinations(GROUP_A,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK2_INDEX)));
        logicalGroupClient.setDestinations(GROUP_B,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX)));

        // Initialize the lists for the tableName of federated, group A, and group B tables
        Set<UUID> tableNameForFederated = new HashSet<>();
        Set<UUID> tableNameForGroupA = new HashSet<>();
        Set<UUID> tableNameForGroupB = new HashSet<>();

        // Open tables on source side, and add the tableName to the lists that store the tableName
        int numTables = 2;
        for (int i = 1; i <= numTables; i++) {
            String tableName = FEDERATED_TABLE_PREFIX + i;
            Table<StringKey, ValueFieldTagOne, Message> federatedTable = sourceCorfuStores.get(SOURCE_INDEX).openTable(
                    NAMESPACE,
                    tableName,
                    StringKey.class,
                    ValueFieldTagOne.class,
                    null,
                    TableOptions.fromProtoSchema(ValueFieldTagOne.class)
            );
            tableNameForFederated.add(CorfuRuntime.getStreamID(federatedTable.getFullyQualifiedTableName()));
            srcFederatedTables.add(federatedTable);
        }

        for (int i = 1; i <= numTables; i++) {
            String tableName = GROUPA_TABLE_PREFIX + i;
            Table<StringKey, SampleGroupMsgA, Message> groupATable = sourceCorfuStores.get(SOURCE_INDEX).openTable(
                    NAMESPACE,
                    tableName,
                    StringKey.class,
                    SampleGroupMsgA.class,
                    null,
                    TableOptions.fromProtoSchema(SampleGroupMsgA.class)
            );
            tableNameForGroupA.add(CorfuRuntime.getStreamID(groupATable.getFullyQualifiedTableName()));
            srcTablesGroupA.add(groupATable);
        }

        for (int i = 1; i <= numTables; i++) {
            String tableName = GROUPB_TABLE_PREFIX + i;
            Table<StringKey, SampleGroupMsgB, Message> groupBTable = sourceCorfuStores.get(SOURCE_INDEX).openTable(
                    NAMESPACE,
                    tableName,
                    StringKey.class,
                    SampleGroupMsgB.class,
                    null,
                    TableOptions.fromProtoSchema(SampleGroupMsgB.class)
            );
            tableNameForGroupB.add(CorfuRuntime.getStreamID(groupBTable.getFullyQualifiedTableName()));
            srcTablesGroupB.add(groupBTable);
        }

        // Open tables on sink side
        openFederatedTable(numTables, sinkCorfuStores.get(SINK1_INDEX), sinkFederatedTables);
        openGroupATable(numTables, sinkCorfuStores.get(SINK2_INDEX), sinkTablesGroupA);
        openGroupBTable(numTables, sinkCorfuStores.get(SINK3_INDEX), sinkTablesGroupB);

        // Initialize the streamListeners for each sink
        int expectedMessageSize = numTables * (NUM_WRITES + 1);
        CountDownLatch streamingSinkSnapshotCompletionForFederated = new CountDownLatch(expectedMessageSize);
        CountDownLatch numTxLatchForFederated = new CountDownLatch(numTables);
        CountDownLatch streamingSinkSnapshotCompletionForGroupA = new CountDownLatch(expectedMessageSize);
        CountDownLatch numTxLatchForGroupA = new CountDownLatch(numTables);
        CountDownLatch streamingSinkSnapshotCompletionForGroupB = new CountDownLatch(expectedMessageSize);
        CountDownLatch numTxLatchForGroupB = new CountDownLatch(numTables);
        LogReplicationAbstractIT.StreamingSinkListener listenerForFederated =
                new LogReplicationAbstractIT.StreamingSinkListener(streamingSinkSnapshotCompletionForFederated,
                        numTxLatchForFederated, tableNameForFederated);
        LogReplicationAbstractIT.StreamingSinkListener listenerForGroupA =
                new LogReplicationAbstractIT.StreamingSinkListener(streamingSinkSnapshotCompletionForGroupA,
                        numTxLatchForGroupA, tableNameForGroupA);
        LogReplicationAbstractIT.StreamingSinkListener listenerForGroupB =
                new LogReplicationAbstractIT.StreamingSinkListener(streamingSinkSnapshotCompletionForGroupB,
                        numTxLatchForGroupB, tableNameForGroupB);

        // Subscribe the listeners on the sink side
        sinkCorfuStores.get(SINK1_INDEX).subscribeListener(listenerForFederated, NAMESPACE, STREAMING_TAG);
        sinkCorfuStores.get(SINK2_INDEX).subscribeListener(listenerForGroupA, NAMESPACE, STREAMING_TAG);
        sinkCorfuStores.get(SINK3_INDEX).subscribeListener(listenerForGroupB, NAMESPACE, STREAMING_TAG);

        // Write the data to tables on source side
        writeDataOnSource(0, NUM_WRITES);

        // Start log replication
        startReplicationServers();

        // Verify all the sessions' snapshot sync completed
        verifySessionInLogEntrySyncState(SINK1_INDEX, SessionManager.getDefaultSubscriber());
        verifySessionInLogEntrySyncState(SINK2_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());
        verifySessionInLogEntrySyncState(SINK3_INDEX, SessionManager.getDefaultLogicalGroupSubscriber());

        // Verify the data is successfully replicated
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);

        // Wait until listeners get all the update
        streamingSinkSnapshotCompletionForFederated.await();
        streamingSinkSnapshotCompletionForGroupA.await();
        streamingSinkSnapshotCompletionForGroupB.await();

        numTxLatchForFederated.await();
        numTxLatchForGroupA.await();
        numTxLatchForGroupB.await();

        // Verify the listeners get all the update
        assertThat(listenerForFederated.messages).hasSize(expectedMessageSize);
        assertThat(listenerForGroupA.messages).hasSize(expectedMessageSize);
        assertThat(listenerForGroupB.messages).hasSize(expectedMessageSize);
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
     * Verify the replication session with the given Sink is already in log entry sync status.
     *
     * @param sinkIndex  Sink index that indicates which Sink should be verified.
     * @param subscriber Replication subscriber that is supposed to be verified.
     */
    private void verifySessionInLogEntrySyncState(int sinkIndex, ReplicationSubscriber subscriber) {
        LogReplicationSession session = LogReplicationSession.newBuilder()
                .setSourceClusterId(DefaultClusterConfig.getSourceClusterIds().get(SOURCE_INDEX))
                .setSinkClusterId(DefaultClusterConfig.getSinkClusterIds().get(sinkIndex))
                .setSubscriber(subscriber)
                .build();

        ReplicationStatus status = null;

        while (status == null || !status.getSourceStatus().getReplicationInfo().getSyncType().equals(SyncType.LOG_ENTRY)
                || !status.getSourceStatus().getReplicationInfo().getSnapshotSyncInfo().getStatus()
                .equals(SyncStatus.COMPLETED)) {
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

    private void verifyFederatedTableData(CorfuStore corfuStore, int expectedConsecutiveWrites,
                                          List<Table<StringKey, ValueFieldTagOne, Message>> tables) {
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

    private void verifyGroupATableData(CorfuStore corfuStore, int expectedConsecutiveWrites,
                                       List<Table<StringKey, SampleGroupMsgA, Message>> tables) {
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

    private void verifyGroupBTableData(CorfuStore corfuStore, int expectedConsecutiveWrites,
                                       List<Table<StringKey, SampleGroupMsgB, Message>> tables) {
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
     * This helper function is for testLocalWrite(), since the local data needs to be written
     * on the sink side before log replication.
     *
     * @param startNum Start number that will be written to sink side tables
     * @param numWrites
     */
    private void writeDataOnSink(int startNum, int numWrites) {
        for (int i = startNum; i < startNum + numWrites; i++) {
            StringKey key = StringKey.newBuilder().setKey(String.valueOf(i)).build();
            ValueFieldTagOne federatedTablePayload = ValueFieldTagOne.newBuilder().setPayload(String.valueOf(i)).build();
            SampleGroupMsgA groupATablePayload = SampleGroupMsgA.newBuilder().setPayload(String.valueOf(i)).build();
            SampleGroupMsgB groupBTablePayload = SampleGroupMsgB.newBuilder().setPayload(String.valueOf(i)).build();

            try (TxnContext txn = sinkCorfuStores.get(SINK1_INDEX).txn(NAMESPACE)) {
                for (int j = 0; j < sinkFederatedTables.size(); j++) {
                    txn.putRecord(sinkFederatedTables.get(j), key, federatedTablePayload, null);
                }
                txn.commit();
            }
            try (TxnContext txn = sinkCorfuStores.get(SINK2_INDEX).txn(NAMESPACE)) {
                for (int j = 0; j < sinkTablesGroupA.size(); j++) {
                    txn.putRecord(sinkTablesGroupA.get(j), key, groupATablePayload, null);
                }
                txn.commit();
            }
            try (TxnContext txn = sinkCorfuStores.get(SINK3_INDEX).txn(NAMESPACE)) {
                for (int j = 0; j < sinkTablesGroupB.size(); j++) {
                    txn.putRecord(sinkTablesGroupB.get(j), key, groupBTablePayload, null);
                }
                txn.commit();
            }
        }
    }
}
