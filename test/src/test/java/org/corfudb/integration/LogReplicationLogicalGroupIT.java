package org.corfudb.integration;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultSnapshotSyncPlugin;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.infrastructure.logreplication.proto.Sample.StringKey;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.integration.LogReplicationAbstractIT.SnapshotSyncPluginListener;
import org.corfudb.integration.LogReplicationAbstractIT.StreamingSinkListener;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.ExampleSchemas;
import org.corfudb.runtime.ExampleSchemas.SnapshotSyncPluginValue;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.LogReplication.ReplicationStatus;
import org.corfudb.runtime.LogReplication.ReplicationSubscriber;
import org.corfudb.runtime.LogReplication.SnapshotSyncInfo;
import org.corfudb.runtime.LogReplication.SyncStatus;
import org.corfudb.runtime.LogReplication.SyncType;
import org.corfudb.runtime.LogReplicationLogicalGroupClient;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.test.SampleSchema.SampleGroupMsgA;
import org.corfudb.test.SampleSchema.SampleGroupMsgB;
import org.corfudb.test.SampleSchema.ValueFieldTagOne;
import org.corfudb.util.Sleep;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.integration.LogReplicationAbstractIT.NAMESPACE;
import static org.corfudb.integration.LogReplicationAbstractIT.checkpointAndTrimCorfuStore;
import static org.corfudb.runtime.LogReplicationUtils.REPLICATION_STATUS_TABLE_NAME;
import static org.junit.Assert.fail;


/**
 * This suite of tests verifies multiple workflows in LOGICAL_GROUP log replication model. The default topology that
 * these test based contains 1 Source and 3 Sinks, where 1 of the Sinks supports FULL_TABLE model only, while the other
 * 2 Sinks support LOGICAL_GROUP model only.
 */
@Slf4j
@SuppressWarnings("checkstyle:magicnumber")
public class LogReplicationLogicalGroupIT extends CorfuReplicationMultiSourceSinkIT {

    /**
     * Static fields to facilitate the test whose names are self-explanatory.
     */

    private static final int NUM_WRITES = 500;

    private static final int PLUGIN_UPDATES_PER_SNAPSHOT_SYNC = 2;

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

    /**
     * Note that the following lists are used for storing the references of tables opened on Source or Sink side.
     * They do not provide guarantees to guard against from which side the tables are actually opened.
     */

    // A list of federated tables opened on Source side
    private final List<Table<StringKey, ValueFieldTagOne, Message>> srcFederatedTables = new ArrayList<>();

    // A list of federated tables opened on Sink side
    private final List<Table<StringKey, ValueFieldTagOne, Message>> sinkFederatedTables = new ArrayList<>();

    // A list of tables open on Source side whose logical group is GROUP_A
    private final List<Table<StringKey, SampleGroupMsgA, Message>> srcTablesGroupA = new ArrayList<>();

    // A list of tables open on Sink side whose logical group is GROUP_A
    private final List<Table<StringKey, SampleGroupMsgA, Message>> sinkTablesGroupA = new ArrayList<>();

    // A list of tables open on Source side whose logical group is GROUP_B
    private final List<Table<StringKey, SampleGroupMsgB, Message>> srcTablesGroupB = new ArrayList<>();

    // A list of tables open on Sink side whose logical group is GROUP_B
    private final List<Table<StringKey, SampleGroupMsgB, Message>> sinkTablesGroupB = new ArrayList<>();

    /**
     * Initialize the 1-Source & 3-Sink topology and open the replication status table on Source side.
     */
    @Before
    public void setUp() throws Exception {
        setUp(numSource, numSink, DefaultClusterManager.TP_MIXED_MODEL_THREE_SINK);
        openLogReplicationStatusTable();
    }

    /**
     * This test verifies the basic snapshot sync and log entry sync workflow for LOGICAL_GROUP replication model.
     * (1) Bring up the 1-Source & 3-Sink topology
     * (2) Register the logical group client and setup initial group destination mapping as
     *     {GROUP_A: [SINK2]; GROUP_B: [SINK3]}
     * (3) Open tables for replication on both Source and Sink, and write data to tables on Source side
     * (4) Start replication servers and verify snapshot sync for all the 3 sessions completed
     * (5) Write more data on Source side for log entry sync, verify data on Sink side
     */
    @Test
    public void testBasicLogicalGroupReplication() throws Exception {
        // Register client and setup initial group destinations mapping
        CorfuRuntime clientRuntime = getClientRuntime();

        LogReplicationLogicalGroupClient logicalGroupClient =
                new LogReplicationLogicalGroupClient(clientRuntime, "00000000-0000-0000-0000-0000000000009");
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
        verifySessionInLogEntrySyncState(SINK1_INDEX, LogReplicationConfigManager.getDefaultSubscriber());
        // TODO: should be replaced by real client name after Sink session creation workflow is introduced
        //verifySessionInLogEntrySyncState(SINK2_INDEX, logReplicationConfigManager.clientReplicationSubscriber);
        //verifySessionInLogEntrySyncState(SINK3_INDEX, logReplicationConfigManager.clientReplicationSubscriber);

        // Verify tables' content on Sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
        //verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);

        // Write more data for log entry sync
        writeDataOnSource(NUM_WRITES, NUM_WRITES);

        // Verify tables' content on Sink side
        int targetWrites = 2 * NUM_WRITES;
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), targetWrites, sinkFederatedTables);
        //verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), targetWrites, sinkTablesGroupA);
        //verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), targetWrites, sinkTablesGroupB);

    }

    /**
     * This test verifies the client registered without initializing group destinations before log replication starts.
     * (1) Bring up the 1-Source & 3-Sink topology
     * (2) Register the logical group client without initial group destination mapping
     * (3) Open tables for replication on both Source and Sink, and write data to tables on Source side
     * (4) Start replication servers and verify snapshot sync for all the 3 sessions completed. (Note that the sessions
     *     with Sink2 and Sink3 will only have MERGE_ONLY streams for snapshot sync)
     * (5) Setup group destinations as {GROUP_A: [SINK2]; GROUP_B: [SINK3]}
     * (6) Verify a new round of snapshot sync happened with Sink2 and Sink3, and verify the data were replicated.
     */
    @Test
    public void testNoGroupSetupBeforeLogReplication() throws Exception {
        // Register client without initializing group destinations mapping
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

        // Subscribe snapshot plugin listener on each Sink cluster, Sink1 will only have 1 snapshot sync while Sink2
        // and Sink3 will have 2 snapshot sync for each, where 1 snapshot sync will update the listener twice.
        CountDownLatch latchSink1 = new CountDownLatch(PLUGIN_UPDATES_PER_SNAPSHOT_SYNC);
        SnapshotSyncPluginListener snapshotSyncPluginListenerSink1 = new SnapshotSyncPluginListener(latchSink1);
        subscribeToSnapshotSyncPluginTable(sinkCorfuStores.get(SINK1_INDEX), snapshotSyncPluginListenerSink1);

        CountDownLatch latchSink2 = new CountDownLatch(2 * PLUGIN_UPDATES_PER_SNAPSHOT_SYNC);
        SnapshotSyncPluginListener snapshotSyncPluginListenerSink2 = new SnapshotSyncPluginListener(latchSink2);
        subscribeToSnapshotSyncPluginTable(sinkCorfuStores.get(SINK2_INDEX), snapshotSyncPluginListenerSink2);

        CountDownLatch latchSink3 = new CountDownLatch(2 * PLUGIN_UPDATES_PER_SNAPSHOT_SYNC);
        SnapshotSyncPluginListener snapshotSyncPluginListenerSink3 = new SnapshotSyncPluginListener(latchSink3);
        subscribeToSnapshotSyncPluginTable(sinkCorfuStores.get(SINK3_INDEX), snapshotSyncPluginListenerSink3);

        // Write data to Source side tables
        writeDataOnSource(0, NUM_WRITES);

        // Start log replication
        startReplicationServers();

        // Verify all the sessions' snapshot sync completed. Note that the replication session with Sink2 and Sink3
        // will only replicate MERGE_ONLY streams (RegistryTable and ProtobufDescriptorTable)
        verifySessionInLogEntrySyncState(SINK1_INDEX, LogReplicationConfigManager.getDefaultSubscriber());
        // TODO: should be replaced by real client name after Sink session creation workflow is introduced
        //verifySessionInLogEntrySyncState(SINK2_INDEX, LogReplicationConfigManager.getDefaultLogicalGroupSubscriber());
        //verifySessionInLogEntrySyncState(SINK3_INDEX, LogReplicationConfigManager.getDefaultLogicalGroupSubscriber());

        // Verify tables' content on Sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        //verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), 0, sinkTablesGroupA);
        //verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), 0, sinkTablesGroupB);

        // Add group destination now and verify data replicated successfully. Adding group destination for GROUP_A and
        // GROUP_B will trigger snapshot sync with Sink2 and Sink3.
        logicalGroupClient.setDestinations(GROUP_A,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK2_INDEX)));
        logicalGroupClient.setDestinations(GROUP_B,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX)));

        // Verify tables' content on Sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        //verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
        //verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);

        // Await for the completion of countdown latches. Verify there was a new round of snapshot sync happened with
        // Sink2 and Sink3.
        latchSink1.await();
        latchSink2.await();
        latchSink3.await();
    }

    /**
     * This test verifies client registered twice while log replication is still working normally.
     * (1) Bring up the 1-Source & 3-Sink topology
     * (2) Register same client twice (two LogReplicationLogicalGroupClient instances returned)
     * (3) Setup group destination mapping via those 2 clients {GROUP_A: [SINK2]; GROUP_B: [SINK3]}.
     * (4) Start replication servers and verify snapshot sync and log entry sync succeeded.
     * (5) Verify both client instances could get the correct group destination mapping
     */
    @Test
    public void testSameClientDuplicateRegistration() throws Exception {
        // Register same client twice (two LogReplicationLogicalGroupClient instances returned)
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
        verifySessionInLogEntrySyncState(SINK1_INDEX, LogReplicationConfigManager.getDefaultSubscriber());
        // TODO: should be replaced by real client name after Sink session creation workflow is introduced
        //verifySessionInLogEntrySyncState(SINK2_INDEX, LogReplicationConfigManager.getDefaultLogicalGroupSubscriber());
        //verifySessionInLogEntrySyncState(SINK3_INDEX, LogReplicationConfigManager.getDefaultLogicalGroupSubscriber());

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

        // Verify both client instances could get the correct group destination mapping
        assertThat(logicalGroupClient.getDestinations(GROUP_B))
                .isEqualTo(Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX)));
        assertThat(duplicateClient.getDestinations(GROUP_A))
                .isEqualTo(Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK2_INDEX)));
    }

    /**
     * This test verifies group destination is added while no tables opened for that group at the beginning.
     * (1) Bring up the 1-Source & 3-Sink topology
     * (2) Register the logical group client and setup initial group destination mapping as
     *     {GROUP_A: [SINK2]; GROUP_B: [SINK3]}
     * (3) Open federated tables and GROUP_B tables on Source side, and write to these tables on Source side.
     * (4) Start replication servers and verify snapshot sync succeeded. (Sink2 will only receive RegistryTable and
     *     ProtobufDescriptorTable for snapshot sync)
     * (5) Open GROUP_A tables on Source side and write data to them
     * (6) Verify that GROUP_A tables' data get replicated to Sink2.
     */
    @Test
    public void testGroupAddedWithNoTableOpened() throws Exception {
        // Register client and initialize group destinations
        CorfuRuntime clientRuntime = getClientRuntime();
        LogReplicationLogicalGroupClient logicalGroupClient =
                new LogReplicationLogicalGroupClient(clientRuntime, SAMPLE_CLIENT_NAME);
        logicalGroupClient.setDestinations(GROUP_A,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK2_INDEX)));
        logicalGroupClient.setDestinations(GROUP_B,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX)));

        // Open tables for replication on both side
        int numTables = 2;
        // Source side only open federated tables and GROUP_B tables
        openFederatedTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcFederatedTables);
        openGroupBTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupB);
        // Sink side
        openFederatedTable(numTables, sinkCorfuStores.get(SINK1_INDEX), sinkFederatedTables);
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

        // Verify all the sessions' snapshot sync completed. (Note that Sink2 will only receive RegistryTable and
        // ProtobufDescriptorTable for snapshot sync)
        verifySessionInLogEntrySyncState(SINK1_INDEX, LogReplicationConfigManager.getDefaultSubscriber());
        // TODO: should be replaced by real client name after Sink session creation workflow is introduced
        //verifySessionInLogEntrySyncState(SINK2_INDEX, LogReplicationConfigManager.getDefaultLogicalGroupSubscriber());
        //verifySessionInLogEntrySyncState(SINK3_INDEX, LogReplicationConfigManager.getDefaultLogicalGroupSubscriber());

        // Verify tables' content on Sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);

        // Open and write to GROUP_A tables on Source side
        openGroupATable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupA);
        // Open and write to GROUP_A tables on Sink side (Sink2)
        openGroupATable(numTables, sinkCorfuStores.get(SINK2_INDEX), sinkTablesGroupA);

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

        // Open and write to GROUP_A tables data get replicated to Sink side (Sink2)
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
    }

    /**
     * This test verifies local writes are cleared during the snapshot sync of LOGICAL_GROUP replication
     * (1) Bring up the 1-Source & 3-Sink topology
     * (2) Register the logical group client and setup initial group destination mapping as
     *     {GROUP_A: [SINK2]; GROUP_B: [SINK3]}
     * (3) Open tables for replication on both Source and Sink side.
     * (4) Write data to Source side tables from 0 to 500, while write data to Sink side tables from 500 to 1000
     * (5) Start replication servers and verify snapshot sync succeeded.
     * (6) Verify tables' data on Sink side is overwritten by 0 to 500
     */
    @Test
    public void testSinkLocalWritesClearing() throws Exception {
        // Register client and initialize group destinations
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

        // Write data to Source side tables from 0 to 500
        writeDataOnSource(0, NUM_WRITES);

        // Write data to Sink side tables from 500 to 1000
        for (int i = NUM_WRITES; i < NUM_WRITES + NUM_WRITES; i++) {
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

        // Start log replication
        startReplicationServers();

        // Verify all the sessions' snapshot sync completed.
        verifySessionInLogEntrySyncState(SINK1_INDEX, LogReplicationConfigManager.getDefaultSubscriber());
        //verifySessionInLogEntrySyncState(SINK2_INDEX, LogReplicationConfigManager.getDefaultLogicalGroupSubscriber());
        //verifySessionInLogEntrySyncState(SINK3_INDEX, LogReplicationConfigManager.getDefaultLogicalGroupSubscriber());

        // Verify tables' content on Sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);
    }

    /**
     * This test verifies tables for replication are only opened on Source side, and Sink side could correctly apply
     * those tables' data.
     * (1) Bring up the 1-Source & 3-Sink topology
     * (2) Register the logical group client and setup initial group destination mapping as
     *     {GROUP_A: [SINK2]; GROUP_B: [SINK3]}
     * (3) Open tables for replication on Source side only, and write data to Source side
     * (4) Start replication servers and verify snapshot sync succeeded.
     */
    @Test
    public void testSourceNewTablesReplication() throws Exception {
        // Register client and initialize group destinations
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

        // Write data to tables opened on Source
        writeDataOnSource(0, NUM_WRITES);

        // Start log replication
        startReplicationServers();

        // Verify all the sessions' snapshot sync completed
        verifySessionInLogEntrySyncState(SINK1_INDEX, LogReplicationConfigManager.getDefaultSubscriber());
        //verifySessionInLogEntrySyncState(SINK2_INDEX, LogReplicationConfigManager.getDefaultLogicalGroupSubscriber());
        //verifySessionInLogEntrySyncState(SINK3_INDEX, LogReplicationConfigManager.getDefaultLogicalGroupSubscriber());

        // Open tables on Sink side
        openFederatedTable(numTables, sinkCorfuStores.get(SINK1_INDEX), sinkFederatedTables);
        openGroupATable(numTables, sinkCorfuStores.get(SINK2_INDEX), sinkTablesGroupA);
        openGroupBTable(numTables, sinkCorfuStores.get(SINK3_INDEX), sinkTablesGroupB);

        // Verify tables' content on Sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);
    }


    /**
     * This test verifies group destinations change through the client during log replication
     * (1) Bring up the 1-Source & 3-Sink topology
     * (2) Register the logical group client and setup initial group destination mapping as
     *     {GROUP_A: [SINK2]; GROUP_B: [SINK3]}
     * (3) Open tables for replication on both Source and Sink side, and write data to Source side
     * (4) Start replication servers and verify snapshot sync succeeded.
     * (5) Add GROUP_A to Sink3, and add GROUP_B to Sink2, so now the mapping is
     *     {GROUP_A: [SINK2, SINK3]; GROUP_B: [SINK2, SINK3]}
     * (6) Verify forced snapshot sync with Sink2 and Sink3 is triggered because of adding group destination
     * (7) Remove GROUP_B from Sink3, and now the mapping is {GROUP_A: [SINK2, SINK3]; GROUP_B: [SINK2]}
     * (8) Write more data to GROUP_B tables on Source side, and verify new data is only replicated to Sink2
     */
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
        CountDownLatch latchSink1 = new CountDownLatch(PLUGIN_UPDATES_PER_SNAPSHOT_SYNC);
        SnapshotSyncPluginListener snapshotSyncPluginListenerSink1 = new SnapshotSyncPluginListener(latchSink1);
        subscribeToSnapshotSyncPluginTable(sinkCorfuStores.get(SINK1_INDEX), snapshotSyncPluginListenerSink1);

        CountDownLatch latchSink2 = new CountDownLatch(2 * PLUGIN_UPDATES_PER_SNAPSHOT_SYNC);
        SnapshotSyncPluginListener snapshotSyncPluginListenerSink2 = new SnapshotSyncPluginListener(latchSink2);
        subscribeToSnapshotSyncPluginTable(sinkCorfuStores.get(SINK2_INDEX), snapshotSyncPluginListenerSink2);

        CountDownLatch latchSink3 = new CountDownLatch(2 * PLUGIN_UPDATES_PER_SNAPSHOT_SYNC);
        SnapshotSyncPluginListener snapshotSyncPluginListenerSink3 = new SnapshotSyncPluginListener(latchSink3);
        subscribeToSnapshotSyncPluginTable(sinkCorfuStores.get(SINK3_INDEX), snapshotSyncPluginListenerSink3);

        // Write data to Source side tables
        writeDataOnSource(0, NUM_WRITES);

        // Start log replication for all sessions
        startReplicationServers();

        // Verify all the sessions' snapshot sync completed
        verifySessionInLogEntrySyncState(SINK1_INDEX, LogReplicationConfigManager.getDefaultSubscriber());
        // TODO: should be replaced by real client name after Sink session creation workflow is introduced
        //verifySessionInLogEntrySyncState(SINK2_INDEX, LogReplicationConfigManager.getDefaultLogicalGroupSubscriber());
        //verifySessionInLogEntrySyncState(SINK3_INDEX, LogReplicationConfigManager.getDefaultLogicalGroupSubscriber());

        // Verify tables' content on Sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);

        // Add GROUP_A to Sink3 and verify the GROUP_A tables' data is successfully replicated to Sink3.
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

        // Add GROUP_B to Sink2 and verify the GROUP_B tables' data is successfully replicated to Sink2.
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

    /**
     * This test verifies log replication could work properly if a destination for a group is removed and
     * then added back.
     * (1) Bring up the 1-Source & 3-Sink topology
     * (2) Register the logical group client and setup initial group destination mapping as
     *     {GROUP_A: [SINK2, SINK3]; GROUP_B: [SINK2, SINK3]}
     * (3) Open tables for replication on both Source and Sink side, and write data to Source side
     * (4) Start replication servers and verify snapshot sync succeeded.
     * (5) Remove GROUP_A from Sink3 and GROUP_B from Sink2, now the group destination mapping is
     *     {GROUP_A: [SINK2]; GROUP_B: [SINK3]}
     * (6) On Source side write more data to GROUP_A tables and GROUP_B tables, verify new data is not replicated
     *     to the Sink that was removed in last step
     * (7) Add GROUP_A back to Sink3 and GROUP_B back to Sink2, now the group destination mapping is
     *     {GROUP_A: [SINK2, SINK3]; GROUP_B: [SINK2, SINK3]}
     * (8) Verify the new data added in step 6 is replicated to Sink2 and Sink3 now, and verify a new round of
     *     snapshot sync is triggered with Sink2 and Sink3
     */
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
        CountDownLatch latchSink1 = new CountDownLatch(PLUGIN_UPDATES_PER_SNAPSHOT_SYNC);
        SnapshotSyncPluginListener snapshotSyncPluginListenerSink1 = new SnapshotSyncPluginListener(latchSink1);
        subscribeToSnapshotSyncPluginTable(sinkCorfuStores.get(SINK1_INDEX), snapshotSyncPluginListenerSink1);

        CountDownLatch latchSink2 = new CountDownLatch(2 * PLUGIN_UPDATES_PER_SNAPSHOT_SYNC);
        SnapshotSyncPluginListener snapshotSyncPluginListenerSink2 = new SnapshotSyncPluginListener(latchSink2);
        subscribeToSnapshotSyncPluginTable(sinkCorfuStores.get(SINK2_INDEX), snapshotSyncPluginListenerSink2);

        CountDownLatch latchSink3 = new CountDownLatch(2 * PLUGIN_UPDATES_PER_SNAPSHOT_SYNC);
        SnapshotSyncPluginListener snapshotSyncPluginListenerSink3 = new SnapshotSyncPluginListener(latchSink3);
        subscribeToSnapshotSyncPluginTable(sinkCorfuStores.get(SINK3_INDEX), snapshotSyncPluginListenerSink3);

        // Open tables for replication on both side
        int numTables = 3;
        // Source
        openFederatedTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcFederatedTables);
        openGroupATable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupA);
        openGroupBTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupB);
        // Sink
        openFederatedTable(numTables, sinkCorfuStores.get(SINK1_INDEX), sinkFederatedTables);
        openGroupATable(numTables, sinkCorfuStores.get(SINK2_INDEX), sinkTablesGroupA);
        openGroupBTable(numTables, sinkCorfuStores.get(SINK3_INDEX), sinkTablesGroupB);

        // Write data to Source side tables
        writeDataOnSource(0, NUM_WRITES);

        // Start log replication for all sessions
        startReplicationServers();

        // Verify all the sessions' snapshot sync completed
        verifySessionInLogEntrySyncState(SINK1_INDEX, LogReplicationConfigManager.getDefaultSubscriber());
        // TODO: should be replaced by real client name after Sink session creation workflow is introduced
        //verifySessionInLogEntrySyncState(SINK2_INDEX, LogReplicationConfigManager.getDefaultLogicalGroupSubscriber());
        //verifySessionInLogEntrySyncState(SINK3_INDEX, LogReplicationConfigManager.getDefaultLogicalGroupSubscriber());

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

        // Remove GROUP_A from Sink3 and GROUP_B from Sink2
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

        // Add GROUP_A and GROUP_B back and verify new data is replicated to Sink2 and Sink3
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

    /**
     * This test verifies forced snapshot sync will be triggered after Source is CP/Trimmed, because log entry sync
     * cannot resume from the trimmed address space.
     * (1) Bring up the 1-Source & 3-Sink topology
     * (2) Register the logical group client and setup initial group destination mapping as
     *     {GROUP_A: [SINK2]; GROUP_B: [SINK3]}
     * (3) Open tables for replication on both Source and Sink side, and write data to Source side
     * (4) Start replication servers and verify snapshot sync succeeded.
     * (5) Stop Source replicator and CP/Trim all the Sinks (to trim the shadow streams)
     * (6) Write new data on Source and verify they are not replicated to any Sink
     * (7) CP/Trim Source and restart Source replicator
     * (8) Verify forced snapshot sync is triggered with each Sink and verify all data replicated to Sink side.
     */
    @Test
    public void testCheckpointTrimSourceBetweenSnapshotSync() throws Exception {
        // Register client and initialize group destinations
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

        // Subscribe snapshot plugin listener on each Sink cluster. All the 3 Sinks will have 2 rounds of snapshot
        // sync in this test.
        CountDownLatch latchSink1 = new CountDownLatch(2 * PLUGIN_UPDATES_PER_SNAPSHOT_SYNC);
        SnapshotSyncPluginListener snapshotSyncPluginListenerSink1 = new SnapshotSyncPluginListener(latchSink1);
        subscribeToSnapshotSyncPluginTable(sinkCorfuStores.get(SINK1_INDEX), snapshotSyncPluginListenerSink1);

        CountDownLatch latchSink2 = new CountDownLatch(2 * PLUGIN_UPDATES_PER_SNAPSHOT_SYNC);
        SnapshotSyncPluginListener snapshotSyncPluginListenerSink2 = new SnapshotSyncPluginListener(latchSink2);
        subscribeToSnapshotSyncPluginTable(sinkCorfuStores.get(SINK2_INDEX), snapshotSyncPluginListenerSink2);

        CountDownLatch latchSink3 = new CountDownLatch(2 * PLUGIN_UPDATES_PER_SNAPSHOT_SYNC);
        SnapshotSyncPluginListener snapshotSyncPluginListenerSink3 = new SnapshotSyncPluginListener(latchSink3);
        subscribeToSnapshotSyncPluginTable(sinkCorfuStores.get(SINK3_INDEX), snapshotSyncPluginListenerSink3);

        // Write data to Source side tables
        writeDataOnSource(0, NUM_WRITES);

        // Start log replication for all sessions
        startReplicationServers();

        // Verify all the sessions' snapshot sync completed
        verifySessionInLogEntrySyncState(SINK1_INDEX, LogReplicationConfigManager.getDefaultSubscriber());
        // TODO: should be replaced by real client name after Sink session creation workflow is introduced
        //verifySessionInLogEntrySyncState(SINK2_INDEX, LogReplicationConfigManager.getDefaultLogicalGroupSubscriber());
        //verifySessionInLogEntrySyncState(SINK3_INDEX, LogReplicationConfigManager.getDefaultLogicalGroupSubscriber());

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
        sourceReplicationServers.add(runReplicationServer(sourceReplicationPorts.get(SOURCE_INDEX),
                sourceCorfuPorts.get(SOURCE_INDEX), pluginConfigFilePath));
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

    /**
     * This test verifies if CP/Trim cycle happens at Sink side during log entry sync, it could be resumed after
     * re-negotiation with Source side.
     * (1) Bring up the 1-Source & 3-Sink topology
     * (2) Register the logical group client and setup initial group destination mapping as
     *     {GROUP_A: [SINK2]; GROUP_B: [SINK3]}
     * (3) Open tables for replication on both Source and Sink side, and write data to Source side
     * (4) Start replication servers and verify snapshot sync succeeded.
     * (5) Stop Source replicator and CP/Trim all the Sinks (to trim the shadow streams)
     * (6) Write new data on Source and verify they are not replicated to any Sink
     * (7) Restart Source replicator, verify new data replicated to Sink and verify no snapshot sync happened.
     */
    @Test
    public void testCheckpointTrimSinkDuringLogEntrySync() throws Exception {
        // Register client and initialize group destinations
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
        verifySessionInLogEntrySyncState(SINK1_INDEX, LogReplicationConfigManager.getDefaultSubscriber());
        // TODO: should be replaced by real client name after Sink session creation workflow is introduced
        //verifySessionInLogEntrySyncState(SINK2_INDEX, LogReplicationConfigManager.getDefaultLogicalGroupSubscriber());
        //verifySessionInLogEntrySyncState(SINK3_INDEX, LogReplicationConfigManager.getDefaultLogicalGroupSubscriber());

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

        // Subscribe snapshot plugin listener on each Sink cluster. They should not receive any delta starting from
        // this point, as log entry sync will be resumed.
        SnapshotSyncPluginListener snapshotSyncPluginListenerSink1 = new SnapshotSyncPluginListener(new CountDownLatch(0));
        subscribeToSnapshotSyncPluginTable(sinkCorfuStores.get(SINK1_INDEX), snapshotSyncPluginListenerSink1);

        SnapshotSyncPluginListener snapshotSyncPluginListenerSink2 = new SnapshotSyncPluginListener(new CountDownLatch(0));
        subscribeToSnapshotSyncPluginTable(sinkCorfuStores.get(SINK2_INDEX), snapshotSyncPluginListenerSink2);

        SnapshotSyncPluginListener snapshotSyncPluginListenerSink3 = new SnapshotSyncPluginListener(new CountDownLatch(0));
        subscribeToSnapshotSyncPluginTable(sinkCorfuStores.get(SINK3_INDEX), snapshotSyncPluginListenerSink3);

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
        sourceReplicationServers.add(runReplicationServer(sourceReplicationPorts.get(SOURCE_INDEX),
                sourceCorfuPorts.get(SOURCE_INDEX), pluginConfigFilePath));
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

        // Verify no snapshot sync happened with any Sink
        assertThat(snapshotSyncPluginListenerSink1.getUpdates().size()).isEqualTo(0);
        assertThat(snapshotSyncPluginListenerSink2.getUpdates().size()).isEqualTo(0);
        assertThat(snapshotSyncPluginListenerSink3.getUpdates().size()).isEqualTo(0);
    }

    /**
     * This test verifies discovery service could correctly handle the case where a Sink cluster is removed from
     * the topology (new topology discovered).
     * (1) Bring up the 1-Source & 3-Sink topology
     * (2) Register the logical group client and setup initial group destination mapping as
     *     {GROUP_A: [SINK2]; GROUP_B: [SINK3]}
     * (3) Verify basic snapshot sync and log entry sync upon this topology
     * (4) Apply a new topology that is 1-Source & 2-Sink (Sink3 removed from topology)
     * (5) Write more data on Source side, verify log entry sync continued for Sink1 and Sink2, while stopped for Sink3
     */
    @Test
    public void testLogicalGroupSinkRemoved() throws Exception {
        // Register client and initialize group destinations
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

        //LogReplicationConfigManager logReplicationConfigManager = new LogReplicationConfigManager()

        // Verify all the sessions' snapshot sync completed
        verifySessionInLogEntrySyncState(SINK1_INDEX, LogReplicationConfigManager.getDefaultSubscriber());
        // TODO: should be replaced by real client name after Sink session creation workflow is introduced
        //verifySessionInLogEntrySyncState(SINK2_INDEX, LogReplicationConfigManager.re);
        //verifySessionInLogEntrySyncState(SINK3_INDEX, LogReplicationConfigManager.getDefaultLogicalGroupSubscriber());

        // Verify tables' content on Sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);

        // Subscribe snapshot plugin listener on each Sink cluster. No snapshot sync should happen from this point.
        SnapshotSyncPluginListener snapshotSyncPluginListenerSink1 = new SnapshotSyncPluginListener(new CountDownLatch(0));
        subscribeToSnapshotSyncPluginTable(sinkCorfuStores.get(SINK1_INDEX), snapshotSyncPluginListenerSink1);

        SnapshotSyncPluginListener snapshotSyncPluginListenerSink2 = new SnapshotSyncPluginListener(new CountDownLatch(0));
        subscribeToSnapshotSyncPluginTable(sinkCorfuStores.get(SINK2_INDEX), snapshotSyncPluginListenerSink2);

        SnapshotSyncPluginListener snapshotSyncPluginListenerSink3 = new SnapshotSyncPluginListener(new CountDownLatch(0));
        subscribeToSnapshotSyncPluginTable(sinkCorfuStores.get(SINK3_INDEX), snapshotSyncPluginListenerSink3);

        // Write more data for log entry sync
        writeDataOnSource(NUM_WRITES, NUM_WRITES);

        // Verify tables' content on Sink side
        int targetWrites = 2 * NUM_WRITES;
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), targetWrites, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), targetWrites, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), targetWrites, sinkTablesGroupB);

        // Apply a new topology where Sink3 is removed from the topology.
        Table<ExampleSchemas.ClusterUuidMsg, ExampleSchemas.ClusterUuidMsg, ExampleSchemas.ClusterUuidMsg> configTable =
                sourceCorfuStores.get(SOURCE_INDEX).getTable(DefaultClusterManager.CONFIG_NAMESPACE,
                        DefaultClusterManager.CONFIG_TABLE_NAME);
        try (TxnContext txn = sourceCorfuStores.get(SOURCE_INDEX).txn(DefaultClusterManager.CONFIG_NAMESPACE)) {
            txn.putRecord(configTable, DefaultClusterManager.OP_TWO_SINK_MIXED,
                    DefaultClusterManager.OP_TWO_SINK_MIXED, DefaultClusterManager.OP_TWO_SINK_MIXED);
            txn.commit();
        }

        // Insert a sleep interval to let the new topology take effect
        TimeUnit.SECONDS.sleep(SLEEP_INTERVAL);

        // Write more data for log entry sync
        writeDataOnSource(targetWrites, NUM_WRITES);

        // Verify log entry sync continued with Sink1 and Sink2, but stopped with Sink3
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), targetWrites + NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), targetWrites + NUM_WRITES, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), targetWrites, sinkTablesGroupB);

        // Verify no snapshot sync happened with any Sink
        assertThat(snapshotSyncPluginListenerSink1.getUpdates().size()).isEqualTo(0);
        assertThat(snapshotSyncPluginListenerSink2.getUpdates().size()).isEqualTo(0);
        assertThat(snapshotSyncPluginListenerSink3.getUpdates().size()).isEqualTo(0);
    }


    /**
     * This test verifies Sink side stream listeners could correctly receive updates from the tables written by LR
     * during log entry sync.
     * (1) Bring up the 1-Source & 3-Sink topology
     * (2) Register the logical group client and setup initial group destination mapping as
     *     {GROUP_A: [SINK2]; GROUP_B: [SINK3]}
     * (3) Write some data on Source side and verify snapshot sync completed.
     * (4) Initiate testing stream listeners and subscribe to tag_one on Sink
     * (5) Write more data on Source for log entry sync, verify all the stream listeners received correct updates
     */
    @Test
    public void testSinkStreamingLogEntrySync() throws Exception {
        // Register client and initialize group destinations
        CorfuRuntime clientRuntime = getClientRuntime();
        LogReplicationLogicalGroupClient logicalGroupClient =
                new LogReplicationLogicalGroupClient(clientRuntime, SAMPLE_CLIENT_NAME);
        logicalGroupClient.setDestinations(GROUP_A,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK2_INDEX)));
        logicalGroupClient.setDestinations(GROUP_B,
                Collections.singletonList(DefaultClusterConfig.getSinkClusterIds().get(SINK3_INDEX)));

        // Open tables for replication on both sides
        int numTables = 2;
        // Source
        openFederatedTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcFederatedTables);
        openGroupATable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupA);
        openGroupBTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupB);
        Set<UUID> federatedTableId = srcFederatedTables.stream()
                .map(table -> CorfuRuntime.getStreamID(table.getFullyQualifiedTableName())).collect(Collectors.toSet());
        Set<UUID> groupATableId = srcTablesGroupA.stream()
                .map(table -> CorfuRuntime.getStreamID(table.getFullyQualifiedTableName())).collect(Collectors.toSet());
        Set<UUID> groupBTableId = srcTablesGroupB.stream()
                .map(table -> CorfuRuntime.getStreamID(table.getFullyQualifiedTableName())).collect(Collectors.toSet());
        List<String> federatedTableNames = IntStream.range(1, numTables + 1)
                .mapToObj(i -> FEDERATED_TABLE_PREFIX.concat(String.valueOf(i))).collect(Collectors.toList());
        List<String> groupATableNames = IntStream.range(1, numTables + 1)
                .mapToObj(i -> GROUPA_TABLE_PREFIX.concat(String.valueOf(i))).collect(Collectors.toList());
        List<String> groupBTableNames = IntStream.range(1, numTables + 1)
                .mapToObj(i -> GROUPB_TABLE_PREFIX.concat(String.valueOf(i))).collect(Collectors.toList());
        // Sink
        openFederatedTable(numTables, sinkCorfuStores.get(SINK1_INDEX), sinkFederatedTables);
        openGroupATable(numTables, sinkCorfuStores.get(SINK2_INDEX), sinkTablesGroupA);
        openGroupBTable(numTables, sinkCorfuStores.get(SINK3_INDEX), sinkTablesGroupB);

        // Write data to tables on source side
        writeDataOnSource(0, NUM_WRITES);

        // Start log replication
        startReplicationServers();

        // Verify all the sessions' snapshot sync completed
        verifySessionInLogEntrySyncState(SINK1_INDEX, LogReplicationConfigManager.getDefaultSubscriber());
        //verifySessionInLogEntrySyncState(SINK2_INDEX, LogReplicationConfigManager.getDefaultLogicalGroupSubscriber());
        //verifySessionInLogEntrySyncState(SINK3_INDEX, LogReplicationConfigManager.getDefaultLogicalGroupSubscriber());

        // Verify tables' content on Sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);

        // Initialize testing stream listeners. Note that in log entry sync txn boundaries are preserved.
        int expectedMessageSize = NUM_WRITES * numTables;
        CountDownLatch numMsgLatchSink1 = new CountDownLatch(expectedMessageSize);
        CountDownLatch numTxLatchSink1 = new CountDownLatch(NUM_WRITES);
        StreamingSinkListener listenerSink1 = new StreamingSinkListener(
                numMsgLatchSink1,
                numTxLatchSink1,
                federatedTableId);

        CountDownLatch numMsgLatchSink2 = new CountDownLatch(expectedMessageSize);
        CountDownLatch numTxLatchSink2 = new CountDownLatch(NUM_WRITES);
        StreamingSinkListener listenerSink2 = new StreamingSinkListener(
                numMsgLatchSink2,
                numTxLatchSink2,
                groupATableId);

        CountDownLatch numMsgLatchSink3 = new CountDownLatch(expectedMessageSize);
        CountDownLatch numTxLatchSink3 = new CountDownLatch(NUM_WRITES);
        StreamingSinkListener listenerSink3 = new StreamingSinkListener(
                numMsgLatchSink3,
                numTxLatchSink3,
                groupBTableId);

        // Subscribe the listeners on the sink side
        sinkCorfuStores.get(SINK1_INDEX).subscribeListener(listenerSink1, NAMESPACE, STREAMING_TAG, federatedTableNames);
        sinkCorfuStores.get(SINK2_INDEX).subscribeListener(listenerSink2, NAMESPACE, STREAMING_TAG, groupATableNames);
        sinkCorfuStores.get(SINK3_INDEX).subscribeListener(listenerSink3, NAMESPACE, STREAMING_TAG, groupBTableNames);

        // Write new entries to the tables
        writeDataOnSource(NUM_WRITES, NUM_WRITES);

        // Verify tables' content on Sink side
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES + NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES + NUM_WRITES, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES + NUM_WRITES, sinkTablesGroupB);

        // Wait until listeners get all the update
        numMsgLatchSink1.await();
        numMsgLatchSink2.await();
        numMsgLatchSink3.await();

        numTxLatchSink1.await();
        numTxLatchSink2.await();
        numTxLatchSink3.await();

        // Verify the listeners get all the update
        assertThat(listenerSink1.messages).hasSize(expectedMessageSize);
        assertThat(listenerSink2.messages).hasSize(expectedMessageSize);
        assertThat(listenerSink3.messages).hasSize(expectedMessageSize);
    }

    /**
     * This test verifies Sink side stream listeners could correctly receive updates from the tables written by LR
     * during snapshot sync.
     * (1) Bring up the 1-Source & 3-Sink topology
     * (2) Register the logical group client and setup initial group destination mapping as
     *     {GROUP_A: [SINK2]; GROUP_B: [SINK3]}
     * (3) Open tables with tag_one on Source and Sink
     * (4) Initiate testing stream listeners and subscribe to tag_one on Sink
     * (5) Verify snapshot sync completed and all the stream listeners received correct updates
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

        // Open tables on both side
        int numTables = 2;
        // Source
        openFederatedTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcFederatedTables);
        openGroupATable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupA);
        openGroupBTable(numTables, sourceCorfuStores.get(SOURCE_INDEX), srcTablesGroupB);
        Set<UUID> federatedTableId = srcFederatedTables.stream()
                .map(table -> CorfuRuntime.getStreamID(table.getFullyQualifiedTableName())).collect(Collectors.toSet());
        Set<UUID> groupATableId = srcTablesGroupA.stream()
                .map(table -> CorfuRuntime.getStreamID(table.getFullyQualifiedTableName())).collect(Collectors.toSet());
        Set<UUID> groupBTableId = srcTablesGroupB.stream()
                .map(table -> CorfuRuntime.getStreamID(table.getFullyQualifiedTableName())).collect(Collectors.toSet());

        // Sink
        openFederatedTable(numTables, sinkCorfuStores.get(SINK1_INDEX), sinkFederatedTables);
        openGroupATable(numTables, sinkCorfuStores.get(SINK2_INDEX), sinkTablesGroupA);
        openGroupBTable(numTables, sinkCorfuStores.get(SINK3_INDEX), sinkTablesGroupB);

        // Note that each test table will be added a CLEAR_ENTRY during snapshot sync
        int expectedMessageSize = numTables * (NUM_WRITES + 1);

        // Initialize the streamListeners for each Sink. Note that data is batched during snapshot sync, so the number
        // of transactions each listener will receive is equal to the number of tables.
        CountDownLatch numMsgLatchSink1 = new CountDownLatch(expectedMessageSize);
        CountDownLatch numTxLatchSink1 = new CountDownLatch(numTables);
        StreamingSinkListener listenerSink1 = new StreamingSinkListener(
                numMsgLatchSink1,
                numTxLatchSink1,
                federatedTableId);

        CountDownLatch numMsgLatchSink2 = new CountDownLatch(expectedMessageSize);
        CountDownLatch numTxLatchSink2 = new CountDownLatch(numTables);
        StreamingSinkListener listenerSink2 = new StreamingSinkListener(
                numMsgLatchSink2,
                numTxLatchSink2,
                groupATableId);

        CountDownLatch numMsgLatchSink3 = new CountDownLatch(expectedMessageSize);
        CountDownLatch numTxLatchSink3 = new CountDownLatch(numTables);
        StreamingSinkListener listenerSink3 = new StreamingSinkListener(
                numMsgLatchSink3,
                numTxLatchSink3,
                groupBTableId);

        // Subscribe the listeners on the sink side
        sinkCorfuStores.get(SINK1_INDEX).subscribeListener(listenerSink1, NAMESPACE, STREAMING_TAG);
        sinkCorfuStores.get(SINK2_INDEX).subscribeListener(listenerSink2, NAMESPACE, STREAMING_TAG);
        sinkCorfuStores.get(SINK3_INDEX).subscribeListener(listenerSink3, NAMESPACE, STREAMING_TAG);

        // Write the data to tables on source side
        writeDataOnSource(0, NUM_WRITES);

        // Start log replication
        startReplicationServers();

        // Verify all the sessions' snapshot sync completed
        verifySessionInLogEntrySyncState(SINK1_INDEX, LogReplicationConfigManager.getDefaultSubscriber());
        //verifySessionInLogEntrySyncState(SINK2_INDEX, LogReplicationConfigManager.getDefaultLogicalGroupSubscriber());
        //verifySessionInLogEntrySyncState(SINK3_INDEX, LogReplicationConfigManager.getDefaultLogicalGroupSubscriber());

        // Verify the data is successfully replicated
        verifyFederatedTableData(sinkCorfuStores.get(SINK1_INDEX), NUM_WRITES, sinkFederatedTables);
        verifyGroupATableData(sinkCorfuStores.get(SINK2_INDEX), NUM_WRITES, sinkTablesGroupA);
        verifyGroupBTableData(sinkCorfuStores.get(SINK3_INDEX), NUM_WRITES, sinkTablesGroupB);

        // Wait until listeners get all the update
        numMsgLatchSink1.await();
        numMsgLatchSink2.await();
        numMsgLatchSink3.await();

        numTxLatchSink1.await();
        numTxLatchSink2.await();
        numTxLatchSink3.await();

        // Verify the listeners get all the update
        assertThat(listenerSink1.messages).hasSize(expectedMessageSize);
        assertThat(listenerSink2.messages).hasSize(expectedMessageSize);
        assertThat(listenerSink3.messages).hasSize(expectedMessageSize);
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

    /**
     * Open federated table using the given CorfuStore and add it to the list.
     *
     * @param num Number of federated table to be opened.
     * @param corfuStore CorfuStore that will open the tables.
     * @param tableList List of tables to which the newly opened tables should be added.
     */
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

    /**
     * Open GROUP_A table using the given CorfuStore and add it to the list.
     *
     * @param num Number of GROUP_A table to be opened.
     * @param corfuStore CorfuStore that will open the tables.
     * @param tableList List of tables to which the newly opened tables should be added.
     */
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

    /**
     * Open GROUP_B table using the given CorfuStore and add it to the list.
     *
     * @param num Number of GROUP_B table to be opened.
     * @param corfuStore CorfuStore that will open the tables.
     * @param tableList List of tables to which the newly opened tables should be added.
     */
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
                .isEqualTo(SnapshotSyncInfo.SnapshotSyncType.DEFAULT);
        assertThat(status.getSourceStatus().getReplicationInfo().getSnapshotSyncInfo().getStatus())
                .isEqualTo(SyncStatus.COMPLETED);
    }

    /**
     * Verify the count and actual content of a list of federated tables.
     *
     * @param corfuStore CorfuStore that the given tables were opened from.
     * @param expectedConsecutiveWrites Each table is expected to have a size of expectedConsecutiveWrites, and its
     *                                  content should be from 0 to expectedConsecutiveWrites (exclusive)
     * @param tables A list of federated tables to be verified.
     */
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

            for (int i = 0; i < expectedConsecutiveWrites; i++) {
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
     * Verify the count and actual content of a list of GROUP_A tables.
     *
     * @param corfuStore CorfuStore that the given tables were opened from.
     * @param expectedConsecutiveWrites Each table is expected to have a size of expectedConsecutiveWrites, and its
     *                                  content should be from 0 to expectedConsecutiveWrites (exclusive)
     * @param tables A list of federated tables to be verified.
     */
    private void verifyGroupATableData(CorfuStore corfuStore, int expectedConsecutiveWrites,
                                       List<Table<StringKey, SampleGroupMsgA, Message>> tables) {
        tables.forEach(table -> {
            log.info("table count: {}",table.count());
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
     * Verify the count and actual content of a list of GROUP_B tables.
     *
     * @param corfuStore CorfuStore that the given tables were opened from.
     * @param expectedConsecutiveWrites Each table is expected to have a size of expectedConsecutiveWrites, and its
     *                                  content should be from 0 to expectedConsecutiveWrites (exclusive)
     * @param tables A list of federated tables to be verified.
     */
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

    /**
     * Subscribe SnapshotSyncPluginListener to the CorfuStore at Sink side. The listener should receive one update
     * upon start of snapshot sync and one update at the end of snapshot sync.
     */
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
}
