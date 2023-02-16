package org.corfudb.integration;

import com.google.protobuf.Message;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterManager;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplicationLogicalGroupClient;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.test.SampleSchema.SampleGroupMsgA;
import org.corfudb.test.SampleSchema.SampleGroupMsgB;
import org.corfudb.test.SampleSchema.ValueFieldTagOne;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;



@SuppressWarnings("all")
public class LogReplicationLogicalGroupIT extends CorfuReplicationMultiSourceSinkIT {

    private static final int NUM_WRITES = 500;

    private static final String SAMPLE_CLIENT_NAME = "00000000-0000-0000-0000-0000000000001";

    private static final String GROUP_A = "groupA";

    private static final String GROUP_B = "groupB";

    private int numSource = 1;

    private int numSink = 3;

    private final List<Table<Sample.StringKey, ValueFieldTagOne, Message>> srcFederatedTables = new ArrayList<>();

    private final List<Table<Sample.StringKey, ValueFieldTagOne, Message>> sinkFederatedTables = new ArrayList<>();

    private final List<Table<Sample.StringKey, SampleGroupMsgA, Message>> srcTablesGroupA = new ArrayList<>();

    private final List<Table<Sample.StringKey, SampleGroupMsgA, Message>> sinkTablesGroupA = new ArrayList<>();

    private final List<Table<Sample.StringKey, SampleGroupMsgB, Message>> srcTablesGroupB = new ArrayList<>();

    private final List<Table<Sample.StringKey, SampleGroupMsgB, Message>> sinkTablesGroupB = new ArrayList<>();

    @Test
    public void testLogicalGroupReplicationEndToEnd() throws Exception {
        setUp(numSource, numSink, DefaultClusterManager.TP_MIXED_MODEL);

        openTables();

        CorfuRuntime clientRuntime = getClientRuntime();
        LogReplicationLogicalGroupClient logicalGroupClient =
                new LogReplicationLogicalGroupClient(clientRuntime, SAMPLE_CLIENT_NAME);
        logicalGroupClient.addDestination(GROUP_A, DefaultClusterConfig.getSinkClusterIds().get(1));
        logicalGroupClient.addDestination(GROUP_A, DefaultClusterConfig.getSinkClusterIds().get(2));
        logicalGroupClient.addDestination(GROUP_B, DefaultClusterConfig.getSinkClusterIds().get(2));

        // Write data to tables opened on each source cluster
        for (int i = 0; i < NUM_WRITES; i++) {
            Sample.StringKey key = Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build();
            ValueFieldTagOne federatedTablePayload = ValueFieldTagOne.newBuilder().setPayload(String.valueOf(i)).build();
            SampleGroupMsgA groupATablePayload = SampleGroupMsgA.newBuilder().setPayload(String.valueOf(i)).build();
            SampleGroupMsgB groupBTablePayload = SampleGroupMsgB.newBuilder().setPayload(String.valueOf(i)).build();

            try (TxnContext txn = sourceCorfuStores.get(0).txn(NAMESPACE)) {
                txn.putRecord(srcFederatedTables.get(0), key, federatedTablePayload, null);
                txn.putRecord(srcTablesGroupA.get(0), key, groupATablePayload, null);
                txn.putRecord(srcTablesGroupB.get(0), key, groupBTablePayload, null);
                txn.commit();
            }
        }

        int sizeA = srcTablesGroupA.get(0).count();

        startReplicationServers();

        // Wait until data is fully replicated
        while (sinkFederatedTables.get(0).count() != NUM_WRITES) {
            // Block until expected number of entries is reached
        }
        while (sinkTablesGroupA.get(0).count() != NUM_WRITES) {
            // Block until expected number of entries is reached
        }
        while (sinkTablesGroupB.get(0).count() != NUM_WRITES) {
            // Block until expected number of entries is reached
        }

        for (int i = NUM_WRITES; i < 2 * NUM_WRITES; i++) {
            Sample.StringKey key = Sample.StringKey.newBuilder().setKey(String.valueOf(i)).build();
            ValueFieldTagOne federatedTablePayload = ValueFieldTagOne.newBuilder().setPayload(String.valueOf(i)).build();
            SampleGroupMsgA groupATablePayload = SampleGroupMsgA.newBuilder().setPayload(String.valueOf(i)).build();
            SampleGroupMsgB groupBTablePayload = SampleGroupMsgB.newBuilder().setPayload(String.valueOf(i)).build();

            try (TxnContext txn = sourceCorfuStores.get(0).txn(NAMESPACE)) {
                txn.putRecord(srcFederatedTables.get(0), key, federatedTablePayload, null);
                txn.putRecord(srcTablesGroupA.get(0), key, groupATablePayload, null);
                txn.putRecord(srcTablesGroupB.get(0), key, groupBTablePayload, null);
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
    }

    @Test
    public void testSourceNewTablesReplication() {
        // TODO
    }

    @Test
    public void testSinkSideStreaming() {
        // TODO
    }

    @Test
    public void testGroupDestinationChange() {
        // TODO
    }

    @Test
    public void testForcedSnapshotSync() {
        // TODO
    }

    private CorfuRuntime getClientRuntime() {
        return CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build())
                .parseConfigurationString(DEFAULT_HOST + ":" + DEFAULT_PORT)
                .connect();
    }

    private void openTables() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException {
        Table<Sample.StringKey, ValueFieldTagOne, Message> federatedTable;
        Table<Sample.StringKey, SampleGroupMsgA, Message> tableGroupA;
        Table<Sample.StringKey, SampleGroupMsgB, Message> tableGroupB;
        CorfuStore srcCorfuStore = sourceCorfuStores.get(0);
        int federatedTableIndex = 0;
        int groupATableIndex = 1;
        int groupBTableIndex = 2;

        // Open federated table for FULL_TABLE replication
        federatedTable = srcCorfuStore.openTable(NAMESPACE, tableNames.get(federatedTableIndex),
                        Sample.StringKey.class, ValueFieldTagOne.class,
                        null, TableOptions.fromProtoSchema(ValueFieldTagOne.class));
        srcFederatedTables.add(federatedTable);

        federatedTable = sinkCorfuStores.get(federatedTableIndex).openTable(NAMESPACE, tableNames.get(federatedTableIndex),
                        Sample.StringKey.class, ValueFieldTagOne.class,
                        null, TableOptions.fromProtoSchema(ValueFieldTagOne.class));
        sinkFederatedTables.add(federatedTable);

        // Open logical group tables for LOGICAL_GROUP replication
        tableGroupA = srcCorfuStore.openTable(NAMESPACE, tableNames.get(groupATableIndex),
                        Sample.StringKey.class, SampleGroupMsgA.class,
                        null, TableOptions.fromProtoSchema(SampleGroupMsgA.class));
        srcTablesGroupA.add(tableGroupA);

        tableGroupA = sinkCorfuStores.get(groupATableIndex).openTable(NAMESPACE, tableNames.get(groupATableIndex),
                        Sample.StringKey.class, SampleGroupMsgA.class,
                        null, TableOptions.fromProtoSchema(SampleGroupMsgA.class));
        sinkTablesGroupA.add(tableGroupA);

        tableGroupB = srcCorfuStore.openTable(NAMESPACE, tableNames.get(groupBTableIndex),
                        Sample.StringKey.class, SampleGroupMsgB.class,
                        null, TableOptions.fromProtoSchema(SampleGroupMsgB.class));
        srcTablesGroupB.add(tableGroupB);

        tableGroupB = sinkCorfuStores.get(groupBTableIndex).openTable(NAMESPACE, tableNames.get(groupBTableIndex),
                        Sample.StringKey.class, SampleGroupMsgB.class,
                        null, TableOptions.fromProtoSchema(SampleGroupMsgB.class));
        sinkTablesGroupB.add(tableGroupB);
    }
}
