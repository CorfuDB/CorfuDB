package org.corfudb.integration;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultLogReplicationConfigAdapter;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableSchema;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.test.SampleSchema;
import org.junit.After;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.fail;

@Slf4j
public class CorfuReplicationMultiSourceSinkIT extends AbstractIT {
    protected final int sourceSiteCorfuPort1 = 9000;
    protected final int sourceSiteCorfuPort2 = 9002;
    protected final int sourceSiteCorfuPort3 = 9004;
    protected final int sinkSiteCorfuPort1 = 9001;
    protected final int sinkSiteCorfuPort2 = 9003;
    protected final int sinkSiteCorfuPort3 = 9005;

    protected final int sourceReplicationPort1 = 9010;
    protected final int sourceReplicationPort2 = 9011;
    protected final int sourceReplicationPort3 = 9012;
    protected final int sinkReplicationPort1 = 9020;
    protected final int sinkReplicationPort2 = 9021;
    protected final int sinkReplicationPort3 = 9022;

    protected Process sourceCorfu1 = null;
    protected Process sourceCorfu2 = null;
    protected Process sourceCorfu3 = null;
    protected Process sinkCorfu1 = null;
    protected Process sinkCorfu2 = null;
    protected Process sinkCorfu3 = null;

    protected Process sourceReplicationServer1 = null;
    protected Process sourceReplicationServer2 = null;
    protected Process sourceReplicationServer3 = null;
    protected Process sinkReplicationServer1 = null;
    protected Process sinkReplicationServer2 = null;
    protected Process sinkReplicationServer3 = null;

    protected CorfuRuntime sourceRuntime1;
    protected CorfuRuntime sourceRuntime2;
    protected CorfuRuntime sourceRuntime3;
    protected CorfuRuntime sinkRuntime1;
    protected CorfuRuntime sinkRuntime2;
    protected CorfuRuntime sinkRuntime3;

    protected CorfuStore corfuStoreSource1;
    protected CorfuStore corfuStoreSource2;
    protected CorfuStore corfuStoreSource3;
    protected CorfuStore corfuStoreSink1;
    protected CorfuStore corfuStoreSink2;
    protected CorfuStore corfuStoreSink3;

    protected final String sourceEndpoint1 = DEFAULT_HOST + ":" + sourceSiteCorfuPort1;
    protected final String sourceEndpoint2 = DEFAULT_HOST + ":" + sourceSiteCorfuPort2;
    protected final String sourceEndpoint3 = DEFAULT_HOST + ":" + sourceSiteCorfuPort3;
    protected final String sinkEndpoint1 = DEFAULT_HOST + ":" + sinkSiteCorfuPort1;
    protected final String sinkEndpoint2 = DEFAULT_HOST + ":" + sinkSiteCorfuPort2;
    protected final String sinkEndpoint3 = DEFAULT_HOST + ":" + sinkSiteCorfuPort3;

    protected static final String TABLE_1 = "Table001";
    protected static final String TABLE_2 = "Table002";
    protected static final String TABLE_3 = "Table003";
    protected static final String TABLE_4 = "Table004";

    protected static final String NAMESPACE = DefaultLogReplicationConfigAdapter.NAMESPACE;

    protected static final String STREAM_TAG = DefaultLogReplicationConfigAdapter.TAG_ONE;

    // DefaultClusterConfig contains 3 Source and Sink clusters each.  Depending on how many clusters the test
    // starts, the number of functional/available clusters may be less but 3 is the max number.
    protected static final int MAX_REMOTE_CLUSTERS = 3;

    // The number of updates on the ReplicationStatus table on the Sink during initial startup is 3(one for each
    // Source cluster - the number of available Source clusters may be <3 but the topology from DefaultClusterConfig
    // contains 3 Source clusters)
    protected static final int NUM_INITIAL_REPLICATION_STATUS_UPDATES = MAX_REMOTE_CLUSTERS;

    // The number of updates on the Sink ReplicationStatus Table during Snapshot Sync from single cluster
    // (1) When starting snapshot sync apply : is_data_consistent = false
    // (2) When completing snapshot sync apply : is_data_consistent = true
    protected static final int NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE = 2;

    protected static final int NUM_RECORDS_IN_TABLE = 3;

    protected String pluginConfigFilePath;

    protected ReplicationStatusListener sourceListener1;
    protected ReplicationStatusListener sourceListener2;
    protected ReplicationStatusListener sourceListener3;
    protected ReplicationStatusListener sinkListener1;
    protected ReplicationStatusListener sinkListener2;
    protected ReplicationStatusListener sinkListener3;

    protected void writeData(CorfuStore corfuStore, String tableName, Table table, int startIndex, int numRecords) {
        for (int i = startIndex; i < (startIndex + numRecords); i++) {
            Sample.StringKey key = Sample.StringKey.newBuilder().setKey(tableName + " key " + i).build();
            SampleSchema.ValueFieldTagOne payload = SampleSchema.ValueFieldTagOne.newBuilder().setPayload(
                tableName + " payload " + i).build();
            try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                txn.putRecord(table, key, payload, null);
                txn.commit();
            }
        }
    }

    protected void deleteRecord(CorfuStore corfuStore, String tableName, int index) {
        Sample.StringKey key = Sample.StringKey.newBuilder().setKey(tableName + " key " + index).build();

        try (TxnContext txnContext = corfuStore.txn(NAMESPACE)) {
            txnContext.delete(tableName, key);
            txnContext.commit();
        }
    }

    protected int calculateSnapshotSyncUpdatesOnSinkStatusTable(int numSourceClusters) {
        return numSourceClusters * NUM_SNAPSHOT_SYNC_UPDATES_ON_SINK_STATUS_TABLE;
    }

    @After
    public void tearDown() {
        if (sinkListener1 != null) {
            corfuStoreSink1.unsubscribeListener(sinkListener1);
        }

        if (sinkListener2 != null) {
            corfuStoreSink2.unsubscribeListener(sinkListener2);
        }

        if (sinkListener3 != null) {
            corfuStoreSink3.unsubscribeListener(sinkListener3);
        }

        if (sourceListener1 != null) {
            corfuStoreSource1.unsubscribeListener(sourceListener1);
        }

        if (sourceListener2 != null) {
            corfuStoreSource2.unsubscribeListener(sourceListener2);
        }

        if (sourceListener3 != null) {
            corfuStoreSource3.unsubscribeListener(sourceListener3);
        }
        shutdownCorfuServers();
        shutdownLogReplicationServers();

        if (sourceRuntime1 != null) {
            sourceRuntime1.shutdown();
        }
        if (sourceRuntime2 != null) {
            sourceRuntime2.shutdown();
        }
        if (sourceRuntime3 != null) {
            sourceRuntime3.shutdown();
        }
        if (sinkRuntime1 != null) {
            sinkRuntime1.shutdown();
        }
        if (sinkRuntime2 != null) {
            sinkRuntime2.shutdown();
        }
        if (sinkRuntime3 != null) {
            sinkRuntime3.shutdown();
        }

    }

    private void shutdownCorfuServers() {
        if (sourceCorfu1 != null) {
            sourceCorfu1.destroy();
        }

        if (sourceCorfu2 != null) {
            sourceCorfu2.destroy();
        }

        if (sourceCorfu3 != null) {
            sourceCorfu3.destroy();
        }

        if (sinkCorfu1 != null) {
            sinkCorfu1.destroy();
        }

        if (sinkCorfu2 != null) {
            sinkCorfu2.destroy();
        }

        if (sinkCorfu3 != null) {
            sinkCorfu3.destroy();
        }
    }

    private void shutdownLogReplicationServers() {
        if (sourceReplicationServer1 != null) {
            sourceReplicationServer1.destroy();
        }

        if (sourceReplicationServer2 != null) {
            sourceReplicationServer2.destroy();
        }

        if (sourceReplicationServer3 != null) {
            sourceReplicationServer3.destroy();
        }

        if (sinkReplicationServer1 != null) {
            sinkReplicationServer1.destroy();
        }

        if (sinkReplicationServer2 != null) {
            sinkReplicationServer2.destroy();
        }

        if (sinkReplicationServer3 != null) {
            sinkReplicationServer3.destroy();
        }
    }

    protected class ReplicationStatusListener implements StreamListener {

        @Getter
        List<Boolean> accumulatedStatus = new ArrayList<>();

        private final CountDownLatch countDownLatch;

        public ReplicationStatusListener(CountDownLatch countdownLatch) {
            this.countDownLatch = countdownLatch;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            // Replication Status Table gets cleared on a role change.  Ignore the 'clear' updates
            results.getEntries().forEach((schema, entries) -> entries.forEach(e -> {
                if (e.getOperation() != CorfuStreamEntry.OperationType.CLEAR) {
                    accumulatedStatus.add(((LogReplicationMetadata.ReplicationStatusVal)e.getPayload()).getDataConsistent());
                    countDownLatch.countDown();
                }
            }));
        }

        @Override
        public void onError(Throwable throwable) {
            log.error("Error: ", throwable);
            fail("onError for ReplicationStatusListener");
        }
    }

    protected class ReplicatedStreamsListener implements StreamListener {

        @Getter
        Map<String, List<CorfuStreamEntry.OperationType>> tableToOpTypeMap = new HashMap<>();

        @Setter
        private CountDownLatch countdownLatch;

        @Setter
        private boolean snapshotSync;

        public ReplicatedStreamsListener(CountDownLatch countdownLatch, boolean snapshotSync) {
            this.countdownLatch = countdownLatch;
            this.snapshotSync = snapshotSync;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            results.getEntries().forEach((schema, entries) -> {
                if (snapshotSync) {
                    if (entries.size() > 1) {
                        processUpdate(schema, entries);
                    }
                } else {
                    processUpdate(schema, entries);
                }
            });
        }

        private void processUpdate(TableSchema schema, List<CorfuStreamEntry> entries) {
            List<CorfuStreamEntry.OperationType> opList = tableToOpTypeMap.getOrDefault(
                schema.getTableName(), new ArrayList<>());
            entries.forEach(entry -> opList.add(entry.getOperation()));
            tableToOpTypeMap.put(schema.getTableName(), opList);
            countdownLatch.countDown();
        }

        @Override
        public void onError(Throwable throwable) {
            log.error("Error: ", throwable);
            fail("onError for ReplicatedStreamsListener");
        }

        public void clearTableToOpTypeMap() {
            tableToOpTypeMap.clear();
        }
    }
}
