package org.corfudb.integration;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultClusterConfig;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusVal;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusVal.SyncType;
import org.corfudb.infrastructure.logreplication.proto.Sample.IntValue;
import org.corfudb.infrastructure.logreplication.proto.Sample.IntValueTag;
import org.corfudb.infrastructure.logreplication.proto.Sample.Metadata;
import org.corfudb.infrastructure.logreplication.proto.Sample.StringKey;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.runtime.CorfuStoreMetadata.TableDescriptors;
import org.corfudb.runtime.CorfuStoreMetadata.TableMetadata;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This suite of tests verifies snapshot writer and log entry writer could correctly
 * apply the entries for registry table during log replication.
 */
@Slf4j
public class LogReplicationSinkWriterIT extends LogReplicationAbstractIT {

    private static final int NUM_WRITES = 500;

    private static final String REPLICATION_STATUS_TABLE = "LogReplicationStatus";


    /**
     * This test verifies SinkWriter's behavior against registry table entries during snapshot sync
     * and log entry sync.
     *
     * 1. Setup streams to replicate for both source and sink, in total 8 tables will be used for verification
     * 2. The first 4 tables will be used for snapshot sync
     *    1). Table001 and Table002 - open on both sides with different metadata
     *    2). Table003 and Table004 - open on source side only
     * 3. Verify the table records and contents after snapshot sync
     * 4. The last 4 tables will be used for log entry sync with similar setup
     *    1). Table005 and Table006 - open on both sides with different metadata
     *    2). Table007 and Table008 - open on source side only
     * 5. Verify the table records and contents after log entry sync
     */
    @Test
    @SuppressWarnings("checkstyle:magicnumber")
    public void testFilterAndApplyRegistryTableEntries() throws Exception {
        setupActiveAndStandbyCorfu();
        openLogReplicationStatusTable();

        // Initial setup for streams to replicate for both source and sink side
        mapNameToMapActive = new HashMap<>();
        mapNameToMapStandby = new HashMap<>();

        /* The first 4 tables are for snapshot sync */

        // Table001 and Table002 - Open in both Source and Sink, while on Sink side they will be opened with
        // different metadata. Verify that Sink side's records are retained.
        // Table003 and Table004 - Open only in Source side. Verify that their records are replicated to Sink.
        prepareTablesToVerify(1, 2, 4);

        // Add Data for Snapshot Sync
        writeToActive(0, NUM_WRITES);
        // Confirm data does exist on Source Cluster
        for(Table<StringKey, IntValueTag, Metadata> map : mapNameToMapActive.values()) {
            assertThat(map.count()).isEqualTo(NUM_WRITES);
        }

        // Confirm data does not exist on Sink Cluster
        for(Table<StringKey, IntValueTag, Metadata> map : mapNameToMapStandby.values()) {
            assertThat(map.count()).isEqualTo(0);
        }

        // Set the plugin config file path before starting the process
        pluginConfigFilePath = nettyConfig;
        startLogReplicatorServers();

        // Verify snapshot sync completed and cluster went into log entry sync
        verifyInLogEntrySyncState();
        // Verify Sink side records for Table001 and Table002 are retained on the Registry table, and records for
        // Table003 and Table004 are replicated.
        // Also verify all the table's contents for snapshot sync
        verifyTableContentsAndRecords(1, 2, 4);


        /* The last 4 tables are for log entry sync */

        // It will be verified that records already exist in Sink side registry table won't be overwritten during
        // Log Entry sync. LR needs to be stopped to open some tables on both Source and Sink with different metadata,
        // otherwise it is possible that registry table entries from Source get replicated before Sink side opens them.
        stopActiveLogReplicator();
        // Clear these two map as different set of tables will be verified
        mapNameToMapActive.clear();
        mapNameToMapStandby.clear();

        // Table005 and Table006 - Open in both Source and Sink, while on Sink side they will be opened with
        // different metadata. Verify that Sink side's records are retained.
        // Table007 and Table008 - Open only in Source side. Verify that their records are replicated to Sink.
        prepareTablesToVerify(5, 6, 8);

        // Restart Source LR after opening the tables
        startActiveLogReplicator();

        // Write data to Source side, note that mapNameToMapActive now only have the last 4 tables
        writeToActive(0, NUM_WRITES);

        // At this point mapNameToMapStandby only has 2 tables. This step is also used as a barrier to indicate
        // it's a good point to check records on Sink side
        verifyDataOnStandby(NUM_WRITES);
        // Verify Sink side records for Table005 and Table006 are retained on the Registry table, and records for
        // Table007 and Table008 are replicated.
        // Also verify all the table's contents for log entry sync
        verifyTableContentsAndRecords(5, 6, 8);
    }

    /**
     * Open tables on Source and Sink side accordingly.
     *
     * Tables from start to splitter - Open on both Source and Sink side, while on Sink side they will be
     * opened with different metadata.
     *
     * Tables from splitter to end - Open on Source side only, after log replication verify their records
     * exist in Sink side registry table.
     */
    private void prepareTablesToVerify(int start, int splitter, int end) throws Exception {
        // From start to splitter - Open in both Source and Sink, while on Sink side they will be opened with
        // different metadata. Verify that Sink side's records are retained.
        for (int i = start; i <= splitter; i++) {
            String mapName = TABLE_PREFIX + i;
            Table<StringKey, IntValueTag, Metadata> tableActive =
                    corfuStoreActive.openTable(NAMESPACE, mapName, StringKey.class,
                            IntValueTag.class, Metadata.class,
                            TableOptions.fromProtoSchema(IntValueTag.class, TableOptions.builder().build()));
            mapNameToMapActive.put(mapName, tableActive);

            Table<StringKey, IntValueTag, Metadata> tableStandby =
                    corfuStoreStandby.openTable(NAMESPACE, mapName, StringKey.class,
                            IntValueTag.class, Metadata.class,
                            TableOptions.fromProtoSchema(IntValue.class, TableOptions.builder().build()));
            mapNameToMapStandby.put(mapName, tableStandby);
        }

        // From splitter to end - Open only in Source side. Verify that their records are replicated to Sink.
        for (int i = splitter + 1; i <= end; i++) {
            String mapName = TABLE_PREFIX + i;
            Table<StringKey, IntValueTag, Metadata> tableActive =
                    corfuStoreActive.openTable(NAMESPACE, mapName, StringKey.class,
                            IntValueTag.class, Metadata.class,
                            TableOptions.fromProtoSchema(IntValueTag.class, TableOptions.builder().build()));
            mapNameToMapActive.put(mapName, tableActive);
        }
    }

    /**
     * Verify tables records and contents after log replication.
     */
    private void verifyTableContentsAndRecords(int start, int splitter, int end) throws Exception {
        // Verify Sink side records for tables before the splitter are retained, and records for the rest of
        // the tables are replicated. Add the tables from splitter to end to mapNameToMapStandby to verify all the
        // tables' contents
        for (int i = start; i <= end; i++) {
            String tableName = TABLE_PREFIX + i;
            TableName tableNameKey = TableName.newBuilder()
                    .setTableName(tableName)
                    .setNamespace(NAMESPACE)
                    .build();
            CorfuRecord<TableDescriptors, TableMetadata> corfuRecord =
                    standbyRuntime.getTableRegistry().getRegistryTable().get(tableNameKey);
            if (i <= splitter) {
                Assert.assertFalse(corfuRecord.getMetadata().getTableOptions().getIsFederated());
            } else {
                Assert.assertTrue(corfuRecord.getMetadata().getTableOptions().getIsFederated());
                Table<StringKey, IntValueTag, Metadata> tableStandby =
                        corfuStoreStandby.openTable(NAMESPACE, tableName, StringKey.class,
                                IntValueTag.class, Metadata.class,
                                TableOptions.fromProtoSchema(IntValueTag.class, TableOptions.builder().build()));
                mapNameToMapStandby.put(tableName, tableStandby);
            }
        }
        // Verify contents
        verifyDataOnStandby(NUM_WRITES);
    }

    private void verifyInLogEntrySyncState() throws InterruptedException {
        LogReplicationMetadata.ReplicationStatusKey key =
                LogReplicationMetadata.ReplicationStatusKey
                        .newBuilder()
                        .setClusterId(new DefaultClusterConfig().getStandbyClusterIds().get(0))
                        .build();

        ReplicationStatusVal replicationStatusVal = null;

        while (replicationStatusVal == null || !replicationStatusVal.getSyncType().equals(SyncType.LOG_ENTRY)
        || !replicationStatusVal.getSnapshotSyncInfo().getStatus().equals(LogReplicationMetadata.SyncStatus.COMPLETED)) {
            TimeUnit.SECONDS.sleep(1);
            try (TxnContext txn = corfuStoreActive.txn(LogReplicationMetadataManager.NAMESPACE)) {
                replicationStatusVal = (ReplicationStatusVal) txn.getRecord(REPLICATION_STATUS_TABLE, key).getPayload();
                txn.commit();
            }
        }

        // Snapshot sync should have completed and log entry sync is ongoing
        assertThat(replicationStatusVal.getSyncType())
                .isEqualTo(SyncType.LOG_ENTRY);
        assertThat(replicationStatusVal.getStatus())
                .isEqualTo(LogReplicationMetadata.SyncStatus.ONGOING);

        assertThat(replicationStatusVal.getSnapshotSyncInfo().getType())
                .isEqualTo(LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType.DEFAULT);
        assertThat(replicationStatusVal.getSnapshotSyncInfo().getStatus())
                .isEqualTo(LogReplicationMetadata.SyncStatus.COMPLETED);
    }


    private void openLogReplicationStatusTable() throws Exception {
        corfuStoreActive.openTable(LogReplicationMetadataManager.NAMESPACE,
                REPLICATION_STATUS_TABLE,
                LogReplicationMetadata.ReplicationStatusKey.class,
                ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(ReplicationStatusVal.class));

        corfuStoreStandby.openTable(LogReplicationMetadataManager.NAMESPACE,
                REPLICATION_STATUS_TABLE,
                LogReplicationMetadata.ReplicationStatusKey.class,
                ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(ReplicationStatusVal.class));
    }
}
