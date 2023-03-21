package org.corfudb.integration;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusVal;
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
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

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
     *    1). Table001 - open on both sides with different metadata, Sink side open it with is_federated = false
     *    2). Table002 - open on both side with same metadata (is_federated = true)
     *    3). Table003 and Table004 - open on source side only (is_federated = true)
     * 3. Verify the table records and contents after snapshot sync
     * 4. The last 4 tables will be used for log entry sync with similar setup
     *    1). Table005 - open on both sides with different metadata, Sink side open it with is_federated = false
     *    2). Table006 - open on both side with same metadata (is_federated = true)
     *    4). Table007 and Table008 - open on source side only (is_federated = true)
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
        // Verify Sink side record for Table001 is retained on the Registry table, and records for Table003 and
        // Table004 are replicated.
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

        prepareTablesToVerify(5, 6, 8);

        // Restart Source LR after opening the tables
        startActiveLogReplicator();

        // Write data to Source side, note that mapNameToMapActive now only have the last 4 tables
        writeToActive(0, NUM_WRITES);

        // At this point mapNameToMapStandby only has 1 table. This step is also used as a barrier to indicate
        // it's a good point to check records on Sink side
        verifyDataOnStandby(NUM_WRITES);
        // Verify Sink side record for Table005 is retained on the Registry table, and records for Table007 and
        // Table008 are replicated.
        // Also verify all the table's contents for log entry sync
        verifyTableContentsAndRecords(5, 6, 8);
    }

    /**
     * Open tables on Source and Sink side accordingly.
     *
     * Tables from start(inclusive) to splitter(exclusive) - Open on both Source and Sink side, while on Sink side they will be
     * opened with different metadata.
     *
     * Table at splitter, open on both Source and Sink with same metadata, which will be used to check log entry sync
     * status.
     *
     * Tables from splitter(exclusive) to end(inclusive) - Open on Source side only, after log replication verify their records
     * exist in Sink side registry table.
     */
    private void prepareTablesToVerify(int start, int splitter, int end) throws Exception {
        // From start to splitter - Open in both Source and Sink, while on Sink side they will be opened with
        // different metadata. Verify that Sink side's records are retained.
        for (int i = start; i < splitter; i++) {
            String mapName = TABLE_PREFIX + i;
            Table<StringKey, IntValueTag, Metadata> tableActive =
                    corfuStoreActive.openTable(NAMESPACE, mapName, StringKey.class,
                            IntValueTag.class, Metadata.class,
                            TableOptions.fromProtoSchema(IntValueTag.class));
            mapNameToMapActive.put(mapName, tableActive);

            // Open tables on Sink side with different schema options
            corfuStoreStandby.openTable(NAMESPACE, mapName, StringKey.class, IntValueTag.class, Metadata.class,
                    TableOptions.fromProtoSchema(IntValue.class));
        }

        // From splitter to end - Open only in Source side. Verify that their records are replicated to Sink.
        for (int i = splitter; i <= end; i++) {
            String mapName = TABLE_PREFIX + i;
            Table<StringKey, IntValueTag, Metadata> tableActive =
                    corfuStoreActive.openTable(NAMESPACE, mapName, StringKey.class,
                            IntValueTag.class, Metadata.class,
                            TableOptions.fromProtoSchema(IntValueTag.class));
            mapNameToMapActive.put(mapName, tableActive);
            if (i == splitter) {
                // Open tables on Sink side with different schema options
                Table<StringKey, IntValueTag, Metadata> tableStandby =
                        corfuStoreStandby.openTable(NAMESPACE, mapName, StringKey.class,
                                IntValueTag.class, Metadata.class,
                                TableOptions.fromProtoSchema(IntValueTag.class));
                mapNameToMapStandby.put(mapName, tableStandby);
            }
        }
    }

    /**
     * Verify tables records and contents after log replication.
     */
    private void verifyTableContentsAndRecords(int start, int splitter, int end) throws Exception {
        // Verify Sink side records for tables before the splitter are overwritten, and records for the rest of
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
            if (i < splitter) {
                // Table with is_federated = false will be dropped
                Assert.assertFalse(corfuRecord.getMetadata().getTableOptions().getIsFederated());
                Table<StringKey, IntValueTag, Metadata> tableStandby =
                        corfuStoreStandby.openTable(NAMESPACE, tableName, StringKey.class,
                                IntValueTag.class, Metadata.class,
                                TableOptions.fromProtoSchema(IntValue.class, TableOptions.builder().build()));
                Assert.assertEquals(tableStandby.count(), 0);
            } else {
                Assert.assertTrue(corfuRecord.getMetadata().getTableOptions().getIsFederated());
                Assert.assertEquals(corfuRecord.getMetadata().getTableOptions().getStreamTagCount(), 1);
                Assert.assertEquals(corfuRecord.getMetadata().getTableOptions().getStreamTag(0), "test");
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
