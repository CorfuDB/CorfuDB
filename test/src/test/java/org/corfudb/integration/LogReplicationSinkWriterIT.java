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
        setupSourceAndSinkCorfu();
        openLogReplicationStatusTable();

        // Initial setup for streams to replicate for both source and sink side
        mapNameToMapSource = new HashMap<>();
        mapNameToMapSink = new HashMap<>();

        /* The first 4 tables are for snapshot sync */

        // Table001 and Table002 - Open in both Source and Sink, while on Sink side they will be opened with
        // different metadata. Verify that Sink side's records are retained.
        // Table003 and Table004 - Open only in Source side. Verify that their records are replicated to Sink.
        prepareTablesToVerify(1, 2, 4);

        // Add Data for Snapshot Sync
        writeToSource(0, NUM_WRITES);
        // Confirm data does exist on Source Cluster
        for(Table<StringKey, IntValueTag, Metadata> map : mapNameToMapSource.values()) {
            assertThat(map.count()).isEqualTo(NUM_WRITES);
        }

        // Confirm data does not exist on Sink Cluster
        for(Table<StringKey, IntValueTag, Metadata> map : mapNameToMapSink.values()) {
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
        stopSourceLogReplicator();
        // Clear these two map as different set of tables will be verified
        mapNameToMapSource.clear();
        mapNameToMapSink.clear();

        // Table005 and Table006 - Open in both Source and Sink, while on Sink side they will be opened with
        // different metadata. Verify that Sink side's records are retained.
        // Table007 and Table008 - Open only in Source side. Verify that their records are replicated to Sink.
        prepareTablesToVerify(5, 6, 8);

        // Restart Source LR after opening the tables
        startSourceLogReplicator();

        // Write data to Source side, note that mapNameToMapSource now only have the last 4 tables
        writeToSource(0, NUM_WRITES);

        // At this point mapNameToMapSink only has 2 tables. This step is also used as a barrier to indicate
        // it's a good point to check records on Sink side
        verifyDataOnSink(NUM_WRITES);
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
            Table<StringKey, IntValueTag, Metadata> tableSource =
                    corfuStoreSource.openTable(NAMESPACE, mapName, StringKey.class,
                            IntValueTag.class, Metadata.class,
                            TableOptions.fromProtoSchema(IntValueTag.class));
            mapNameToMapSource.put(mapName, tableSource);

            // Open tables on Sink side with different schema options
            Table<StringKey, IntValueTag, Metadata> tableSink =
                    corfuStoreSink.openTable(NAMESPACE, mapName, StringKey.class,
                            IntValueTag.class, Metadata.class,
                            TableOptions.fromProtoSchema(IntValue.class));
            mapNameToMapSink.put(mapName, tableSink);
        }

        // From splitter to end - Open only in Source side. Verify that their records are replicated to Sink.
        for (int i = splitter + 1; i <= end; i++) {
            String mapName = TABLE_PREFIX + i;
            Table<StringKey, IntValueTag, Metadata> tableSource =
                    corfuStoreSource.openTable(NAMESPACE, mapName, StringKey.class,
                            IntValueTag.class, Metadata.class,
                            TableOptions.fromProtoSchema(IntValueTag.class));
            mapNameToMapSource.put(mapName, tableSource);
        }
    }

    /**
     * Verify tables records and contents after log replication.
     */
    private void verifyTableContentsAndRecords(int start, int splitter, int end) throws Exception {
        // Verify Sink side records for tables before the splitter are overwritten, and records for the rest of
        // the tables are replicated. Add the tables from splitter to end to mapNameToMapSink to verify all the
        // tables' contents
        for (int i = start; i <= end; i++) {
            String tableName = TABLE_PREFIX + i;
            TableName tableNameKey = TableName.newBuilder()
                    .setTableName(tableName)
                    .setNamespace(NAMESPACE)
                    .build();
            CorfuRecord<TableDescriptors, TableMetadata> corfuRecord =
                    sinkRuntime.getTableRegistry().getRegistryTable().get(tableNameKey);
            Assert.assertTrue(corfuRecord.getMetadata().getTableOptions().getIsFederated());
            Assert.assertEquals(corfuRecord.getMetadata().getTableOptions().getStreamTagCount(), 1);
            Assert.assertEquals(corfuRecord.getMetadata().getTableOptions().getStreamTag(0), "test");
            if (i > splitter) {
                Table<StringKey, IntValueTag, Metadata> tableSink =
                        corfuStoreSink.openTable(NAMESPACE, tableName, StringKey.class,
                                IntValueTag.class, Metadata.class,
                                TableOptions.fromProtoSchema(IntValueTag.class, TableOptions.builder().build()));
                mapNameToMapSink.put(tableName, tableSink);
            }
        }
        // Verify contents
        verifyDataOnSink(NUM_WRITES);
    }

    private void openLogReplicationStatusTable() throws Exception {
        corfuStoreSource.openTable(LogReplicationMetadataManager.NAMESPACE,
                REPLICATION_STATUS_TABLE,
                LogReplicationMetadata.ReplicationStatusKey.class,
                ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(ReplicationStatusVal.class));

        corfuStoreSink.openTable(LogReplicationMetadataManager.NAMESPACE,
                REPLICATION_STATUS_TABLE,
                LogReplicationMetadata.ReplicationStatusKey.class,
                ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(ReplicationStatusVal.class));
    }
}
