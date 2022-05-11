
package org.corfudb.integration;

import com.google.protobuf.Message;
import lombok.Getter;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultLogReplicationConfigAdapter;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.proto.Sample;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.*;
import org.corfudb.test.SampleSchema;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

public class LogReplicationDynamicStreamRouting extends AbstractIT {

    private final int sourceSiteCorfuPort = 9000;
    private final int sinkSiteCorfuPort1 = 9001;
    private final int sinkSiteCorfuPort2 = 9002;
    private final int sinkSiteCorfuPort3 = 9003;

    private final int sourceReplicationPort = 9010;
    private final int sinkReplicationPort1 = 9021;
    private final int sinkReplicationPort2 = 9022;
    private final int sinkReplicationPort3 = 9023;

    private Process sourceCorfu = null;
    private Process sinkCorfu1 = null;
    private Process sinkCorfu2 = null;
    private Process sinkCorfu3 = null;
    private Process sourceReplicationServer = null;
    private Process sinkReplicationServer1 = null;
    private Process sinkReplicationServer2 = null;
    private Process sinkReplicationServer3 = null;

    private CorfuRuntime sourceRuntime;
    private CorfuRuntime sinkRuntime1;
    private CorfuRuntime sinkRuntime2;
    private CorfuRuntime sinkRuntime3;

    private CorfuStore corfuStoreSource;
    private CorfuStore corfuStoreSink1;
    private CorfuStore corfuStoreSink2;
    private CorfuStore corfuStoreSink3;

    private final String sourceEndpoint1 = DEFAULT_HOST + ":" + sourceSiteCorfuPort;
    private final String sinkEndpoint1 = DEFAULT_HOST + ":" + sinkSiteCorfuPort1;
    private final String sinkEndpoint2 = DEFAULT_HOST + ":" + sinkSiteCorfuPort2;
    private final String sinkEndpoint3 = DEFAULT_HOST + ":" + sinkSiteCorfuPort3;

    private final String TABLE_1 = "Table_Directory_Group-1";
    private final String TABLE_2 = "Table_Directory_Group-2";
    private final String TABLE_3 = "Table_Directory_Group-3";
    private final String GLOBAL_TABLE = "Table_Directory_Global";
    private final String DOMAIN_TABLE = "Table_Domains_Mapping";
    public static final String REPLICATION_STATUS_TABLE = "LogReplicationStatus";
    public static final String CORFU_NAMESPACE = CORFU_SYSTEM_NAMESPACE;
    private final String NAMESPACE = DefaultLogReplicationConfigAdapter.NAMESPACE;

    private final int THREE = 3;
    private final int NINE = 9;
    private final int SIX = 6;
    private final int TEN = 10;
    private final int TWELVE = 12;
    private final int HUNDRED = 100;
    private final int TWO_HUNDRED = 200;
    private final int THREE_HUNDRED = 300;
    private final int EIGHTEEN = 18;


    public String pluginConfigFilePath;

    private Table<SampleSchema.Uuid, SampleSchema.SampleDomainGroup,
            SampleSchema.SampleMetadataDynamicStreamRouting> table1;
    private Table<SampleSchema.Uuid, SampleSchema.SampleDomainGroup,
            SampleSchema.SampleMetadataDynamicStreamRouting> table2;
    private Table<SampleSchema.Uuid, SampleSchema.SampleDomainGroup,
            SampleSchema.SampleMetadataDynamicStreamRouting> table3;
    private Table<SampleSchema.Uuid, SampleSchema.SampleDomainGroup,
            SampleSchema.SampleMetadataDynamicStreamRouting> table_atLM1;
    private Table<SampleSchema.Uuid, SampleSchema.SampleDomainGroup,
            SampleSchema.SampleMetadataDynamicStreamRouting> table_atLM2;
    private Table<SampleSchema.Uuid, SampleSchema.SampleDomainGroup,
            SampleSchema.SampleMetadataDynamicStreamRouting> table_atLM3;

    private Table<Sample.SampleDomainTableKey, Sample.SampleDomainTableValue,
            SampleSchema.SampleMetadataDynamicStreamRouting> domainTable;

    private Table<LogReplicationMetadata.ReplicationStatusKey,
            LogReplicationMetadata.ReplicationStatusVal, Message> replicationStatusTable_LM1;
    private Table<LogReplicationMetadata.ReplicationStatusKey,
            LogReplicationMetadata.ReplicationStatusVal, Message> replicationStatusTable_LM2;
    private Table<LogReplicationMetadata.ReplicationStatusKey,
            LogReplicationMetadata.ReplicationStatusVal, Message> replicationStatusTable_LM3;



    @Test
    public void testMergeTable() throws Exception {
        // Setup Corfu, 1 LR Source Sites and 3 LR Sink Sites
        setupSourceAndSinkCorfu();

        // Open maps on the Source and Destination Sites
        openMaps();

//        CountDownLatch statusUpdateLatch = new CountDownLatch(SIX);
//        ReplicationStatusListener statusListener_LM1 =
//                new ReplicationStatusListener(statusUpdateLatch, "LogReplicationStatus116e4567-e89b-12d3-a456-111664440011");
//        ReplicationStatusListener statusListener_LM2 =
//                new ReplicationStatusListener(statusUpdateLatch, "LogReplicationStatus226e4567-e89b-12d3-a456-111664440022");
//        ReplicationStatusListener statusListener_LM3 =
//                new ReplicationStatusListener(statusUpdateLatch, "LogReplicationStatus336e4567-e89b-12d3-a456-111664440033");


        System.out.println("Stream tags for LM1 is: ");
        for(UUID tag : replicationStatusTable_LM1.getStreamTags()) {
            System.out.print(" " + tag);
        }

        System.out.println("Stream tags for LM2 is: ");
        for(UUID tag : replicationStatusTable_LM2.getStreamTags()) {
            System.out.print(" " + tag);
        }

        System.out.println("Stream tags for LM3 is: ");
        for(UUID tag : replicationStatusTable_LM3.getStreamTags()) {
            System.out.print(" " + tag);
        }

        System.out.println();
        System.out.println("==============================================");
        System.out.println("Step 1: Write 3 records locally on each LM");
        System.out.println("==============================================");

        // Write data to all the source sites
        writeData(corfuStoreSource, TABLE_1, table1, HUNDRED);
        writeData(corfuStoreSource, TABLE_2, table2, TWO_HUNDRED);
        writeData(corfuStoreSource, TABLE_3, table3, THREE_HUNDRED);

        System.out.println("\n\n");
        System.out.println("==============================================");
        System.out.println("Step 2: Verify No Data Found on the GM");
        // Assert no data on the sink site
        Assert.assertEquals(0, table_atLM1.count());
        Assert.assertEquals(0, table_atLM2.count());
        Assert.assertEquals(0, table_atLM3.count());
        System.out.println("Size Of Table_Directory_Group-1: " + table_atLM1.count());
        System.out.println("Size Of Table_Directory_Group-1: " + table_atLM2.count());
        System.out.println("Size Of Table_Directory_Group-1: " + table_atLM3.count());
        System.out.println("==============================================");

        Assert.assertEquals(0, table_atLM1.count());
        Assert.assertEquals(0, table_atLM2.count());
        Assert.assertEquals(0, table_atLM3.count());


        System.out.println("\n\n");
        System.out.println("==============================================");
        System.out.println("Step 3: Start Log Replication on LMs and GM");
        System.out.println("==============================================");
        // Start Log Replication Servers
        startReplicationServers();

        System.out.println("\n\n");
        System.out.println("==============================================");
        System.out.println("Step 4: Verify All Records were written on GM");
        System.out.println("==============================================");

        while(table_atLM1.count() != SIX || table_atLM2.count() != SIX || table_atLM3.count() != THREE) {
        }

        Assert.assertEquals(SIX, table_atLM1.count());
        Assert.assertEquals(SIX, table_atLM2.count());
        Assert.assertEquals(THREE, table_atLM3.count());

        System.out.println("==============================================");
        System.out.println("Step 5: Add deltas");
        System.out.println("==============================================");

        writeMoreData(corfuStoreSource, TABLE_1, table1, THREE, HUNDRED + THREE);
        writeMoreData(corfuStoreSource, TABLE_2, table2, THREE, TWO_HUNDRED + THREE);
        writeMoreData(corfuStoreSource, TABLE_3, table3, THREE, THREE_HUNDRED + THREE);

        while(table_atLM1.count() != TWELVE || table_atLM2.count() != TWELVE || table_atLM3.count() != SIX) {
        }

        Assert.assertEquals(TWELVE, table_atLM1.count());
        Assert.assertEquals(TWELVE, table_atLM2.count());
        Assert.assertEquals(SIX, table_atLM3.count());

        System.out.println("==============================================");
        System.out.println("Step 6: Change domain");
        System.out.println("==============================================");

        System.out.println("Currently Domain1: LM1, LM2");
        System.out.println("Add LM3 to Domain1");

        Set<String> newDdestinations = new HashSet<String>(){{ add("116e4567-e89b-12d3-a456-111664440011");
            add("226e4567-e89b-12d3-a456-111664440022"); add("336e4567-e89b-12d3-a456-111664440033");}};

        changeDomain("Domain1", newDdestinations);

        System.out.println("==============================================");
        System.out.println("Step 7: Add more data");
        System.out.println("==============================================");


        writeMoreData(corfuStoreSource, TABLE_1, table1, THREE, HUNDRED + SIX);
        writeMoreData(corfuStoreSource, TABLE_2, table2, THREE, TWO_HUNDRED + SIX);
        writeMoreData(corfuStoreSource, TABLE_3, table3, THREE, THREE_HUNDRED + SIX);

        while(table_atLM1.count() != EIGHTEEN || table_atLM2.count() != EIGHTEEN || table_atLM3.count() != EIGHTEEN) {
        }

        Assert.assertEquals(EIGHTEEN, table_atLM1.count());
        Assert.assertEquals(EIGHTEEN, table_atLM2.count());
        Assert.assertEquals(EIGHTEEN, table_atLM3.count());

        try (TxnContext txn = corfuStoreSource.txn(NAMESPACE)) {
            LogReplicationMetadata.ReplicationStatusKey key = LogReplicationMetadata.ReplicationStatusKey.newBuilder().setClusterId("116e4567-e89b-12d3-a456-111664440011").build();
            CorfuStoreEntry<LogReplicationMetadata.ReplicationStatusKey, LogReplicationMetadata.ReplicationStatusVal, Message> record = txn.getRecord(replicationStatusTable_LM1, key);
            Assert.assertTrue(record.getPayload().getSnapshotSyncInfo().getType().equals(LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType.DEFAULT));
            System.out.println("Forced snapshotSync not triggerd on: LM1");
        }

        try (TxnContext txn = corfuStoreSource.txn(NAMESPACE)) {
            LogReplicationMetadata.ReplicationStatusKey key = LogReplicationMetadata.ReplicationStatusKey.newBuilder().setClusterId("226e4567-e89b-12d3-a456-111664440022").build();
            CorfuStoreEntry<LogReplicationMetadata.ReplicationStatusKey, LogReplicationMetadata.ReplicationStatusVal, Message> record = txn.getRecord(replicationStatusTable_LM2, key);
            Assert.assertTrue(record.getPayload().getSnapshotSyncInfo().getType().equals(LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType.DEFAULT));
            System.out.println("Forced snapshotSync not triggerd on: LM2");
        }

        try (TxnContext txn = corfuStoreSource.txn(NAMESPACE)) {
            LogReplicationMetadata.ReplicationStatusKey key = LogReplicationMetadata.ReplicationStatusKey.newBuilder().setClusterId("336e4567-e89b-12d3-a456-111664440033").build();
            CorfuStoreEntry<LogReplicationMetadata.ReplicationStatusKey, LogReplicationMetadata.ReplicationStatusVal, Message> record = txn.getRecord(replicationStatusTable_LM3, key);
            Assert.assertTrue(record.getPayload().getSnapshotSyncInfo().getType().equals(LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType.FORCED));
            System.out.println("Forced snapshotSync triggerd on: LM3");
        }

        System.out.println("==========================================================");
        System.out.println("Step 8: Change domain by removing a site from a Domain");
        System.out.println("==========================================================");

        System.out.println("Currently Domain1: LM1, LM2, LM3 && Domain3: LM1, LM3");
        System.out.println("Removing LM1 from Domain1");

        newDdestinations = new HashSet<String>(){{
            add("226e4567-e89b-12d3-a456-111664440022"); add("336e4567-e89b-12d3-a456-111664440033");}};

        changeDomain("Domain1", newDdestinations);


        while(true) {
            try (TxnContext txn = corfuStoreSource.txn(NAMESPACE)) {
                LogReplicationMetadata.ReplicationStatusKey key = LogReplicationMetadata.ReplicationStatusKey.newBuilder().setClusterId("116e4567-e89b-12d3-a456-111664440011").build();
                CorfuStoreEntry<LogReplicationMetadata.ReplicationStatusKey, LogReplicationMetadata.ReplicationStatusVal, Message> record = txn.getRecord(replicationStatusTable_LM1, key);
                if (record.getPayload().getSnapshotSyncInfo().getType().equals(LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType.FORCED)) {
                    Assert.assertTrue(record.getPayload().getSnapshotSyncInfo().getType().equals(LogReplicationMetadata.SnapshotSyncInfo.SnapshotSyncType.FORCED));
                    if (record.getPayload().getSnapshotSyncInfo().getStatus().equals(LogReplicationMetadata.SyncStatus.COMPLETED)) {
                        break;
                    }
                }
            }
        }

        Assert.assertEquals(NINE, table_atLM1.count());
        Assert.assertEquals(EIGHTEEN, table_atLM2.count());
        Assert.assertEquals(EIGHTEEN, table_atLM3.count());

        System.out.println("Force Snapshot Sync triggered on only LM1");


        shutdownCorfuServers();
        shutdownLogReplicationServers();
        sourceRuntime.shutdown();
        sinkRuntime1.shutdown();
        sinkRuntime2.shutdown();
        sinkRuntime3.shutdown();
    }

    private void setupSourceAndSinkCorfu() throws Exception {

        sourceCorfu = runServer(sourceSiteCorfuPort, true);
        sinkCorfu1 = runServer(sinkSiteCorfuPort1, true);
        sinkCorfu2 = runServer(sinkSiteCorfuPort2, true);
        sinkCorfu3 = runServer(sinkSiteCorfuPort3, true);

        // Setup the runtimes to each Corfu server
        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();

        sourceRuntime = CorfuRuntime.fromParameters(params);
        sourceRuntime.parseConfigurationString(sourceEndpoint1);
        sourceRuntime.connect();

        sinkRuntime1 = CorfuRuntime.fromParameters(params);
        sinkRuntime1.parseConfigurationString(sinkEndpoint1);
        sinkRuntime1.connect();

        sinkRuntime2 = CorfuRuntime.fromParameters(params);
        sinkRuntime2.parseConfigurationString(sinkEndpoint2);
        sinkRuntime2.connect();

        sinkRuntime3 = CorfuRuntime.fromParameters(params);
        sinkRuntime3.parseConfigurationString(sinkEndpoint3);
        sinkRuntime3.connect();

        corfuStoreSource = new CorfuStore(sourceRuntime);
        corfuStoreSink1 = new CorfuStore(sinkRuntime1);
        corfuStoreSink2= new CorfuStore(sinkRuntime2);
        corfuStoreSink3 = new CorfuStore(sinkRuntime3);
    }

    private void shutdownCorfuServers() {
        if (sourceCorfu != null) {
            sourceCorfu.destroy();
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
        if (sourceReplicationServer != null) {
            sourceReplicationServer.destroy();
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

    private void openMaps() throws Exception {
        domainTable = corfuStoreSource.openTable(NAMESPACE, DOMAIN_TABLE,
                Sample.SampleDomainTableKey.class,
                Sample.SampleDomainTableValue.class, null,
                TableOptions.fromProtoSchema(Sample.SampleDomainTableKey.class));

        table1 = corfuStoreSource.openTable(NAMESPACE, TABLE_1,
                SampleSchema.Uuid.class,
                SampleSchema.SampleDomainGroup.class, SampleSchema.SampleMetadataDynamicStreamRouting.class,
                TableOptions.fromProtoSchema(SampleSchema.SampleDomainGroup.class));

        table2 = corfuStoreSource.openTable(NAMESPACE, TABLE_2,
                SampleSchema.Uuid.class,
                SampleSchema.SampleDomainGroup.class, SampleSchema.SampleMetadataDynamicStreamRouting.class,
                TableOptions.fromProtoSchema(SampleSchema.SampleDomainGroup.class));

        table3 = corfuStoreSource.openTable(NAMESPACE, TABLE_3,
                SampleSchema.Uuid.class,
                SampleSchema.SampleDomainGroup.class, SampleSchema.SampleMetadataDynamicStreamRouting.class,
                TableOptions.fromProtoSchema(SampleSchema.SampleDomainGroup.class));

        String id = "116e4567-e89b-12d3-a456-111664440011";
        TableOptions.TableOptionsBuilder optionsBuilder_LM1 = TableOptions.builder();
        optionsBuilder_LM1.schemaOptions(CorfuOptions.SchemaOptions.newBuilder()
                .addStreamTag(id).build());
        replicationStatusTable_LM1 = corfuStoreSource.openTable(CORFU_NAMESPACE,
                REPLICATION_STATUS_TABLE + id,
                LogReplicationMetadata.ReplicationStatusKey.class,
                LogReplicationMetadata.ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));

        id = "226e4567-e89b-12d3-a456-111664440022";
        TableOptions.TableOptionsBuilder optionsBuilder_LM2 = TableOptions.builder();
        optionsBuilder_LM2.schemaOptions(CorfuOptions.SchemaOptions.newBuilder()
                .addStreamTag(id).build());
        replicationStatusTable_LM2 = corfuStoreSource.openTable(CORFU_NAMESPACE,
                REPLICATION_STATUS_TABLE + id,
                LogReplicationMetadata.ReplicationStatusKey.class,
                LogReplicationMetadata.ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));

        id = "336e4567-e89b-12d3-a456-111664440033";
        TableOptions.TableOptionsBuilder optionsBuilder_LM3 = TableOptions.builder();
        optionsBuilder_LM3.schemaOptions(CorfuOptions.SchemaOptions.newBuilder()
                .addStreamTag(id).build());
        replicationStatusTable_LM3 = corfuStoreSource.openTable(CORFU_NAMESPACE,
                REPLICATION_STATUS_TABLE + id,
                LogReplicationMetadata.ReplicationStatusKey.class,
                LogReplicationMetadata.ReplicationStatusVal.class,
                null,
                TableOptions.fromProtoSchema(LogReplicationMetadata.ReplicationStatusVal.class));

        table_atLM1 = corfuStoreSink1.openTable(NAMESPACE, GLOBAL_TABLE,
                SampleSchema.Uuid.class,
                SampleSchema.SampleDomainGroup.class, SampleSchema.SampleMetadataDynamicStreamRouting.class,
                TableOptions.fromProtoSchema(SampleSchema.SampleDomainGroup.class));

        table_atLM2 = corfuStoreSink2.openTable(NAMESPACE, GLOBAL_TABLE,
                SampleSchema.Uuid.class,
                SampleSchema.SampleDomainGroup.class, SampleSchema.SampleMetadataDynamicStreamRouting.class,
                TableOptions.fromProtoSchema(SampleSchema.SampleDomainGroup.class));

        table_atLM3 = corfuStoreSink3.openTable(NAMESPACE, GLOBAL_TABLE,
                SampleSchema.Uuid.class,
                SampleSchema.SampleDomainGroup.class, SampleSchema.SampleMetadataDynamicStreamRouting.class,
                TableOptions.fromProtoSchema(SampleSchema.SampleDomainGroup.class));
    }

    private void writeData(CorfuStore corfuStore, String tableName,
                           Table table, int startNum) {
        for (int i=startNum; i<startNum + THREE; i++) {
            SampleSchema.Uuid key =
                    SampleSchema.Uuid.newBuilder().setLsb(i + startNum).setMsb(i + startNum).build();


            switch(tableName) {
                case TABLE_1:
                    SampleSchema.SampleDomainGroup payload1 = SampleSchema.SampleDomainGroup.newBuilder().setPayload(
                        tableName + " payload " + i).build();
                    SampleSchema.SampleMetadataDynamicStreamRouting metadata1 = SampleSchema.SampleMetadataDynamicStreamRouting
                            .newBuilder().setDestinationName("Domain1").build();
                    try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                        txn.putRecord(table, key, payload1, metadata1);
                        txn.commit();
                    }
                    break;
                case TABLE_2:
                    SampleSchema.SampleDomainGroup payload2 = SampleSchema.SampleDomainGroup.newBuilder().setPayload(
                            tableName + " payload " + i).build();
                    SampleSchema.SampleMetadataDynamicStreamRouting metadata2 = SampleSchema.SampleMetadataDynamicStreamRouting
                            .newBuilder().setDestinationName("Domain2").build();
                    try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                        txn.putRecord(table, key, payload2, metadata2);
                        txn.commit();
                    }
                    break;
                case TABLE_3:
                    SampleSchema.SampleDomainGroup payload3 = SampleSchema.SampleDomainGroup.newBuilder().setPayload(
                            tableName + " payload " + i).build();
                    SampleSchema.SampleMetadataDynamicStreamRouting metadata3 = SampleSchema.SampleMetadataDynamicStreamRouting
                            .newBuilder().setDestinationName("Domain3").build();
                    try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                        txn.putRecord(table, key, payload3, metadata3);
                        txn.commit();
                    }
                    break;
            }

        }
    }

    private void writeMoreData(CorfuStore corfuStore, String tableName, Table table, int numOfInserts, int start) {
        for (int i = start; i < start + numOfInserts; ++i) {
            SampleSchema.Uuid key =
                    SampleSchema.Uuid.newBuilder().setLsb(i * TEN).setMsb(i * TEN).build();

            SampleSchema.SampleDomainGroup payload =
                    SampleSchema.SampleDomainGroup.newBuilder().setPayload(
                            tableName + " delta payload " + i).build();

            try (TxnContext txn = corfuStore.txn(NAMESPACE)) {
                txn.putRecord(table, key, payload, null);
                txn.commit();
            }
        }
    }

    private void changeDomain(String domainName, Set<String> newDestinationSites) {
        try (TxnContext txn = corfuStoreSource.txn(NAMESPACE)) {
            Sample.SampleDomainTableKey key = Sample.SampleDomainTableKey
                    .newBuilder().setLogicalGroupName(domainName).build();
            Sample.SampleDomainTableValue value = Sample.SampleDomainTableValue
                    .newBuilder().addAllSites(newDestinationSites).build();
            txn.putRecord(domainTable, key, value, null);
            txn.commit();
        }
    }

    private void startReplicationServers() throws Exception {
        sourceReplicationServer = runReplicationServer(sourceReplicationPort, pluginConfigFilePath);
        sinkReplicationServer1 = runReplicationServer(sinkReplicationPort1, pluginConfigFilePath);
        sinkReplicationServer2 = runReplicationServer(sinkReplicationPort2, pluginConfigFilePath);
        sinkReplicationServer3 = runReplicationServer(sinkReplicationPort3, pluginConfigFilePath);
    }
}