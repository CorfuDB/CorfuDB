package org.corfudb.integration;

import org.corfudb.runtime.*;
import org.corfudb.runtime.collections.*;
import org.corfudb.test.SampleSchema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@SuppressWarnings("checkstyle:magicnumber")
public class LivenessUpdaterIT extends AbstractIT {

    private static String corfuSingleNodeHost;

    private static int corfuStringNodePort;

    private static String singleNodeEndpoint;

    /* A helper method that takes host and port specification, start a single server and
     *  returns a process. */
    private Process runSinglePersistentServer(String host, int port) throws
            IOException {
        return new AbstractIT.CorfuServerRunner()
                .setHost(host)
                .setPort(port)
                .setLogPath(getCorfuServerLogPath(host, port))
                .setSingle(true)
                .runServer();
    }

    /**
     * Load properties for a single node corfu server before each test
     */
    @Before
    public void loadProperties() {
        corfuSingleNodeHost = PROPERTIES.getProperty("corfuSingleNodeHost");
        corfuStringNodePort = Integer.valueOf(PROPERTIES.getProperty("corfuSingleNodePort"));
        singleNodeEndpoint = String.format(
                "%s:%d",
                corfuSingleNodeHost,
                corfuStringNodePort
        );
    }

    @Test
    public void livenessUpdaterTest() throws Exception {

        final String namespace = "namespace";
        final String tableName = "table";
        runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);
        runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);

        final Table<SampleSchema.Uuid, SampleSchema.FirewallRule, SampleSchema.Uuid> table = store.openTable(
                namespace,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.FirewallRule.class,
                SampleSchema.Uuid.class,
                TableOptions.fromProtoSchema(SampleSchema.FirewallRule.class));

        CheckpointLivenessUpdater livenessUpdater = new CheckpointLivenessUpdater(store);
        CorfuStoreMetadata.TableName TableName = CorfuStoreMetadata.TableName.newBuilder().setNamespace(namespace).setTableName(tableName).build();
        Duration INTERVAL = Duration.ofSeconds(15);

        CompactorMetadataTables compactorMetadataTables = new CompactorMetadataTables(store);
        TxnContext txn = store.txn(CORFU_SYSTEM_NAMESPACE);
        txn.putRecord(compactorMetadataTables.getActiveCheckpointsTable(), TableName,
                CorfuCompactorManagement.ActiveCPStreamMsg.newBuilder().build(),
                null);
        txn.commit();

        livenessUpdater.updateLiveness(TableName);
        try {
            TimeUnit.SECONDS.sleep(INTERVAL.getSeconds());
        } catch (InterruptedException e) {
            System.out.println("Sleep interrupted: " + e);
        }
        livenessUpdater.shutdown();

        TxnContext newTxn = store.txn(CORFU_SYSTEM_NAMESPACE);
        CorfuCompactorManagement.ActiveCPStreamMsg newStatus = (CorfuCompactorManagement.ActiveCPStreamMsg)
                newTxn.getRecord(CompactorMetadataTables.ACTIVE_CHECKPOINTS_TABLE_NAME, Optional.of(TableName).get()).getPayload();
        Assert.assertEquals(1, newStatus.getSyncHeartbeat());
    }
}