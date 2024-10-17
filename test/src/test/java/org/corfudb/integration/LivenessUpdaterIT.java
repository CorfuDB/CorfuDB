package org.corfudb.integration;

import org.corfudb.runtime.CheckpointLivenessUpdater;
import org.corfudb.runtime.CompactorMetadataTables;
import org.corfudb.runtime.CorfuCompactorManagement;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.test.SampleSchema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
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
        CorfuRuntime runtime = createRuntime(singleNodeEndpoint);
        CorfuStore store = new CorfuStore(runtime);

        store.openTable(
                namespace,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.FirewallRule.class,
                SampleSchema.Uuid.class,
                TableOptions.fromProtoSchema(SampleSchema.FirewallRule.class));

        CheckpointLivenessUpdater livenessUpdater = new CheckpointLivenessUpdater(store);
        livenessUpdater.start();
        CorfuStoreMetadata.TableName tableNameBuilder = CorfuStoreMetadata.TableName.newBuilder().
                setNamespace(namespace).setTableName(tableName).build();
        Duration interval = Duration.ofSeconds(15);

        CompactorMetadataTables compactorMetadataTables = new CompactorMetadataTables(store);
        try (TxnContext txn = store.txn(CORFU_SYSTEM_NAMESPACE)) {
            txn.putRecord(compactorMetadataTables.getActiveCheckpointsTable(), tableNameBuilder,
                    CorfuCompactorManagement.ActiveCPStreamMsg.newBuilder().build(),
                    null);
            txn.commit();
        } catch (Exception e) {
            Assert.fail("Transaction Failed due to " + e.getStackTrace());
        }

        livenessUpdater.setCurrentTable(tableNameBuilder);
        try {
            TimeUnit.SECONDS.sleep(interval.getSeconds());
        } catch (InterruptedException e) {
            System.out.println("Sleep interrupted: " + e);
        }
        livenessUpdater.unsetCurrentTable();

        try (TxnContext txn = store.txn(CORFU_SYSTEM_NAMESPACE)) {
            CorfuCompactorManagement.ActiveCPStreamMsg newStatus = (CorfuCompactorManagement.ActiveCPStreamMsg)
                    txn.getRecord(CompactorMetadataTables.ACTIVE_CHECKPOINTS_TABLE_NAME,
                    Optional.of(tableNameBuilder).get()).getPayload();
            txn.close();
            Assert.assertEquals(1, newStatus.getSyncHeartbeat());
        } catch (Exception e) {
            Assert.fail("Transaction Failed due to " + e.getStackTrace());
        } finally {
            livenessUpdater.shutdown();
        }
    }
}
