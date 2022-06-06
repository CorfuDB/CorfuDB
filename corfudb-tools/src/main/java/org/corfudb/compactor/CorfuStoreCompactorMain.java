package org.corfudb.compactor;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.DistributedCompactor;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.proto.RpcCommon.TokenMsg;

import java.util.concurrent.TimeUnit;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * Invokes the startCheckpointing() method of DistributedCompactor to checkpoint remaining tables that weren't
 * checkpointed by any of the clients
 */
@Slf4j
public class CorfuStoreCompactorMain {

    private final CorfuStoreCompactorConfig config;
    private final CorfuRuntime corfuRuntime;
    private final CorfuStore corfuStore;
    private final DistributedCompactor distributedCompactor;

    private Table<StringKey, TokenMsg, Message> checkpointTable;
    private int retryCheckpointing = 1;

    public CorfuStoreCompactorMain(String[] args) {
        this.config = new CorfuStoreCompactorConfig();
        config.parseAndBuildRuntimeParameters(args);

        String connectionString = config.getPort() + ":" + config.getHostname();
        CorfuRuntime cpRuntime = (CorfuRuntime.fromParameters(config.getParams())).parseConfigurationString(connectionString).connect();
        corfuRuntime = (CorfuRuntime.fromParameters(config.getParams())).parseConfigurationString(connectionString).connect();
        corfuStore = new CorfuStore(corfuRuntime);
        distributedCompactor = new DistributedCompactor(corfuRuntime, cpRuntime, config.getPersistedCacheRoot());
        try {
            this.checkpointTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    DistributedCompactor.CHECKPOINT,
                    StringKey.class,
                    TokenMsg.class,
                    null,
                    TableOptions.fromProtoSchema(TokenMsg.class));

        } catch (Exception e) {
            log.error("Caught an exception while opening Compaction management tables ", e);
        }
    }

    /**
     * Entry point to invoke Client checkpointing by the CorfuServer
     *
     * @param args command line argument strings
     */
    public static void main(String[] args) {
        try {
            CorfuStoreCompactorMain corfuCompactorMain = new CorfuStoreCompactorMain(args);
            corfuCompactorMain.startCompaction();
        } catch (Throwable t) {
            log.error("Error in CorfuStoreComactor execution, ", t);
            throw t;
        }
    }

    private void startCompaction() {
        Thread.currentThread().setName("CorfuStore-" + config.getPort() + "-chkpter");
        if (config.isUpgrade()) {
            upgrade();
        }
        checkpoint();
    }

    private void upgrade() {
        retryCheckpointing = CorfuStoreCompactorConfig.CHECKPOINT_RETRY_UPGRADE;

        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            txn.putRecord(checkpointTable, DistributedCompactor.UPGRADE_KEY, TokenMsg.getDefaultInstance(),
                    null);
            txn.commit();
        } catch (Exception e) {
            log.warn("Unable to write UpgradeKey to checkpoint table, ", e);
        }
    }

    private void checkpoint() {
        try {
            for (int i = 0; i < retryCheckpointing; i++) {
                //startCheckpointing() returns the num of tables checkpointed
                if (distributedCompactor.startCheckpointing() > 0) {
                    break;
                }
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (Throwable throwable) {
            log.error("CorfuStoreCompactorMain crashed with error:", CorfuStoreCompactorConfig.CORFU_LOG_CHECKPOINT_ERROR, throwable);
        }
    }
}