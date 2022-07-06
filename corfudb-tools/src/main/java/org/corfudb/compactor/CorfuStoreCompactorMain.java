package org.corfudb.compactor;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.*;
import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.proto.RpcCommon.TokenMsg;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * Invokes the startCheckpointing() method of DistributedCompactor to checkpoint tables that weren't
 * checkpointed by any of the clients
 */
@Slf4j
public class CorfuStoreCompactorMain {

    private final CorfuStoreCompactorConfig config;
    private final CorfuStore corfuStore;
    private final DistributedCheckpointer distributedCheckpointer;

    private Table<StringKey, TokenMsg, Message> checkpointTable;
    private int retryCheckpointing = 1;

    public CorfuStoreCompactorMain(String[] args) throws Exception {
        this.config = new CorfuStoreCompactorConfig(args);

        CorfuRuntime cpRuntime = (CorfuRuntime.fromParameters(
                config.getParams())).parseConfigurationString(config.getNodeLocator().toEndpointUrl()).connect();
        CorfuRuntime corfuRuntime = (CorfuRuntime.fromParameters(
                config.getParams())).parseConfigurationString(config.getNodeLocator().toEndpointUrl()).connect();
        corfuStore = new CorfuStore(corfuRuntime);

        CompactorMetadataTables compactorMetadataTables = new CompactorMetadataTables(corfuStore);
        this.checkpointTable = compactorMetadataTables.getCheckpointTable();

        this.distributedCheckpointer = new ServerTriggeredCheckpointer(CheckpointerBuilder.builder()
            .corfuRuntime(corfuRuntime)
            .cpRuntime(Optional.of(cpRuntime))
            .persistedCacheRoot(config.getPersistedCacheRoot())
            .isClient(false)
            .build(), corfuStore, compactorMetadataTables);
    }

    /**
     * Entry point to invoke checkpointing
     *
     * @param args command line argument strings
     */
    public static void main(String[] args) {
        try {
            CorfuStoreCompactorMain corfuCompactorMain = new CorfuStoreCompactorMain(args);
            corfuCompactorMain.startCheckpointing();
        } catch (Exception e) {
            log.error("Exception during checkpointing: {}, StackTrace: {}", e.getMessage(), e.getStackTrace());
        }
    }

    private void startCheckpointing() {
        Thread.currentThread().setName("CorfuStore-" + config.getNodeLocator().getPort() + "-chkpter");
        if (config.isUpgrade()) {
            upgrade();
        }
        checkpoint();
    }

    private void upgrade() {
        retryCheckpointing = CorfuStoreCompactorConfig.CHECKPOINT_RETRY_UPGRADE;

        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            txn.putRecord(checkpointTable, CompactorMetadataTables.UPGRADE_KEY, TokenMsg.getDefaultInstance(), null);
            txn.commit();
        } catch (Exception e) {
            log.warn("Unable to write UpgradeKey to checkpoint table, ", e);
        }
    }

    private void checkpoint() {
        try {
            for (int i = 0; i < retryCheckpointing; i++) {
                if (DistributedCheckpointerHelper.hasCompactionStarted(corfuStore)) {
                    distributedCheckpointer.checkpointTables();
                    break;
                }
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (InterruptedException ie) {
            log.error("Sleep interrupted with exception: ", ie);
        } catch (Exception e) {
            log.error("CorfuStoreCompactorMain crashed with error: {}, Exception: ",
                    CorfuStoreCompactorConfig.CORFU_LOG_CHECKPOINT_ERROR, e);
        }
    }
}
