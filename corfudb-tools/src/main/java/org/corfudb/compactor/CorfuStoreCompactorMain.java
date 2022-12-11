package org.corfudb.compactor;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CheckpointerBuilder;
import org.corfudb.runtime.CompactorMetadataTables;
import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.DistributedCheckpointer;
import org.corfudb.runtime.DistributedCheckpointerHelper;
import org.corfudb.runtime.DistributedCheckpointerHelper.UpdateAction;
import org.corfudb.runtime.ServerTriggeredCheckpointer;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.proto.RpcCommon;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Invokes the startCheckpointing() method of DistributedCompactor to checkpoint tables that weren't
 * checkpointed by any of the clients
 */
@Slf4j
public class CorfuStoreCompactorMain {

    private final CompactorMetadataTables compactorMetadataTables;
    private final CorfuRuntime corfuRuntime;
    private final CorfuRuntime cpRuntime;
    private final CorfuStore corfuStore;
    private final CorfuStoreCompactorConfig config;
    private final DistributedCheckpointerHelper distributedCheckpointerHelper;
    private final UpgradeDescriptorTable upgradeDescriptorTable;
    private final Table<StringKey, RpcCommon.TokenMsg, Message> compactionControlsTable;

    private static final int RETRY_CHECKPOINTING = 5;
    private static final int RETRY_CHECKPOINTING_SLEEP_SECOND = 10;

    public CorfuStoreCompactorMain(String[] args) throws Exception {
        this.config = new CorfuStoreCompactorConfig(args);

        Thread.currentThread().setName("CorfuStore-" + config.getNodeLocator().getPort() + "-chkpter");
        this.cpRuntime = (CorfuRuntime.fromParameters(
                config.getParams())).parseConfigurationString(config.getNodeLocator().toEndpointUrl()).connect();
        this.corfuRuntime = (CorfuRuntime.fromParameters(
                config.getParams())).parseConfigurationString(config.getNodeLocator().toEndpointUrl()).connect();
        this.corfuStore = new CorfuStore(corfuRuntime);
        this.upgradeDescriptorTable = new UpgradeDescriptorTable(corfuRuntime);

        this.compactorMetadataTables = new CompactorMetadataTables(corfuStore);
        this.compactionControlsTable = compactorMetadataTables.getCompactionControlsTable();
        this.distributedCheckpointerHelper = new DistributedCheckpointerHelper(corfuStore);
    }

    /**
     * Entry point to invoke checkpointing
     *
     * @param args command line argument strings
     */
    public static void main(String[] args) {
        try {
            CorfuStoreCompactorMain corfuCompactorMain = new CorfuStoreCompactorMain(args);
            corfuCompactorMain.doCompactorAction();
        } catch (Exception e) {
            log.error("CorfuStoreCompactorMain crashed with error: {}, Exception: ",
                    CorfuStoreCompactorConfig.CORFU_LOG_CHECKPOINT_ERROR, e);
        }
        log.info("Exiting CorfuStoreCompactor");
    }

    private void doCompactorAction() {
        if (config.isFreezeCompaction()) {
            log.info("Freezing compaction...");
            distributedCheckpointerHelper.updateCompactionControlsTable(compactionControlsTable, CompactorMetadataTables.FREEZE_TOKEN, UpdateAction.PUT);
        } else if (config.isUnfreezeCompaction()) {
            log.info("Unfreezing compaction...");
            distributedCheckpointerHelper.updateCompactionControlsTable(compactionControlsTable, CompactorMetadataTables.FREEZE_TOKEN, UpdateAction.DELETE);
        }
        if (config.isUpgradeDescriptorTable()) {
            log.info("Upgrading descriptor table...");
            upgradeDescriptorTable.syncProtobufDescriptorTable();
        }
        if (config.isInstantTriggerCompaction()) {
            if (config.isTrim()) {
                log.info("Enabling instant compaction trigger with trim...");
                distributedCheckpointerHelper.updateCompactionControlsTable(compactionControlsTable,
                        CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM, UpdateAction.PUT);
            } else {
                log.info("Enabling instant compactor trigger...");
                distributedCheckpointerHelper.updateCompactionControlsTable(compactionControlsTable,
                        CompactorMetadataTables.INSTANT_TIGGER, UpdateAction.PUT);
            }
        }
        if (config.isStartCheckpointing()) {
            checkpoint();
        }
    }

    private void checkpoint() {
        DistributedCheckpointer distributedCheckpointer = new ServerTriggeredCheckpointer(CheckpointerBuilder.builder()
                .corfuRuntime(corfuRuntime)
                .cpRuntime(Optional.of(cpRuntime))
                .persistedCacheRoot(config.getPersistedCacheRoot())
                .isClient(false)
                .build(), corfuStore, compactorMetadataTables);
        try {
            for (int i = 0; i < RETRY_CHECKPOINTING; i++) {
                if (distributedCheckpointerHelper.hasCompactionStarted()) {
                    distributedCheckpointer.checkpointTables();
                    break;
                }
                log.info("Compaction cycle hasn't started yet...");
                TimeUnit.SECONDS.sleep(RETRY_CHECKPOINTING_SLEEP_SECOND);
            }
        } catch (InterruptedException ie) {
            log.error("Sleep interrupted with exception: ", ie);
        } catch (Exception e) {
            log.error("Exception during checkpointing: {}, StackTrace: {}", e.getMessage(), e.getStackTrace());
        } finally {
            distributedCheckpointer.shutdown();
        }
    }
}
