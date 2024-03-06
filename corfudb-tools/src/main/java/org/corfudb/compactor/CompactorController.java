package org.corfudb.compactor;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.DistributedCheckpointerHelper;
import org.corfudb.runtime.collections.CorfuStore;

/**
 * Helps with inserting the required compactor controls key into the CompactionControlsTable
 * These keys are used by the manager to make decisions regarding triggering the next cycle
 */
@Slf4j
public class CompactorController {
    private final CompactorControllerConfig config;
    @Getter
    private CorfuRuntime corfuRuntime;
    private CorfuStore corfuStore;
    private DistributedCheckpointerHelper distributedCheckpointerHelper;
    public CompactorController(String[] args) {
        this.config = new CompactorControllerConfig(args);
        Thread.currentThread().setName("Cmpt-ctrls-" + config.getNodeLocator().getPort());
    }

    public CompactorController(CompactorControllerConfig config, CorfuRuntime corfuRuntime, CorfuStore corfuStore,
                               DistributedCheckpointerHelper distributedCheckpointerHelper) {
        this.config = config;
        this.corfuRuntime = corfuRuntime;
        this.corfuStore = corfuStore;
        this.distributedCheckpointerHelper = distributedCheckpointerHelper;
    }

    public void initCompactorController() throws Exception {
        this.corfuRuntime = (CorfuRuntime.fromParameters(
                config.getParams())).parseConfigurationString(config.getNodeLocator().toEndpointUrl());
        this.corfuRuntime.connect();
        this.corfuStore = new CorfuStore(corfuRuntime);
        this.distributedCheckpointerHelper = new DistributedCheckpointerHelper(corfuStore);
    }

    /**
     * Entry point to invoke compactor controls operations
     *
     * @param args command line argument strings
     */
    public static void main(String[] args) {
        CompactorController corfuStoreCompactorControls = new CompactorController(args);
        try {
            corfuStoreCompactorControls.initCompactorController();
            corfuStoreCompactorControls.doCompactorAction();
        } catch (Exception e) {
            log.error("CorfuStoreCompactorMain crashed with error: {}, Exception: ",
                    CompactorBaseConfig.CORFU_LOG_CHECKPOINT_ERROR, e);
        } finally {
            corfuStoreCompactorControls.shutdown();
        }
        log.info("Exiting CorfuStoreCompactor");
    }

    protected void doCompactorAction() {
        if (config.isFreezeCompaction() || config.isDisableCompaction()) {
            if (config.isDisableCompaction()) {
                log.info("Disabling compaction...");
                distributedCheckpointerHelper.disableCompaction();
            }
            if (config.isFreezeCompaction()) {
                log.info("Freezing compaction...");
                distributedCheckpointerHelper.freezeCompaction();
            }
            return;
        }
        if (config.isUnfreezeCompaction()) {
            log.info("Unfreezing compaction...");
            distributedCheckpointerHelper.unfreezeCompaction();
        }
        if (config.isEnableCompaction()) {
            log.info("Enabling compaction...");
            distributedCheckpointerHelper.enableCompaction();
        }
        if (config.isInstantTriggerCompaction()) {
            if (config.isTrim()) {
                log.info("Enabling instant compaction trigger with trim...");
                distributedCheckpointerHelper.instantTrigger(true);

            } else {
                log.info("Enabling instant compactor trigger...");
                distributedCheckpointerHelper.instantTrigger(false);
            }
        }
    }

    public void shutdown() {
        if (corfuRuntime != null) {
            corfuRuntime.shutdown();
        }
    }
}
