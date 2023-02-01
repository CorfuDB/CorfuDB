package org.corfudb.compactor;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.DistributedCheckpointerHelper;
import org.corfudb.runtime.collections.CorfuStore;

/**
 *
 */
@Slf4j
public class CorfuCompactorControls {
    private final CorfuRuntime corfuRuntime;
    private final CorfuStore corfuStore;
    private final CorfuCompactorControlsConfig config;
    private final DistributedCheckpointerHelper distributedCheckpointerHelper;
    public CorfuCompactorControls(String[] args) throws Exception {
        this.config = new CorfuCompactorControlsConfig(args);

        Thread.currentThread().setName("CorfuStore-" + config.getNodeLocator().getPort() + "-chkpter");
        this.corfuRuntime = (CorfuRuntime.fromParameters(
                config.getParams())).parseConfigurationString(config.getNodeLocator().toEndpointUrl()).connect();
        this.corfuStore = new CorfuStore(corfuRuntime);

        this.distributedCheckpointerHelper = new DistributedCheckpointerHelper(corfuStore);
    }

    public CorfuCompactorControls(CorfuCompactorControlsConfig config, CorfuRuntime corfuRuntime, CorfuStore corfuStore,
                                  DistributedCheckpointerHelper distributedCheckpointerHelper) {
        this.config = config;
        this.corfuRuntime = corfuRuntime;
        this.corfuStore = corfuStore;
        this.distributedCheckpointerHelper = distributedCheckpointerHelper;
    }

    /**
     * Entry point to invoke checkpointing
     *
     * @param args command line argument strings
     */
    public static void main(String[] args) {
        try {
            CorfuCompactorControls corfuStoreCompactorControls = new CorfuCompactorControls(args);
            corfuStoreCompactorControls.doCompactorAction();
        } catch (Exception e) {
            log.error("CorfuStoreCompactorMain crashed with error: {}, Exception: ",
                    CorfuCompactorConfig.CORFU_LOG_CHECKPOINT_ERROR, e);
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
}
