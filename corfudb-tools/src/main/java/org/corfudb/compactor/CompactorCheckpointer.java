package org.corfudb.compactor;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CheckpointerBuilder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.DistributedCheckpointer;
import org.corfudb.runtime.DistributedCheckpointerHelper;
import org.corfudb.runtime.ServerTriggeredCheckpointer;
import org.corfudb.runtime.collections.CorfuStore;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Invokes the startCheckpointing() method of DistributedCompactor to checkpoint tables that weren't
 * checkpointed by other clients
 */
@Slf4j
public class CompactorCheckpointer {
    private CompactorBaseConfig config;
    private DistributedCheckpointerHelper distributedCheckpointerHelper;
    private DistributedCheckpointer distributedCheckpointer;
    private CorfuRuntime cpRuntime;
    private CorfuRuntime corfuRuntime;
    public static final int RETRY_CHECKPOINTING = 5;
    private static final int RETRY_CHECKPOINTING_SLEEP_SECOND = 10;

    public CompactorCheckpointer(String[] args) {
        config = new CompactorBaseConfig(args, "", "");
        Thread.currentThread().setName("Cmpt-chkpter-" + config.getNodeLocator().getPort());
    }

    public CompactorCheckpointer(CorfuRuntime cpRuntime,
                                 CorfuRuntime corfuRuntime,
                                 DistributedCheckpointerHelper distributedCheckpointerHelper,
                                 DistributedCheckpointer distributedCheckpointer) {
        this.cpRuntime = cpRuntime;
        this.corfuRuntime = corfuRuntime;
        this.distributedCheckpointerHelper = distributedCheckpointerHelper;
        this.distributedCheckpointer = distributedCheckpointer;
    }

    public void initCheckpointer() throws Exception {
        cpRuntime = (CorfuRuntime.fromParameters(
                this.config.getParams())).parseConfigurationString(config.getNodeLocator().toEndpointUrl());
        cpRuntime.connect();

        corfuRuntime = (CorfuRuntime.fromParameters(
                config.getParams())).parseConfigurationString(config.getNodeLocator().toEndpointUrl());
        corfuRuntime.connect();

        CorfuStore corfuStore = new CorfuStore(corfuRuntime);

        this.distributedCheckpointerHelper = new DistributedCheckpointerHelper(corfuStore);
        this.distributedCheckpointer = new ServerTriggeredCheckpointer(CheckpointerBuilder.builder()
                .corfuRuntime(corfuRuntime)
                .cpRuntime(Optional.of(cpRuntime))
                .persistedCacheRoot(config.getPersistedCacheRoot())
                .isClient(false)
                .build(), corfuStore, distributedCheckpointerHelper.getCompactorMetadataTables());
    }

    /**
     * Entry point to invoke checkpointing by the client jvm
     *
     * @param args command line argument strings
     */
    public static void main(String[] args) {
        CompactorCheckpointer corfuCompactorMain = new CompactorCheckpointer(args);
        try {
            corfuCompactorMain.initCheckpointer();
            corfuCompactorMain.startCheckpointing();
        } catch (Exception e) {
            log.error("CorfuStoreCompactorMain crashed with error: {}, Exception: ",
                    CompactorBaseConfig.CORFU_LOG_CHECKPOINT_ERROR, e);
        } finally {
            corfuCompactorMain.shutdown();
        }
        log.info("Exiting CorfuStoreCompactor");
    }

    protected void startCheckpointing() {
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
        }
    }

    public void shutdown() {
        if (cpRuntime != null) {
            cpRuntime.shutdown();
        }
        if (corfuRuntime != null) {
            corfuRuntime.shutdown();
        }
        distributedCheckpointer.shutdown();
    }
}
