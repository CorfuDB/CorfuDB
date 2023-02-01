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
 * checkpointed by any of the clients
 */
@Slf4j
public class CorfuCompactorCheckpointer {
    private final DistributedCheckpointerHelper distributedCheckpointerHelper;
    private final DistributedCheckpointer distributedCheckpointer;
    public static final int RETRY_CHECKPOINTING = 5;
    private static final int RETRY_CHECKPOINTING_SLEEP_SECOND = 10;

    public CorfuCompactorCheckpointer(String[] args) throws Exception {
        CorfuCompactorConfig config = new CorfuCompactorConfig(args, "", "");

        Thread.currentThread().setName("CorfuStore-" + config.getNodeLocator().getPort() + "-chkpter");
        CorfuRuntime cpRuntime = (CorfuRuntime.fromParameters(
                config.getParams())).parseConfigurationString(config.getNodeLocator().toEndpointUrl()).connect();
        CorfuRuntime corfuRuntime = (CorfuRuntime.fromParameters(
                config.getParams())).parseConfigurationString(config.getNodeLocator().toEndpointUrl()).connect();
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);

        this.distributedCheckpointerHelper = new DistributedCheckpointerHelper(corfuStore);
        this.distributedCheckpointer = new ServerTriggeredCheckpointer(CheckpointerBuilder.builder()
                .corfuRuntime(corfuRuntime)
                .cpRuntime(Optional.of(cpRuntime))
                .persistedCacheRoot(config.getPersistedCacheRoot())
                .isClient(false)
                .build(), corfuStore, distributedCheckpointerHelper.getCompactorMetadataTables());
    }

    public CorfuCompactorCheckpointer(DistributedCheckpointerHelper distributedCheckpointerHelper,
                                      DistributedCheckpointer distributedCheckpointer) {
        this.distributedCheckpointerHelper = distributedCheckpointerHelper;
        this.distributedCheckpointer = distributedCheckpointer;
    }

    /**
     * Entry point to invoke checkpointing
     *
     * @param args command line argument strings
     */
    public static void main(String[] args) {
        try {
            CorfuCompactorCheckpointer corfuCompactorMain = new CorfuCompactorCheckpointer(args);
            corfuCompactorMain.startCheckpointing();
        } catch (Exception e) {
            log.error("CorfuStoreCompactorMain crashed with error: {}, Exception: ",
                    CorfuCompactorConfig.CORFU_LOG_CHECKPOINT_ERROR, e);
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
        } finally {
            distributedCheckpointer.shutdown();
        }
    }
}
