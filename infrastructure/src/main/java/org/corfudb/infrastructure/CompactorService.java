package org.corfudb.infrastructure;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.NonNull;
import org.corfudb.runtime.CompactorMetadataTables;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.concurrent.SingletonResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * Orchestrates distributed compaction
 * <p>
 * Created by Sundar Sridharan on 3/2/22.
 */
public class CompactorService implements ManagementService {

    private final ServerContext serverContext;
    private final SingletonResource<CorfuRuntime> runtimeSingletonResource;
    private final ScheduledExecutorService orchestratorThread;
    private final InvokeCheckpointing checkpointerJvmManager;

    private CompactionTriggerPolicy compactionTriggerPolicy;
    private CompactorLeaderServices compactorLeaderServices;
    private CorfuStore corfuStore;
    private TrimLog trimLog;

    private final Logger syslog;

    CompactorService(@NonNull ServerContext serverContext,
                     @NonNull SingletonResource<CorfuRuntime> runtimeSingletonResource,
                     @NonNull InvokeCheckpointing checkpointerJvmManager) {
        this.serverContext = serverContext;
        this.runtimeSingletonResource = runtimeSingletonResource;

        this.orchestratorThread = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setNameFormat("Cmpt-" + serverContext.getServerConfig().get("<port>") + "-chkpter")
                        .build());
        this.checkpointerJvmManager = checkpointerJvmManager;
        syslog = LoggerFactory.getLogger("syslog");
    }

    CorfuRuntime getCorfuRuntime() {
        return runtimeSingletonResource.get();
    }

    /**
     * Starts the long running service.
     *
     * @param interval interval to run the service
     */
    @Override
    public void start(Duration interval) {
        if (getCorfuRuntime().getParameters().getCheckpointTriggerFreqMillis() <= 0) {
            return;
        }

        this.corfuStore = new CorfuStore(getCorfuRuntime());
        this.compactorLeaderServices = new CompactorLeaderServices(getCorfuRuntime(), serverContext.getLocalEndpoint(), corfuStore);
        this.trimLog = new TrimLog(getCorfuRuntime(), corfuStore);
        this.compactionTriggerPolicy = new DynamicTriggerPolicy(getCorfuRuntime());

        orchestratorThread.scheduleWithFixedDelay(
                this::runOrchestrator,
                interval.toMillis(),
                interval.toMillis(),
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * Invokes and cleans up the CorfuStoreCompactor jvm based on the status of CompactionManager
     * Additionally, If the current node is the leader,
     * a. Invokes ValidateLiveness() to keep track of checkpointing progress by each client
     * b. Triggers the distributed compaction cycle based on the TriggerPolicy
     */
    private void runOrchestrator() {
        boolean isLeader = isNodePrimarySequencer(updateLayoutAndGet());
        CheckpointingStatus managerStatus = null;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            managerStatus = (CheckpointingStatus) txn.getRecord(
                    CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME,
                    CompactorMetadataTables.COMPACTION_MANAGER_KEY).getPayload();
            txn.commit();
        } catch (Exception e) {
            syslog.warn("Unable to acquire manager status: {}", e.getStackTrace());
        }
        try {
            if (managerStatus != null) {
                if (managerStatus.getStatus() == StatusType.FAILED || managerStatus.getStatus() == StatusType.COMPLETED) {
                    checkpointerJvmManager.shutdown();
                } else if (managerStatus.getStatus() == StatusType.STARTED_ALL && !checkpointerJvmManager.isRunning()
                        && !checkpointerJvmManager.isInvoked()) {
                    checkpointerJvmManager.invokeCheckpointing();
                    checkpointerJvmManager.shutdown();
                }
            }

            if (isLeader) {
                if (managerStatus != null && (managerStatus.getStatus() == StatusType.STARTED ||
                        managerStatus.getStatus() == StatusType.STARTED_ALL)) {
                    compactorLeaderServices.validateLiveness();
                } else if (compactionTriggerPolicy.shouldTrigger(getCorfuRuntime().getParameters().getCheckpointTriggerFreqMillis())) {
                    compactionTriggerPolicy.markCompactionCycleStart();
                    trimLog.invokePrefixTrim();
                    compactorLeaderServices.initCompactionCycle();
                }
            }
        } catch (Exception ex) {
            syslog.warn("Exception in runOrcestrator(): {}", ex.getStackTrace());
        }
    }

    private Layout updateLayoutAndGet() {
        return getCorfuRuntime()
                .invalidateLayout()
                .join();
    }

    private boolean isNodePrimarySequencer(Layout layout) {
        return layout.getPrimarySequencer().equals(serverContext.getLocalEndpoint());
    }

    /**
     * Clean up.
     */
    @Override
    public void shutdown() {
        checkpointerJvmManager.shutdown();
        orchestratorThread.shutdownNow();
        syslog.info("Compactor Orchestrator service shutting down.");
    }
}
