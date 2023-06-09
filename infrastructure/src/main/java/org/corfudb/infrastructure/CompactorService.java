package org.corfudb.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.NonNull;
import org.corfudb.infrastructure.health.Component;
import org.corfudb.infrastructure.health.HealthMonitor;
import org.corfudb.infrastructure.health.Issue;
import org.corfudb.runtime.CompactorMetadataTables;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.DistributedCheckpointerHelper;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.LambdaUtils;
import org.corfudb.util.concurrent.SingletonResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
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
    private final CompactionTriggerPolicy compactionTriggerPolicy;

    private Optional<CompactorLeaderServices> optionalCompactorLeaderServices = Optional.empty();
    private Optional<CorfuStore> optionalCorfuStore = Optional.empty();
    private Optional<DistributedCheckpointerHelper> optionalDistributedCheckpointerHelper = Optional.empty();
    private TrimLog trimLog;
    private final Logger log;
    private static final Duration LIVENESS_TIMEOUT = Duration.ofMinutes(1);

    CompactorService(@NonNull ServerContext serverContext,
                     @NonNull SingletonResource<CorfuRuntime> runtimeSingletonResource,
                     @NonNull InvokeCheckpointing checkpointerJvmManager,
                     @NonNull CompactionTriggerPolicy compactionTriggerPolicy) {
        this.serverContext = serverContext;
        this.runtimeSingletonResource = runtimeSingletonResource;

        this.orchestratorThread = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setNameFormat("Cmpt-" + serverContext.getServerConfig().get("<port>") + "-chkpter")
                        .build());
        this.checkpointerJvmManager = checkpointerJvmManager;
        this.compactionTriggerPolicy = compactionTriggerPolicy;
        this.log = LoggerFactory.getLogger("compactor-leader");
    }

    private CorfuRuntime getCorfuRuntime() {
        return runtimeSingletonResource.get();
    }

    /**
     * Starts the long-running service.
     *
     * @param interval interval to run the service
     */
    @Override
    public void start(Duration interval) {
        log.info("Starting Compaction service...");
        if (getCorfuRuntime().getParameters().getCheckpointTriggerFreqMillis() <= 0) {
            return;
        }

        this.trimLog = new TrimLog(getCorfuRuntime(), getCorfuStore());

        orchestratorThread.scheduleWithFixedDelay(
                () -> LambdaUtils.runSansThrow(this::runOrchestrator),
                interval.toMillis(),
                interval.toMillis(),
                TimeUnit.MILLISECONDS
        );
        HealthMonitor.resolveIssue(Issue.createInitIssue(Component.COMPACTOR));
    }

    @VisibleForTesting
    public CompactorLeaderServices getCompactorLeaderServices() throws Exception {
        if (!optionalCompactorLeaderServices.isPresent()) {
            try {
                optionalCompactorLeaderServices = Optional.of(new CompactorLeaderServices(getCorfuRuntime(),
                        serverContext.getLocalEndpoint(), getCorfuStore(),
                        new LivenessValidator(getCorfuRuntime(), getCorfuStore(), LIVENESS_TIMEOUT)));
            } catch (Exception ex) {
                log.error("Unable to create CompactorLeaderServices object. Will retry on next attempt. Exception: ", ex);
                throw ex;
            }
        }
        return optionalCompactorLeaderServices.get();
    }

    @VisibleForTesting
    public CorfuStore getCorfuStore() {
        if (!this.optionalCorfuStore.isPresent()) {
            this.optionalCorfuStore = Optional.of(new CorfuStore(getCorfuRuntime()));
        }
        return this.optionalCorfuStore.get();
    }

    private DistributedCheckpointerHelper getDistributedCheckpointerHelper() throws Exception {
        if (!optionalDistributedCheckpointerHelper.isPresent()) {
            try {
                optionalDistributedCheckpointerHelper = Optional.of(new DistributedCheckpointerHelper(getCorfuStore()));
            } catch (Exception ex) {
                log.error("Failed to obtain a DistributedCheckpointerHelper. Will retry on next attempt. Exception: ", ex);
                throw ex;
            }
        }
        return optionalDistributedCheckpointerHelper.get();
    }

    /**
     * Invokes the CorfuStoreCompactor jvm based on the status of CompactionManager
     * Additionally, If the current node is the leader,
     * a. Invokes ValidateLiveness() to keep track of checkpointing progress by each client
     * b. Triggers the distributed compaction cycle based on the TriggerPolicy
     */
    private void runOrchestrator() {
        try {
            boolean isLeader = isNodePrimarySequencer(updateLayoutAndGet());
            log.trace("Current node isLeader: {}", isLeader);

            CompactorLeaderServices compactorLeaderServices = getCompactorLeaderServices();

            CheckpointingStatus managerStatus = null;
            try (TxnContext txn = getCorfuStore().txn(CORFU_SYSTEM_NAMESPACE)) {
                managerStatus = (CheckpointingStatus) txn.getRecord(
                        CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME,
                        CompactorMetadataTables.COMPACTION_MANAGER_KEY).getPayload();
                if (managerStatus == null && isLeader) {
                    txn.putRecord(compactorLeaderServices.getCompactorMetadataTables().getCompactionManagerTable(),
                            CompactorMetadataTables.COMPACTION_MANAGER_KEY,
                            CheckpointingStatus.newBuilder().setStatus(StatusType.IDLE).setCycleCount(0).build(), null);
                }
                txn.commit();
            } catch (Exception ex) {
                log.error("Unable to acquire manager status: ", ex);
                return;
            }
            if (isLeader) {
                if (managerStatus != null && managerStatus.getStatus() == StatusType.STARTED) {
                    if (getDistributedCheckpointerHelper().isCompactionDisabled()) {
                        log.info("Compaction has been disabled. Force finish compaction cycle as it already started");
                        compactorLeaderServices.finishCompactionCycle();
                    } else {
                        compactorLeaderServices.validateLiveness();
                    }
                } else if (compactionTriggerPolicy.shouldTrigger(
                        getCorfuRuntime().getParameters().getCheckpointTriggerFreqMillis(), getCorfuStore())) {
                    trimLog.invokePrefixTrim();
                    compactionTriggerPolicy.markCompactionCycleStart();
                    compactorLeaderServices.initCompactionCycle();
                }
            }
            if (managerStatus != null) {
                if (managerStatus.getStatus() == StatusType.FAILED || managerStatus.getStatus() == StatusType.COMPLETED) {
                    checkpointerJvmManager.shutdown();
                } else if (managerStatus.getStatus() == StatusType.STARTED && !checkpointerJvmManager.isRunning()
                        && !checkpointerJvmManager.isInvoked()) {
                    checkpointerJvmManager.invokeCheckpointing();
                }
            }
        } catch (Exception ex) {
          log.error("Exception in runOrchestrator(): ", ex);
        } catch (Throwable t) {
            log.error("Encountered unexpected exception in runOrchestrator(): ", t);
            throw t;
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
        log.info("Compactor Orchestrator service shutting down.");
        HealthMonitor.reportIssue(Issue.createInitIssue(Component.COMPACTOR));
    }
}
