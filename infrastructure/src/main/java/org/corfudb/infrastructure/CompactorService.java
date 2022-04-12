package org.corfudb.infrastructure;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.DistributedCompactor;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.concurrent.SingletonResource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Orchestrates distributed compaction
 * <p>
 * Created by Sundar Sridharan on 3/2/22.
 */
@Slf4j
public class CompactorService implements ManagementService {

    //TODO: make this tunable
    private static final Duration TRIGGER_INTERVAL = Duration.ofMinutes(15);
    @Setter
    private static int LIVENESS_TIMEOUT = 60000;

    private final ServerContext serverContext;
    private final SingletonResource<CorfuRuntime> runtimeSingletonResource;

    private final ScheduledExecutorService orchestratorThread;
    private final ScheduledExecutorService spawnJvm;
    private final IInvokeCheckpointing invokeCheckpointing;

    private ICompactionTriggerPolicy compactionTriggerPolicy;
    private CompactorLeaderServices compactorLeaderServices;
    private CorfuStore corfuStore;
    private boolean isLeader;

    //TODO: make it a prop file and maybe pass it from the server
    List<TableName> sensitiveTables = new ArrayList<>();

    CompactorService(@NonNull ServerContext serverContext,
                     @NonNull SingletonResource<CorfuRuntime> runtimeSingletonResource,
                     @NonNull IInvokeCheckpointing invokeCheckpointing,
                     @NonNull ICompactionTriggerPolicy compactionTriggerPolicy) {
        this.serverContext = serverContext;
        this.runtimeSingletonResource = runtimeSingletonResource;
        this.orchestratorThread = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setNameFormat(serverContext.getThreadPrefix() + "CompactorService")
                        .build());
        this.spawnJvm = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setNameFormat(serverContext.getThreadPrefix() + "CompactorService")
                        .build());
        this.invokeCheckpointing = invokeCheckpointing;
        this.compactionTriggerPolicy = compactionTriggerPolicy;
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
        this.compactorLeaderServices = new CompactorLeaderServices(getCorfuRuntime(), serverContext.getNodeId());
        this.corfuStore = new CorfuStore(getCorfuRuntime());
        this.compactionTriggerPolicy.setCorfuRuntime(getCorfuRuntime());

        orchestratorThread.scheduleAtFixedRate(
            this::runOrchestrator,
            interval.toMillis(),
            interval.toMillis(),
            TimeUnit.MILLISECONDS
        );

        spawnJvm.scheduleAtFixedRate(
            this::runCheckpointer,
            interval.toMillis(),
            interval.toMillis(),
            TimeUnit.MILLISECONDS
        );
    }

    private void runCheckpointer() {
        try(TxnContext txn = corfuStore.txn(TableRegistry.CORFU_SYSTEM_NAMESPACE)) {
            CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(
                    DistributedCompactor.COMPACTION_MANAGER_TABLE_NAME,
                    CompactorLeaderServices.getCOMPACTION_MANAGER_KEY()).getPayload();
            txn.commit();
            log.info("runcp managerStatus: {}", managerStatus != null ? managerStatus : "null");
            if (managerStatus != null && managerStatus.getStatus() == StatusType.STARTED) {
                invokeCheckpointing.invokeCheckpointing();
            }
        }
    }

    private void runOrchestrator() {
        this.isLeader = isNodePrimarySequencer(updateLayoutAndGet());
        compactorLeaderServices.setLeader(this.isLeader);
        if (!isLeader) {
            return;
        }
        try(TxnContext txn = corfuStore.txn(TableRegistry.CORFU_SYSTEM_NAMESPACE)) {
            CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(
                    DistributedCompactor.COMPACTION_MANAGER_TABLE_NAME,
                    CompactorLeaderServices.getCOMPACTION_MANAGER_KEY()).getPayload();
            txn.commit();
            log.info("managerStatus: {}", managerStatus != null ? managerStatus : "null");
            if (managerStatus != null && managerStatus.getStatus() == StatusType.STARTED){
                compactorLeaderServices.validateLiveness(LIVENESS_TIMEOUT);
            } else if (compactionTriggerPolicy.shouldTrigger(TRIGGER_INTERVAL.getSeconds())) {
                compactorLeaderServices.init();
            }
        } catch (Exception e) {
            log.warn("Exception in runOrchestrator: ", e);
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
        invokeCheckpointing.shutdown();
        orchestratorThread.shutdownNow();
        spawnJvm.shutdownNow();
        log.info("Compactor Orchestrator service shutting down.");
    }
}
