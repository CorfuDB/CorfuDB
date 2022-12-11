package org.corfudb.infrastructure;

import org.corfudb.runtime.CompactorMetadataTables;
import org.corfudb.runtime.DistributedCheckpointerHelper;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.proto.RpcCommon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

public class DynamicTriggerPolicy implements CompactionTriggerPolicy {
    /**
     * What time did the previous cycle start
     */
    private long lastCompactionCycleStartTS;
    private final Logger log;

    public DynamicTriggerPolicy() {
        this.lastCompactionCycleStartTS = System.currentTimeMillis();
        this.log = LoggerFactory.getLogger("compactor-leader");
    }

    @Override
    public void markCompactionCycleStart() {
        this.lastCompactionCycleStartTS = System.currentTimeMillis();
    }

    private boolean shouldForceTrigger(CorfuStore corfuStore) {
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            RpcCommon.TokenMsg instantTrigger = (RpcCommon.TokenMsg) txn.getRecord(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE,
                    CompactorMetadataTables.INSTANT_TIGGER).getPayload();
            RpcCommon.TokenMsg instantTrimTrigger = (RpcCommon.TokenMsg) txn.getRecord(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE,
                    CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM).getPayload();
            txn.commit();
            if (instantTrigger != null || instantTrimTrigger != null) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if it has been interval time since the previous trigger or
     * if force trigger condition is met
     *
     * @param interval   - trigger interval in ms
     * @param corfuStore - CorfuStore of the current runtime
     * @return true if compaction cycle should run, false otherwise
     */
    @Override
    public boolean shouldTrigger(long interval, CorfuStore corfuStore) {
        DistributedCheckpointerHelper distributedCheckpointerHelper = new DistributedCheckpointerHelper(corfuStore);
        if (distributedCheckpointerHelper.isCheckpointFrozen()) {
            log.warn("Compaction has been frozen");
            return false;
        }

        if (shouldForceTrigger(corfuStore)) {
            log.info("Force triggering compaction");
            return true;
        }

        final long currentTime = System.currentTimeMillis();
        final long timeSinceLastCycleMillis = currentTime - lastCompactionCycleStartTS;

        if (timeSinceLastCycleMillis > interval) {
            log.info("DynamicTriggerPolicy: Trigger as elapsedTime {} > safeTrimPeriod {}",
                    TimeUnit.MILLISECONDS.toSeconds(timeSinceLastCycleMillis),
                    TimeUnit.MILLISECONDS.toSeconds(interval));
            return true;
        }

        return false;
    }
}
