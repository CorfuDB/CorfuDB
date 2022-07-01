package org.corfudb.infrastructure;

import lombok.Getter;
import org.corfudb.runtime.CompactorMetadataTables;
import org.corfudb.runtime.CorfuRuntime;
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
    @Getter
    private long lastCompactionCycleStartTS = 0;

    private final  CorfuRuntime corfuRuntime;
    private final Logger syslog;

    public DynamicTriggerPolicy(CorfuRuntime corfuRuntime) {
        this.corfuRuntime = corfuRuntime;
        this.lastCompactionCycleStartTS = System.currentTimeMillis();
        this.syslog = LoggerFactory.getLogger("syslog");
    }

    @Override
    public void markCompactionCycleStart() {
        this.lastCompactionCycleStartTS = System.currentTimeMillis();
    }

    private boolean shouldForceTrigger() {
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            RpcCommon.TokenMsg upgradeToken = (RpcCommon.TokenMsg) txn.getRecord(CompactorMetadataTables.CHECKPOINT,
                    CompactorMetadataTables.UPGRADE_KEY).getPayload();
            txn.commit();
            if (upgradeToken != null) {
                return true;
            }
        }
        return false;
    }

    /**
     * 1. if ((currentTime - lastCompactionCycleStart) > minTimeBetweenCompactionStarts)
     * if (lastAddressSpaceSizeOnTrim == 0)
     * return true // no record of previous trim & safe trim period elapsed
     * ... once trim happens, we record the lastAddressSpaceSizeOnTrim
     * ... once checkpoint starts we record the lastCompactionCycleStartTS
     * 2. if ((currentTime - lastCompactionCycleStart) > maxTimeBetweenCompactionStarts)
     *
     * @param interval - trigger interval in ms
     * @return true if compaction cycle should run, false otherwise
     */
    @Override
    public boolean shouldTrigger(long interval) {

        if (shouldForceTrigger()) {
            syslog.info("Force triggering compaction");
            return true;
        }

        final long currentTime = System.currentTimeMillis();
        final long timeSinceLastCycleMillis = currentTime - lastCompactionCycleStartTS;

        if (timeSinceLastCycleMillis > interval * 2) {
            syslog.info("DynamicTriggerPolicy: Trigger as elapsedTime {} > maxTimeToChkpt {}",
                    TimeUnit.MILLISECONDS.toMinutes(timeSinceLastCycleMillis),
                    TimeUnit.MILLISECONDS.toMinutes(interval * 2));
            return true;
        }

        if (timeSinceLastCycleMillis > interval) {
            syslog.info("DynamicTriggerPolicy: Trigger as elapsedTime {} > safeTrimPeriod {}",
                    TimeUnit.MILLISECONDS.toSeconds(timeSinceLastCycleMillis),
                    TimeUnit.MILLISECONDS.toSeconds(interval));
            return true;
        }

        return false;
    }
}
