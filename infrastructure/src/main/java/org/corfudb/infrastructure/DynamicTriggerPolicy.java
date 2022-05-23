package org.corfudb.infrastructure;

import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.DistributedCompactor;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.proto.RpcCommon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

public class DynamicTriggerPolicy implements ICompactionTriggerPolicy {
    /**
     * What time did the previous cycle start
     */
    @Getter
    private long lastCompactionCycleStartTS = 0;
    private long lastAddressSpaceSizeAfterTrim = 0;

    private CorfuRuntime corfuRuntime;
    private Logger syslog;

    public DynamicTriggerPolicy() {
        lastCompactionCycleStartTS = System.currentTimeMillis();
        lastAddressSpaceSizeAfterTrim = 0;
        syslog = LoggerFactory.getLogger("SYSLOG");
    }

    @Override
    public void markCompactionCycleStart() {
        this.lastCompactionCycleStartTS = System.currentTimeMillis();
    }

    @Override
    public long getLastCompactionCycleStartTime() {
        return this.lastCompactionCycleStartTS;
    }

    @Override
    public void markTrimComplete() {
        this.lastAddressSpaceSizeAfterTrim = getCurrentAddressSpaceSize();
    }

    private long getCurrentAddressSpaceSize() {
        long currentAddressSpaceSize = 0;
        final int maxRetries = 10;
        for (int retry = 0; retry < maxRetries; retry++) {
            try {
                currentAddressSpaceSize = corfuRuntime.getAddressSpaceView().getLogTail()
                        - corfuRuntime.getAddressSpaceView().getTrimMark().getSequence();
            } catch (Exception e) {
                syslog.warn("getCurrentAddressSpaceSize hit exception {}. StackTrace {}", e, e.getStackTrace());
            }
        }
        return currentAddressSpaceSize;
    }

    private boolean shouldForceTrigger() {
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            RpcCommon.TokenMsg upgradeToken = (RpcCommon.TokenMsg) txn.getRecord(DistributedCompactor.CHECKPOINT,
                    DistributedCompactor.UPGRADE_KEY).getPayload();
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
     * @param interval - ignored for now
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
        if (timeSinceLastCycleMillis > interval && lastAddressSpaceSizeAfterTrim == 0) {
            // no record of previous trim & safe trim period elapsed when lastAddressSpaceSizeAfterTrim is 0
            syslog.info("DynamicTriggerPolicy: Trigger as elapsedTime {} > safeTrimPeriod {}",
                    TimeUnit.MILLISECONDS.toSeconds(timeSinceLastCycleMillis),
                    TimeUnit.MILLISECONDS.toSeconds(interval));
            return true;
        }

        if (timeSinceLastCycleMillis > interval * 2) {
            syslog.info("DynamicTriggerPolicy: Trigger as elapsedTime {} > maxTimeToChkpt {}",
                    TimeUnit.MILLISECONDS.toMinutes(timeSinceLastCycleMillis),
                    TimeUnit.MILLISECONDS.toMinutes(interval * 2));
            return true;
        }

        return false;
    }

    @Override
    public void setCorfuRuntime(CorfuRuntime corfuRuntime) {
        this.corfuRuntime = corfuRuntime;
    }
}
