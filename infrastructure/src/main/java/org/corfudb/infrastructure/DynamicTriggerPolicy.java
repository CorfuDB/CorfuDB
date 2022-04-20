package org.corfudb.infrastructure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.stream.StreamAddressSpace;

import java.util.*;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DynamicTriggerPolicy implements ICompactionTriggerPolicy{
    /**
     * What time did the previous cycle start
     */
    @Getter
    private long lastCompactionCycleStartTS = 0;
    private long lastAddressSpaceSizeAfterTrim = 0;

    private final List<UUID> sensitiveStreams = new ArrayList<>();
    private Map<UUID, Long> previousSensitiveStreamsSize = new HashMap<>();
    private Long previousAddressSpaceSize = Long.MAX_VALUE;
    private Long previousTriggerMillis = Long.valueOf(0);
    private CorfuRuntime corfuRuntime;

    public DynamicTriggerPolicy() {
        lastCompactionCycleStartTS = System.currentTimeMillis();
        lastAddressSpaceSizeAfterTrim = 0;
    }

    public void setSensitiveStreams(List<TableName> sensitiveTables) {
        for (TableName table : sensitiveTables) {
            sensitiveStreams.add(CorfuRuntime.getStreamID(
                    TableRegistry.getFullyQualifiedTableName(table.getNamespace(), table.getTableName())));
        }
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
                log.warn("getCurrentAddressSpaceSize hit exception {}. StackTrace {}", e, e.getStackTrace());
            }
        }
        return currentAddressSpaceSize;
    }

    /**
     * 1. if ((currentTime - lastCompactionCycleStart) > minTimeBetweenCompactionStarts)
     *        if (lastAddressSpaceSizeOnTrim == 0)
     *            return true // no record of previous trim & safe trim period elapsed
     *            ... once trim happens, we record the lastAddressSpaceSizeOnTrim
     *            ... once checkpoint starts we record the lastCompactionCycleStartTS
     * 2. if ((currentTime - lastCompactionCycleStart) > maxTimeBetweenCompactionStarts)
     *
     * @param interval - ignored for now
     * @return true if compaction cycle should run, false otherwise
     */
    @Override
    public boolean shouldTrigger(long interval) {
        final long currentTime = System.currentTimeMillis();
        final long timeSinceLastCycleMillis = currentTime - lastCompactionCycleStartTS;
        if (timeSinceLastCycleMillis > interval) {
            if (lastAddressSpaceSizeAfterTrim == 0) { // no record of previous trim & safe trim period elapsed
                log.info("DynamicTriggerPolicy: Trigger as elapsedTime {} > safeTrimPeriod{}",
                        TimeUnit.MILLISECONDS.toSeconds(timeSinceLastCycleMillis),
                        TimeUnit.MILLISECONDS.toSeconds(interval));
                return true;
            }
        }

        if (timeSinceLastCycleMillis > interval*2) {
            log.info("DynamicTriggerPolicy: Trigger as elapsedTime {} > maxTimeToChkpt{}",
                    TimeUnit.MILLISECONDS.toSeconds(timeSinceLastCycleMillis),
                    TimeUnit.MILLISECONDS.toSeconds(interval*2));
            return true;
        }
        return false;
    }

    @Override
    public void setCorfuRuntime(CorfuRuntime corfuRuntime) {
        this.corfuRuntime = corfuRuntime;
    }

    private void setPreviousSensitiveStreamsSize() {
        for (UUID streamId : sensitiveStreams) {
            StreamAddressSpace streamAddressSpace = corfuRuntime.getSequencerView()
                    .getStreamAddressSpace(new StreamAddressRange(streamId, Address.MAX, Address.NON_ADDRESS));
            previousSensitiveStreamsSize.put(streamId, streamAddressSpace.size());
        }
    }
}
