package org.corfudb.infrastructure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.stream.StreamAddressSpace;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DynamicTriggerPolicy implements ICompactionTriggerPolicy{

    /**
     * This is the first trigger policy parameter. No new checkpointing will start
     * if less than this time has elapsed to prevent slow consumers from hitting TrimmedException.
     */
    private final Duration defaultMinTimeBetweenCompactionStarts = Duration.ofMinutes(5);
    private final Duration minTimeBetweenCompactionStarts = defaultMinTimeBetweenCompactionStarts;

    /**
     * This is the max amount of time between compaction cycles
     * If this time is hit, compaction will be triggered even if other policies
     * do not necessitate a compaction cycle.
     */
    private final Duration defaultMaxTimeBetweenCompactionStarts = Duration.ofMinutes(30);
    private final Duration maxTimeBetweenCompactionStarts = defaultMaxTimeBetweenCompactionStarts;

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
    public long getLastCheckpointStartTime() {
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
        log.info("DynamicTriggerPolicy: lastCompactionCycleStartTS={}. lastAddressSpaceSizeOnTrim={}",
                lastCompactionCycleStartTS, lastAddressSpaceSizeAfterTrim);
        final long currentTime = System.currentTimeMillis();
        final long timeSinceLastCycleMillis = currentTime - lastCompactionCycleStartTS;
        if (timeSinceLastCycleMillis > minTimeBetweenCompactionStarts.toMillis()) {
            if (lastAddressSpaceSizeAfterTrim == 0) { // no record of previous trim & safe trim period elapsed
                log.info("DynamicTriggerPolicy: Trigger as elapsedTime {} > safeTrimPeriod{}",
                        TimeUnit.MILLISECONDS.toMinutes(timeSinceLastCycleMillis),
                        minTimeBetweenCompactionStarts.toMinutes());
                return true;
            }
        }

        if (timeSinceLastCycleMillis > maxTimeBetweenCompactionStarts.toMillis()) {
            log.info("DynamicTriggerPolicy: Trigger as elapsedTime {} > maxTimeToChkpt{}",
                    TimeUnit.MILLISECONDS.toMinutes(timeSinceLastCycleMillis),
                    maxTimeBetweenCompactionStarts.toMinutes());
            return true;
        }
        return false;

        /**
        Long currentAddressSpaceSize = corfuRuntime.getAddressSpaceView().getLogTail()
                - corfuRuntime.getAddressSpaceView().getTrimMark().getSequence();
        boolean shouldTrigger = false;
        for (UUID streamId : sensitiveStreams) {
            StreamAddressSpace streamAddressSpace = corfuRuntime.getSequencerView()
                    .getStreamAddressSpace(new StreamAddressRange(streamId, Address.MAX, Address.NON_ADDRESS));

            Long previousSize = (!previousSensitiveStreamsSize.containsKey(streamId)) ? Long.MAX_VALUE :
                    previousSensitiveStreamsSize.get(streamId);
            if (streamAddressSpace.size() > previousSize) {
                shouldTrigger = true;
            }
        }

        if (currentAddressSpaceSize > previousAddressSpaceSize ||
                System.currentTimeMillis() - previousTriggerMillis >= interval) {
            shouldTrigger = true;
        }

        if (shouldTrigger) {
             setPreviousSensitiveStreamsSize();
             previousAddressSpaceSize = currentAddressSpaceSize;
             previousTriggerMillis = System.currentTimeMillis();
         }

        log.info("shouldTrigger: {}", shouldTrigger);
        return shouldTrigger;

         */
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
