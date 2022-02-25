package org.corfudb.compactor;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.proto.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.compactor.DistributedCompactorWithLock.EventStatus;

@Slf4j
public class DistributedCompactorEventListener implements StreamListener{
    private static final String COMPACTION_MANAGER_TABLE_NAME = "CompactionManager";
    private static final String COMPACTION_MANAGER_KEY = "CompactionManagerKey";

    @Override
    public void onNext(CorfuStreamEntries results) {
        log.info("onNext invoked with {}", results.getEntries().size());
        results.getEntries().forEach((schema, entries) -> {
            if (!schema.getTableName().equals(COMPACTION_MANAGER_TABLE_NAME)) {
                log.warn("Not the subscribed table {}", schema);
                return;
            }
            entries.forEach(entry -> {
                if (entry.getKey().equals(COMPACTION_MANAGER_KEY)) {
                    CheckpointingStatus compactionManagerStatus = (CheckpointingStatus) entry.getPayload();
                    if (compactionManagerStatus.getStatus() == CheckpointingStatus.StatusType.STARTED) {
                        try {
                            DistributedCompactorWithLock.input(EventStatus.START_CHECKPOINTING);
                            log.info("Added START_CHECKPOINTING event to the eventQueue");
                        } catch (InterruptedException e) {
                            log.warn("onNext: {}", e.getCause());
                        }
                    }
                }
            });
        });
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Subscriber hit error {}", throwable);
    }
}