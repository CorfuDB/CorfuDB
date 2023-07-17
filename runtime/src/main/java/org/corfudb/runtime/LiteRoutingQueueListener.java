package org.corfudb.runtime;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.TableOptions;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.corfudb.runtime.LogReplicationUtils.REPLICATED_QUEUE_TAG;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public abstract class LiteRoutingQueueListener implements StreamListener {

    private final CorfuStore corfuStore;


    public LiteRoutingQueueListener(CorfuStore corfuStore) {
        this.corfuStore = corfuStore;
        try {
            corfuStore.openQueue(CORFU_SYSTEM_NAMESPACE, LogReplicationUtils.REPLICATED_QUEUE_NAME,
                    Queue.RoutingTableEntryMsg.class,
                    TableOptions.builder().schemaOptions(
                                    CorfuOptions.SchemaOptions.newBuilder()
                                            .addStreamTag(LogReplicationUtils.REPLICATED_QUEUE_TAG)
                                            .build())
                            .build());
        } catch (Exception e) {
            log.error("Failed to open replicated queue due to exception!", e);
        }
    }

    @Override
    public void onNext(CorfuStreamEntries results) {
        log.info("LR LiteRoutingQueueListener received updates!");
        List<CorfuStreamEntry> entries = results.getEntries().entrySet().stream()
                .map(Map.Entry::getValue).findFirst().get();
        for (CorfuStreamEntry entry : entries) {
            if (entry.getOperation().equals(CorfuStreamEntry.OperationType.CLEAR)) {
                log.warn("CLEAR_ENTRY received, snapshot sync is ongoing, skip this entry");
                continue;
            }
            Queue.RoutingTableEntryMsg msg = (Queue.RoutingTableEntryMsg) entry.getPayload();
            if (msg.getReplicationType()
                    .equals(Queue.ReplicationType.LOG_ENTRY_SYNC)) {
                log.info("Process log entry sync msg: {}", msg);
                processUpdatesInLogEntrySync(Collections.singletonList(msg));
            } else {
                log.info("Process snapshot sync msg: {}", msg);
                processUpdatesInSnapshotSync(Collections.singletonList(msg));
            }
        }
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("onError:: resubscribing LiteRoutingQueueListener ", throwable);
        corfuStore.subscribeListenerFromTrimMark(this, CORFU_SYSTEM_NAMESPACE, REPLICATED_QUEUE_TAG);
    }

    protected abstract boolean processUpdatesInSnapshotSync(List<Queue.RoutingTableEntryMsg> updates);

    protected abstract boolean processUpdatesInLogEntrySync(List<Queue.RoutingTableEntryMsg> updates);
}
