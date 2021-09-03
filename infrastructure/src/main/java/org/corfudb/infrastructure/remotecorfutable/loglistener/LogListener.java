package org.corfudb.infrastructure.remotecorfutable.loglistener;

import com.google.common.reflect.TypeToken;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.remotecorfutable.DatabaseHandler;
import org.corfudb.infrastructure.remotecorfutable.loglistener.smr.SMROperation;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.util.concurrent.SingletonResource;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * This class reads updates from the Corfu Log and applies the updates to the RCT database.
 *
 * Created by nvaishampayan517 on 08/30/21
 */
@Slf4j
@RequiredArgsConstructor
public class LogListener implements AutoCloseable {
    private final SingletonResource<CorfuRuntime> singletonRuntime;
    private final DatabaseHandler handler;
    private final ScheduledExecutorService operationExecutor;
    private final long operationDelay;
    private final RemoteCorfuTableListeningService listener;

    @Getter(lazy = true)
    private final CorfuTable<UUID, UUID> tableRegistry = singletonRuntime.get().getObjectsView().build()
            .setTypeToken(new TypeToken<CorfuTable<UUID, UUID>>() {})
            .setStreamName("remotecorfutable.globalrctregistry")
            .open();
    private final ConcurrentMap<UUID, ScheduledFuture<?>> listening = new ConcurrentHashMap<>();
    private final Set<Map.Entry<UUID, ScheduledFuture<?>>> removalSet = new HashSet<>();

    /**
     * Begins the listener scheduled executor.
     */
    public void startListening() {
        operationExecutor.scheduleWithFixedDelay(this::updateStreamsListening, 0, operationDelay, TimeUnit.MILLISECONDS);
    }

    private void applyNextOperation(UUID streamId) {
        SMROperation op = listener.getTask(streamId);
        if (op != null) {
            try {
                op.applySMRMethod(handler);
            } catch (Exception e) {
                log.error("Error applying log update to database", e);
                listener.removeStream(streamId);
                listening.remove(streamId);
            }
        }
    }

    private void updateStreamsListening() {
        removalSet.clear();
        Set<UUID> currentView = getTableRegistry().keySet();
        for (Map.Entry<UUID, ScheduledFuture<?>> entry : listening.entrySet()) {
            if (!currentView.contains(entry.getKey())) {
                log.trace("Removing stream {} from listener", entry.getKey());
                removalSet.add(entry);
                listener.removeStream(entry.getKey());
                listening.get(entry.getKey()).cancel(true);
            }
        }
        listening.entrySet().removeAll(removalSet);
        for (UUID stream: currentView) {
            listening.computeIfAbsent(stream, id -> {
                log.trace("Adding stream {} to listener", id);
                ScheduledFuture<?> streamFuture = operationExecutor.scheduleWithFixedDelay(
                        () -> applyNextOperation(id), 0, operationDelay, TimeUnit.MILLISECONDS);
                listener.addStream(id);
                return streamFuture;
            });
        }
    }

    /**
     * This method will allow readers to wait until the Log Listener has applied
     * entries at the desired timestamp.
     * @param streamID Requested stream ID.
     * @param timestamp Timestamp to wait until.
     * @throws InterruptedException Error in waiting.
     */
    public void waitForStream(UUID streamID, long timestamp) throws InterruptedException {
        log.trace("Awaiting listener end of stream check");
        SMROperation op = listener.awaitEndOfStreamCheck(streamID, timestamp);
        //if op is null we are already at a consistent state
        if (op != null) {
            log.trace("Beginning wait on SMR op");
            //wait until the operation is applied to be consistent with the request
            op.waitUntilApply();
        }
        log.trace("End of stream check resulted in already consistent state");
    }

    /**
     * Shuts down the listener.
     * @throws Exception An error in shutting down the service.
     */
    @Override
    public void close() throws Exception {
        operationExecutor.shutdownNow();
        getTableRegistry().close();
    }
}
