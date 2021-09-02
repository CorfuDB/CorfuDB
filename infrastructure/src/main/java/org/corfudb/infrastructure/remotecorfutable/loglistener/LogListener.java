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
import org.rocksdb.RocksDBException;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
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
    private final int amountOfWorkers;
    private final long operationDelay;
    private final RemoteCorfuTableListeningService listener;

    @Getter(lazy = true)
    private final CorfuTable<UUID, UUID> tableRegistry = singletonRuntime.get().getObjectsView().build()
            .setTypeToken(new TypeToken<CorfuTable<UUID, UUID>>() {})
            .setStreamName("remotecorfutable.globalrctregistry")
            .open();
    //no reason for listening set to be concurrent, as only the update thread can update it
    private final Set<UUID> listening = new HashSet<>();
    private final Set<UUID> removalSet = new HashSet<>();

    /**
     * Begins the listener scheduled executor.
     */
    public void startListening() {
        operationExecutor.scheduleWithFixedDelay(this::updateStreamsListening, 0, operationDelay, TimeUnit.MILLISECONDS);
        for (int i = 0; i < amountOfWorkers; i++) {
            operationExecutor.scheduleWithFixedDelay(this::applyNextOperation, 0, operationDelay, TimeUnit.MILLISECONDS);
        }
    }

    private void applyNextOperation() {
        SMROperation op = listener.getTask();
        if (op != null) {
            try {
                op.applySMRMethod(handler);
            } catch (Exception e) {
                log.error("Error applying log update to database", e);
                listener.removeStream(op.getStreamId());
                listening.remove(op.getStreamId());
            }
        }
    }

    private void updateStreamsListening() {
        removalSet.clear();
        Set<UUID> currentView = getTableRegistry().keySet();
        for (UUID stream : listening) {
            if (!currentView.contains(stream)) {
                removalSet.add(stream);
                listener.removeStream(stream);
            }
        }
        listening.removeAll(removalSet);
        for (UUID stream: currentView) {
            if (!listening.contains(stream)) {
                listening.add(stream);
                listener.addStream(stream);
            }
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
        SMROperation op = listener.awaitEndOfStreamCheck(streamID, timestamp);
        //if op is null we are already at a consistent state
        if (op != null) {
            //wait until the operation is applied to be consistent with the request
            op.waitUntilApply();
        }
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
