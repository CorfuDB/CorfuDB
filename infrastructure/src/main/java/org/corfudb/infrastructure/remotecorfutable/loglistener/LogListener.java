package org.corfudb.infrastructure.remotecorfutable.loglistener;

import com.google.common.reflect.TypeToken;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.remotecorfutable.DatabaseHandler;
import org.corfudb.infrastructure.remotecorfutable.loglistener.smr.SMROperation;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.rocksdb.RocksDBException;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@RequiredArgsConstructor
public class LogListener implements AutoCloseable {
    private final CorfuRuntime runtime;
    private final DatabaseHandler handler;
    private final ScheduledExecutorService operationExecutor;
    private final int amountOfWorkers;
    private final long operationDelay;
    private final RemoteCorfuTableListeningService listener;

    @Getter(lazy = true)
    private final CorfuTable<UUID, UUID> tableRegistry = runtime.getObjectsView().build()
            .setTypeToken(new TypeToken<CorfuTable<UUID, UUID>>() {})
            .setStreamName("remotecorfutable.globalrctregistry")
            .open();
    //no reason for listening set to be concurrent, as only the update thread can update it
    private final Set<UUID> listening = new HashSet<>();
    private final Set<UUID> removalSet = new HashSet<>();

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
            } catch (RocksDBException e) {
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

    @Override
    public void close() throws Exception {
        operationExecutor.shutdownNow();
        getTableRegistry().close();
    }
}
