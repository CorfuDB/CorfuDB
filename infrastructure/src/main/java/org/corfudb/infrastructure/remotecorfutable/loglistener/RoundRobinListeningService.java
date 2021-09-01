package org.corfudb.infrastructure.remotecorfutable.loglistener;

import lombok.RequiredArgsConstructor;
import org.corfudb.infrastructure.remotecorfutable.loglistener.smr.SMROperation;
import org.corfudb.infrastructure.remotecorfutable.loglistener.smr.SMROperationFactory;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.concurrent.SingletonResource;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
public class RoundRobinListeningService implements RemoteCorfuTableListeningService {
    private final ScheduledExecutorService executor;
    private final ConcurrentMap<UUID, ScheduledFuture<?>> trackingStreams = new ConcurrentHashMap<>();
    private final SingletonResource<CorfuRuntime> singletonRuntime;
    private final ConcurrentLinkedQueue<SMROperation> taskQueue = new ConcurrentLinkedQueue<>();
    private final long listeningDelay;

    @Override
    public void addStream(UUID streamId) {
        trackingStreams.computeIfAbsent(streamId, id -> {
            IStreamView stream = singletonRuntime.get().getStreamsView().get(id);
            return executor.scheduleWithFixedDelay(() -> {
                LogData data = (LogData) stream.next();
                if (data != null) {
                    taskQueue.offer(SMROperationFactory.getSMROperation(data, streamId));
                }
                //TODO: configure delay from ServerContext
            }, 0L,listeningDelay , TimeUnit.MILLISECONDS);
        });
    }

    @Override
    public void removeStream(UUID streamId) {
        ScheduledFuture<?> scheduledTasks = trackingStreams.remove(streamId);
        scheduledTasks.cancel(false);
    }

    @Override
    public SMROperation getTask() {
        return taskQueue.poll();
    }

    @Override
    public void shutdown() {
        //in practice this shutdown will only be called during LogUnitServer shutdown
        //thus, there is no reason to wait for existing tasks to finish
        executor.shutdownNow();
    }
}
