package org.corfudb.infrastructure.remotecorfutable.loglistener;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.corfudb.infrastructure.remotecorfutable.loglistener.smr.SMROperation;
import org.corfudb.infrastructure.remotecorfutable.loglistener.smr.SMROperationFactory;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.stream.IStreamView;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
public class RoundRobinListeningService extends RemoteCorfuTableListeningService {
    private final ExecutorService executor;
    private final ConcurrentMap<UUID, IStreamView> trackingStreams = new ConcurrentHashMap<>();
    private final CorfuRuntime runtime;

    @Override
    public void addStream(UUID streamId) {
        IStreamView prev = trackingStreams.putIfAbsent(streamId, runtime.getStreamsView().get(streamId));
        if (prev == null) {
            executor.submit(new ReadStreamTask(streamId));
        }
    }

    @Override
    public void removeStream(UUID streamId) {
        trackingStreams.remove(streamId);
    }

    @Override
    public void shutdown() {
        //in practice this shutdown will only be called during LogUnitServer shutdown
        //thus, there is no reason to wait for existing tasks to finish
        executor.shutdownNow();
    }

    @AllArgsConstructor
    private class ReadStreamTask implements Runnable {
        private final UUID streamId;
        @Override
        public void run() {
            IStreamView streamToRead = trackingStreams.get(streamId);
            if (streamToRead != null) {
                LogData data = (LogData) streamToRead.next();
                if (data != null) {
                    SMROperation dbOp = SMROperationFactory.getSMROperation(data, streamId);
                    setChanged();
                    notifyObservers(dbOp);
                }
                executor.submit(new ReadStreamTask(streamId));
            }
        }
    }
}
