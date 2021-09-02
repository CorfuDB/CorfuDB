package org.corfudb.infrastructure.remotecorfutable.loglistener;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.corfudb.infrastructure.remotecorfutable.loglistener.smr.SMROperation;
import org.corfudb.infrastructure.remotecorfutable.loglistener.smr.SMROperationFactory;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.concurrent.SingletonResource;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
public class RoundRobinListeningService implements RemoteCorfuTableListeningService {
    private final ScheduledExecutorService executor;
    private final ConcurrentMap<UUID, ListeningTaskFuture> trackingStreams = new ConcurrentHashMap<>();
    private final SingletonResource<CorfuRuntime> singletonRuntime;
    private final ConcurrentLinkedDeque<SMROperation> taskQueue = new ConcurrentLinkedDeque<>();
    private final long listeningDelay;

    @Override
    public void addStream(UUID streamId) {
        trackingStreams.computeIfAbsent(streamId, id -> {
            IStreamView stream = singletonRuntime.get().getStreamsView().get(id);
            ListeningTask task = new ListeningTask(streamId, stream);
            //TODO: configure delay from ServerContext
            ScheduledFuture<?> future = executor.scheduleWithFixedDelay(task,
                    0L,listeningDelay , TimeUnit.MILLISECONDS);
            return new ListeningTaskFuture(future, task);
        });
    }

    @AllArgsConstructor
    private class ListeningTaskFuture {
        @NonNull
        public final ScheduledFuture<?> future;
        @NonNull
        public final ListeningTask task;
    }

    @NotThreadSafe
    @RequiredArgsConstructor
    public class ListeningTask implements Runnable {
        @NonNull
        private final UUID streamId;
        @NonNull
        private final IStreamView stream;
        private long lastReadPos;
        private final PriorityBlockingQueue<TimestampedLock> accessWaits = new PriorityBlockingQueue<>();

        @Override
        public void run() {
            LogData data = (LogData) stream.next();
            if (data != null) {
                taskQueue.offer(SMROperationFactory.getSMROperation(data, streamId));
                long currTime = stream.getCurrentGlobalPosition();
                lastReadPos = currTime;
                TimestampedLock waitingReadRequests = accessWaits.poll();
                while (waitingReadRequests != null && waitingReadRequests.timestamp <= currTime) {
                    waitingReadRequests.lock.countDown();
                    waitingReadRequests = accessWaits.poll();
                }
                //replace request that did not meet condition
                if (waitingReadRequests != null) {
                    accessWaits.put(waitingReadRequests);
                }
            } else {
                LinkedList<TimestampedLock> allWaitingRequests = new LinkedList<>();
                accessWaits.drainTo(allWaitingRequests);
                allWaitingRequests.forEach(req -> req.lock.countDown());
            }
        }

        @EqualsAndHashCode
        @RequiredArgsConstructor
        private class TimestampedLock implements Comparable<TimestampedLock> {
            public final long timestamp;
            public final CountDownLatch lock = new CountDownLatch(1);

            @Override
            public int compareTo(@NonNull TimestampedLock o) {
                return Long.compareUnsigned(this.timestamp, o.timestamp);
            }
        }

        public CountDownLatch addReadRequest(long timestamp) {
            TimestampedLock tsLock = new TimestampedLock(timestamp);
            accessWaits.put(tsLock);
            return tsLock.lock;
        }
    }

    @Override
    public void removeStream(UUID streamId) {
        ListeningTaskFuture taskFuture = trackingStreams.remove(streamId);
        if (taskFuture != null) {
            taskFuture.future.cancel(false);
            LinkedList<ListeningTask.TimestampedLock> cleanup = new LinkedList<>();
            taskFuture.task.accessWaits.drainTo(cleanup);
            cleanup.forEach(tsLock -> tsLock.lock.countDown());
        }
    }

    @Override
    public SMROperation awaitEndOfStreamCheck(UUID streamId, long timestamp) throws InterruptedException {
        ListeningTaskFuture taskFuture = trackingStreams.get(streamId);
        if (taskFuture == null) {
            throw new IllegalArgumentException("Specified stream ID is not being tracked");
        }
        if (taskFuture.future.isCancelled()) {
            throw new IllegalStateException("Specified stream has been removed");
        }
        if (taskFuture.task.lastReadPos >= timestamp) {
            //we've already read the state at the timestamp, no reason to wait for log read
            return taskQueue.peekLast();
        }
        CountDownLatch streamPosLatch = taskFuture.task.addReadRequest(timestamp);
        streamPosLatch.await();
        if (taskFuture.future.isCancelled()) {
            throw new IllegalStateException("Specified stream has been removed");
        }
        return taskQueue.peekLast();
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
