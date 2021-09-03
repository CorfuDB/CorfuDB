package org.corfudb.infrastructure.remotecorfutable.loglistener;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.remotecorfutable.loglistener.smr.SMROperation;
import org.corfudb.infrastructure.remotecorfutable.loglistener.smr.SMROperationFactory;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.Sleep;
import org.corfudb.util.concurrent.SingletonResource;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
@RequiredArgsConstructor
public class RoundRobinListeningService implements RemoteCorfuTableListeningService {
    private final ScheduledExecutorService executor;
    private final ConcurrentMap<UUID, ListeningTaskFuture> trackingStreams = new ConcurrentHashMap<>();
    private final SingletonResource<CorfuRuntime> singletonRuntime;
    private final ConcurrentMap<UUID, ConcurrentLinkedQueue<SMROperation>> taskQueue = new ConcurrentHashMap<>();
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

    public class ListeningTask implements Runnable {
        @NonNull
        private final UUID streamId;
        @NonNull
        private final IStreamView stream;

        private SMROperation lastReadOperation;
        private final PriorityBlockingQueue<TimestampedLock> accessWaits = new PriorityBlockingQueue<>();
        private final ConcurrentLinkedQueue<SMROperation> streamTaskQueue;

        public ListeningTask(@NonNull UUID streamId, @NonNull IStreamView stream) {
            this.streamId = streamId;
            this.stream = stream;
            streamTaskQueue = taskQueue.computeIfAbsent(streamId, id -> new ConcurrentLinkedQueue<>());
        }


        @Override
        public void run() {
            try {
                LogData data = (LogData) stream.next();
                if (data != null) {
                    final SMROperation currOp = SMROperationFactory.getSMROperation(data, streamId);
                    streamTaskQueue.offer(currOp);
                    log.trace("Added most recent read operation to task queue from {}", streamId);
                    long currTime = currOp.getTimestamp();
                    lastReadOperation = currOp;
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
                    log.trace("No entries to read in stream {}", streamId);
                    LinkedList<TimestampedLock> allWaitingRequests = new LinkedList<>();
                    accessWaits.drainTo(allWaitingRequests);
                    allWaitingRequests.forEach(req -> req.lock.countDown());
                }
            } catch (Exception e) {
                log.info("Listening service for stream {} failed with error {}", streamId, e.getMessage());
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
        //TODO: configure with server params
        int retries = 60;
        ListeningTaskFuture taskFuture = null;
        while (retries > 0) {
            taskFuture = trackingStreams.get(streamId);
            if (taskFuture != null) {
                break;
            }
            log.trace("Stream ID {} is not yet tracked", streamId);
            retries--;
            Sleep.sleepUninterruptibly(Duration.of(100, ChronoUnit.MILLIS));
        }
        if (taskFuture == null) {
            throw new RetryExhaustedException(
                    String.format("After %s retries, could not find a tracked stream %s", retries, streamId));
        }
        if (taskFuture.future.isCancelled()) {
            log.trace("Wait failed since future has been cancelled");
            throw new IllegalStateException("Specified stream has been removed");
        }
        SMROperation currLatest = taskFuture.task.lastReadOperation;
        if (currLatest != null && currLatest.getTimestamp() >= timestamp) {
            log.trace("Early return from wait since requested timestamp is already read");
            //we've already read the state at the timestamp, no reason to wait for log read
            return currLatest;
        }
        log.trace("Acquired countdown latch to wait on");
        CountDownLatch streamPosLatch = taskFuture.task.addReadRequest(timestamp);
        streamPosLatch.await();
        log.trace("Countdown latch unlocked");
        if (taskFuture.future.isCancelled()) {
            log.trace("Future cancelled during wait");
            throw new IllegalStateException("Specified stream has been removed");
        }
        log.trace("Returning last SMROp");
        return taskFuture.task.lastReadOperation;
    }

    @Override
    public SMROperation getTask(UUID streamId) {
        return taskQueue.computeIfAbsent(streamId, id -> new ConcurrentLinkedQueue<>()).poll();
    }

    @Override
    public void shutdown() {
        //in practice this shutdown will only be called during LogUnitServer shutdown
        //thus, there is no reason to wait for existing tasks to finish
        executor.shutdownNow();
    }
}
