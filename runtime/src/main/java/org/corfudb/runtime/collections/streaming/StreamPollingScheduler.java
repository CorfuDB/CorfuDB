package org.corfudb.runtime.collections.streaming;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.exceptions.StreamingException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.SequencerView;
import org.corfudb.runtime.view.stream.StreamAddressSpace;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * This class implements a scheduler that manages multiple streaming tasks {@link StreamingTask}. A streaming task
 * is simply a listener attached to a corfu stream and expects to receive ordered change notifications for certain
 * tables of interest. The StreamPollingScheduler manages the life cycle of streaming tasks, this constitutes four
 * major states, polling a stream, reading deltas, notifying the listeners of the changes and propagating errors.
 * Each stream is managed according to the diagram below:
 * <p>
 /*
 *                    │  Register
 *           ┌────────▼──────────┐
 *           │                   │◄───┐
 *    ┌─────►│     Runnable      │    │
 *    │      └─────────┬─────────┘    │
 *    │                │            No│Deltas
 *    │                │ Poll         │
 *    │                ▼              │
 *    │      ┌───────────────────┐    │
 * Synced    │    Scheduling     ├────┘
 *    │      └─────────┬─────────┘
 *    │                │Dispatch
 *    │                ▼
 *    │       ┌───────────────────┐
 *    │       │                   │ Produce
 *    └───────┤      Syncing      │◄────┐
 *            └────────┬──────────┴─────┘
 *                     │ Exception
 *                     ▼
 *            ┌───────────────────┐
 *            │      Error        │
 *            └───────────────────┘
 * <p>
 * On every tick the scheduler will poll/dispatch tasks only when needed, for example, when a listener is not
 * consuming the deltas fast enough the scheduler can decide to not poll or schedule extra sync tasks until
 * it can, this prevents superfluous polling and overwhelming the listener. Furthermore, syncing and listener
 * notification is executed on a shared thread pool in round robin fashion, therefore if a stream produces
 * more deltas relative to other streams that particular stream will have more CPU time than other tasks.
 */


@Slf4j
public class StreamPollingScheduler {

    /**
     * All registered Streaming tasks
     */
    private final Map<StreamListener, StreamingTask> allTasks = new ConcurrentHashMap<>();

    /**
     * A single threaded executor that runs the scheduler logic
     */
    private final ScheduledExecutorService scheduler;

    /**
     * The worker pool that the scheduler dispatches the sync tasks to
     */
    private final ExecutorService workers;

    /**
     * The minimum amount of available buffer space that a stream should have before it is polled
     */
    final int pollThreshold;

    /**
     * Number of stream tags to query in a single request
     */
    final int pollBatchSize;

    /**
     * The frequency of the scheduler
     */
    final Duration pollPeriod;

    private final SequencerView sequencerView;
    private final CorfuRuntime runtime;


    public StreamPollingScheduler(CorfuRuntime runtime, ScheduledExecutorService scheduler, ExecutorService workers,
                                  Duration pollPeriod, int pollBatchSize, int pollThreshold) {
        Preconditions.checkArgument(pollBatchSize > 1, "pollBatchSize=%s has to be > 1",
                pollBatchSize);
        Preconditions.checkArgument(pollThreshold > 1, "pollThreshold=%s has to be > 1",
                pollThreshold);
        Preconditions.checkArgument(pollPeriod.toMillis() > 1, "pollPeriod=%s has to be > 1ms",
                pollPeriod.toMillis());

        this.scheduler = scheduler;
        this.workers = workers;
        this.pollPeriod = pollPeriod;
        this.pollBatchSize = pollBatchSize;
        this.pollThreshold = pollThreshold;
        this.runtime = runtime;
        this.sequencerView = runtime.getSequencerView();
        tick();
    }

    public class Tick implements Runnable {
        @Override
        public void run() {
            try {
                schedule();
            } catch (InterruptedException ie) {
                log.warn("Scheduler thread has been interrupted!", ie);
                Thread.currentThread().interrupt();
            } catch (Throwable t) {
                log.error("Unexpected throwable!", t);
            }
        }
    }

    private void tick() {
        scheduler.submit(new Tick());
    }

    public void addTask(@Nonnull StreamListener streamListener, @Nonnull String namespace,
                        @Nonnull String streamTag, @Nonnull List<String> tablesOfInterest,
                        long lastAddress, int bufferSize) {

        Preconditions.checkArgument(bufferSize >= pollThreshold);
        synchronized (allTasks) {
            if (allTasks.containsKey(streamListener)) {
                // Multiple subscribers subscribing to same namespace and table is allowed
                // as long as the hashcode() and equals() method of the listeners are different.
                throw new StreamingException("StreamingManager::subscribe: listener already registered "
                        + streamListener, StreamingException.ExceptionCause.LISTENER_SUBSCRIBED);
            }
            StreamingTask task = new StreamingTask(runtime, workers, namespace, streamTag, streamListener,
                    tablesOfInterest, lastAddress, bufferSize);
            allTasks.put(streamListener, task);
            log.info("addTask: added {} {} {} address {}", streamListener, namespace, streamTag, lastAddress);
            allTasks.notifyAll();
        }
    }

    public void addLRTask(@Nonnull StreamListener streamListener,
                        @Nonnull Map<String, String> nsToStreamTags,
                        @Nonnull Map<String, List<String>> nsToTables, long lastAddress,
                        int bufferSize) {
        Preconditions.checkArgument(bufferSize >= pollThreshold);
        synchronized (allTasks) {
            if (allTasks.containsKey(streamListener)) {
                // Multiple subscribers subscribing to same namespace and table is allowed
                // as long as the hashcode() and equals() method of the listeners are different.
                throw new StreamingException(
                        "StreamingManager::subscribe: listener already registered " + streamListener);
            }
            StreamingTask task = new LRStreamingTask(runtime, workers, nsToStreamTags, nsToTables, streamListener,
                    lastAddress, bufferSize);
            allTasks.put(streamListener, task);
            log.info("addTask: added {} for {} address {}", streamListener, nsToStreamTags, lastAddress);
            allTasks.notifyAll();
        }
    }

    public void removeTask(@Nonnull StreamListener streamListener) {
        synchronized (allTasks) {
            allTasks.remove(streamListener);
            allTasks.notifyAll();
        }
    }

    /**
     * This method is called by the scheduler thread to block until at least one thread is registered.
     */
    private void waitForTasks() throws InterruptedException {
        synchronized (allTasks) {
            while (allTasks.isEmpty()) {
                allTasks.wait();
            }
        }
    }

    private List<StreamingTask> getTasks(StreamStatus status, Predicate<StreamingTask> predicate) {
        return allTasks.values().stream()
                .filter(predicate.and(task -> task.getStatus() == status))
                .collect(Collectors.toList());
    }

    private List<StreamingTask> getTasks(StreamStatus status) {
        return getTasks(status, t -> true);
    }

    private List<StreamAddressRange> getPollQueries(List<StreamingTask> tasks) {
        List<StreamAddressRange> pollRequests = new ArrayList<>(tasks.size());
        for (StreamingTask task : tasks) {
            DeltaStream deltaStream = task.getStream();
            if (task instanceof LRStreamingTask) {
                List<UUID> streamsTracked = ((LRDeltaStream)deltaStream).getStreamsTracked();
                streamsTracked.forEach(streamTracked -> pollRequests.add(new StreamAddressRange(streamTracked,
                        Address.MAX, deltaStream.getMaxAddressSeen())));
            } else {
                pollRequests.add(new StreamAddressRange(deltaStream.getStreamId(), Address.MAX,
                        deltaStream.getMaxAddressSeen()));
            }
        }
        return pollRequests;
    }

    /**
     * The poll operation tries to discover newly written addresses for DeltaStreams. This method will
     * generate a query for each DeltaStream, coalesce similar stream requests and batch the query requests
     * to the sequencer. It will then transfer all discovered addresses to the corresponding DeltaStreams
     * via refresh, which adds the new addresses to the internal read buffers.
     *
     * @param tasks the list of tasks to poll
     */
    private void poll(List<StreamingTask> tasks) {

        List<StreamAddressRange> queries = getPollQueries(tasks);

        // In order to increase the efficiency of batching, we need to coalesce all queries before batching, that
        // way each batch will have unique queries. If we coalesce after batching, then its possible to have common
        // stream queries across batches, this means that the same stream is queried multiple times, but it could
        // have only been polled once.

        List<StreamAddressRange> coalesced = coalesce(queries);
        List<List<StreamAddressRange>> batches = Lists.partition(coalesced, pollBatchSize);

        Map<UUID, StreamAddressSpace> allQueryResults = new HashMap<>(coalesced.size());

        for (List<StreamAddressRange> batch : batches) {
            long pollStartTime = System.nanoTime();
            Map<UUID, StreamAddressSpace> res = sequencerView.getStreamsAddressSpace(batch);
            MicroMeterUtils.time(Duration.ofNanos(System.nanoTime() - pollStartTime), "StreamPollingScheduler.poll");
            Preconditions.checkState(res.size() == batch.size());
            allQueryResults.putAll(res);
        }

        validateQuerySize(tasks, queries);

        int queryIdx = 0;
        for (StreamingTask task : tasks) {
            StreamAddressSpace sas;
            try {
                if (task instanceof LRStreamingTask) {
                    sas = getMergedAddressSpace((LRStreamingTask)task, queries, allQueryResults, queryIdx);
                    // LRDeltaStream(used in LRStreamingTask) tracks 2 streams.  The list of streamAddressRangeQueries is a
                    // flattened list of all streams across all tasks.  For LRDeltaStream, the mapping of task -> query will not
                    // be 1:1.  So queryIdx is incremented by the number of streams tracked
                    queryIdx += ((LRDeltaStream)task.getStream()).getStreamsTracked().size();
                } else {
                    StreamAddressRange taskQuery = queries.get(queryIdx);
                    Preconditions.checkState(task.getStream().getStreamId().equals(taskQuery.getStreamID()));
                    Preconditions.checkState(allQueryResults.containsKey(taskQuery.getStreamID()),
                        "StreamAddressSpace missing for %s", task.getStream().getStreamId());
                    sas = allQueryResults.get(task.getStream().getStreamId()).getAddressesInRange(taskQuery);
                    queryIdx++;
                }
                task.getStream().refresh(sas);
            } catch (Throwable throwable) {
                task.setError(throwable);
                log.error("StreamingPollingScheduler: encountered exception {} during streaming task scheduling. " +
                    "Notify stream listener {} with id={} onError.", throwable, task.getListener(), task.getListenerId());
            }
        }
    }

    private void validateQuerySize(List<StreamingTask> tasks, List<StreamAddressRange> addressRangeQueries) {
        int numLRStreamingTasks = 0;
        for (StreamingTask task : tasks) {
            if (task instanceof LRStreamingTask) {
                numLRStreamingTasks++;
            }
        }

        int numRemainingTasks = tasks.size() - numLRStreamingTasks;

        // LRDeltaStream(used in LRStreamingTask) tracks 2 streams for which the stream address space is queried.  So
        // the number of queries should be numLRStreamingTasks*2 + numRemainingStreamingTasks
        int numExpectedQueries = numLRStreamingTasks*2 + numRemainingTasks;
        Preconditions.checkState(numExpectedQueries == addressRangeQueries.size());
    }

    /**
     * Return a merged address space of the streams tracked by this LRStreamingTask
     */
    private StreamAddressSpace getMergedAddressSpace(LRStreamingTask task, List<StreamAddressRange> queries,
                                                     Map<UUID, StreamAddressSpace> allQueryResults, int queryIdx) {

        int idx = queryIdx;
        List<StreamAddressSpace> streamAddressSpaces = new ArrayList<>();

        for (UUID stream : ((LRDeltaStream)task.getStream()).getStreamsTracked()) {
            StreamAddressRange taskQuery = queries.get(idx);
            Preconditions.checkState(stream.equals(taskQuery.getStreamID()));
            Preconditions.checkState(allQueryResults.containsKey(stream),
                    "StreamAddressSpace missing for %s", task.getStream().getStreamId());
            streamAddressSpaces.add(allQueryResults.get(stream).getAddressesInRange(taskQuery));
            idx++;
        }

        StreamAddressSpace mergedStreamAddressSpace = streamAddressSpaces.get(0);
        for (int i = 1; i < streamAddressSpaces.size(); i++) {
            mergedStreamAddressSpace = StreamAddressSpace.merge(mergedStreamAddressSpace,
                streamAddressSpaces.get(i));
        }
        return mergedStreamAddressSpace;
    }

    @Data
    private static class Interval {
        private final long x;
        private final long y;

        static Interval merge(Interval interval, StreamAddressRange range) {
            long newMinX = Math.min(interval.x, range.getEnd());
            long newMaxY = Math.max(interval.y, range.getStart());
            return new Interval(newMinX, newMaxY);
        }
    }

    /**
     * It's possible that multiple streaming tasks need to query different/overlapping ranges
     * of the same stream. In order to prevent issuing multiple queries for the same stream,
     * all range queries for the same stream are coalesced into a single request thereby requiring
     * less sequencer queries to discover new writes. For example, the following queries will all
     * be merged into one query (i.e., (-1, 100])
     * Query1  (-1, 100)
     * Query2  (2, 5)
     * Query3  (3, 10)
     *
     */
    private List<StreamAddressRange> coalesce(List<StreamAddressRange> ranges) {
        Map<UUID, Interval> adjusted = new HashMap<>();

        for (StreamAddressRange range : ranges) {
            Interval interval = adjusted.getOrDefault(range.getStreamID(), new Interval(Long.MAX_VALUE, Long.MIN_VALUE));
            Interval expandedInterval = Interval.merge(interval, range);
            Preconditions.checkState(expandedInterval.x >= Address.NEVER_READ);
            Preconditions.checkState(expandedInterval.x < expandedInterval.y);

            adjusted.put(range.getStreamID(), expandedInterval);
        }

        List<StreamAddressRange> adjustedList = new ArrayList<>();
        for (Map.Entry<UUID, Interval> entry : adjusted.entrySet()) {
            adjustedList.add(new StreamAddressRange(entry.getKey(), entry.getValue().y, entry.getValue().x));
        }
        return adjustedList;
    }

    private void dispatchSyncTasks(List<StreamingTask> tasks) {
        for (StreamingTask task : tasks) {
            if (task.getStream().hasNext()) {
                // this will fail for tasks that are already syncing
                // for tasks to be scheduled to sync, they must be in the scheduling state
                task.move(StreamStatus.SCHEDULING, StreamStatus.SYNCING);
                workers.execute(task);
            } else {
                // no new deltas to sync
                // This stream has no entries to sync after the poll, set it to runnable so that it can
                // be polled next scheduling cycle
                task.move(StreamStatus.SCHEDULING, StreamStatus.RUNNABLE);
            }
        }
    }

    private void handleFailures(List<StreamingTask> failedTasks) {
        // Need to remove the tasks from allTasks before propagating the error just incase they try to
        // re-register the task?
        failedTasks.forEach(t -> allTasks.remove(t.getListener()));

        for (StreamingTask task : failedTasks) {
            workers.execute(task::propagateError);
        }
    }

    /**
     * This is the main scheduling logic. The scheduler runs on a single thread and makes scheduling
     * decisions periodically. If there are no threads it will just block. The scheduler mainly does three things,
     * it discovers new deltas by querying the sequencer and passes them to the DeltaStreams, it schedules
     * StreamingTasks to consume new deltas and propagates errors to listeners and unregisters them (i.e., remove
     * them from the scheduler).
     */
    public void schedule() throws InterruptedException {
        long cycleStartTimeNs = 0L;

        try {
            // Block thread until there's work to do
            waitForTasks();

            cycleStartTimeNs = System.nanoTime();

            final List<StreamingTask> runnableTasks = getTasks(StreamStatus.RUNNABLE);
            final List<StreamingTask> syncingTasks = getTasks(StreamStatus.SYNCING,
                    t -> t.getStream().availableSpace() >= pollThreshold);
            final List<StreamingTask> failedTasks = getTasks(StreamStatus.ERROR);

            runnableTasks.forEach(t -> {
                try {
                    t.move(StreamStatus.RUNNABLE, StreamStatus.SCHEDULING);
                } catch (Exception e) {
                    t.setError(e);
                    log.error("StreamingPollingScheduler: encountered exception {} while moving to 'scheduling' status, " +
                            "listener={} with id={} onError()", e, t.getListener(), t.getListenerId());
                }
            });
            poll(runnableTasks);
            dispatchSyncTasks(runnableTasks);

            // Since these tasks are already syncing, they shouldn't be dispatched again
            poll(syncingTasks);

            // handle tasks that have failed (i.e., remove them and propagate the errors to the listeners)
            handleFailures(failedTasks);
        } catch (InterruptedException ie) {
            throw ie;
        } catch (Exception e) {
            // In the event that scheduling fails due to an exception not related to a specific task
            // we'll error all tasks to give it the opportunity to recover
            log.error("StreamPollingScheduler: unrecoverable error, fail all tasks, so listeners can recover.", e);
            getTasks(StreamStatus.RUNNABLE).forEach(t -> t.setError(e));
            getTasks(StreamStatus.SCHEDULING).forEach(t -> t.setError(e));
            getTasks(StreamStatus.SYNCING).forEach(t -> t.setError(e));
            handleFailures(getTasks(StreamStatus.ERROR));
        } finally {
            long elapsedTimeNs = cycleStartTimeNs != 0L ? (System.nanoTime() - cycleStartTimeNs) : 0L;
            MicroMeterUtils.time(Duration.ofNanos(elapsedTimeNs), "schedule.cycle");
            long nextSchedule = Math.max(0, pollPeriod.toNanos() - elapsedTimeNs);
            Preconditions.checkState(nextSchedule <= pollPeriod.toNanos());
            scheduler.schedule(this::tick, nextSchedule, TimeUnit.NANOSECONDS);
        }
    }

    public void shutdown() {
        this.allTasks.clear();
        this.scheduler.shutdown();
        this.workers.shutdown();
    }
}
