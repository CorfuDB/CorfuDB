package org.corfudb.generator.replayer;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.replayer.replayOperations.Configuration;
import org.corfudb.generator.replayer.replayOperations.OperationUtils;
import org.corfudb.runtime.CorfuRuntime;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Replayer replays the events in files provided to replayer.
 * Created by Sam Behnam.
 */
@Slf4j
public class Replayer {
    private static final int INITIAL_DELAY = 0;
    private static final int BEFORE_START_WARM_UP_DELAY = 30;
    // Wait scale can be used to scale down or up the wait time for executing events
    private static final double EVENT_WAIT_SCALE_RATIO = 1;
    // Recording scale which is used to configure replay.
    // Use (0,1) for compressing,(1,m] for decompressing, and 1 for simulating recorded timings.
    private static final double REPLAY_SCALE_RATIO = 0.1;
    private static final int EXPERIMENT_TIMEOUT = 1;
    private static final int CAPACITY_OF_WORKER_QUEUES = 50000;
    private static final int LOAD_HEAD_START_TIME = 60;
    public static final int DUMP_TIME_PERIOD = 180;

    private final CorfuRuntime runtime;

    private Map<String, Thread> threadMap = new HashMap<>();
    private Map<String, BlockingQueue<Event>> threadOperationMap = new HashMap<>();
    private Configuration configuration;
    private long recordStartTimestamp = 0L;
    private long replayStartTimestamp = 0L;


    public Replayer(@NonNull final CorfuRuntime runtime) throws InterruptedException {
        this.runtime = runtime;
    }

    /** Convenience method, replay the events in the provided queues and returns the
     * micro benchmark time of replay in nanosecond */
    public static long replayEventList(@NonNull List<String> pathsToEventQueues,
                                       @NonNull CorfuRuntime runtime) throws InterruptedException {
        // Setup replayer
        Replayer replayer = new Replayer(runtime);

        // Setup event loader using fastLoadSetup or asynchronousLoadSetup
         replayer.fastLoadSetup(pathsToEventQueues);

        // Replay and return execution time
        final long startTime = System.nanoTime();
        replayer.replay();
        final long endTime = System.nanoTime();
        return endTime - startTime;
    }

    /** Set up the operation queues and load all events in advance */
    public void fastLoadSetup(@NonNull List<String> pathsToEventQueues) {
        final Set<String> streamIdSet = setupReplayerWithAllEvents(pathsToEventQueues);
        configuration = new Configuration(runtime, streamIdSet);
        clearMaps();
    }

    /** Set up the operation queues and schedules loading events gradually */
    public void asynchronousLoadSetup(@NonNull List<String> pathsToEventQueues) {
        final Set<String> streamIdSet = setupReplayerWithoutEvents(pathsToEventQueues);
        configuration = new Configuration(runtime, streamIdSet);
        clearMaps();
        startThreadOperationTaskLoader(new ConcurrentLinkedQueue<>(pathsToEventQueues));
    }

    /** Start to replay events on corresponding threads */
    public void replay() throws InterruptedException {
        // Initialize replay start time at the current time plus a delay to separate queue setups
        // from starting the threads
        final long delayToStartReplay = TimeUnit.NANOSECONDS.convert(INITIAL_DELAY, TimeUnit.SECONDS);
        this.replayStartTimestamp = System.nanoTime() + delayToStartReplay;

        // Start all replayer threads
        for (Thread thread : threadMap.values()) {
            thread.start();
        }

        // Wait for all the events to be replayed on replayer threads
        for (Thread thread : threadMap.values()) {
            thread.join();
        }

        log.info("Replaying is complete.");
    }

    /** Represent the runnable task of replaying events recorded for a specific thread */
    private Runnable replayRunnable(@NonNull final String threadId) {
        return () -> {
            final BlockingQueue<Event> threadEventQueue = threadOperationMap.get(threadId);

            while (true) {
                Event event = null;
                try {
                    event = threadEventQueue.poll(EXPERIMENT_TIMEOUT, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    log.error("InterruptedException", e);
                }
                if (event == null) {
                    break;
                }

                sleepToRelativelyCatchTo(event.getTimestamp());
                reportReplayAccuracy(event);
                OperationUtils.replayableOperationOf(event, configuration)
                        .execute(event);
            }
            Thread.currentThread().interrupt();
        };
    }

    /** Initialize the replay queue by processing all events and returns a set of streamIds */
    private Set<String> setupReplayerWithAllEvents(List<String> pathsToEventQueues) {
        Set<String> streamIdSet = new HashSet<>();
        for (String pathToEventQueue : pathsToEventQueues) {
            for (Event event : ReplayerUtil.load(pathToEventQueue)) {
                // Initialize streamSet
                streamIdSet.add(event.getMapId());

                // Initialize threadMap for replayer threads and threadOperationMap for to-be-replayed event queues
                String threadId = event.getThreadId();
                if (!threadMap.containsKey(threadId)) {
                    threadOperationMap.put(threadId, new LinkedBlockingQueue<>());
                    threadMap.put(threadId,
                            new Thread(replayRunnable(threadId), "Replayer" + threadId));
                }

                // Add to-be-replayed events to threadOperationMap
                threadOperationMap.get(threadId).add(event);

                // Initialize recorder start time
                if (recordStartTimestamp == 0L) {
                    recordStartTimestamp = event.getTimestamp();
                }
            }
        }
        return streamIdSet;
    }

    /** Initialize the replay queue without assigning events and returns a set of streamIds */
    private Set<String> setupReplayerWithoutEvents(List<String> pathsToEventQueues) {
        Set<String> streamIdSet = new HashSet<>();
        for (String pathToEventQueue : pathsToEventQueues) {
            for (Event event : ReplayerUtil.load(pathToEventQueue)) {
                // Initialize streamSet
                streamIdSet.add(event.getMapId());

                // Initialize threadMap and threadOperationMap
                String threadId = event.getThreadId();
                if (!threadMap.containsKey(threadId)) {
                    threadOperationMap.put(threadId, new LinkedBlockingQueue<>());
                    threadMap.put(threadId,
                            new Thread(replayRunnable(threadId), "Replayer" + threadId));
                }

                // Initialize recorder start time
                if (recordStartTimestamp == 0L) {
                    recordStartTimestamp = event.getTimestamp();
                }
            }
        }
        return streamIdSet;
    }

    /** Gradually and asynchronously load the events and distributes to replay queues */
    private void startThreadOperationTaskLoader(ConcurrentLinkedQueue<String> pathsToEventQueues) {
        final ScheduledExecutorService service = Executors
                .newSingleThreadScheduledExecutor();

        // loadTask is responsible for taking a file from queue and adding its content to
        // the threadOperationQueues. It shuts down the executor once the files are processed.
        Runnable loadTask = () -> {

            String pathToEventQueue = null;
            if (!service.isShutdown()) {
                pathToEventQueue = pathsToEventQueues.poll();
            }
            if (pathToEventQueue == null) {
                service.shutdown();
                return;
            }

            log.info("Loading events from files");
            distributeEventToThreadReplayQueues(ReplayerUtil.load(pathToEventQueue));
        };

        // loadDelay is the wait time for loading the next batch of events. It's equal to the
        // expected consumption time of events minus a head start for loading task.
        final long loadDelay = (long) (DUMP_TIME_PERIOD * REPLAY_SCALE_RATIO
                - LOAD_HEAD_START_TIME);
        service.scheduleWithFixedDelay(loadTask, 0, loadDelay, TimeUnit.SECONDS);
    }

    /** Initialize a queue with recorded operations for each thread */
    private void distributeEventToThreadReplayQueues(final List<Event> eventList) {
        for (Event event : eventList) {
            final String threadId = event.getThreadId();
            long timeoutForFillingThreadEventQueue = DUMP_TIME_PERIOD * 10;

            // threadOperationMap is initialized with for all threadIds
            try {
                final boolean isEventAdded = threadOperationMap.get(threadId).offer(event,
                        timeoutForFillingThreadEventQueue, TimeUnit.SECONDS);
                if (!isEventAdded) {
                    throw new TimeoutException(String.format("Not able to add events to replayer for thread:%s. " +
                            "at queue capacity: %d", threadId, CAPACITY_OF_WORKER_QUEUES));
                }
            } catch (InterruptedException e) {
                log.error("InterruptedException", e);
                System.exit(1);
            } catch (TimeoutException e) {
                log.error("TimeoutException", e);
                System.exit(1);
            }
        }
    }

    /** Clear the current content of maps */
    private void clearMaps() {
        // Clearing all the maps required in a replay
        for (Map<String, Object> map : configuration.getMaps().values()) {
            map.clear();
        }
        log.info("Maps are cleared.");
        log.info("Replayer is initialized.");
    }

    /** Sleep to mimic the relative time distance of event replay time from beginning of replay
     * corresponding to event record time from the beginning of recording. There won't be any sleep
     * if the replayer is behind. */
    private void sleepToRelativelyCatchTo(final long eventTime) {
        final long currentTimestamp = System.nanoTime();
        final long relativeTimePassedFromReplayStartTime = currentTimestamp - replayStartTimestamp;
        final long relativeEventTimeFromRecordingStartTime = eventTime - recordStartTimestamp;

        // Time to sleep is calculated in regard the recorded time once the replay scale ratio is applied
        final long timeToSleepToCatchUp = (long) (relativeEventTimeFromRecordingStartTime * REPLAY_SCALE_RATIO)
                - relativeTimePassedFromReplayStartTime;

        // Time to catch up is timeToSleep once the event wait time is applied
        final long timeToSleepToCatchUpScaled = (long) (timeToSleepToCatchUp * EVENT_WAIT_SCALE_RATIO);
        log.info("Catch-up time: {} ScaledTo {}",
                timeToSleepToCatchUp, timeToSleepToCatchUpScaled);

        // Will not sleep if replay is behind schedule
        if (timeToSleepToCatchUpScaled <= 0) {
            return;
        }
        try {
            TimeUnit.NANOSECONDS.sleep(timeToSleepToCatchUpScaled);
        } catch (InterruptedException e) {
            log.error("InterruptedException", e);
        }
    }

    /** Log statistics regarding each event replay. */
    private void reportReplayAccuracy(final Event event) {
        final long currentAsActualReplayTimestamp = System.nanoTime();
        final long eventRecordTimestamp = event.getTimestamp();
        final long relativeTimeFromRecordingStart = eventRecordTimestamp - recordStartTimestamp;
        final long relativeTimeFromReplayStart = currentAsActualReplayTimestamp - replayStartTimestamp;
        final long replayInaccuracy = relativeTimeFromReplayStart - relativeTimeFromRecordingStart;

        // Precision represents the ratio of recording time from beginning to actual replay time from beginning.
        // the closer to 1.000 represent closer to exact time replication or recorded execution
        // in comparision to recorded timings. Scaling will skew this.
        final double precision = (double) relativeTimeFromRecordingStart / relativeTimeFromReplayStart;

        log.info("Replay:[{}] Thread = {} " +
                        "| Rltv Rec t = {} " +
                        "| Rltv Rply t = {} " +
                        "| Rply lag = {} " +
                        "| Rltv Precision = {} " +
                        "| Rec t = {} " +
                        "| Rply t = {}",
                event.getEventType(),
                Thread.currentThread().getId(),
                relativeTimeFromRecordingStart,
                relativeTimeFromReplayStart,
                replayInaccuracy,
                precision,
                event.getTimestamp(), currentAsActualReplayTimestamp);
    }

    void warmUpDelay() throws InterruptedException {
        try {
            TimeUnit.SECONDS.sleep(BEFORE_START_WARM_UP_DELAY);
        } catch (InterruptedException e) {
            log.error("InterruptedException", e);
            throw e;
        }
    }
}
