package org.corfudb.infrastructure.logreplication;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.replication.fsm.IllegalTransitionException;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationFSM;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationState;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationStateType;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.infrastructure.logreplication.runtime.fsm.IllegalRuntimeTransitionException;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeEvent;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeState;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeStateType;
import org.corfudb.infrastructure.logreplication.runtime.fsm.sink.LogReplicationSinkEvent;
import org.corfudb.infrastructure.logreplication.runtime.fsm.sink.RemoteSourceLeadershipManager;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is a singleton that manages the tasks submitted by the runtime FSM, the replication FSM and also for the
 * sink tasks when SINK is the connection starter. All sessions share the same thread pool per fsm type.
 *
 * The order of processing the enqueued events, for a given session, is guaranteed by maintaining a list of incoming
 * events for session. Any new events get appended to the list.
 * When a thread is assigned a task, we check against this list. If the task appears at index 0, the thread is allowed
 * to process the event, otherwise, the thread waits until its notified by another thread processing the event at index 0.
 */
@Slf4j
public class FsmTaskManager {

    private ScheduledExecutorService runtimeWorker = null;
    private ScheduledExecutorService replicationWorker = null;
    private ScheduledExecutorService sinkTaskWorker = null;

    // This data structure is used to maintain the order of runtime fsm events processed for a given session.
    //
    // The key is a sessionName instead of the session because protobuf's hashcode is not consistent. If a same session is
    // stopped and started, the hashcode may change. Since we synchronize within a session to guarantee the order of
    // events being processed, the inconsistency of the hashcode can block a thread for ever causing the fsm to freeze.
    private final Map<String, LinkedList<UUID>> sessionToRuntimeEventIdMap = new ConcurrentHashMap<>();

    //This data structure is used to maintain the order of replication fsm events processed for a given session.
    // In order for the replication event processing method to look similar to that of runtime's processing method, the
    // value of this data structure is a list containing only 1 object per session.
    //
    // The key is a sessionName instead of the session because protobuf's hashcode is not consistent. If a same session is
    // stopped and started, the hashcode may change. Since we synchronize within a session to guarantee the order of
    // events being processed, the inconsistency of the hashcode can block a thread for ever causing the fsm to freeze.
    private final Map<String, List<ReplicationEventOrderManager>> sessionToReplicationEventOrderManager = new ConcurrentHashMap<>();

    // This data structure is used to maintain the order of sink events processed for a given session.
    //
    // The key is a sessionName instead of the session because protobuf's hashcode is not consistent. If a same session is
    // stopped and started, the hashcode may change. Since we synchronize within a session to guarantee the order of
    // events being processed, the inconsistency of the hashcode can block a thread for ever causing the fsm to freeze.
    private final Map<String, LinkedList<UUID>> sessionToSinkEventIdMap = new ConcurrentHashMap<>();


    public synchronized void createRuntimeTaskManager(String threadName, int threadCount) {
        if (runtimeWorker == null) {
            runtimeWorker = Executors.newScheduledThreadPool(threadCount, new ThreadFactoryBuilder().setNameFormat(threadName + "-%d").build());
        }
    }

    public synchronized void createReplicationTaskManager(String threadName, int threadCount) {
        if (replicationWorker == null) {
            replicationWorker = Executors.newScheduledThreadPool(threadCount, new ThreadFactoryBuilder().setNameFormat(threadName + "-%d").build());
        }
    }

    public synchronized void createSinkTaskManager(String threadName, int threadCount) {
        if (sinkTaskWorker == null) {
            sinkTaskWorker = Executors.newScheduledThreadPool(threadCount, new ThreadFactoryBuilder().setNameFormat(threadName + "-%d").build());
        }
    }

    public <E, F> void addTask(E event, FsmEventType fsmType, long delay, F fsm) {
        addEventToSessionEventMap(event, fsmType, delay, fsm);
        if (fsmType.equals(FsmEventType.LogReplicationRuntimeEvent)) {
            runtimeWorker.schedule(() -> processRuntimeTask((LogReplicationRuntimeEvent) event, (CorfuLogReplicationRuntime) fsm),
                    delay, TimeUnit.MILLISECONDS);
        } else if (fsmType.equals(FsmEventType.LogReplicationEvent)){
            replicationWorker.schedule(() -> processReplicationTask((LogReplicationEvent) event,(LogReplicationFSM) fsm),
                    delay, TimeUnit.MILLISECONDS);
        } else {
            sinkTaskWorker.schedule(() -> processSinkTask((LogReplicationSinkEvent) event, (RemoteSourceLeadershipManager) fsm),
                    delay, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Add event to in-memory session->event maps. This is to ensure the order of event processing for a given session.
     * Currently the "delay" is non-zero for only the replicating FSM.
     */
    private <E, F> void addEventToSessionEventMap(E event, FsmEventType fsmType, long delay, F fsm) {
        if (fsmType.equals(FsmEventType.LogReplicationRuntimeEvent)) {
            String sessionName = ((CorfuLogReplicationRuntime) fsm).getSessionName();
            sessionToRuntimeEventIdMap.putIfAbsent(sessionName, new LinkedList<>());
            // makes the value part of the map thread safe.
            sessionToRuntimeEventIdMap.computeIfPresent(sessionName, (sessionKey, eventList) -> {
                eventList.add(((LogReplicationRuntimeEvent) event).getEventId());
                return eventList;
            });
        } else if (fsmType.equals(FsmEventType.LogReplicationEvent)){
            String sessionName = ((LogReplicationFSM) fsm).getSessionName();
            sessionToReplicationEventOrderManager.putIfAbsent(sessionName, Collections.singletonList(new ReplicationEventOrderManager()));
            sessionToReplicationEventOrderManager.get(sessionName).get(0).addEvent(((LogReplicationEvent) event).getEventId(), delay);
        } else if (fsmType.equals(FsmEventType.LogReplicationSinkEvent)){
            String sessionName = ((RemoteSourceLeadershipManager) fsm).getSessionName();
            sessionToSinkEventIdMap.putIfAbsent(sessionName, new LinkedList<>());
            // makes the value part of the map thread safe.
            sessionToSinkEventIdMap.computeIfPresent(sessionName, (sessionKey, eventList) -> {
                eventList.add(((LogReplicationSinkEvent) event).getEventId());
                return eventList;
            });
        } else {
            log.warn("Ignored {}. Unexpected FSM event type", fsmType);
        }
    }

    private void processSinkTask(LogReplicationSinkEvent event, RemoteSourceLeadershipManager sourceLeadershipManager) {
        String sessionName = sourceLeadershipManager.getSessionName();
        synchronized (sessionToSinkEventIdMap.get(sessionName)) {
            while (!sessionToSinkEventIdMap.get(sessionName).get(0).equals(event.getEventId())) {
                try {
                    sessionToSinkEventIdMap.get(sessionName).wait();
                } catch (InterruptedException e) {
                    log.error("Wait for session {} was interrupted {}", sessionName, e.getMessage());
                }
            }
        }

        sourceLeadershipManager.processEvent(event);

        synchronized (sessionToSinkEventIdMap.get(sessionName)) {
            sessionToSinkEventIdMap.get(sessionName).remove(0);
            sessionToSinkEventIdMap.get(sessionName).notifyAll();
        }
    }

    private void processRuntimeTask(LogReplicationRuntimeEvent event, CorfuLogReplicationRuntime fsm) {
        String sessionName = fsm.getSessionName();
        // for a given session, The fsm event should be processed in the order they are queued. This condition ensures
        // that in the event of 2 threads contending for the monitor, only the task submitted first would be processed.
        synchronized (sessionToRuntimeEventIdMap.get(sessionName)) {
            while (!sessionToRuntimeEventIdMap.get(sessionName).get(0).equals(event.getEventId())) {
                try {
                    sessionToRuntimeEventIdMap.get(sessionName).wait();
                } catch (InterruptedException e) {
                    log.error("[{}]:: Wait was interrupted {}", sessionName, e.getMessage());
                }
            }
        }

        LogReplicationRuntimeState currState = fsm.getState();
        if (currState.getType() == LogReplicationRuntimeStateType.STOPPED) {
            log.info("[{}]:: Log Replication Communication State Machine has been stopped. " +
                    "No more events will be processed.", sessionName);
            return;
        }


        try {
            LogReplicationRuntimeState newState = currState.processEvent(event);
            if (newState != null) {
                fsm.transition(currState, newState);
                fsm.setState(newState);

            }
        } catch (IllegalRuntimeTransitionException illegalState) {
            log.error("[{}]:: Illegal log replication event {} when in state {}", sessionName, event.getType(), currState.getType());
        }

        synchronized(sessionToRuntimeEventIdMap.get(sessionName)) {
            sessionToRuntimeEventIdMap.get(sessionName).remove(0);
            sessionToRuntimeEventIdMap.get(sessionName).notifyAll();
        }
    }

    private void processReplicationTask(LogReplicationEvent currEvent, LogReplicationFSM fsm) {
        String sessionName = fsm.getSessionName();
        // for a given session, the fsm events should be processed in the order they are queued. This snippets ensures
        // that in the event of 2 threads contending for the monitor, only the task submitted first would be processed.
        synchronized (sessionToReplicationEventOrderManager.get(sessionName)) {
            while (sessionToReplicationEventOrderManager.get(sessionName).get(0).cannotProcessEvent(currEvent.getEventId())) {
                try {
                    sessionToReplicationEventOrderManager.get(sessionName).wait();
                } catch (InterruptedException e) {
                    log.error("[{}]:: Wait was interrupted {}", sessionName, e.getMessage());
                }
            }
        }

        LogReplicationState currState = fsm.getState();
        if (currState.getType() == LogReplicationStateType.ERROR) {
            log.info("Log Replication State Machine has been stopped. No more events will be processed.");
            return;
        }

        // TODO (Anny): consider strategy for continuously failing snapshot sync (never ending cancellation)

        try {
            LogReplicationState newState = currState.processEvent(currEvent);
            log.trace("Transition from {} to {}", currState, newState);

            fsm.transition(currState, newState);
            fsm.setState(newState);
            fsm.getNumTransitions().setValue(fsm.getNumTransitions().getValue() + 1);

        } catch (org.corfudb.infrastructure.logreplication.replication.fsm.IllegalTransitionException illegalState) {
            // Ignore LOG_ENTRY_SYNC_REPLICATED events for logging purposes as they will likely come in frequently,
            // as it is used for update purposes but does not imply a transition.
            if (!currEvent.getType().equals(LogReplicationEvent.LogReplicationEventType.LOG_ENTRY_SYNC_REPLICATED)) {
                log.error("Illegal log replication event {} when in state {}", currEvent.getType(), currState.getType());
            }
        }

        // For testing purpose to notify the event generator the stop of the event.
        if (currEvent.getType() == LogReplicationEvent.LogReplicationEventType.REPLICATION_STOP) {
            synchronized (currEvent) {
                currEvent.notifyAll();
            }
        }

        synchronized (sessionToReplicationEventOrderManager.get(sessionName)) {
            sessionToReplicationEventOrderManager.get(sessionName).get(0).removeEventAfterProcessing(currEvent.getEventId());
            sessionToReplicationEventOrderManager.get(sessionName).notifyAll();
        }

    }

    public void shutdown() {
        if (runtimeWorker != null) {
            runtimeWorker.shutdown();
        }

        if (replicationWorker != null) {
            replicationWorker.shutdown();
        }

        if (sinkTaskWorker != null) {
            sinkTaskWorker.shutdown();
        }
    }

    @VisibleForTesting
    //Only used in LogReplicationFSMTest
    public void shutdownReplicationTaskWorkerPool() {
        if (replicationWorker != null) {
            replicationWorker.shutdown();
        }
    }

    public static enum FsmEventType {
        LogReplicationEvent,
        LogReplicationRuntimeEvent,
        LogReplicationSinkEvent
    }

    /**
     * This class holds the enqueued LogReplicationEvent. Since replication events may run at 0-delay or non-zero delay,
     * this wrapper class assists in determining if the current event picked by a thread can be processed.
     * This also ensures that there is only 1 thread active for a session at any time.
     */
    private class ReplicationEventOrderManager{
        // Contains events with 0-delay. This list helps maintain the order of events processed.
        private final LinkedList<UUID> processImmediately;

        // This list has the same intention as above, but with a slight difference in kind of event it holds.
        // This contains the events which have to be scheduled after a non-zero delay.
        // Currently will have the snapshot-apply-verification events and (only relevant for routing queue model) the check for data function
        private final LinkedList<UUID> processWithDelay;

        // set to true when an event is being actively processed for the session
        private boolean currentlyProcessingEvent;

        ReplicationEventOrderManager() {
            processImmediately = new LinkedList<>();
            processWithDelay = new LinkedList<>();
            currentlyProcessingEvent = false;
        }

        synchronized boolean cannotProcessEvent(UUID eventID) {
            if (currentlyProcessingEvent) {
                return true;
            }
            boolean cannotProcess = !(!processImmediately.isEmpty() && processImmediately.get(0).equals(eventID) ||
                    !processWithDelay.isEmpty() && processWithDelay.get(0).equals(eventID));

            if (!cannotProcess) {
                // the current event will be processed.
                currentlyProcessingEvent = true;
            }
            return cannotProcess;
        }

        synchronized void addEvent(UUID eventID, long delay) {
            if (delay == 0) {
                processImmediately.add(eventID);
            } else {
                processWithDelay.add(eventID);
            }
        }

        synchronized void removeEventAfterProcessing(UUID eventID) {
            if(!processImmediately.isEmpty() && processImmediately.get(0).equals(eventID)) {
                processImmediately.remove(0);
            } else {
                processWithDelay.remove(0);
            }
            currentlyProcessingEvent = false;
        }
    }
}
