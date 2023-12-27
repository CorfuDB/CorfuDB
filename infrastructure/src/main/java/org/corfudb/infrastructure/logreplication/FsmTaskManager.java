package org.corfudb.infrastructure.logreplication;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationFSM;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationState;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationStateType;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.infrastructure.logreplication.runtime.fsm.IllegalTransitionException;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeEvent;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeState;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeStateType;
import org.corfudb.infrastructure.logreplication.runtime.fsm.sink.LogReplicationSinkEvent;
import org.corfudb.infrastructure.logreplication.runtime.fsm.sink.RemoteSourceLeadershipManager;
import org.corfudb.runtime.LogReplication.LogReplicationSession;

import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This class manages the tasks submitted by the runtime FSM, the replication FSM and for the sink tasks when SINK is
 * the connection starter.
 *
 * The thread pools are shared by all the sessions traversing an FSM.
 */
@Slf4j
public class FsmTaskManager {

    private ScheduledExecutorService runtimeWorker = null;
    private ScheduledExecutorService replicationWorker = null;
    private ScheduledExecutorService sinkTaskWorker = null;

    // Session -> list of runtime event IDs. This data structure is used to maintain the order of events processed for a given session.
    private final Map<LogReplicationSession, LinkedList<UUID>> sessionToRuntimeEventIdMap = new ConcurrentHashMap<>();

    // Session -> list of replication event IDs. This data structure is used to maintain the order of events processed for a given session.
    private final Map<LogReplicationSession, LinkedList<UUID>> sessionToReplicationEventIdMap = new ConcurrentHashMap<>();

    // Session -> list of replication event IDs. This is different than the above in the way that the events in this
    // structure are to be processed after a delay.
    // Currently will have the snapshot-apply-verification events and (only relevant for routing queue model) the check for data event
    private final Map<LogReplicationSession, LinkedList<UUID>> sessionToDelayedReplicationEventIdMap = new ConcurrentHashMap<>();

    // Session -> list of sink event IDs. This data structure is used to maintain the order of events processed for a given session.
    private final Map<LogReplicationSession, LinkedList<UUID>> sessionToSinkEventIdMap = new ConcurrentHashMap<>();


    public void createRuntimeTaskManager(String threadName, int threadCount) {
        if (runtimeWorker == null) {
            runtimeWorker = Executors.newScheduledThreadPool(threadCount, new ThreadFactoryBuilder().setNameFormat(threadName + "-%d").build());
        }
    }

    public void createReplicationTaskManager(String threadName, int threadCount) {
        if (replicationWorker == null) {
            replicationWorker = Executors.newScheduledThreadPool(threadCount, new ThreadFactoryBuilder().setNameFormat(threadName + "-%d").build());
        }
    }

    public void createSinkTaskManager(String threadName, int threadCount) {
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
            LogReplicationSession session = ((CorfuLogReplicationRuntime) fsm).getSession();
            sessionToRuntimeEventIdMap.putIfAbsent(session, new LinkedList<>());
            // makes the value part of the map thread safe.
            sessionToRuntimeEventIdMap.computeIfPresent(session, (sessionKey, eventList) -> {
                eventList.add(((LogReplicationRuntimeEvent) event).getEventId());
                return eventList;
            });
        } else if (fsmType.equals(FsmEventType.LogReplicationEvent)){
            LogReplicationSession session = ((LogReplicationFSM) fsm).getSession();
            if (delay > 0) {
                sessionToDelayedReplicationEventIdMap.putIfAbsent(session, new LinkedList<>());
                // makes the value part of the map thread safe.
                sessionToDelayedReplicationEventIdMap.computeIfPresent(session, (sessionKey, eventList) -> {
                    eventList.add(((LogReplicationEvent) event).getEventId());
                    return eventList;
                });
            } else {
                sessionToReplicationEventIdMap.putIfAbsent(session, new LinkedList<>());
                // makes the value part of the map thread safe.
                sessionToReplicationEventIdMap.computeIfPresent(session, (sessionKey, eventList) -> {
                    eventList.add(((LogReplicationEvent) event).getEventId());
                    return eventList;
                });
            }
        } else if (fsmType.equals(FsmEventType.LogReplicationSinkEvent)){
            LogReplicationSession session = ((RemoteSourceLeadershipManager) fsm).getSession();
            sessionToSinkEventIdMap.putIfAbsent(session, new LinkedList<>());
            // makes the value part of the map thread safe.
            sessionToSinkEventIdMap.computeIfPresent(session, (sessionKey, eventList) -> {
                eventList.add(((LogReplicationSinkEvent) event).getEventId());
                return eventList;
            });
        } else {
            log.warn("Ignored {}. Unexpected FSM event type", fsmType);
        }
    }

    private void processSinkTask(LogReplicationSinkEvent event, RemoteSourceLeadershipManager sourceLeadershipManager) {
        LogReplicationSession session = sourceLeadershipManager.getSession();
        synchronized (sourceLeadershipManager) {
            while (!sessionToSinkEventIdMap.get(session).get(0).equals(event.getEventId())) {
                try {
                    sourceLeadershipManager.wait();
                } catch (InterruptedException e) {
                    log.error("Wait for session {} was interrupted {}", session, e.getMessage());
                }
            }
        }

        sourceLeadershipManager.processEvent(event);

        synchronized (sourceLeadershipManager) {
            sessionToSinkEventIdMap.get(session).remove(0);
            sourceLeadershipManager.notifyAll();
        }
    }

    private void processRuntimeTask(LogReplicationRuntimeEvent event, CorfuLogReplicationRuntime fsm) {
        LogReplicationSession session = fsm.getSession();
        // for a given session, The fsm event should be processed in the order they are queued. This condition ensures
        // that in the event of 2 threads contending for the monitor, only the task submitted first would be processed.
        synchronized (fsm) {
            while (!sessionToRuntimeEventIdMap.get(session).get(0).equals(event.getEventId())) {
                try {
                    fsm.wait();
                } catch (InterruptedException e) {
                    log.error("Wait for session {} was interrupted {}", session, e.getMessage());
                }
            }
        }

        LogReplicationRuntimeState currState = fsm.getState();
        if (currState.getType() == LogReplicationRuntimeStateType.STOPPED) {
            log.info("Log Replication Communication State Machine has been stopped. No more events will be processed.");
            return;
        }


        try {
            LogReplicationRuntimeState newState = currState.processEvent(event);
            if (newState != null) {
                fsm.transition(currState, newState);
                fsm.setState(newState);

            }
        } catch (IllegalTransitionException illegalState) {
            log.error("Illegal log replication event {} when in state {}", event.getType(), currState.getType());
        }

        synchronized(fsm) {
            sessionToRuntimeEventIdMap.get(session).remove(0);
            fsm.notifyAll();
        }
    }

    private boolean cannotProcessEventForSession(LogReplicationSession session, UUID currEventId) {
        // The order between the delayed list and the regular list is not maintained. But since only 1 thread is active
        // at any time for a session, in a stable scenario, its guaranteed that a 0-delay event will not be generated
        // before a delayed event is executed.
        return !(sessionToReplicationEventIdMap.get(session) != null &&
                !sessionToReplicationEventIdMap.get(session).isEmpty() &&
                sessionToReplicationEventIdMap.get(session).get(0).equals(currEventId) ||
                (sessionToDelayedReplicationEventIdMap.get(session) != null &&
                        !sessionToDelayedReplicationEventIdMap.get(session).isEmpty() &&
                        sessionToDelayedReplicationEventIdMap.get(session).get(0).equals(currEventId)));
    }

    private void processReplicationTask(LogReplicationEvent currEvent, LogReplicationFSM fsm) {
        LogReplicationSession session = fsm.getSession();
        // for a given session, the fsm events should be processed in the order they are queued. This snippets ensures
        // that in the event of 2 threads contending for the monitor, only the task submitted first would be processed.
        synchronized(fsm) {
            while (cannotProcessEventForSession(session, currEvent.getEventId())) {
                try {
                    fsm.wait();
                } catch (InterruptedException e) {
                    log.error("Wait for session {} was interrupted {}", session, e.getMessage());
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

            removeReplicationEventIdFromMap(session, currEvent.getEventId());
            fsm.notifyAll();
        }

    }

    private void removeReplicationEventIdFromMap(LogReplicationSession session, UUID currEventID) {
        if (sessionToReplicationEventIdMap.get(session) != null && !sessionToReplicationEventIdMap.get(session).isEmpty() &&
                sessionToReplicationEventIdMap.get(session).get(0).equals(currEventID)) {
            sessionToReplicationEventIdMap.get(session).remove(0);
        } else {
            sessionToDelayedReplicationEventIdMap.get(session).remove(0);
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
}
