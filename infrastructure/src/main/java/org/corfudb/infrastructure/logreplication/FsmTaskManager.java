package org.corfudb.infrastructure.logreplication;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationState;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationStateType;
import org.corfudb.infrastructure.logreplication.runtime.fsm.IllegalTransitionException;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeEvent;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeState;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeStateType;
import org.corfudb.infrastructure.logreplication.runtime.fsm.sink.LogReplicationSinkEvent;
import org.corfudb.runtime.LogReplication.LogReplicationSession;

import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This class manages the tasks submitted by the runtime FSM and the replication FSM.
 * Both the FSMs have their own instances of this class to which tasks are submitted and executed.
 *
 * The threads are shared by all the sessions traversing an FSM.
 */
@Slf4j
public class FsmTaskManager {

    private ScheduledExecutorService fsmWorker;

    private final Map<LogReplicationSession, LinkedList<UUID>> sessionToRuntimeEventIdMap;

    private final Map<LogReplicationSession, LinkedList<UUID>> sessionToReplicationEventIdMap;

    private final Map<LogReplicationSession, LinkedList<UUID>> sessionToDelayedReplicationEventIdMap;

    private final Map<LogReplicationSession, LinkedList<UUID>> sessionToSinkEventIdMap;

    public FsmTaskManager(String threadName, int threadCount) {
        fsmWorker = Executors.newScheduledThreadPool(threadCount, new ThreadFactoryBuilder().setNameFormat(threadName+"-%d").build());
        sessionToRuntimeEventIdMap = new ConcurrentHashMap<>();
        sessionToReplicationEventIdMap = new ConcurrentHashMap<>();
        sessionToSinkEventIdMap = new ConcurrentHashMap<>();
        sessionToDelayedReplicationEventIdMap = new ConcurrentHashMap<>();
    }

    public <E> void addTask(E event, FsmEventType fsm, long delay) {
        addEventToSessionEventMap(event, fsm, delay);
        if (fsm.equals(FsmEventType.LogReplicationRuntimeEvent)) {
            fsmWorker.schedule(() -> processRuntimeTask((LogReplicationRuntimeEvent) event), delay, TimeUnit.MILLISECONDS);
        } else if (fsm.equals(FsmEventType.LogReplicationEvent)){
            fsmWorker.schedule(() -> processReplicationTask((LogReplicationEvent) event), delay, TimeUnit.MILLISECONDS);
        } else {
            fsmWorker.schedule(() -> processSinkTask((LogReplicationSinkEvent) event), delay, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Add event to in-memory session->event maps. This is to ensure the order of event processing for a given session.
     * Currently the "delay" is non-zero for only the replicating FSM.
     */
    private <E> void addEventToSessionEventMap(E event, FsmEventType fsm, long delay) {
        if (fsm.equals(FsmEventType.LogReplicationRuntimeEvent)) {
            LogReplicationSession session = ((LogReplicationRuntimeEvent) event).getRuntimeFsm().getSession();
            sessionToRuntimeEventIdMap.putIfAbsent(session, new LinkedList<>());
            // makes the value part of the map thread safe.
            sessionToRuntimeEventIdMap.computeIfPresent(session, (sessionKey, eventList) -> {
                eventList.add(((LogReplicationRuntimeEvent) event).getEventId());
                return eventList;
            });
        } else if (fsm.equals(FsmEventType.LogReplicationEvent)){
            LogReplicationSession session = ((LogReplicationEvent) event).getReplicationFsm().getSession();
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
        } else {
            LogReplicationSession session = ((LogReplicationSinkEvent) event).getSourceLeadershipManager().getSession();
            sessionToSinkEventIdMap.putIfAbsent(session, new LinkedList<>());
            // makes the value part of the map thread safe.
            sessionToSinkEventIdMap.computeIfPresent(session, (sessionKey, eventList) -> {
                eventList.add(((LogReplicationSinkEvent) event).getEventId());
                return eventList;
            });
        }
    }

    private void processSinkTask(LogReplicationSinkEvent event) {
        LogReplicationSession session = event.getSourceLeadershipManager().getSession();
        synchronized (event.getSourceLeadershipManager()) {
            while (!sessionToSinkEventIdMap.get(session).get(0).equals(event.getEventId())) {
                try {
                    event.getSourceLeadershipManager().wait();
                } catch (InterruptedException e) {
                    log.error("Wait for session {} was interrupted {}", session, e.getMessage());
                }
            }
        }

        event.getSourceLeadershipManager().processEvent(event);

        synchronized (event.getSourceLeadershipManager()) {
            sessionToSinkEventIdMap.get(session).remove(0);
            event.getSourceLeadershipManager().notifyAll();
        }
    }

    private void processRuntimeTask(LogReplicationRuntimeEvent event) {
        LogReplicationSession session = event.getRuntimeFsm().getSession();
        // for a given session, The fsm event should be processed in the order they are queued. This condition ensures
        // that in the event of 2 threads contending for the monitor, only the task submitted first would be processed.
        synchronized (event.getRuntimeFsm()) {
            while (!sessionToRuntimeEventIdMap.get(session).get(0).equals(event.getEventId())) {
                try {
                    event.getRuntimeFsm().wait();
                } catch (InterruptedException e) {
                    log.error("Wait for session {} was interrupted {}", session, e.getMessage());
                }
            }
        }

        LogReplicationRuntimeState currState = event.getRuntimeFsm().getState();
        if (currState.getType() == LogReplicationRuntimeStateType.STOPPED) {
            log.info("Log Replication Communication State Machine has been stopped. No more events will be processed.");
            return;
        }


        try {
            LogReplicationRuntimeState newState = currState.processEvent(event);
            if (newState != null) {
                event.getRuntimeFsm().transition(currState, newState);
                event.getRuntimeFsm().setState(newState);

            }
        } catch (IllegalTransitionException illegalState) {
            log.error("Illegal log replication event {} when in state {}", event.getType(), currState.getType());
        }

        synchronized(event.getRuntimeFsm()) {
            sessionToRuntimeEventIdMap.get(session).remove(0);
            event.getRuntimeFsm().notifyAll();
        }
    }

    private boolean canProcessEventForSession(LogReplicationSession session, UUID currEventId) {
            return (sessionToReplicationEventIdMap.get(session) != null &&
                        !sessionToReplicationEventIdMap.get(session).isEmpty() &&
                        sessionToReplicationEventIdMap.get(session).get(0).equals(currEventId)) ||
                    (sessionToDelayedReplicationEventIdMap.get(session) != null &&
                            !sessionToDelayedReplicationEventIdMap.get(session).isEmpty() &&
                            sessionToDelayedReplicationEventIdMap.get(session).get(0).equals(currEventId));
    }

    private void processReplicationTask(LogReplicationEvent currEvent) {
        LogReplicationSession session = currEvent.getReplicationFsm().getSession();
        // for a given session, the fsm events should be processed in the order they are queued. This snippets ensures
        // that in the event of 2 threads contending for the monitor, only the task submitted first would be processed.
        synchronized(currEvent.getReplicationFsm()) {
            while (true) {
                if (canProcessEventForSession(session, currEvent.getEventId())) {
                    break;
                } else {
                    try {
                        currEvent.getReplicationFsm().wait();
                    } catch (InterruptedException e) {
                        log.error("Wait for session {} was interrupted {}", session, e.getMessage());
                    }
                }
            }

            LogReplicationState currState = currEvent.getReplicationFsm().getState();
            if (currState.getType() == LogReplicationStateType.ERROR) {
                log.info("Log Replication State Machine has been stopped. No more events will be processed.");
                return;
            }

            // TODO (Anny): consider strategy for continuously failing snapshot sync (never ending cancellation)

            try {
                LogReplicationState newState = currState.processEvent(currEvent);
                log.trace("Transition from {} to {}", currState, newState);

                currEvent.getReplicationFsm().transition(currState, newState);
                currEvent.getReplicationFsm().setState(newState);
                currEvent.getReplicationFsm().getNumTransitions().setValue(currEvent.getReplicationFsm().getNumTransitions().getValue() + 1);

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
            currEvent.getReplicationFsm().notifyAll();
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
        fsmWorker.shutdown();
    }

    public static enum FsmEventType {
        LogReplicationEvent,
        LogReplicationRuntimeEvent,
        LogReplicationSinkEvent
    }
}
