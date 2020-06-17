package org.corfudb.transport.logreplication;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.transport.client.IClientChannelAdapter;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class represents the Log Replication Communication Finite State Machine
 *
 * @author amartinezman
 */
@Slf4j
public class LogReplicationCommunicationFSM {

    public static final int DEFAULT_TIMEOUT = 5000;

    /**
     * Current state of the FSM.
     */
    @Getter
    private volatile CommunicationState state;

    /**
     * Map of all Log Replication Communication FSM States (reuse single instance for each state)
     */
    @Getter
    private Map<CommunicationStateType, CommunicationState> states = new HashMap<>();

    /**
     * Executor service for FSM state tasks
     */
    @Getter
    private ExecutorService communicationFSMWorkers;

    /**
     * Executor service for FSM event queue consume
     */
    private ExecutorService communicationFSMConsumer;

    /**
     * A queue of events.
     */
    private final LinkedBlockingQueue<CommunicationEvent> eventQueue = new LinkedBlockingQueue<>();

    private IClientChannelAdapter channelAdapter;

    private LogReplicationClientRouter router;

    @Getter
    private volatile Set<String> connectedEndpoints = ConcurrentHashMap.newKeySet();

    private volatile Optional<String> leaderEndpoint = Optional.empty();

    /**
     * Default Constructor
     *
     * @param channelAdapter channel/communication adapter
     * @param router corfu log replication
     */
    public LogReplicationCommunicationFSM(IClientChannelAdapter channelAdapter, LogReplicationClientRouter router) {
        this.channelAdapter = channelAdapter;
        this.router = router;

        initializeStates();
        this.state = states.get(CommunicationStateType.INIT);
        this.communicationFSMWorkers = Executors.newFixedThreadPool(2, new
                ThreadFactoryBuilder().setNameFormat("comm-fsm-worker").build());
        this.communicationFSMConsumer = Executors.newSingleThreadExecutor(new
                ThreadFactoryBuilder().setNameFormat("comm-fsm-consumer").build());

        communicationFSMConsumer.submit(this::consume);

        // Start Initial Connection to Remote Cluster
        this.channelAdapter.connectAsync();

        log.info("Log Replication Communication State Machine initialized");
    }

    /**
     * Initialize all states for the Log Replication Communication FSM.
     */
    private void initializeStates() {
        /*
         * Log Replication State instances are kept in a map to be reused in transitions, avoid creating one
         * per every transition (reduce GC cycles).
         */
        states.put(CommunicationStateType.INIT, new IdleState(this));
        states.put(CommunicationStateType.VERIFY_LEADER, new VerifyLeaderState(this, communicationFSMWorkers, router));
        states.put(CommunicationStateType.NEGOTIATE, new NegotiateState(this, communicationFSMWorkers, router));
        states.put(CommunicationStateType.REPLICATE, new ReplicateState(this));
        states.put(CommunicationStateType.STOP, new CommunicationStopState());
    }

    /**
     * Input function of the FSM.
     *
     * This method enqueues communication events for further processing.
     *
     * @param event CommunicationEvent to process.
     */
    public synchronized void input(CommunicationEvent event) {
        try {
            if (state.getType().equals(CommunicationStateType.STOP)) {
                // Not accepting events, in stopped state
                return;
            }
            eventQueue.put(event);
        } catch (InterruptedException ex) {
            log.error("Log Replication interrupted Exception: ", ex);
        }
    }

    /**
     * Consumer of the eventQueue.
     *
     * This method consumes the log replication events and does the state transition.
     */
    private void consume() {
        try {
            if (state.getType() == CommunicationStateType.STOP) {
                log.info("Log Replication Communication State Machine has been stopped. No more events will be processed.");
                return;
            }

            //  Block until an event shows up in the queue.
            CommunicationEvent event = eventQueue.take();

            try {
                CommunicationState newState = state.processEvent(event);
                transition(state, newState);
                state = newState;
            } catch (IllegalTransitionException illegalState) {
                log.error("Illegal log replication event {} when in state {}", event.getType(), state.getType());
            }

            communicationFSMConsumer.submit(this::consume);

        } catch (Throwable t) {
            log.error("Error on event consumer: ", t);
        }
    }

    /**
     * Perform transition between states.
     *
     * @param from initial state
     * @param to final state
     */
    private void transition(CommunicationState from, CommunicationState to) {
        log.trace("Transition from {} to {}", from, to);
        from.onExit(to);
        to.clear();
        to.onEntry(from);
    }

    /**
     * Re-establish connection to remote endpoint
     *
     * @param endpoint remote endpoint
     */
    public void reconnectAsync(String endpoint) {
        log.info("Reconnecting to remote endpoint {}", endpoint);
        channelAdapter.connectAsync(endpoint);
    }

    public synchronized void updateConnectedEndpoints(String endpoint) {
        connectedEndpoints.add(endpoint);
    }

    public synchronized void updateDisconnectedEndpoints(String endpoint) {
        connectedEndpoints.remove(endpoint);
    }

    public synchronized void setLeaderEndpoint(String leader) {
        leaderEndpoint = Optional.ofNullable(leader);
    }

    public synchronized Optional<String> getLeader() {
        return leaderEndpoint;
    }

    public void start() {
        // When connection is established to the remote leader node, the connectionFuture will be completed.
        channelAdapter.connectAsync();
    }
}
