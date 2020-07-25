package org.corfudb.utils.lock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.utils.lock.LockClient.ClientContext;
import org.corfudb.utils.lock.states.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A Lock, identified by a <class>LockDataTypes.LockId</class>, attempts to acquire and renew lease on behalf of
 * the application interested in the lock. Once a Lock instance is able to acquire a lock it will try to renew it's lease
 * periodically. If for some reason (e.g. jvm pause, stuck thread, jvm slowness, instance fail stopped, etc.) the
 * instance is not able to renew the lease before it's expiration, another instance can acquire the lock assuming
 * this instance has fail stopped.
 * Lock Acquisition and renewal are done in the <code>LockStore</code> which is accessed by all instances contending
 * for the lock.
 * Lock Finite State Machine:
 *
 * <pre>
 *      +------------+      LEASE_ACQUIRED           +------------+
 *      |            |+----------------------------->|            |+----------+
 *      |            |      LEASE_REVOKED            |            |           |
 *      |  NO_LEASE  |<------------------------------+  HAS_LEASE |           |
 *      |            |      LEASE_EXPIRED            |            |<----------+
 *      |            |<------------------------------+            |     LEASE_RENEWED
 *      +-----+------+                               +------+-----+
 *            |                                             |
 *            |                                             |
 *            |                                             |
 *            |UNRECOVERABLE_ERROR                          |UNRECOVERABLE_ERROR
 *            |               +----------+                  |
 *            |               |          |                  |
 *            +-------------->| STOPPED  |<-----------------+
 *                            |          |
 *                            +----------+
 *
 * </pre>
 * <pre>
 *  STATES:
 * <p>
 *     <ul>
 *         <li
 *         <li>NO_LEASE: Lock has not been able to acquired lease. Will periodically attempt to acquire the lease.</li>
 *         <li>HAS_LEASE: Lock has acquired lease. Will periodically attempt to renew the lease. </li>
 *         <li>STOPPED: Unrecoverable error occurred. Lock FSM has stopped. Lock will not try to acquire or renew
 *         lease.</li>
 *     </ul>
 * <p>
 *  EVENTS:
 *      <ul>
 *          <li>LEASE_ACQUIRED:lease for the lock has been acquired</li>
 *          <li>LEASE_RENEWED:lease for the lock has been renewed</li>
 *          <li>LEASE_REVOKED:lease for the lock has been revoked, some other instance got the lock</li>
 *          <li>LEASE_EXPIRED:lease for the lock has expired as it could not be renewed</li>
 *          <li>UNRECOVERABLE_ERROR:unrecoverable error occurred</li>
 *      </ul>
 *
 * </pre>
 *
 * @author mdhawan
 * @since 04/17/2020
 */

@Slf4j
public class Lock {

    // lease duration in 60 seconds
    @Setter
    @VisibleForTesting
    public static int leaseDuration = 60;

    // id of the lock
    @Getter
    private final LockDataTypes.LockId lockId;

    //Common context object
    @Getter
    private final ClientContext clientContext;

    // Application registers this interface to receive lockAcquired and lockLost notifications
    @Getter
    private final LockListener lockListener;

    // FSM event queue
    private final LinkedBlockingQueue<LockEvent> eventQueue = new LinkedBlockingQueue<>();

    // Executor service to consume FSM events
    private final ExecutorService eventConsumer;

    // current state of the FSM
    @Getter
    private volatile LockState state;

    //Map of pre-created FSM state objects
    @Getter
    private Map<LockStateType, LockState> states = new HashMap<>();

    /**
     * Constructor
     *
     * @param lockId
     * @param lockListener
     * @param clientContext
     */
    public Lock(LockDataTypes.LockId lockId, LockListener lockListener, LockClient.ClientContext clientContext) {
        this.lockId = lockId;
        this.clientContext = clientContext;
        this.lockListener = lockListener;
        this.eventConsumer = Executors.newSingleThreadExecutor(new
                ThreadFactoryBuilder().setNameFormat("lock-event-consumer").build());

        initializeStates();
        // set initial state as NO_LEASE
        this.state = states.get(LockStateType.NO_LEASE);

        eventConsumer.submit(this::consume);
    }

    /**
     * Initialize all states for Lock FSM.
     */
    private void initializeStates() {
        states.put(LockStateType.NO_LEASE, new NoLeaseState(this));
        states.put(LockStateType.HAS_LEASE, new HasLeaseState(this));
        states.put(LockStateType.STOPPED, new HasLeaseState(this));
    }

    /**
     * Input function of the FSM.
     * <p>
     * This method enqueues log replication events for further processing.
     *
     * @param event LogReplicationEvent to process.
     */
    public void input(LockEvent event) {
        try {
            if (state.getType().equals(LockStateType.STOPPED)) {
                // Log: not accepting events, in stopped state
                log.warn("Lock State Machine has been stopped. Event {} will be dropped.", event);
                return;
            }
            eventQueue.put(event);
        } catch (InterruptedException ex) {
            log.error("Log Replication interrupted Exception: ", ex);
        }
    }

    /**
     * Consumer of the eventQueue.
     * <p>
     * This method consumes the lock events and does state transitions.
     */
    private void consume() {
        try {
            if (state.getType() == LockStateType.STOPPED) {
                log.info("Lock State Machine has been stopped. No more events will be processed.");
                return;
            }

            // Block until an event shows up in the queue.
            LockEvent event = eventQueue.take();
            try {
                Optional<LockState> newState = state.processEvent(event);
                if (newState.isPresent()) {
                    log.trace("Transition from {} to {}", state, newState);
                    transition(state, newState.get());
                    state = newState.get();
                }
            } catch (IllegalTransitionException illegalState) {
                log.error("Illegal lock event {} when in state {}", event, state.getType());
                input(LockEvent.UNRECOVERABLE_ERROR);
            }
            // Consume one event in the queue and re-submit
            eventConsumer.submit(this::consume);
        } catch (Throwable t) {
            log.error("Error on event consumer: ", t);
            input(LockEvent.UNRECOVERABLE_ERROR);
        }
    }

    /**
     * Perform transition between states.
     *
     * @param from initial state
     * @param to   final state
     */
    void transition(LockState from, LockState to) {
        from.onExit(to);
        to.clear();
        to.onEntry(from);
    }
}
